# Pipeline Doctor

An agent that diagnoses broken data pipelines and proposes fixes. It listens for a failed Airflow task or dbt test, collects evidence with read-only tools, names the root cause in plain language, and prepares a fix. Nothing is written until a named human approves, and even then the output is a draft pull request.

```text
Airflow on_failure_callback ──► POST /webhooks/airflow ──► incident
                                                            │
     ┌──────────────────────── LangGraph state machine ─────┴──────────────────────────┐
     │ detect → gather_context → hypothesize → verify → assess ─┬─► explain → propose_fix │
     │                             ▲     ▲                      │                  │      │
     │                             │     └─ needs more evidence ┘                  ▼      │
     │                             └──── reinvestigate ◄──────────────────── JEV review   │
     │                                   revise_fix ──► propose_fix ◄─────────────┤      │
     │                                   proceed / escalate ──► request_approval ◄┘      │
     └─────────────────────────────────────────────────────────────────────────────────┘
        │ task logs   │ manifest / run_results / sources.json   │ git diff since last green
        │ schema drift (declared vs information_schema)         │ guarded SQL checks │ incident memory
                                                            │
                         human: /incidents/{id}/approve ──► draft PR (local branch or GitHub)
```

## What it does

1. **Detect.** The DAG's `on_failure_callback` posts the failed task to the API. A dbt-only webhook exists too. Retries of the same task attempt are deduplicated.
2. **Gather context.** It reads the Airflow task log, the failing dbt nodes from `run_results.json` and `sources.json`, and the lineage from `manifest.json` (upstream sources and models, downstream exposures). It traces column lineage through CTEs with sqlglot. It finds commits between the last green run and the commit the failed run deployed that touch the failing lineage, and flags which ones change SQL logic as opposed to comments or docs. It detects schema drift between declared source columns and the warehouse. It also recalls similar past incidents.
3. **Hypothesize, then verify.** The reasoner proposes ranked hypotheses, each with typed diagnostic checks: null rate by load day, duplicate keys across loads, freshness, unexpected values, orphaned keys inside or outside the parent key range, column profile with format detection, and guarded custom SQL.
4. **Assess.** It either concludes or loops back for more evidence, up to `max_investigation_rounds`.
5. **Explain.** It writes a technical summary for engineers and a plain-language one for stakeholders ("why is my dashboard wrong?") that names the impacted dashboards.
6. **Propose a fix.** Options are a model change, a schema patch, a test change, or an operational escalation when the data owner has to act. Edits are exact-snippet patches, validated before anything is shown. A patch that does not apply is sent back to the reasoner once with the error. Reverts undo only the suspect commit's hunks, so later edits to the same file survive. If the investigation runs out of rounds without a confident conclusion, no code change is proposed.
7. **JEV review (optional).** A Judge / Evaluator / Verifier layer decides what happens to the proposal. See [JEV decision layer](#jev-decision-layer).
8. **Wait for approval.** On approval, the fix is committed to `pipeline-doctor/<incident-id>` and opened as a draft PR. The resolution is stored in memory so recurring failures are recognised.

### Root-cause taxonomy (10 categories)

| Upstream data | Code regressions |
| --- | --- |
| `source_column_renamed`, `source_type_changed`, `source_null_spike`, `source_duplicate_rows`, `source_stale`, `source_new_category`, `source_missing_rows` | `code_sql_error`, `code_join_fanout`, `code_logic_regression` |

## Quickstart

```bash
uv sync --all-extras
uv run pipeline-doctor demo setup                         # dbt project under git + DuckDB raw data + green baseline run
uv run pipeline-doctor demo faults                        # list the injectable faults
uv run pipeline-doctor demo break renamed_orders_status   # inject a fault and run the DAG's tasks
uv run pipeline-doctor diagnose                           # diagnose the failure, print the report
uv run pipeline-doctor approve <incident-id> --approver you
uv run pipeline-doctor eval --workers 6                   # 36-case eval, writes evals/results/<run>/
```

Use an LLM reasoner with `PIPELINE_DOCTOR_REASONER=anthropic` or `openai`, plus the provider's API key. If the provider fails, the reasoner falls back to the heuristic reasoner one step at a time, so an outage degrades the diagnosis instead of failing it.

Other entry points:

- `uv run pipeline-doctor serve` starts the FastAPI app: webhooks, `GET /incidents/{id}/report`, approve/reject, and a Slack `/doctor explain|status|approve <id>` command with signature verification. Approve and reject need a personal reviewer token (`PIPELINE_DOCTOR_REVIEWER_TOKENS`), and the reviewer's identity is taken from that token. The CLI `approve` command is for operators who already have filesystem access to the deployment.
- `uv run pipeline-doctor mcp` serves the same read-only tools over MCP (stdio) for IDEs and chat agents.
- [airflow/dags/shop_analytics_daily.py](airflow/dags/shop_analytics_daily.py) is the DAG: `dbt source freshness` → `dbt build` → tag `pipeline-doctor/last-green`, with the failure callback wired in.

## Run with Airflow (Docker)

[`compose.yaml`](compose.yaml) runs Airflow 2.10 (standalone: scheduler, webserver, SQLite) with the `shop_analytics_daily` DAG next to the Pipeline Doctor API. Both containers share a `workspace` volume that holds the demo dbt project, its git repo, and the DuckDB warehouse. On the first start, Pipeline Doctor creates the workspace and Airflow runs a green baseline.

```bash
docker compose up --build -d
# Airflow UI: http://localhost:8080 (admin / admin)    Doctor API: http://localhost:8000 (Bearer dev-token)

docker compose exec pipeline-doctor pipeline-doctor demo faults
docker compose exec pipeline-doctor pipeline-doctor demo break renamed_orders_status --workspace /workspace/demo --no-run
# trigger shop_analytics_daily in the Airflow UI; the failed task posts to /webhooks/airflow

curl -H "Authorization: Bearer dev-token" localhost:8000/incidents
curl -H "Authorization: Bearer dev-token" localhost:8000/incidents/<id>/report
curl -X POST -H "Authorization: Bearer dev-reviewer-token" localhost:8000/incidents/<id>/approve

docker compose down -v   # stop and reset the workspace
```

Pipeline Doctor reads task logs through the Airflow REST API (basic auth), and the DAG runs with `SHOP_ANALYTICS_RETRIES=0`, so the callback fires on the first failure. Set `PIPELINE_DOCTOR_REASONER=anthropic` and `ANTHROPIC_API_KEY` in your shell before `docker compose up` to use an LLM reasoner. Set `PIPELINE_DOCTOR_JEV_ENABLED=false` the same way to run without the JEV decision layer. These credentials are for local use only; do not expose the ports.

## JEV decision layer

JEV ([`jev/`](src/pipeline_doctor/jev/)) sits between `propose_fix` and the human. The reasoner proposes. JEV decides whether the proposal deserves a reviewer's time.

| Stage | Question | Checks |
| --- | --- | --- |
| Judge | Does the evidence support this diagnosis? | category named, confidence above `jev_min_confidence`, culprit in the failing lineage, grounded in a successful query, schema drift, or a SQL-changing commit, reasoner converged |
| Evaluate | Does the fix address the cause? | remedy fits the category ([`REMEDY_POLICY`](src/pipeline_doctor/jev/policy.py)), fix is actionable, edits stay in the lineage, no test removed or loosened (`severity: warn`, `enabled: false`, `where:`) |
| Verify | Is the patch safe to show? | patch re-applies and changes something, no DDL/DML or `pre_hook`/`post_hook`/`run_query` added |

Each check is weighted; the verdict carries a score and the failed checks. The decision routes the graph:

| Decision | When | Next step |
| --- | --- | --- |
| `proceed` | every check passes | `request_approval` |
| `reinvestigate` | a Judge check fails and rounds remain | `hypothesize`, with the critique added to the LLM prompt |
| `revise_fix` | an Evaluate or Verify check fails and `jev_max_fix_revisions` remain | `propose_fix`, with the failed checks as feedback |
| `escalate` | budget exhausted | code fix replaced by an operational handoff to the on-call engineer |

JEV is on by default. Turn it off with `PIPELINE_DOCTOR_JEV_ENABLED=false`, or per run with `--no-jev` on `diagnose` and `eval`. When off, `propose_fix` goes straight to `request_approval`, incidents carry no verdict, and the human reviewer is the only gate. Eval runs without JEV are saved as `<run>_nojev` so you can compare both.

The rule reviewer is deterministic and free. Set `PIPELINE_DOCTOR_JEV_LLM_JUDGE=true` with an LLM reasoner to add an LLM-as-judge behind it. The judge runs only after the rules pass, can veto but never overturn a rule failure, and falls back to the rule verdict if the model call fails. When JEV is on, `ApprovalService` refuses to open a PR for any code fix whose verdict is not `proceed`. The verdict appears in the incident report, the Slack message, the `agent.jev` span, and the eval table (proceed rate, proceed precision, catch rate).

## Evals

The catalogue has two splits:

- `dev`: [`evals/cases.py`](src/pipeline_doctor/evals/cases.py), 26 faults across all 10 categories plus 10 hard variants that add a misleading recent commit (a docs edit, a comment-only SQL change, or a real SQL change outside the failing lineage). The heuristic reasoner was written against these 36 cases.
- `holdout`: 12 cases nobody tuned against. [`holdout.py`](src/pipeline_doctor/evals/holdout.py) has 7 single faults on new tables, columns, and mechanisms: an over-broad filter in a staging model, late-arriving customers, a column renamed in a model, a prefixed string id, an aggregate grain change, CDC updates appended as rows, and upper-cased enum values. [`compound.py`](src/pipeline_doctor/evals/compound.py) has 3 cases with two simultaneous faults. [`nofault.py`](src/pipeline_doctor/evals/nofault.py) has 2 runs that fail for infrastructure reasons with green data and no suspect commits: a held DuckDB write lock and an OOM-killed worker. Keep holdout untuned: once a reasoner, prompt, or JEV rule is changed to pass a holdout case, move that case to `dev` and add a fresh one.

For each case and repeat the harness:

1. clones a green workspace (dbt project, git history, DuckDB)
2. injects the fault: mutates raw data, commits a code change as a developer would, or reproduces the declared infrastructure failure
3. runs the pipeline exactly like the DAG, and checks that the run actually fails
4. runs the agent on the resulting Airflow failure event
5. scores each part of the answer separately: **category**, affected **relation**, culprit **column** (when the ground truth names one), **fix kind** (acceptable for the case), and **fix validity**. A code fix only counts if the patch applies, `dbt source freshness && dbt build` goes green afterwards, and every baseline test still runs, so deleting a test does not count as a fix. An operational fix only counts when escalation is the right answer, such as a stale feed or deleted parent rows.

A compound case accepts either fault as the root cause. **Primary hit** means category and relation match one of them, and the patch is validated against that fault alone. A no-fault case is correct when the agent answers `unknown` and proposes no code change. **False-positive rate** is the share of no-fault runs where the agent proposed a code change or named a culprit with confidence at or above `jev_min_confidence`.

Run ids and workspace paths are random, so nothing in the agent's context names the fault. A fault that fails to break the pipeline counts as a miss.

```bash
uv run pipeline-doctor eval --split holdout          # dev | holdout | all (default)
uv run pipeline-doctor eval --repeats 3 --workers 6  # each run in a fresh workspace
```

With `--repeats N` the report adds mean, std dev, min, and max of every metric across repeats, plus per-case agreement (share of repeats with the same outcome). Each split and case kind gets accuracy next to cost, LLM calls, and p50/p95 latency, and the per-case table lists cost, latency, LLM calls, and queries. Results go to `evals/results/<run>/` as `summary.md`, `summary.json`, `results.jsonl`, and `spans.jsonl`, with a per-step latency table built from the traces.

`heuristic`, `--repeats 2` (no LLM, so $0 and 0 LLM calls):

| Slice | Cases x repeats | Category | Relation | Column | Culprit | Fix kind | Fix valid | Primary hit | FP rate | p50 / p95 latency |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dev` | 36 x 2 | 100% | 100% | 100% | 100% | 100% | 100% | 100% | n/a | 0.10s / 0.15s |
| `holdout` (all) | 12 x 2 | 91.7% | 100% | 77.8% | 91.7% | 83.3% | 83.3% | 91.7% | 0% | 0.09s / 0.12s |
| holdout compound | 3 x 2 | 100% | 100% | 100% | 100% | 100% | 100% | 100% | n/a | 0.09s / 0.10s |
| holdout no-fault | 2 x 2 | 100% | 100% | n/a | 100% | 100% | 100% | 100% | 0% | 0.04s / 0.04s |
| `anthropic` / `openai` | run `pipeline-doctor eval --reasoner anthropic --repeats 3` | | | | | | | | | |

Across both splits JEV proceeds on 89.6% of runs with 100% precision and holds back every wrong diagnosis. Std dev is 0 for every accuracy metric. Per-case agreement is 99%: `compound_null_customer_ids_and_cents` flips between its two causes because dbt reports the two failing tests in nondeterministic order. The two holdout misses are `holdout_type_customers_id_prefixed` (right table, but read as missing rows, so wrong category and column) and `holdout_model_column_renamed` (right category and model at 0.35 confidence, so it escalated instead of patching).

Read the `dev` row as a regression baseline, not a headline: the rules were written alongside those cases. The `holdout` rows and an LLM reasoner with repeats are the numbers to compare.

## Guardrails

| Risk | Control |
| --- | --- |
| Warehouse writes | DuckDB opens with `read_only=True` and `enable_external_access=false`. Snowflake must use a SELECT-only role without stage, sequence, or external-function privileges, and sessions carry `QUERY_TAG` and `STATEMENT_TIMEOUT_IN_SECONDS`. |
| Arbitrary SQL | [`SqlGuard`](src/pipeline_doctor/warehouse/guard.py) parses with sqlglot. It allows exactly one `SELECT`/set-operation and rejects DML, DDL, `COPY`, `ATTACH`, `PRAGMA`, `SELECT INTO`, table functions, file and network functions (`read_*`, `glob`, ...), Snowflake stages (`@stage`), sequences (`.nextval`), user-defined functions, and unknown functions outside a small allowlist. Tables must be schema-qualified, in `allowed_schemas`, and in the configured database. Every query is wrapped in `LIMIT max_result_rows`. The guard is defence in depth; the read-only credentials are the primary control. |
| Cost / runaway loops | Per-incident `QueryBudget` (query count, per-query timeout, and on Snowflake the bytes scanned estimated through `EXPLAIN`; DuckDB has no scan estimate), a bounded number of investigation rounds, and LLM tokens and cost metered per run. |
| Unsafe patches | Edits are limited to `.sql`/`.yml`/`.md`/`.csv` under `models/`, `tests/`, `macros/`, `snapshots/`, `seeds/`, `analyses/`. Absolute paths, `..`, and symlinks anywhere on the path are rejected. The `original` snippet must occur exactly once. Files owned by installed dbt packages are left out of the editable context. |
| Untrusted proposals | When enabled, JEV gates every proposal before a human sees it, and approval refuses code fixes JEV did not clear. |
| Unreviewed writes | The agent has no write tools. Only `ApprovalService.approve` can publish, it requires an incident in `awaiting_approval` and an authenticated reviewer, and it opens a **draft** PR. Approval endpoints fail closed until reviewer tokens are configured, and the webhook service token cannot approve. The local publisher commits with git plumbing, so the working tree and index are never touched. |
| Webhook/Slack spoofing | Bearer token on the API. Slack requests need a valid HMAC signature with a 5-minute replay window, and only users in `PIPELINE_DOCTOR_SLACK_APPROVERS` can approve. |
| Shell injection | git and dbt run with fixed argument vectors and no shell. Refs and paths are validated. |

## Observability

Every graph node and tool call is an OpenTelemetry span (`agent.*`, `tool.*`, `llm.*`) carrying token usage (`gen_ai.usage.*`), cost, query counts, and the diagnosis. Spans always go to `.pipeline_doctor/traces/spans.jsonl`. Set `OTEL_EXPORTER_OTLP_ENDPOINT` (and headers) to also export them to Langfuse or any OTLP backend. Each incident records latency, LLM calls, tokens, cost in USD, and queries run or blocked.

## Project layout

```text
src/pipeline_doctor/
  agent/            LangGraph state machine, toolkit, context gathering, fixes, reasoners (heuristic, LLM)
  diagnostics/      typed diagnostic checks, schema drift
  warehouse/        Warehouse protocol, DuckDB + Snowflake adapters, SQL guard and budget
  dbt_tools/        artifact parsing, lineage + column lineage, dbt runner
  airflow/          callback payloads, task logs (local folder or REST API)
  vcs/              git wrapper, commit history relevant to a lineage
  memory/           incident vector store (hashing embedder, swappable)
  jev/              Judge / Evaluator / Verifier decision layer, remedy policy, LLM judge
  incidents/        domain models, repository, markdown report
  publishing/       approval service, local-git and GitHub draft PR publishers
  observability/    tracing setup, usage/cost meter
  evals/            fault catalogue, injection, harness, report
  api/              FastAPI app, Slack
  demo/             demo data generator, workspace + pipeline simulator
demo_project/       dbt project template (sources, staging, intermediate, marts, tests, exposures)
airflow/dags/       production DAG with the failure callback
docker/             images for the local Airflow + Pipeline Doctor stack (compose.yaml)
tests/              unit tests + integration tests that run real dbt against DuckDB
```

## Development

```bash
uv run pytest                 # 169 tests, ~80s (integration tests build real dbt workspaces)
uv run pytest -m "not integration"
uv run ruff check src tests && uv run ruff format --check src tests
```

## Roadmap

- Run the eval with Claude and GPT reasoners, and publish accuracy against cost and latency.
- A JEV adversarial eval: mutate correct diagnoses and fixes (wrong culprit, deleted test, write SQL) and measure the catch rate.
- Held-out faults the heuristic was not tuned on: multi-fault incidents, silent filter regressions, late-arriving dimensions.
- Point the dbt profile at Snowflake. The adapter, guard dialect, and fix transpilation (sqlglot) are already in place.
- A Langfuse dashboard on the OTLP export, and Slack interactive buttons in place of slash commands.
