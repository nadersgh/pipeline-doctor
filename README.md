# Pipeline Doctor

Pipeline Doctor is a LangGraph agent that diagnoses failed dbt and Airflow runs. It gathers evidence with read-only tools (task logs, dbt lineage, git diff, schema drift, guarded SQL checks), explains the root cause in plain language, and proposes a fix that a named human approves as a draft pull request.

## Results

51 injected faults, each breaking a real dbt project on DuckDB: the `dev` split (39 cases across 10 root-cause categories, 3 carrying prompt-injection payloads), which the heuristic reasoner was written against, and an untuned `holdout` split (12 cases: unseen faults, 3 compound faults, 2 infrastructure failures with no code fault). The agent runs on the Airflow failure, then its fix is applied and the project rebuilt. Run on 2026-10-14, JEV on, one run per case, Claude reasoner `claude-sonnet-5-5` (priced at $2 / $10 per million tokens).

**Dev split (39 cases).** The heuristic scores 100% because it was written alongside these cases, so read that column as a regression baseline.

| Fault type (root-cause category) | Cases | Heuristic: category / culprit / fix valid | Claude: category / culprit / fix valid |
| --- | --- | --- | --- |
| `source_column_renamed` | 5 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_type_changed` | 3 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_null_spike` | 5 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_duplicate_rows` | 4 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_stale` | 4 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_new_category` | 4 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_missing_rows` | 3 | 100% / 100% / 100% | 100% / 100% / 100% |
| `code_sql_error` | 5 | 100% / 100% / 100% | 100% / 100% / 100% |
| `code_join_fanout` | 3 | 100% / 100% / 100% | 67% / 100% / 100% |
| `code_logic_regression` | 3 | 100% / 100% / 100% | 100% / 100% / 100% |
| **All** | **39** | **100% / 100% / 100%** | **97% / 100% / 100%** |

| Metric (dev) | Heuristic | Claude |
| --- | --- | --- |
| Category accuracy with a misleading recent commit | 100% (10/10) | 100% (10/10) |
| JEV proceed rate / precision | 100% / 100% | 100% / 97% |
| Latency per incident, mean / p95 | 0.14s / 0.23s | 21.4s / 51.0s |
| Diagnostic queries per incident, mean | 1.67 | 6.0 |
| LLM cost, 39 cases | $0 | $2.18 (123 calls, about $0.056 per incident) |

**Holdout split (12 cases, untuned).** Too small to separate the two reasoners: one case is 8 points.

| Metric (holdout) | Heuristic | Claude |
| --- | --- | --- |
| Category accuracy | 92% (11/12) | 92% (11/12) |
| Culprit accuracy (relation and column) | 92% | 92% |
| Fix validity | 83% (10/12) | 92% (11/12) |
| False positives on no-fault cases | 0% | 0% |
| LLM cost | $0 | $0.82 (44 calls) |

Both reasoners missed `holdout_type_customers_id_prefixed` (an id column that starts arriving as a prefixed string). The heuristic guessed `source_missing_rows`; Claude answered `unknown` at 0.5 confidence and JEV escalated to a human, which is the safer failure. The heuristic's other miss was the right category with an operational fix where a code change was needed. Claude's only other miss, on dev, called a join fan-out a logic regression, but its patch was valid.

**Model comparison, all 51 cases.** Haiku was run 3 times per case (mean, with the spread across repeats); Sonnet and the heuristic once, so the differences below are within run-to-run noise.

| Metric (all 51 cases) | Heuristic | Claude Sonnet 5.5 | Claude Haiku 5.5 |
| --- | --- | --- | --- |
| Category accuracy | 98.0% | 96.1% | 96.7% (std 3.0, range 94.1% to 100%) |
| Fix validity | 96.1% | 98.0% | 97.4% (std 1.1, range 96.1% to 98.0%) |
| Holdout category / fix validity (12 cases) | 92% / 83% | 92% / 92% | 92% / 92% |
| JEV proceed precision | 100% | 97.9% | 95.0% |
| Latency per incident, mean / p95 | 0.14s / 0.26s | 22.8s / 53.8s | 19.6s / 40.9s |
| Diagnostic queries per incident, mean | 1.7 | 6.0 | 3.6 |
| Cost per full 51-case pass | $0 | $2.99 | $0.17 |
| Cost per incident | $0 | $0.059 | $0.0034 |

Haiku's fix validity matches Sonnet's within run-to-run noise, for about 1/18th of the price. Its weak spot is naming the culprit column for `source_missing_rows` (58% across repeats); a few cases flip between runs, which is why per-case agreement is 97%. Prices are $2 / $10 (Sonnet) and $0.10 / $0.50 (Haiku) per million tokens. To reproduce the Haiku run:

```bash
PIPELINE_DOCTOR_LLM_MODEL=claude-haiku-5-5 PIPELINE_DOCTOR_LLM_INPUT_COST_PER_MTOK=0.10 PIPELINE_DOCTOR_LLM_OUTPUT_COST_PER_MTOK=0.50 \
  ANTHROPIC_API_KEY=... uv run pipeline-doctor eval --reasoner anthropic --split all --workers 4 --repeats 3
```

A fix is valid when the patch applies and `dbt source freshness && dbt build` goes green with every baseline test still running, or when the agent correctly escalates to the data owner. The same harness runs any reasoner:

```bash
ANTHROPIC_API_KEY=... uv run pipeline-doctor eval --reasoner anthropic --split all --workers 4   # or OPENAI_API_KEY + --reasoner openai
```

The OpenAI reasoner has not been run through the eval yet.

Without a working key every LLM step falls back to the heuristic, and the run reports heuristic numbers under the LLM's name. Before publishing an LLM run, check that `Total LLM cost / calls` is non-zero and that the log has no `failed to hypothesize`, `failed to assess` or `failed to propose a fix` lines.

## Demo

![A renamed source column fails dbt_build; Pipeline Doctor diagnoses it, JEV clears the fix, and approval commits it to a draft PR branch](docs/demo.gif)

An upstream system renames `raw.orders.status`, and the Airflow `dbt_build` task fails. The agent finds the schema drift, proposes a one-line patch to `stg_orders`, JEV clears it, and approval commits it to `pipeline-doctor/<incident-id>`. Heuristic reasoner, no API key, 68 seconds. Re-record with `vhs docs/demo.tape`.

## How it works

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

1. **Detect.** The DAG's `on_failure_callback` posts the failed task to the API. A dbt-only webhook exists too. Retries of the same task attempt are deduplicated.
2. **Gather context.** The Airflow task log, the failing dbt nodes from `run_results.json` and `sources.json`, and the lineage from `manifest.json` (upstream sources and models, downstream exposures), with column lineage traced through CTEs by sqlglot. Commits between the last green run and the failed run's commit that touch the failing lineage, flagged when they change SQL logic rather than comments or docs. Schema drift between declared source columns and the warehouse. Similar past incidents.
3. **Hypothesize, then verify.** The reasoner proposes ranked hypotheses, each with typed diagnostic checks: null rate by load day, duplicate keys across loads, freshness, unexpected values, orphaned keys inside or outside the parent key range, column profile with format detection, and guarded custom SQL.
4. **Assess.** Conclude, or loop back for more evidence, up to `max_investigation_rounds`.
5. **Explain.** A technical summary for engineers and a plain-language one for stakeholders ("why is my dashboard wrong?") that names the impacted dashboards.
6. **Propose a fix.** A model change, a schema patch, a test change, or an operational escalation when the data owner has to act. Edits are exact-snippet patches, validated before anything is shown. A patch that does not apply goes back to the reasoner once with the error. Reverts undo only the suspect commit's hunks, so later edits to the same file survive. If the investigation runs out of rounds without a confident conclusion, no code change is proposed.
7. **JEV review (optional).** A Judge / Evaluator / Verifier layer decides what happens to the proposal. See [JEV decision layer](#jev-decision-layer).
8. **Wait for approval.** On approval, the fix is committed to `pipeline-doctor/<incident-id>` and opened as a draft PR. The resolution is stored in memory so recurring failures are recognised.

### Root-cause taxonomy (10 categories)

| Upstream data | Code regressions |
| --- | --- |
| `source_column_renamed`, `source_type_changed`, `source_null_spike`, `source_duplicate_rows`, `source_stale`, `source_new_category`, `source_missing_rows` | `code_sql_error`, `code_join_fanout`, `code_logic_regression` |

## Design decisions and what I'd do differently

- **Heuristic reasoner as baseline and fallback.** I wrote a rule-based reasoner that works from evidence signals (schema drift, failing test type, SQL-changing commits) before adding LLMs. It gives the eval a free, deterministic baseline, and `FallbackReasoner` uses it for any step where the LLM call fails. The catch: it was tuned on the same catalogue, so its 100% says nothing about generalisation, and a missing API key quietly turns an LLM eval into a heuristic one. I'd record fallback steps per case and fail the run when the LLM never answered.
- **Typed checks over free SQL.** The reasoner picks from 8 typed checks (`null_rate_by_load_day`, `orphan_keys`, `column_profile`, ...) whose SQL I wrote and tested, with `custom_sql` as the escape hatch. Typed checks are easy to review and keep cost down (1.69 queries per incident). They also limit what the agent can discover: a new failure mode needs custom SQL or a new check kind.
- **sqlglot guard as defence in depth.** DuckDB opens `read_only=True`, and Snowflake expects a SELECT-only role. `SqlGuard` still parses every query and rejects anything but one schema-qualified SELECT. The credentials are the real boundary; a parser-based allowlist will always have gaps, so I don't rely on it alone.
- **Snippet edits, validated before anyone sees them.** A fix is a list of exact `original` → `replacement` snippets that must match once, inside dbt folders, with no `..` or symlinks. Snippets avoid the line-number bookkeeping that makes model-written unified diffs fail to apply, and verifying one is a string match. They suit hotfixes, not refactors.
- **JEV is a rule gate, the LLM only vetoes.** The rules are deterministic, free, and name each failed check. The optional LLM judge can hold back a `proceed` but cannot overturn a rule failure, so a persuasive model cannot argue past a removed test. Rules only catch what I anticipated, and the eval has no wrong diagnoses yet (catch rate n/a), so JEV's value is unmeasured. The adversarial eval on the roadmap is the fix.
- **Hashing embedder for incident memory.** Signed feature hashing of unigrams and bigrams into 512 dimensions: no model download, deterministic in tests. It matches recurring failures that share table and error tokens and misses paraphrases. `TextEmbedder` is a protocol, so a real embedding model is a one-class swap once there is incident history to evaluate it on.
- **DuckDB for the demo, Snowflake not exercised live.** DuckDB lets the eval clone 51 workspaces and run them on a laptop in about a minute with 6 workers. The Snowflake adapter (`QUERY_TAG`, statement timeout, `EXPLAIN` scan estimate) and sqlglot transpilation of fix SQL exist, but only the guard's Snowflake dialect is unit-tested. I would run the eval against a Snowflake account before claiming support.
- **Airflow 2.10, not 3.** The stack pins `apache/airflow:2.10.5`, and the log reader calls the `/api/v1` REST API, which Airflow 3 replaces with `/api/v2`. Porting the log reader and re-testing the callback is the main work.
- **FastAPI background tasks, not a queue.** The webhook stores the incident and runs the diagnosis in a `BackgroundTasks` job in the same process, and incidents are JSON files on disk. That is enough for one team and a demo. A restart drops in-flight diagnoses and two replicas would race on the files, so production needs a durable job queue and a database-backed repository.

## Quickstart

```bash
uv sync --all-extras
uv run pipeline-doctor demo setup                         # dbt project under git + DuckDB raw data + green baseline run
uv run pipeline-doctor demo faults                        # list the injectable faults
uv run pipeline-doctor demo break renamed_orders_status   # inject a fault and run the DAG's tasks
uv run pipeline-doctor diagnose                           # diagnose the failure, print the report
uv run pipeline-doctor approve <incident-id> --approver you
uv run pipeline-doctor eval --workers 6                   # 39-case dev eval, writes evals/results/<run>/
```

Use an LLM reasoner with `PIPELINE_DOCTOR_REASONER=anthropic` or `openai`, plus the provider's API key. If the provider fails, the reasoner falls back to the heuristic reasoner one step at a time, so an outage degrades the diagnosis instead of failing it.

Other entry points:

- `uv run pipeline-doctor serve` starts the FastAPI app (it refuses to start without `PIPELINE_DOCTOR_API_TOKEN` unless `PIPELINE_DOCTOR_ALLOW_UNAUTHENTICATED=true`): webhooks, `GET /incidents/{id}/report`, approve/reject, and a Slack `/doctor explain|status|approve <id>` command with signature verification. Approve and reject need a personal reviewer token (`PIPELINE_DOCTOR_REVIEWER_TOKENS`), and the reviewer's identity is taken from that token. The CLI `approve` command is for operators who already have filesystem access to the deployment.
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

- `dev`: [`evals/cases.py`](src/pipeline_doctor/evals/cases.py), 26 faults across all 10 categories plus 10 hard variants that add a misleading recent commit (a docs edit, a comment-only SQL change, or a real SQL change outside the failing lineage). [`evals/adversarial.py`](src/pipeline_doctor/evals/adversarial.py) adds 3 prompt-injection variants: a real fault plus a payload in a commit message, a source column value, or a dbt error. The heuristic reasoner was written against the 36 non-adversarial cases.
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
uv run pipeline-doctor eval --split holdout          # dev (default, the regression set) | holdout | all
uv run pipeline-doctor eval --repeats 3 --workers 6  # each run in a fresh workspace
```

With `--repeats N` the report adds mean, std dev, min, and max of every metric across repeats, plus per-case agreement (share of repeats with the same outcome). Each split and case kind gets accuracy next to cost, LLM calls, and p50/p95 latency, and the per-case table lists cost, latency, LLM calls, and queries. Results go to `evals/results/<run>/` as `summary.md`, `summary.json`, `results.jsonl`, and `spans.jsonl`, with a per-step latency table built from the traces.

`heuristic`, `--repeats 2` (no LLM, so $0 and 0 LLM calls):

| Slice | Cases x repeats | Category | Relation | Column | Culprit | Fix kind | Fix valid | Primary hit | FP rate | p50 / p95 latency |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `dev` | 39 x 2 | 100% | 100% | 100% | 100% | 100% | 100% | 100% | n/a | 0.10s / 0.16s |
| `holdout` (all) | 12 x 2 | 91.7% | 100% | 77.8% | 91.7% | 83.3% | 83.3% | 91.7% | 0% | 0.09s / 0.13s |
| holdout compound | 3 x 2 | 100% | 100% | 100% | 100% | 100% | 100% | 100% | n/a | 0.10s / 0.12s |
| holdout no-fault | 2 x 2 | 100% | 100% | n/a | 100% | 100% | 100% | 100% | 0% | 0.04s / 0.04s |
| `anthropic` / `openai` | run `pipeline-doctor eval --reasoner anthropic --repeats 3` | | | | | | | | | |

Across both splits JEV proceeds on 90.2% of runs with 100% precision and holds back every wrong diagnosis. Std dev is 0 for every accuracy metric. Per-case agreement is 99%: `compound_null_customer_ids_and_cents` flips between its two causes because dbt reports the two failing tests in nondeterministic order. The two holdout misses are `holdout_type_customers_id_prefixed` (right table, but read as missing rows, so wrong category and column) and `holdout_model_column_renamed` (right category and model at 0.35 confidence, so it escalated instead of patching).

Read the `dev` row as a regression baseline, not a headline: the rules were written alongside those cases. The `holdout` rows and an LLM reasoner with repeats are the numbers to compare.

## Guardrails

| Risk | Control |
| --- | --- |
| Warehouse writes | DuckDB opens with `read_only=True` and `enable_external_access=false`. Snowflake must use a SELECT-only role without stage, sequence, or external-function privileges, and sessions carry `QUERY_TAG` and `STATEMENT_TIMEOUT_IN_SECONDS`. |
| Arbitrary SQL | [`SqlGuard`](src/pipeline_doctor/warehouse/guard.py) parses with sqlglot. It allows exactly one `SELECT`/set-operation and rejects DML, DDL, `COPY`, `ATTACH`, `PRAGMA`, `SELECT INTO`, table functions, file and network functions (`read_*`, `glob`, ...), Snowflake stages (`@stage`), sequences (`.nextval`), user-defined functions, and unknown functions outside a small allowlist. Tables must be schema-qualified, in `allowed_schemas`, and in the configured database. Every query is wrapped in `LIMIT max_result_rows`. The guard is defence in depth; the read-only credentials are the primary control. |
| Cost / runaway loops | Per-incident `QueryBudget` (query count, per-query timeout, and on Snowflake the bytes scanned estimated through `EXPLAIN`; DuckDB has no scan estimate), a bounded number of investigation rounds, and LLM tokens and cost metered per run. |
| Unsafe patches | Edits are limited to `.sql`/`.yml`/`.md`/`.csv` under `models/`, `tests/`, `macros/`, `snapshots/`, `seeds/`, `analyses/`. Absolute paths, `..`, and symlinks anywhere on the path are rejected. The `original` snippet must occur exactly once. Files owned by installed dbt packages are left out of the editable context. |
| Untrusted proposals | When enabled, JEV gates every proposal before a human sees it, and approval refuses code fixes JEV did not clear. |
| Prompt injection | Task logs, dbt messages, compiled SQL, commits, project files, query results, and the proposal under review reach prompts inside `<untrusted>` blocks the content cannot close. The system prompts say text inside them is data, never instructions. |
| Unreviewed writes | The agent has no write tools. Only `ApprovalService.approve` can publish, it requires an incident in `awaiting_approval` and an authenticated reviewer, and it opens a **draft** PR. Approval endpoints fail closed until reviewer tokens are configured, and the webhook service token cannot approve. The local publisher commits with git plumbing, so the working tree and index are never touched. |
| Webhook/Slack spoofing | Bearer token on the API. Slack requests need a valid HMAC signature with a 5-minute replay window, and only Slack member IDs listed in `PIPELINE_DOCTOR_SLACK_APPROVERS` can approve. |
| Shell injection | git and dbt run with fixed argument vectors and no shell. Refs and paths are validated. |

[`security/untrusted.py`](src/pipeline_doctor/security/untrusted.py) also flags injection-like text (instruction overrides, role tags, DDL/DML, tool-call lookalikes) and records it on the incident and in its report, keeping the evidence. The detector is a signal for the reviewer, not a control. The controls that hold are read-only credentials, `SqlGuard`, JEV, path validation, and human approval. [`tests/test_prompt_injection.py`](tests/test_prompt_injection.py) scripts a model that obeys the payload: its `DROP`/`DELETE` queries are blocked, the warehouse row counts do not change, JEV escalates its fix (a `post_hook` delete, an edit outside the lineage, a deleted test), and approval publishes nothing. The heuristic reasoner diagnoses the adversarial eval cases correctly because it scores structured signals and never acts on free text.

**Snowflake status.** Verified: the adapter against a fake connector (role, `QUERY_TAG`, statement timeout, row limit, error mapping, `EXPLAIN` bytes into the budget, `information_schema` columns into schema drift), the guard in the snowflake dialect, and every diagnostic check's SQL transpiled to Snowflake through the guard. Not verified: no live run yet, as there is no CI account. To run the opt-in live pass on a trial account, set `PIPELINE_DOCTOR_SNOWFLAKE_ACCOUNT`/`_USER`/`_PASSWORD` and run `uv run pytest -m snowflake_live`. Role provisioning and details are in [docs/snowflake.md](docs/snowflake.md).

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
  security/         untrusted-input fencing, prompt-injection detector
  incidents/        domain models, repository, markdown report
  publishing/       approval service, local-git and GitHub draft PR publishers
  observability/    tracing setup, usage/cost meter
  evals/            fault catalogue, injection, harness, report
  api/              FastAPI app, Slack
  demo/             demo data generator, workspace + pipeline simulator
demo_project/       dbt project template (sources, staging, intermediate, marts, tests, exposures)
airflow/dags/       production DAG with the failure callback
docker/             images for the local Airflow + Pipeline Doctor stack (compose.yaml)
docs/               demo.tape (vhs script) and the rendered demo.gif
tests/              unit tests + integration tests that run real dbt against DuckDB
```

## Development

```bash
uv run pytest                 # 314 tests, ~140s (integration tests build real dbt workspaces)
uv run pytest -m "not integration"
uv run ruff check src tests && uv run ruff format --check src tests
uv run mypy                   # strict, src only
uv run pre-commit install     # once: runs hygiene, ruff, mypy, uv-lock and gitleaks checks on every commit
uv run pre-commit run --all-files
```

## CI

- Every PR and push to `main` ([`ci.yml`](.github/workflows/ci.yml)): ruff lint and format check, mypy, the full pytest suite, then the heuristic eval on the 39 `dev` cases with `--gate evals/gate.toml`. The gate fails the job and lists each metric below its minimum. The eval `summary.md` goes to the job summary and the run folder is uploaded as an artifact.
- To change the baseline on purpose, edit the minimums in [`evals/gate.toml`](evals/gate.toml) in the same PR and say why. Check locally with `uv run pipeline-doctor eval --reasoner heuristic --gate evals/gate.toml`.
- Nightly LLM eval ([`llm-evals.yml`](.github/workflows/llm-evals.yml)): runs the `anthropic` and `openai` reasoners, report only, never gates. Needs repository secrets `ANTHROPIC_API_KEY` and `OPENAI_API_KEY`; a reasoner whose secret is missing is skipped with a notice. Manual runs pick the reasoner, a comma-separated case subset to cap API cost, and the worker count.

## Roadmap

- Run the eval with the GPT reasoner and with repeats on Sonnet, and grow the holdout split beyond 12 cases.
- A JEV adversarial eval: mutate correct diagnoses and fixes (wrong culprit, deleted test, write SQL) and measure the catch rate.
- Point the dbt profile at Snowflake. The adapter, guard dialect, and fix transpilation (sqlglot) are already in place.
- A Langfuse dashboard on the OTLP export, and Slack interactive buttons in place of slash commands.

[![M8ven Score](https://m8ven.ai/badge/mcp/nadersgh-pipeline-doctor-6ypi0h?v=1041a332d53066530b1e27366b962d21)](https://m8ven.ai/mcp/nadersgh-pipeline-doctor-6ypi0h?s=readme)