# Pipeline Doctor

An agent that diagnoses broken data pipelines and proposes fixes. It listens for a failed Airflow task or dbt test, collects evidence with read-only tools, names the root cause in plain language, and prepares a fix. Nothing is written until a named human approves, and even then the output is a draft pull request.

```text
Airflow on_failure_callback ──► POST /webhooks/airflow ──► incident
                                                            │
     ┌──────────────────────── LangGraph state machine ─────┴───────────────────────────┐
     │ detect → gather_context → hypothesize → verify → assess ─┬─► explain → propose_fix │
     │                                ▲                          │        → request_approval
     │                                └──── needs more evidence ─┘   (max N rounds)     │
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
7. **Wait for approval.** On approval, the fix is committed to `pipeline-doctor/<incident-id>` and opened as a draft PR. The resolution is stored in memory so recurring failures are recognised.

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

## Evals

[`evals/cases.py`](src/pipeline_doctor/evals/cases.py) defines 26 faults across all 10 categories, plus 10 hard variants that add a misleading recent commit: a docs edit, a comment-only SQL change, or a real SQL change outside the failing lineage. That makes 36 cases. For each case the harness:

1. clones a green workspace (dbt project, git history, DuckDB)
2. injects the fault: mutates raw data, or commits a code change as a developer would
3. runs the pipeline exactly like the DAG, and checks that the fault actually breaks it
4. runs the agent on the resulting Airflow failure event
5. scores the **category**, the **culprit** (source table or changed model, plus the column when one is involved), and the **fix**. A code fix only counts if the patch applies, `dbt source freshness && dbt build` goes green afterwards, and every baseline test still runs, so deleting a test does not count as a fix. An operational fix only counts when escalation is the right answer, such as a stale feed or deleted parent rows.

Run ids and workspace paths are random, so nothing in the agent's context names the fault. A fault that fails to break the pipeline counts as a miss.

Results go to `evals/results/<run>/` as `summary.md`, `summary.json`, `results.jsonl`, and `spans.jsonl`, with a per-step latency table built from the traces.

| Reasoner | Category acc. | Culprit acc. | Fix validity | With distractors | Mean latency | LLM cost |
| --- | --- | --- | --- | --- | --- | --- |
| `heuristic` | 100% (36/36) | 100% | 100% | 100% (10/10) | 0.10s | $0 |
| `anthropic` / `openai` | run `pipeline-doctor eval --reasoner anthropic` | | | | | |

Read the heuristic row as a regression baseline, not a headline. The rules were written alongside the fault catalogue, so they cover every case by construction. They reason from evidence signals rather than case ids, and the eval would catch a regression in any tool, query, or fix template. The interesting comparison is an LLM reasoner on the same harness, together with new held-out faults it has never seen (see roadmap).

## Guardrails

| Risk | Control |
| --- | --- |
| Warehouse writes | DuckDB opens with `read_only=True` and `enable_external_access=false`. Snowflake must use a SELECT-only role without stage, sequence, or external-function privileges, and sessions carry `QUERY_TAG` and `STATEMENT_TIMEOUT_IN_SECONDS`. |
| Arbitrary SQL | [`SqlGuard`](src/pipeline_doctor/warehouse/guard.py) parses with sqlglot. It allows exactly one `SELECT`/set-operation and rejects DML, DDL, `COPY`, `ATTACH`, `PRAGMA`, `SELECT INTO`, table functions, file and network functions (`read_*`, `glob`, ...), Snowflake stages (`@stage`), sequences (`.nextval`), user-defined functions, and unknown functions outside a small allowlist. Tables must be schema-qualified, in `allowed_schemas`, and in the configured database. Every query is wrapped in `LIMIT max_result_rows`. The guard is defence in depth; the read-only credentials are the primary control. |
| Cost / runaway loops | Per-incident `QueryBudget` (query count, per-query timeout, and on Snowflake the bytes scanned estimated through `EXPLAIN`; DuckDB has no scan estimate), a bounded number of investigation rounds, and LLM tokens and cost metered per run. |
| Unsafe patches | Edits are limited to `.sql`/`.yml`/`.md`/`.csv` under `models/`, `tests/`, `macros/`, `snapshots/`, `seeds/`, `analyses/`. Absolute paths, `..`, and symlinks anywhere on the path are rejected. The `original` snippet must occur exactly once. Files owned by installed dbt packages are left out of the editable context. |
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
  incidents/        domain models, repository, markdown report
  publishing/       approval service, local-git and GitHub draft PR publishers
  observability/    tracing setup, usage/cost meter
  evals/            fault catalogue, injection, harness, report
  api/              FastAPI app, Slack
  demo/             demo data generator, workspace + pipeline simulator
demo_project/       dbt project template (sources, staging, intermediate, marts, tests, exposures)
airflow/dags/       production DAG with the failure callback
tests/              unit tests + integration tests that run real dbt against DuckDB
```

## Development

```bash
uv run pytest                 # 115 tests, ~45s (integration tests build real dbt workspaces)
uv run pytest -m "not integration"
uv run ruff check src tests && uv run ruff format --check src tests
```

## Roadmap

- Run the eval with Claude and GPT reasoners, and publish accuracy against cost and latency.
- Held-out faults the heuristic was not tuned on: multi-fault incidents, silent filter regressions, late-arriving dimensions.
- Point the dbt profile at Snowflake. The adapter, guard dialect, and fix transpilation (sqlglot) are already in place.
- A Langfuse dashboard on the OTLP export, and Slack interactive buttons in place of slash commands.
