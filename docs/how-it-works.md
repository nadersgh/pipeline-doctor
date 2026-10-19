# How it works

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
7. **JEV review (optional).** A Judge / Evaluator / Verifier layer decides what happens to the proposal. See [JEV decision layer](jev.md).
8. **Wait for approval.** On approval, the fix is committed to `pipeline-doctor/<incident-id>` and opened as a draft PR. The resolution is stored in memory so recurring failures are recognised.

## Root-cause taxonomy (10 categories)

| Upstream data | Code regressions |
| --- | --- |
| `source_column_renamed`, `source_type_changed`, `source_null_spike`, `source_duplicate_rows`, `source_stale`, `source_new_category`, `source_missing_rows` | `code_sql_error`, `code_join_fanout`, `code_logic_regression` |

## Running it

Use an LLM reasoner with `PIPELINE_DOCTOR_REASONER=anthropic` or `openai`, plus the provider's API key. If the provider fails, the reasoner falls back to the heuristic reasoner one step at a time, so an outage degrades the diagnosis instead of failing it.

Other entry points:

- `uv run pipeline-doctor serve` starts the FastAPI app (it refuses to start without `PIPELINE_DOCTOR_API_TOKEN` unless `PIPELINE_DOCTOR_ALLOW_UNAUTHENTICATED=true`): webhooks, `GET /incidents/{id}/report`, approve/reject, and a Slack `/doctor explain|status|approve <id>` command with signature verification. Approve and reject need a personal reviewer token (`PIPELINE_DOCTOR_REVIEWER_TOKENS`), and the reviewer's identity is taken from that token. The CLI `approve` command is for operators who already have filesystem access to the deployment.
- `uv run pipeline-doctor mcp` serves the same read-only tools over MCP (stdio) for IDEs and chat agents.
- [airflow/dags/shop_analytics_daily.py](../airflow/dags/shop_analytics_daily.py) is the DAG: `dbt source freshness` → `dbt build` → tag `pipeline-doctor/last-green`, with the failure callback wired in.
