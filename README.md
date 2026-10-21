[![M8ven Score](https://m8ven.ai/badge/mcp/nadersgh-pipeline-doctor-6ypi0h?v=1041a332d53066530b1e27366b962d21)](https://m8ven.ai/mcp/nadersgh-pipeline-doctor-6ypi0h?s=readme)



# Pipeline Doctor

Pipeline Doctor is a LangGraph agent that diagnoses failed dbt and Airflow runs. It gathers evidence with read-only tools (task logs, dbt lineage, git diff, schema drift, guarded SQL checks), explains the root cause in plain language, and proposes a fix that a named human approves as a draft pull request.

## Demo

![A renamed source column fails dbt_build; Pipeline Doctor diagnoses it, JEV clears the fix, and approval commits it to a draft PR branch](docs/demo.gif)

An upstream system renames `raw.orders.status`, and the Airflow `dbt_build` task fails. The agent finds the schema drift, proposes a one-line patch to `stg_orders`, JEV clears it, and approval commits it to `pipeline-doctor/<incident-id>`. Heuristic reasoner, no API key, 68 seconds. Re-record with `vhs docs/demo.tape`.

## Results

53 injected failures on a real dbt project (DuckDB): 39 `dev` cases the rule-based reasoner was written against, and 14 untuned `holdout` cases that include 4 infrastructure failures with no code fault. Each fix is applied and the project rebuilt, and the unpatched project is rerun to check that the failure really came back.

| | Rules (heuristic) | Claude Sonnet 5.5 | Claude Haiku 5.5 |
| --- | --- | --- | --- |
| Fix validity | 96.2% | 98.0% | 97.5% |
| Abstained on a real fault | 0% | 2.0% | 2.0% |
| Confident wrong answers | 0% | 2.0% | 3.1% |
| Latency per incident | 0.09s | 22.8s | 19.6s |
| Cost per incident | $0 | $0.059 | $0.0033 |

On known failure types the rules tie the LLMs, and the heuristic was written against the dev set, so read it as a baseline. The LLMs earn their keep on faults nobody wrote a rule for, and when unsure they answer `unknown` at low confidence and escalate to a human. Haiku reaches Sonnet's fix validity for about 1/18th of the price. The holdout is 14 cases and Sonnet ran once on 51 of the 53 cases, so differences of a point or two are noise. Per-category tables, the misses and how to reproduce: [docs/results.md](docs/results.md).

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

1. **Detect.** The DAG's failure callback posts the failed task to the API. Retries of the same attempt are deduplicated.
2. **Gather context.** Task log, failing dbt nodes, lineage, commits that change SQL in the failing lineage, schema drift, and similar past incidents.
3. **Hypothesize, then verify.** Ranked hypotheses, each with typed read-only diagnostic checks.
4. **Assess.** Conclude, or loop back for more evidence, up to a bounded number of rounds.
5. **Explain.** A technical summary and a plain-language one that names the impacted dashboards.
6. **Propose a fix.** A model change, schema patch, test change, or an operational escalation. Edits are exact-snippet patches, validated before anyone sees them.
7. **JEV review.** A rule gate (with an optional LLM veto) decides whether the proposal deserves a reviewer's time: [docs/jev.md](docs/jev.md).
8. **Human approval.** On approval the fix is committed and opened as a draft PR.

More detail: [docs/how-it-works.md](docs/how-it-works.md). Why it is built this way, and what I would change: [docs/design-decisions.md](docs/design-decisions.md).

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

No API key is needed for the demo. For an LLM reasoner set `PIPELINE_DOCTOR_REASONER=anthropic` or `openai`, the provider's API key, and optionally `PIPELINE_DOCTOR_LLM_MODEL`. If a provider call fails, that step falls back to the heuristic reasoner, and the eval fails when any step did.

- `uv run pipeline-doctor serve` starts the FastAPI app: webhooks, incident reports, approve and reject with personal reviewer tokens, and a Slack `/doctor` command.
- `uv run pipeline-doctor mcp` serves the same read-only tools over MCP for IDEs and chat agents.
- A local Airflow plus Pipeline Doctor stack runs with `docker compose up`: [docs/airflow-docker.md](docs/airflow-docker.md).

## Guardrails

The agent has no write tools, and the controls that hold do not depend on the model behaving:

- **Read-only warehouse access.** DuckDB opens read-only, Snowflake needs a SELECT-only role, and a sqlglot allowlist (`SqlGuard`) accepts one schema-qualified `SELECT` with a row limit as defence in depth.
- **Typed checks and budgets.** The reasoner picks from typed diagnostic checks, with a per-incident query budget, timeouts and a bounded number of rounds.
- **Constrained patches.** Only `.sql`, `.yml`, `.md` and `.csv` under dbt folders, no `..` or symlinks, and each snippet must match the file exactly once.
- **Untrusted input.** Logs, data rows, commits and query results reach prompts fenced as data, and injection-like text is flagged for the reviewer. It is a signal, not a control.
- **JEV, then a human.** A deterministic gate checks each proposal, and only an authenticated reviewer can approve, which opens a draft PR.

The full table, the Snowflake status and the prompt-injection tests: [docs/guardrails.md](docs/guardrails.md).

## Documentation

| Page | What is in it |
| --- | --- |
| [docs/results.md](docs/results.md) | Full results, per-category tables, abstention and rerun metrics, how to reproduce |
| [docs/evals.md](docs/evals.md) | The fault catalogue, dev and holdout splits, scoring, how the harness works |
| [docs/jev.md](docs/jev.md) | The Judge / Evaluator / Verifier decision layer |
| [docs/guardrails.md](docs/guardrails.md) | Security controls, Snowflake status |
| [docs/design-decisions.md](docs/design-decisions.md) | Trade-offs and what I would do differently |
| [docs/how-it-works.md](docs/how-it-works.md) | The graph step by step, root-cause taxonomy |
| [docs/airflow-docker.md](docs/airflow-docker.md) | Local Airflow and API stack |
| [docs/development.md](docs/development.md) | Tests, CI, observability, project layout |
| [docs/snowflake.md](docs/snowflake.md) | Snowflake role and live test |

## Roadmap

- Run the eval with the GPT reasoner and with repeats on Sonnet, and grow the holdout split beyond 16 cases.
- A JEV adversarial eval: mutate correct diagnoses and fixes (wrong culprit, deleted test, write SQL) and measure the catch rate.
- Point the dbt profile at Snowflake. The adapter, guard dialect, and fix transpilation (sqlglot) are already in place.
- A Langfuse dashboard on the OTLP export, and Slack interactive buttons in place of slash commands.

## License

MIT, see [LICENSE](LICENSE).

