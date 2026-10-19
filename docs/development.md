# Development

```bash
uv run pytest                 # about 340 tests, a few minutes (integration tests build real dbt workspaces)
uv run pytest -m "not integration"
uv run ruff check src tests && uv run ruff format --check src tests
uv run mypy                   # strict, src only
uv run pre-commit install     # once: runs hygiene, ruff, mypy, uv-lock and gitleaks checks on every commit
uv run pre-commit run --all-files
```

## CI

- Every PR and push to `main` ([`ci.yml`](../.github/workflows/ci.yml)): ruff lint and format check, mypy, the full pytest suite, then the heuristic eval on the 39 `dev` cases with `--gate evals/gate.toml`. The gate fails the job and lists each metric below its minimum. The eval `summary.md` goes to the job summary and the run folder is uploaded as an artifact.
- To change the baseline on purpose, edit the minimums in [`evals/gate.toml`](../evals/gate.toml) in the same PR and say why. Check locally with `uv run pipeline-doctor eval --reasoner heuristic --gate evals/gate.toml`.
- Nightly LLM eval ([`llm-evals.yml`](../.github/workflows/llm-evals.yml)): runs the `anthropic` and `openai` reasoners, report only, never gates. Needs repository secrets `ANTHROPIC_API_KEY` and `OPENAI_API_KEY`; a reasoner whose secret is missing is skipped with a notice. Manual runs pick the reasoner, a comma-separated case subset to cap API cost, and the worker count.

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
docs/               results, evals, JEV, guardrails, design decisions, Docker stack, demo.tape and demo.gif
tests/              unit tests + integration tests that run real dbt against DuckDB
```
