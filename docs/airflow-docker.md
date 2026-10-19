# Run with Airflow (Docker)

[`compose.yaml`](../compose.yaml) runs Airflow 2.10 (standalone: scheduler, webserver, SQLite) with the `shop_analytics_daily` DAG next to the Pipeline Doctor API. Both containers share a `workspace` volume that holds the demo dbt project, its git repo, and the DuckDB warehouse. On the first start, Pipeline Doctor creates the workspace and Airflow runs a green baseline.

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
