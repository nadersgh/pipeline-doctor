"""Daily dbt pipeline for the shop analytics demo, wired to Pipeline Doctor on failure.

Environment expected on the Airflow workers:
- DBT_PROJECT_DIR / DBT_PROFILES_DIR: the dbt project checkout and profiles
- PIPELINE_DOCTOR_URL: base URL of the Pipeline Doctor API (e.g. http://pipeline-doctor:8000)
- PIPELINE_DOCTOR_API_TOKEN: bearer token matching the API's PIPELINE_DOCTOR_API_TOKEN
- SHOP_ANALYTICS_RETRIES: task retries before the failure callback fires (default 1)
"""

import json
import os
import urllib.request
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)


def dbt(command: str) -> str:
    # dbt only accepts --project-dir/--profiles-dir after the subcommand.
    return f"dbt --no-use-colors {command} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"


def notify_pipeline_doctor(context: dict[str, Any]) -> None:
    """on_failure_callback: hand the failed task to Pipeline Doctor. Never raises, so alerting cannot mask the failure."""
    base_url = os.environ.get("PIPELINE_DOCTOR_URL")
    if not base_url:
        return
    task_instance = context["task_instance"]
    dag_run = context.get("dag_run")
    payload = {
        "dag_id": task_instance.dag_id,
        "task_id": task_instance.task_id,
        "run_id": task_instance.run_id,
        "try_number": task_instance.try_number,
        "run_started_at": dag_run.start_date.isoformat() if dag_run and dag_run.start_date else None,
        "git_sha": os.environ.get("DBT_GIT_SHA"),
        "exception": str(context.get("exception", ""))[:2000],
    }
    request = urllib.request.Request(  # noqa: S310 - URL comes from deployment config
        f"{base_url.rstrip('/')}/webhooks/airflow",
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {os.environ.get('PIPELINE_DOCTOR_API_TOKEN', '')}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10):  # noqa: S310
            pass
    except OSError as error:
        print(f"Pipeline Doctor notification failed: {error}")


with DAG(
    dag_id="shop_analytics_daily",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    default_args={
        "retries": int(os.environ.get("SHOP_ANALYTICS_RETRIES", "1")),
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": notify_pipeline_doctor,
    },
    tags=["dbt", "pipeline-doctor"],
) as dag:
    source_freshness = BashOperator(task_id="dbt_source_freshness", bash_command=dbt("source freshness"))
    build = BashOperator(task_id="dbt_build", bash_command=dbt("build"))
    mark_last_green = BashOperator(
        task_id="mark_last_green",
        bash_command=f"git -C {DBT_PROJECT_DIR} tag --force pipeline-doctor/last-green HEAD",
    )

    source_freshness >> build >> mark_last_green
