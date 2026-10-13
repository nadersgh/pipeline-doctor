from pathlib import Path

import httpx
import pytest
from pydantic import SecretStr

from pipeline_doctor.airflow.events import AirflowFailureCallback
from pipeline_doctor.airflow.logs import AirflowApiLogs, AirflowLogNotFoundError, LocalAirflowLogs, task_log_path
from pipeline_doctor.incidents.models import FailureSource


def test_local_airflow_logs_reads_the_attempt_file(tmp_path: Path) -> None:
    # Setup
    path = task_log_path(tmp_path, "dag", "run", "task", 2)
    path.parent.mkdir(parents=True)
    path.write_text("ERROR boom")

    # Act
    log = LocalAirflowLogs(tmp_path).get_task_log("dag", "run", "task", 2)

    # Assert
    assert log == "ERROR boom"


def test_local_airflow_logs_raises_when_missing(tmp_path: Path) -> None:
    with pytest.raises(AirflowLogNotFoundError):
        LocalAirflowLogs(tmp_path).get_task_log("dag", "run", "task", 1)


def test_airflow_api_logs_calls_the_stable_rest_endpoint() -> None:
    # Setup
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.raw_path.decode())
        return httpx.Response(200, text="dbt build failed")

    client = httpx.Client(base_url="http://airflow", transport=httpx.MockTransport(handler))

    # Act
    log = AirflowApiLogs("http://airflow", "u", SecretStr("p"), client=client).get_task_log(
        "shop", "scheduled__2026-10-13T06:00:00+00:00", "dbt_build", 1
    )

    # Assert
    assert log == "dbt build failed"
    assert seen == [
        "/api/v1/dags/shop/dagRuns/scheduled__2026-10-13T06%3A00%3A00%2B00%3A00/taskInstances/dbt_build/logs/1?full_content=true"
    ]


def test_airflow_callback_becomes_failure_event() -> None:
    # Act
    event = AirflowFailureCallback(dag_id="shop", task_id="dbt_build", run_id="r1", exception="boom").to_event()

    # Assert
    assert event.source is FailureSource.AIRFLOW
    assert event.describe() == "Airflow task shop.dbt_build (run r1)"
