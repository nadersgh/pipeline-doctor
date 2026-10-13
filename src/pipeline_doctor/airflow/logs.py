from pathlib import Path
from typing import Protocol
from urllib.parse import quote

import httpx
from loguru import logger
from pydantic import SecretStr


class AirflowLogNotFoundError(LookupError):
    pass


class TaskLogSource(Protocol):
    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> str: ...


class LocalAirflowLogs:
    """Reads task logs from an Airflow log folder (`dag_id=*/run_id=*/task_id=*/attempt=*.log` layout)."""

    def __init__(self, log_dir: Path) -> None:
        self._log_dir = log_dir

    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
        path = task_log_path(self._log_dir, dag_id, run_id, task_id, try_number)
        if not path.exists():
            raise AirflowLogNotFoundError(str(path))
        logger.debug(f"read airflow log {path}")
        return path.read_text()


class AirflowApiLogs:
    """Fetches task logs through the Airflow stable REST API."""

    def __init__(self, base_url: str, username: str, password: SecretStr, client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client(
            base_url=base_url.rstrip("/"), auth=(username, password.get_secret_value()), timeout=30.0
        )

    def get_task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
        path = (
            f"/api/v1/dags/{quote(dag_id, safe='')}/dagRuns/{quote(run_id, safe='')}"
            f"/taskInstances/{quote(task_id, safe='')}/logs/{try_number}"
        )
        logger.info(f"fetch airflow log {dag_id}.{task_id}")
        response = self._client.get(path, params={"full_content": "true"}, headers={"Accept": "text/plain"})
        if response.status_code == httpx.codes.NOT_FOUND:
            raise AirflowLogNotFoundError(path)
        response.raise_for_status()
        return response.text


def task_log_path(log_dir: Path, dag_id: str, run_id: str, task_id: str, try_number: int) -> Path:
    return log_dir / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}" / f"attempt={try_number}.log"
