from datetime import UTC, datetime

from pydantic import BaseModel

from pipeline_doctor.incidents.models import FailureEvent, FailureSource


class AirflowFailureCallback(BaseModel):
    """Payload posted by the DAG's `on_failure_callback` to the Pipeline Doctor webhook."""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int = 1
    run_started_at: datetime | None = None
    git_sha: str | None = None
    exception: str = ""

    def to_event(self) -> FailureEvent:
        return FailureEvent(
            source=FailureSource.AIRFLOW,
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            try_number=self.try_number,
            run_started_at=self.run_started_at,
            git_sha=self.git_sha,
            log_excerpt=self.exception,
            occurred_at=datetime.now(UTC),
        )


class DbtFailureNotification(BaseModel):
    """Payload for a dbt Cloud job or CI step that reports a failed node directly."""

    node_id: str
    message: str = ""
    run_started_at: datetime | None = None

    def to_event(self) -> FailureEvent:
        return FailureEvent(
            source=FailureSource.DBT,
            dbt_node_id=self.node_id,
            log_excerpt=self.message,
            run_started_at=self.run_started_at,
        )
