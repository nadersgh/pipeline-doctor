from datetime import UTC, datetime

from pydantic import BaseModel, Field, field_validator

from pipeline_doctor.incidents.models import FailureEvent, FailureSource

_MAX_IDENTIFIER_CHARS = 250
_MAX_LOG_EXCERPT_CHARS = 20_000


def _truncate_log_excerpt(text: str) -> str:
    return text[:_MAX_LOG_EXCERPT_CHARS]


class AirflowFailureCallback(BaseModel):
    """Payload posted by the DAG's `on_failure_callback` to the Pipeline Doctor webhook."""

    dag_id: str = Field(max_length=_MAX_IDENTIFIER_CHARS)
    task_id: str = Field(max_length=_MAX_IDENTIFIER_CHARS)
    run_id: str = Field(max_length=_MAX_IDENTIFIER_CHARS)
    try_number: int = 1
    run_started_at: datetime | None = None
    git_sha: str | None = Field(default=None, max_length=64)
    exception: str = ""

    _truncate_exception = field_validator("exception")(_truncate_log_excerpt)

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

    node_id: str = Field(max_length=_MAX_IDENTIFIER_CHARS)
    message: str = ""
    run_started_at: datetime | None = None

    _truncate_message = field_validator("message")(_truncate_log_excerpt)

    def to_event(self) -> FailureEvent:
        return FailureEvent(
            source=FailureSource.DBT,
            dbt_node_id=self.node_id,
            log_excerpt=self.message,
            run_started_at=self.run_started_at,
        )
