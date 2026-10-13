from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field

from pipeline_doctor.jev.models import JevVerdict
from pipeline_doctor.security.untrusted import InjectionFinding


class FailureSource(StrEnum):
    AIRFLOW = "airflow"
    DBT = "dbt"


class RootCauseCategory(StrEnum):
    SOURCE_COLUMN_RENAMED = "source_column_renamed"
    SOURCE_TYPE_CHANGED = "source_type_changed"
    SOURCE_NULL_SPIKE = "source_null_spike"
    SOURCE_DUPLICATE_ROWS = "source_duplicate_rows"
    SOURCE_STALE = "source_stale"
    SOURCE_NEW_CATEGORY = "source_new_category"
    SOURCE_MISSING_ROWS = "source_missing_rows"
    CODE_SQL_ERROR = "code_sql_error"
    CODE_JOIN_FANOUT = "code_join_fanout"
    CODE_LOGIC_REGRESSION = "code_logic_regression"
    UNKNOWN = "unknown"


ROOT_CAUSE_DESCRIPTIONS: dict[RootCauseCategory, str] = {
    RootCauseCategory.SOURCE_COLUMN_RENAMED: "An upstream source table renamed or dropped a column that a model selects.",
    RootCauseCategory.SOURCE_TYPE_CHANGED: "An upstream source column changed data type or format, breaking casts.",
    RootCauseCategory.SOURCE_NULL_SPIKE: "Recent source loads contain an abnormal share of nulls in a column.",
    RootCauseCategory.SOURCE_DUPLICATE_ROWS: "A source table received duplicate rows (e.g. a replayed batch).",
    RootCauseCategory.SOURCE_STALE: "A source table stopped receiving new data (ingestion halted).",
    RootCauseCategory.SOURCE_NEW_CATEGORY: "A source column started emitting a value outside the accepted set.",
    RootCauseCategory.SOURCE_MISSING_ROWS: "Parent rows are missing from a source, leaving orphaned foreign keys.",
    RootCauseCategory.CODE_SQL_ERROR: "A recent code change introduced a SQL/compilation error in a model.",
    RootCauseCategory.CODE_JOIN_FANOUT: "A recent code change introduced a join that duplicates rows (fan-out).",
    RootCauseCategory.CODE_LOGIC_REGRESSION: "A recent code change altered business logic and produced wrong values.",
    RootCauseCategory.UNKNOWN: "The evidence is insufficient to name a root cause.",
}


class FixKind(StrEnum):
    MODEL_CHANGE = "model_change"
    TEST_CHANGE = "test_change"
    SCHEMA_PATCH = "schema_patch"
    OPERATIONAL = "operational"


class IncidentStatus(StrEnum):
    DIAGNOSING = "diagnosing"
    AWAITING_APPROVAL = "awaiting_approval"
    PR_OPENED = "pr_opened"
    REJECTED = "rejected"
    RESOLVED_OPERATIONALLY = "resolved_operationally"
    FAILED = "failed"


def _utc_now() -> datetime:
    return datetime.now(UTC)


class FailureEvent(BaseModel):
    source: FailureSource
    dag_id: str | None = None
    task_id: str | None = None
    run_id: str | None = None
    try_number: int = 1
    dbt_node_id: str | None = None
    log_excerpt: str = ""
    git_sha: str | None = Field(default=None, description="Commit the failed run deployed, when known.")
    run_started_at: datetime | None = None
    occurred_at: datetime = Field(default_factory=_utc_now)

    def describe(self) -> str:
        if self.source is FailureSource.AIRFLOW:
            return f"Airflow task {self.dag_id}.{self.task_id} (run {self.run_id})"
        return f"dbt node {self.dbt_node_id or 'unknown'}"


class Culprit(BaseModel):
    relation: str = Field(description="Source as 'raw.orders' or model name as 'stg_orders'.")
    column: str | None = None
    commit_sha: str | None = None

    def describe(self) -> str:
        target = f"{self.relation}.{self.column}" if self.column else self.relation
        return f"{target} (commit {self.commit_sha[:8]})" if self.commit_sha else target


class Diagnosis(BaseModel):
    category: RootCauseCategory
    confidence: float = Field(ge=0.0, le=1.0)
    culprit: Culprit
    technical_summary: str
    plain_summary: str
    evidence: list[str] = Field(default_factory=list)
    impacted_assets: list[str] = Field(default_factory=list)


class FileEdit(BaseModel):
    path: str = Field(description="Path relative to the dbt project root.")
    original: str = Field(description="Exact snippet currently in the file; must occur exactly once.")
    updated: str = Field(description="Replacement for the snippet.")


class FixProposal(BaseModel):
    kind: FixKind
    title: str
    rationale: str
    edits: list[FileEdit] = Field(default_factory=list)
    operational_steps: list[str] = Field(default_factory=list)
    diff: str = ""


class RunMetrics(BaseModel):
    latency_seconds: float = 0.0
    llm_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    queries_run: int = 0
    queries_blocked: int = 0
    investigation_rounds: int = 0


class Incident(BaseModel):
    id: str = Field(default_factory=lambda: uuid4().hex[:12])
    event: FailureEvent
    status: IncidentStatus = IncidentStatus.DIAGNOSING
    diagnosis: Diagnosis | None = None
    fix: FixProposal | None = None
    verdict: JevVerdict | None = Field(default=None, description="JEV decision on the diagnosis and fix.")
    metrics: RunMetrics = Field(default_factory=RunMetrics)
    reasoner: str = ""
    signature: str = Field(default="", description="Failure fingerprint used to recall similar incidents.")
    injection_findings: list[InjectionFinding] = Field(
        default_factory=list, description="Injection-like text seen in logs, commits, or query results."
    )
    error: str | None = None
    reviewer_note: str | None = None
    approved_by: str | None = None
    pull_request_url: str | None = None
    created_at: datetime = Field(default_factory=_utc_now)
    updated_at: datetime = Field(default_factory=_utc_now)
