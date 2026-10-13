from enum import StrEnum
from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class WarehouseKind(StrEnum):
    DUCKDB = "duckdb"
    SNOWFLAKE = "snowflake"


class ReasonerKind(StrEnum):
    HEURISTIC = "heuristic"
    ANTHROPIC = "anthropic"
    OPENAI = "openai"


class PublisherKind(StrEnum):
    LOCAL_GIT = "local_git"
    GITHUB = "github"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PIPELINE_DOCTOR_", env_file=".env", extra="ignore")

    dbt_project_dir: Path = Path("demo_workspace/project")
    dbt_target_dir: Path | None = None
    state_dir: Path = Path(".pipeline_doctor")
    git_since_ref: str = "pipeline-doctor/last-green"
    git_max_commits: int = 10

    warehouse: WarehouseKind = WarehouseKind.DUCKDB
    duckdb_path: Path = Path("demo_workspace/warehouse.duckdb")
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: SecretStr = SecretStr("")
    snowflake_role: str = "PIPELINE_DOCTOR_READONLY"
    snowflake_warehouse: str = ""
    snowflake_database: str = ""

    allowed_schemas: list[str] = Field(default_factory=lambda: ["raw", "analytics", "main"])
    max_queries_per_incident: int = 25
    query_timeout_seconds: int = 30
    max_result_rows: int = 200
    max_bytes_scanned: int = 10 * 1024**3

    airflow_base_url: str = ""
    airflow_username: str = ""
    airflow_password: SecretStr = SecretStr("")
    airflow_log_dir: Path = Path("demo_workspace/airflow_logs")

    reasoner: ReasonerKind = ReasonerKind.HEURISTIC
    llm_model: str = ""
    llm_input_cost_per_mtok: float = 3.0
    llm_output_cost_per_mtok: float = 15.0
    max_investigation_rounds: int = 2

    jev_enabled: bool = Field(default=True, description="Gate fix proposals with the JEV decision layer.")
    jev_min_confidence: float = Field(default=0.6, ge=0.0, le=1.0)
    jev_llm_judge: bool = Field(default=False, description="Add an LLM-as-judge veto behind the JEV rule gate.")
    jev_max_fix_revisions: int = 1

    publisher: PublisherKind = PublisherKind.LOCAL_GIT
    github_token: SecretStr = SecretStr("")
    github_repository: str = ""
    github_base_branch: str = "main"
    github_project_subdir: str = ""

    api_token: SecretStr = SecretStr("")
    allow_unauthenticated: bool = Field(
        default=False, description="Serve webhooks and incident reads without a bearer token (local development)."
    )
    reviewer_tokens: dict[str, SecretStr] = Field(
        default_factory=dict, description="Reviewer name -> personal API token; approvals are refused when empty."
    )
    slack_approvers: list[str] = Field(default_factory=list)
    slack_webhook_url: SecretStr = SecretStr("")
    slack_signing_secret: SecretStr = SecretStr("")
    public_base_url: str = "http://localhost:8000"

    @model_validator(mode="after")
    def _require_github_credentials(self) -> "Settings":
        if self.publisher is PublisherKind.GITHUB and not (
            self.github_repository and self.github_token.get_secret_value()
        ):
            raise ValueError("publisher=github requires github_repository and github_token")
        return self

    @property
    def target_dir(self) -> Path:
        return self.dbt_target_dir or self.dbt_project_dir / "target"
