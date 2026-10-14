from pathlib import Path

from pipeline_doctor.agent.graph import PipelineDoctor
from pipeline_doctor.agent.reasoning.base import FallbackReasoner, Reasoner
from pipeline_doctor.agent.reasoning.heuristic import HeuristicReasoner
from pipeline_doctor.agent.reasoning.llm import LLMReasoner
from pipeline_doctor.agent.reasoning.llm_clients import AnthropicClient, LLMClient, OpenAIClient
from pipeline_doctor.agent.toolkit import DiagnosticToolkit
from pipeline_doctor.airflow.logs import AirflowApiLogs, LocalAirflowLogs, TaskLogSource
from pipeline_doctor.jev.llm_judge import LLMJudge
from pipeline_doctor.jev.reviewer import JevReviewer, LayeredJev, RuleBasedJev
from pipeline_doctor.memory.store import IncidentMemory
from pipeline_doctor.observability.tracing import configure_tracing
from pipeline_doctor.observability.usage import UsageMeter
from pipeline_doctor.settings import ReasonerKind, Settings, WarehouseKind
from pipeline_doctor.vcs.history import GitHistory
from pipeline_doctor.vcs.repository import GitRepository
from pipeline_doctor.warehouse.base import Warehouse
from pipeline_doctor.warehouse.duckdb_warehouse import DuckDBWarehouse
from pipeline_doctor.warehouse.guard import GuardedWarehouse, QueryBudget, SqlGuard
from pipeline_doctor.warehouse.snowflake_warehouse import SnowflakeConnectionSettings, SnowflakeWarehouse

_DEFAULT_MODELS = {ReasonerKind.ANTHROPIC: "claude-sonnet-5-5", ReasonerKind.OPENAI: "gpt-4.1"}


def build_warehouse(settings: Settings) -> Warehouse:
    if settings.warehouse is WarehouseKind.SNOWFLAKE:
        return SnowflakeWarehouse(
            SnowflakeConnectionSettings(
                account=settings.snowflake_account,
                user=settings.snowflake_user,
                password=settings.snowflake_password,
                role=settings.snowflake_role,
                warehouse=settings.snowflake_warehouse,
                database=settings.snowflake_database,
                statement_timeout_seconds=settings.query_timeout_seconds,
            )
        )
    return DuckDBWarehouse(settings.duckdb_path)


def build_memory(settings: Settings) -> IncidentMemory:
    return IncidentMemory(settings.state_dir / "memory.jsonl")


def build_toolkit(settings: Settings, memory: IncidentMemory) -> DiagnosticToolkit:
    warehouse = build_warehouse(settings)
    guarded = GuardedWarehouse(
        warehouse,
        SqlGuard(
            warehouse.dialect,
            allowed_databases=[warehouse.database],
            allowed_schemas=settings.allowed_schemas,
            max_rows=settings.max_result_rows,
        ),
        QueryBudget(
            max_queries=settings.max_queries_per_incident,
            max_bytes_scanned=settings.max_bytes_scanned,
            timeout_seconds=settings.query_timeout_seconds,
        ),
        settings.max_result_rows,
    )
    history = GitHistory(GitRepository(settings.dbt_project_dir), settings.git_since_ref, settings.git_max_commits)
    return DiagnosticToolkit(
        project_dir=settings.dbt_project_dir,
        warehouse=warehouse,
        guarded_warehouse=guarded,
        history=history,
        logs=_build_log_source(settings),
        memory=memory,
        allowed_schemas=settings.allowed_schemas,
        target_dir=settings.target_dir,
    )


def build_reasoner(settings: Settings, meter: UsageMeter, dialect: str) -> Reasoner:
    heuristic = HeuristicReasoner(dialect)
    if settings.reasoner is ReasonerKind.HEURISTIC:
        return heuristic
    model = settings.llm_model or _DEFAULT_MODELS[settings.reasoner]
    return FallbackReasoner(LLMReasoner(_build_llm_client(settings, meter), model, dialect), heuristic)


def build_jev(settings: Settings, meter: UsageMeter, project_dir: Path) -> JevReviewer | None:
    if not settings.jev_enabled:
        return None
    rules = RuleBasedJev(project_dir, settings.jev_min_confidence)
    if not settings.jev_llm_judge or settings.reasoner is ReasonerKind.HEURISTIC:
        return rules
    model = settings.llm_model or _DEFAULT_MODELS[settings.reasoner]
    return LayeredJev(rules, LLMJudge(_build_llm_client(settings, meter), model))


def build_agent(settings: Settings, memory: IncidentMemory | None = None) -> PipelineDoctor:
    """Builds a fresh agent per incident so query budgets and usage meters never leak between incidents."""
    configure_tracing(settings.state_dir / "traces" / "spans.jsonl")
    meter = UsageMeter(settings.llm_input_cost_per_mtok, settings.llm_output_cost_per_mtok)
    toolkit = build_toolkit(settings, memory or build_memory(settings))
    reasoner = build_reasoner(settings, meter, toolkit.guarded_warehouse.dialect)
    return PipelineDoctor(
        toolkit,
        reasoner,
        meter,
        build_jev(settings, meter, toolkit.project_dir),
        settings.max_investigation_rounds,
        settings.jev_max_fix_revisions,
    )


def _build_llm_client(settings: Settings, meter: UsageMeter) -> LLMClient:
    model = settings.llm_model or _DEFAULT_MODELS[settings.reasoner]
    if settings.reasoner is ReasonerKind.ANTHROPIC:
        return AnthropicClient(model, meter)
    return OpenAIClient(model, meter)


def _build_log_source(settings: Settings) -> TaskLogSource:
    if settings.airflow_base_url:
        return AirflowApiLogs(settings.airflow_base_url, settings.airflow_username, settings.airflow_password)
    return LocalAirflowLogs(Path(settings.airflow_log_dir))
