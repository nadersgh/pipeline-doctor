import asyncio
from typing import Any

import pytest
from pydantic import BaseModel

from pipeline_doctor.agent.factory import build_agent, build_toolkit
from pipeline_doctor.agent.fixes import apply_edits, write_project_files
from pipeline_doctor.agent.graph import PipelineDoctor
from pipeline_doctor.agent.reasoning.llm import AssessmentDraft, FixDraft, HypothesisList, LLMReasoner
from pipeline_doctor.demo.workspace import DemoWorkspace, run_pipeline
from pipeline_doctor.evals.cases import list_eval_cases
from pipeline_doctor.evals.injection import inject_fault
from pipeline_doctor.incidents.models import FixKind, IncidentStatus, RootCauseCategory
from pipeline_doctor.jev.models import JevDecision
from pipeline_doctor.jev.reviewer import RuleBasedJev
from pipeline_doctor.mcp_server import create_mcp_server
from pipeline_doctor.memory.store import IncidentMemory
from pipeline_doctor.observability.usage import UsageMeter
from pipeline_doctor.settings import Settings
from pipeline_doctor.warehouse.base import WarehouseQueryError
from pipeline_doctor.warehouse.duckdb_warehouse import DuckDBWarehouse

pytestmark = pytest.mark.integration


def _break(workspace: DemoWorkspace, case_id: str) -> Any:
    case = next(case for case in list_eval_cases() if case.case_id == case_id)
    inject_fault(workspace, case)
    run = run_pipeline(workspace, run_id=case_id)
    assert run.failure is not None
    return case, run.failure


@pytest.mark.parametrize(
    "case_id",
    ["renamed_orders_status", "duplicates_orders", "code_fanout_fct_orders__comment", "code_bad_ref", "stale_payments"],
)
def test_agent_diagnoses_and_fixes_injected_fault(
    demo_workspace: DemoWorkspace, demo_settings: Settings, case_id: str
) -> None:
    # Setup
    case, failure = _break(demo_workspace, case_id)
    agent = build_agent(demo_settings, IncidentMemory(demo_workspace.root / "memory.jsonl"))

    # Act
    incident = agent.diagnose(failure)

    # Assert
    assert incident.status is IncidentStatus.AWAITING_APPROVAL
    assert incident.diagnosis is not None
    assert incident.diagnosis.category is case.category
    assert incident.verdict is not None
    assert incident.verdict.decision is JevDecision.PROCEED, incident.verdict.feedback()
    assert incident.fix is not None
    if incident.fix.kind is not FixKind.OPERATIONAL:
        write_project_files(demo_workspace.project_dir, apply_edits(demo_workspace.project_dir, incident.fix.edits))
        assert run_pipeline(demo_workspace, run_id=f"{case_id}_fixed").succeeded


def test_duckdb_warehouse_is_read_only_even_without_the_guard(demo_workspace: DemoWorkspace) -> None:
    with pytest.raises(WarehouseQueryError):
        DuckDBWarehouse(demo_workspace.duckdb_path).execute("delete from raw.orders", max_rows=10, timeout_seconds=5)


class ScriptedLLMClient:
    """Plays the role of a model that tries an unsafe query before concluding."""

    def __init__(self, *, confident: bool = True) -> None:
        self._responses: dict[str, dict[str, Any]] = {
            HypothesisList.__name__: {
                "hypotheses": [
                    {
                        "category": "source_null_spike",
                        "culprit": {"relation": "raw.orders", "column": "customer_id"},
                        "rationale": "not_null failed on stg_orders.customer_id",
                        "checks": [
                            {"kind": "null_rate_by_load_day", "relation": "raw.orders", "column": "customer_id"},
                            {"kind": "custom_sql", "sql": "delete from raw.orders where customer_id is null"},
                        ],
                    }
                ]
            },
            AssessmentDraft.__name__: {
                "ready": confident,
                "diagnosis": {
                    "category": "source_null_spike",
                    "confidence": 0.9,
                    "culprit": {"relation": "raw.orders", "column": "customer_id"},
                    "technical_summary": "Recent loads have null customer_id.",
                    "plain_summary": "New orders arrived without a customer, so revenue by customer is incomplete.",
                    "evidence": ["null share of raw.orders.customer_id jumped in the last two loads"],
                },
            },
            FixDraft.__name__: {
                "kind": "operational",
                "title": "Ask the storefront team to re-deliver orders",
                "rationale": "Upstream data issue.",
                "operational_steps": ["Page the storefront on-call."],
            },
        }

    def generate[T: BaseModel](self, system: str, prompt: str, schema: type[T]) -> T:
        return schema.model_validate(self._responses[schema.__name__])


def test_llm_reasoner_runs_through_the_graph_and_unsafe_sql_is_blocked(
    demo_workspace: DemoWorkspace, demo_settings: Settings
) -> None:
    # Setup
    _, failure = _break(demo_workspace, "nulls_orders_customer_id")
    toolkit = build_toolkit(demo_settings, IncidentMemory(demo_workspace.root / "memory.jsonl"))
    reasoner = LLMReasoner(ScriptedLLMClient(), "scripted", "duckdb")
    agent = PipelineDoctor(toolkit, reasoner, UsageMeter(3.0, 15.0), RuleBasedJev(toolkit.project_dir))

    # Act
    incident = agent.diagnose(failure)

    # Assert
    assert incident.diagnosis is not None
    assert incident.diagnosis.category is RootCauseCategory.SOURCE_NULL_SPIKE
    assert incident.metrics.queries_blocked == 1
    assert incident.metrics.queries_run == 1
    assert incident.verdict is not None
    assert incident.verdict.decision is JevDecision.PROCEED


def test_mcp_server_exposes_read_only_lineage_tool(demo_settings: Settings) -> None:
    # Setup
    server = create_mcp_server(demo_settings)

    # Act
    tools = {tool.name for tool in asyncio.run(server.list_tools())}
    result = asyncio.run(server.call_tool("get_lineage", {"node": "fct_orders"}))

    # Assert
    assert {"run_readonly_sql", "get_schema_drift", "diagnose_airflow_failure"} <= tools
    assert "raw.orders" in result.structured_content["upstream"]


def test_unconfident_diagnosis_never_yields_a_code_change(
    demo_workspace: DemoWorkspace, demo_settings: Settings
) -> None:
    # Setup
    _, failure = _break(demo_workspace, "nulls_orders_customer_id")
    toolkit = build_toolkit(demo_settings, IncidentMemory(demo_workspace.root / "memory.jsonl"))
    reasoner = LLMReasoner(ScriptedLLMClient(confident=False), "scripted", "duckdb")
    agent = PipelineDoctor(toolkit, reasoner, UsageMeter(3.0, 15.0), RuleBasedJev(toolkit.project_dir), max_rounds=2)

    # Act
    incident = agent.diagnose(failure)

    # Assert
    assert incident.metrics.investigation_rounds == 2
    assert incident.fix is not None
    assert incident.fix.kind is FixKind.OPERATIONAL
    assert incident.fix.title.startswith("Low-confidence")
    assert incident.verdict is not None
    assert incident.verdict.decision is JevDecision.ESCALATE
