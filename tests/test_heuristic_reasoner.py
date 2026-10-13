import pytest

from pipeline_doctor.agent.context import FailedNode, IncidentContext
from pipeline_doctor.agent.reasoning.base import Investigation
from pipeline_doctor.agent.reasoning.heuristic import HeuristicReasoner
from pipeline_doctor.dbt_tools.artifacts import ResourceType
from pipeline_doctor.diagnostics.checks import CheckKind, CheckRequest, CheckResult
from pipeline_doctor.diagnostics.schema_drift import SchemaDrift
from pipeline_doctor.incidents.models import FailureEvent, FailureSource, FixKind, RootCauseCategory
from pipeline_doctor.vcs.history import CommitChange

_STG_ORDERS = "with source as (\n    select * from {{ source('raw', 'orders') }}\n),\nselect * from source\n"


@pytest.fixture
def reasoner() -> HeuristicReasoner:
    return HeuristicReasoner()


def _context(primary: FailedNode, **overrides: object) -> IncidentContext:
    return IncidentContext(
        event=FailureEvent(source=FailureSource.AIRFLOW, dag_id="d", task_id="dbt_build", run_id="r"),
        failures=[primary],
        primary=primary,
        upstream_sources=["raw.orders"],
        downstream_assets=["fct_orders", "Revenue dashboard"],
        source_consumer_files={"raw.orders": ["models/staging/stg_orders.sql"]},
        project_files={"models/staging/stg_orders.sql": _STG_ORDERS},
        **overrides,
    )


@pytest.fixture
def renamed_column_context() -> IncidentContext:
    primary = FailedNode(
        unique_id="model.shop.stg_orders",
        name="stg_orders",
        resource_type=ResourceType.MODEL,
        status="error",
        message='Binder Error: Referenced column "status" not found in FROM clause!',
    )
    drift = SchemaDrift(source="raw.orders", missing_columns=["status"], added_columns=["order_status"])
    return _context(primary, schema_drift=[drift])


def test_heuristic_attributes_missing_column_to_upstream_rename(
    reasoner: HeuristicReasoner, renamed_column_context: IncidentContext
) -> None:
    # Setup
    investigation = Investigation(context=renamed_column_context)
    investigation.hypotheses = reasoner.hypothesize(investigation)

    # Act
    assessment = reasoner.assess(investigation)

    # Assert
    assert assessment.ready
    assert assessment.diagnosis is not None
    assert assessment.diagnosis.category is RootCauseCategory.SOURCE_COLUMN_RENAMED
    assert assessment.diagnosis.culprit.column == "status"
    assert "Revenue dashboard" in assessment.diagnosis.plain_summary


def test_heuristic_fix_for_renamed_column_maps_new_name_back(
    reasoner: HeuristicReasoner, renamed_column_context: IncidentContext
) -> None:
    # Setup
    investigation = Investigation(context=renamed_column_context)
    investigation.hypotheses = reasoner.hypothesize(investigation)
    investigation.diagnosis = reasoner.assess(investigation).diagnosis

    # Act
    fix = reasoner.propose_fix(investigation, feedback="")

    # Assert
    assert fix.kind is FixKind.SCHEMA_PATCH
    assert fix.edits[0].updated == "select * rename (order_status as status) from {{ source('raw', 'orders') }}"


def test_heuristic_prefers_code_regression_when_column_was_never_in_the_source(reasoner: HeuristicReasoner) -> None:
    # Setup
    primary = FailedNode(
        unique_id="model.shop.stg_orders",
        name="stg_orders",
        resource_type=ResourceType.MODEL,
        status="error",
        message='Binder Error: Referenced column "stauts" not found in FROM clause!',
        file_path="models/staging/stg_orders.sql",
    )
    commit = CommitChange(
        sha="abc1234567",
        author="dev",
        authored_at="2026-10-01T00:00:00Z",
        message="tidy",
        files=["models/staging/stg_orders.sql"],
        lineage_files=["models/staging/stg_orders.sql"],
        changes_lineage_sql=True,
        diff="diff --git a/models/staging/stg_orders.sql b/models/staging/stg_orders.sql\n-        status,\n+        stauts,\n",
    )
    investigation = Investigation(context=_context(primary, commits=[commit]))
    investigation.hypotheses = reasoner.hypothesize(investigation)

    # Act
    diagnosis = reasoner.assess(investigation).diagnosis

    # Assert
    assert diagnosis is not None
    assert diagnosis.category is RootCauseCategory.CODE_SQL_ERROR
    assert diagnosis.culprit.commit_sha == "abc1234567"


def test_heuristic_escalates_stale_sources_operationally(reasoner: HeuristicReasoner) -> None:
    # Setup
    primary = FailedNode(
        unique_id="source.shop.raw.orders",
        name="raw.orders",
        resource_type=ResourceType.SOURCE,
        status="error",
        message="source freshness error: last loaded 73.0 hours ago",
    )
    investigation = Investigation(context=_context(primary))
    investigation.hypotheses = reasoner.hypothesize(investigation)
    investigation.evidence = [
        CheckResult(
            request=CheckRequest(kind=CheckKind.FRESHNESS, relation="raw.orders"),
            metrics={"hours_since_last_load": 73.0},
            summary="Latest row loaded 73 hours ago.",
        )
    ]
    investigation.diagnosis = reasoner.assess(investigation).diagnosis

    # Act
    fix = reasoner.propose_fix(investigation, feedback="")

    # Assert
    assert investigation.diagnosis is not None
    assert investigation.diagnosis.category is RootCauseCategory.SOURCE_STALE
    assert fix.kind is FixKind.OPERATIONAL
    assert fix.edits == []
