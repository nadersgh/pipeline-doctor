from pathlib import Path

import pytest
from polyfactory.factories.pydantic_factory import ModelFactory

from pipeline_doctor.demo.workspace import DemoWorkspace, clone_workspace, create_workspace
from pipeline_doctor.incidents.models import (
    Culprit,
    Diagnosis,
    FailureEvent,
    FailureSource,
    FileEdit,
    FixKind,
    FixProposal,
    Incident,
    IncidentStatus,
    RootCauseCategory,
)
from pipeline_doctor.publishing.pull_requests import PullRequest
from pipeline_doctor.settings import Settings


class RecordingPublisher:
    def __init__(self) -> None:
        self.opened: list[dict[str, str]] = []

    def open_draft(self, incident: Incident, files: dict[str, str]) -> PullRequest:
        self.opened.append(files)
        return PullRequest(url="https://example.test/pr/1", branch="b", title="t")


@pytest.fixture
def recording_publisher() -> RecordingPublisher:
    return RecordingPublisher()


class DiagnosisFactory(ModelFactory[Diagnosis]):
    __model__ = Diagnosis
    confidence = 0.9


class IncidentFactory(ModelFactory[Incident]):
    __model__ = Incident


@pytest.fixture(scope="session")
def demo_template(tmp_path_factory: pytest.TempPathFactory) -> DemoWorkspace:
    return create_workspace(tmp_path_factory.mktemp("demo") / "template")


@pytest.fixture
def demo_workspace(demo_template: DemoWorkspace, tmp_path: Path) -> DemoWorkspace:
    return clone_workspace(demo_template, tmp_path / "workspace")


@pytest.fixture
def demo_settings(demo_workspace: DemoWorkspace) -> Settings:
    return Settings(
        dbt_project_dir=demo_workspace.project_dir,
        dbt_target_dir=demo_workspace.target_dir,
        duckdb_path=demo_workspace.duckdb_path,
        airflow_log_dir=demo_workspace.airflow_log_dir,
        state_dir=demo_workspace.root / ".pipeline_doctor",
        _env_file=None,
    )


@pytest.fixture
def pending_incident() -> Incident:
    edit = FileEdit(
        path="models/staging/stg_orders.sql",
        original="select * from {{ source('raw', 'orders') }}",
        updated="select * rename (order_status as status) from {{ source('raw', 'orders') }}",
    )
    return Incident(
        event=FailureEvent(source=FailureSource.AIRFLOW, dag_id="dag", task_id="dbt_build", run_id="run-1"),
        status=IncidentStatus.AWAITING_APPROVAL,
        diagnosis=DiagnosisFactory.build(
            category=RootCauseCategory.SOURCE_COLUMN_RENAMED, culprit=Culprit(relation="raw.orders", column="status")
        ),
        fix=FixProposal(
            kind=FixKind.SCHEMA_PATCH, title="Map renamed column", rationale="Upstream rename.", edits=[edit]
        ),
        signature="model stg_orders Binder Error: Referenced column status not found",
    )
