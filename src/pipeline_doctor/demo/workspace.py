import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from loguru import logger

from pipeline_doctor.airflow.logs import task_log_path
from pipeline_doctor.dbt_tools.runner import DbtCommand, DbtRunner
from pipeline_doctor.demo.data import generate_raw_data
from pipeline_doctor.incidents.models import FailureEvent, FailureSource
from pipeline_doctor.vcs.repository import GitRepository

DEMO_DAG_ID = "shop_analytics_daily"
LAST_GREEN_TAG = "pipeline-doctor/last-green"
TEMPLATE_DIR = Path(__file__).resolve().parents[3] / "demo_project"
_PIPELINE_TASKS: list[tuple[str, DbtCommand]] = [
    ("dbt_source_freshness", "source freshness"),
    ("dbt_build", "build"),
]


class DemoWorkspaceSetupError(RuntimeError):
    pass


@dataclass(frozen=True)
class DemoWorkspace:
    root: Path

    @property
    def project_dir(self) -> Path:
        return self.root / "project"

    @property
    def duckdb_path(self) -> Path:
        return self.root / "warehouse.duckdb"

    @property
    def airflow_log_dir(self) -> Path:
        return self.root / "airflow_logs"

    @property
    def target_dir(self) -> Path:
        return self.project_dir / "target"

    @property
    def git(self) -> GitRepository:
        return GitRepository(self.project_dir)


@dataclass(frozen=True)
class PipelineRun:
    run_id: str
    succeeded: bool
    failure: FailureEvent | None


def create_workspace(root: Path, template_dir: Path = TEMPLATE_DIR) -> DemoWorkspace:
    """Builds a demo workspace: dbt project under git, raw data in DuckDB, and a green baseline run."""
    workspace = DemoWorkspace(root)
    if root.exists():
        shutil.rmtree(root)
    logger.info(f"create demo workspace {root}")
    shutil.copytree(template_dir, workspace.project_dir, ignore=shutil.ignore_patterns("target", "logs", "*.duckdb"))
    generate_raw_data(workspace.duckdb_path)
    workspace.git.init()
    workspace.git.commit_all("feat: initial shop analytics dbt project")
    baseline = run_pipeline(workspace, "baseline")
    if not baseline.succeeded:
        raise DemoWorkspaceSetupError(f"baseline pipeline failed: {baseline.failure}")
    workspace.git.tag(LAST_GREEN_TAG)
    return workspace


def clone_workspace(source: DemoWorkspace, destination: Path) -> DemoWorkspace:
    if destination.exists():
        shutil.rmtree(destination)
    logger.debug(f"copy demo workspace {destination}")
    shutil.copytree(source.root, destination)
    return DemoWorkspace(destination)


def run_pipeline(workspace: DemoWorkspace, run_id: str) -> PipelineRun:
    """Runs the DAG's tasks in order like Airflow would, writing task logs and stopping at the first failure."""
    runner = DbtRunner(workspace.project_dir, workspace.duckdb_path)
    run_started_at = datetime.now(UTC)
    deployed_sha = workspace.git.resolve("HEAD")
    for task_id, command in _PIPELINE_TASKS:
        invocation = runner.run(command)
        log_path = task_log_path(workspace.airflow_log_dir, DEMO_DAG_ID, run_id, task_id, 1)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(invocation.output)
        if not invocation.succeeded:
            event = FailureEvent(
                source=FailureSource.AIRFLOW,
                dag_id=DEMO_DAG_ID,
                task_id=task_id,
                run_id=run_id,
                run_started_at=run_started_at,
                git_sha=deployed_sha,
                log_excerpt=f"Bash command failed: dbt {command} exited with a non-zero status",
            )
            return PipelineRun(run_id=run_id, succeeded=False, failure=event)
    return PipelineRun(run_id=run_id, succeeded=True, failure=None)
