from datetime import UTC, datetime

import duckdb
from loguru import logger

from pipeline_doctor.airflow.logs import task_log_path
from pipeline_doctor.demo.workspace import DEMO_DAG_ID, DemoWorkspace, PipelineRun, run_pipeline
from pipeline_doctor.evals.cases import EvalCase, InfraFailureKind
from pipeline_doctor.incidents.models import FailureEvent, FailureSource

_KILLED_TASK = "dbt_build"
_KILLED_TRAILER = [
    "[airflow.task.task_runner] WARNING - Received SIGTERM. Terminating subprocesses.",
    "[airflow.task.task_runner] INFO - Sending Signals.SIGTERM to group 4182. PIDs of all processes in the group: [4182]",
    "[airflow.jobs.local_task_job_runner] INFO - Task exited with return code Negsignal.SIGKILL",
    "[airflow.jobs.local_task_job_runner] ERROR - Job 3317 was killed before it finished (likely due to running out "
    "of memory)",
]


def run_case_pipeline(workspace: DemoWorkspace, case: EvalCase, run_id: str) -> PipelineRun:
    """Runs the DAG for a case, reproducing its infrastructure failure when it declares one."""
    match case.infra_failure:
        case None:
            return run_pipeline(workspace, run_id)
        case InfraFailureKind.WAREHOUSE_LOCK:
            return _run_with_warehouse_lock(workspace, run_id)
        case InfraFailureKind.WORKER_KILLED:
            return _run_with_killed_worker(workspace, run_id)


def _run_with_warehouse_lock(workspace: DemoWorkspace, run_id: str) -> PipelineRun:
    # A read-write connection holds DuckDB's file lock, so the dbt subprocess cannot open the warehouse.
    logger.info(f"hold write lock {workspace.duckdb_path}")
    with duckdb.connect(str(workspace.duckdb_path)):
        return run_pipeline(workspace, run_id)


def _run_with_killed_worker(workspace: DemoWorkspace, run_id: str) -> PipelineRun:
    run_started_at = datetime.now(UTC)
    run = run_pipeline(workspace, run_id)
    if not run.succeeded:
        return run
    log_path = task_log_path(workspace.airflow_log_dir, DEMO_DAG_ID, run_id, _KILLED_TASK, 1)
    lines = log_path.read_text().splitlines()
    logger.info(f"truncate task log {log_path}")
    log_path.write_text("\n".join([*lines[: len(lines) // 2], *_KILLED_TRAILER]) + "\n")
    event = FailureEvent(
        source=FailureSource.AIRFLOW,
        dag_id=DEMO_DAG_ID,
        task_id=_KILLED_TASK,
        run_id=run_id,
        run_started_at=run_started_at,
        git_sha=workspace.git.resolve("HEAD"),
        log_excerpt="Task exited with return code Negsignal.SIGKILL",
    )
    return PipelineRun(run_id=run_id, succeeded=False, failure=event)
