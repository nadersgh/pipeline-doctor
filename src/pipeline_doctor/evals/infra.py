from datetime import UTC, datetime

import duckdb
from loguru import logger
from pydantic import BaseModel

from pipeline_doctor.airflow.logs import task_log_path
from pipeline_doctor.demo.workspace import DEMO_DAG_ID, DemoWorkspace, PipelineRun, run_pipeline
from pipeline_doctor.evals.cases import EvalCase, InfraFailureKind
from pipeline_doctor.incidents.models import FailureEvent, FailureSource

_TRUNCATED_TASK = "dbt_build"


class _TruncatedLog(BaseModel):
    """A run that finished green but whose task log ends as if the task had died mid-build."""

    trailer: list[str]
    excerpt: str


_TRUNCATED_LOGS: dict[InfraFailureKind, _TruncatedLog] = {
    InfraFailureKind.WORKER_KILLED: _TruncatedLog(
        trailer=[
            "[airflow.task.task_runner] WARNING - Received SIGTERM. Terminating subprocesses.",
            "[airflow.task.task_runner] INFO - Sending Signals.SIGTERM to group 4182. PIDs of all processes in the "
            "group: [4182]",
            "[airflow.jobs.local_task_job_runner] INFO - Task exited with return code Negsignal.SIGKILL",
            "[airflow.jobs.local_task_job_runner] ERROR - Job 3317 was killed before it finished (likely due to "
            "running out of memory)",
        ],
        excerpt="Task exited with return code Negsignal.SIGKILL",
    ),
    InfraFailureKind.NETWORK_TIMEOUT: _TruncatedLog(
        trailer=[
            "[dbt] ERROR - Database Error in model stg_payments (models/staging/stg_payments.sql)",
            "[dbt] ERROR -   IO Error: Connection reset by peer while reading from the warehouse endpoint",
            "[airflow.task.task_runner] ERROR - Task failed with exception: dbt build exited with code 1",
            "[airflow.task.task_runner] INFO - Marking task as UP_FOR_RETRY. try_number=1, max_tries=2",
        ],
        excerpt="IO Error: Connection reset by peer",
    ),
    InfraFailureKind.HEARTBEAT_LOST: _TruncatedLog(
        trailer=[
            "[airflow.jobs.scheduler_job_runner] ERROR - Detected zombie job: the task heartbeat was not received "
            "for 300 seconds",
            "[airflow.task.task_runner] WARNING - Received SIGTERM. Terminating subprocesses.",
            "[airflow.jobs.local_task_job_runner] INFO - Task exited with return code Negsignal.SIGKILL",
        ],
        excerpt="Detected zombie job: the task heartbeat was not received",
    ),
}


def run_case_pipeline(workspace: DemoWorkspace, case: EvalCase, run_id: str) -> PipelineRun:
    """Runs the DAG for a case, reproducing its infrastructure failure when it declares one."""
    match case.infra_failure:
        case None:
            return run_pipeline(workspace, run_id)
        case InfraFailureKind.WAREHOUSE_LOCK:
            return _run_with_warehouse_lock(workspace, run_id)
        case InfraFailureKind.WORKER_KILLED | InfraFailureKind.NETWORK_TIMEOUT | InfraFailureKind.HEARTBEAT_LOST:
            return _run_with_truncated_log(workspace, run_id, _TRUNCATED_LOGS[case.infra_failure])


def _run_with_warehouse_lock(workspace: DemoWorkspace, run_id: str) -> PipelineRun:
    # A read-write connection holds DuckDB's file lock, so the dbt subprocess cannot open the warehouse.
    logger.info(f"hold write lock {workspace.duckdb_path}")
    with duckdb.connect(str(workspace.duckdb_path)):
        return run_pipeline(workspace, run_id)


def _run_with_truncated_log(workspace: DemoWorkspace, run_id: str, failure: _TruncatedLog) -> PipelineRun:
    run_started_at = datetime.now(UTC)
    run = run_pipeline(workspace, run_id)
    if not run.succeeded:
        return run
    log_path = task_log_path(workspace.airflow_log_dir, DEMO_DAG_ID, run_id, _TRUNCATED_TASK, 1)
    lines = log_path.read_text().splitlines()
    logger.info(f"truncate task log {log_path}")
    log_path.write_text("\n".join([*lines[: len(lines) // 2], *failure.trailer]) + "\n")
    event = FailureEvent(
        source=FailureSource.AIRFLOW,
        dag_id=DEMO_DAG_ID,
        task_id=_TRUNCATED_TASK,
        run_id=run_id,
        run_started_at=run_started_at,
        git_sha=workspace.git.resolve("HEAD"),
        log_excerpt=failure.excerpt,
    )
    return PipelineRun(run_id=run_id, succeeded=False, failure=event)
