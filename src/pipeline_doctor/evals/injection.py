from pathlib import Path

import duckdb
from loguru import logger

from pipeline_doctor.demo.workspace import DemoWorkspace
from pipeline_doctor.evals.cases import EvalCase, FileChange, InjectedCommit, distractor_commit


class FaultInjectionError(RuntimeError):
    pass


def inject_fault(workspace: DemoWorkspace, case: EvalCase) -> list[str]:
    """Applies the case's data corruption and commits; returns the SHAs of injected commits."""
    if case.data_sql:
        logger.info(f"inject data fault {case.case_id} into {workspace.duckdb_path}")
        with duckdb.connect(str(workspace.duckdb_path)) as connection:
            for statement in case.data_sql:
                connection.execute(statement)
    commits = [*case.commits, *([distractor_commit(case.distractor)] if case.distractor else [])]
    return [_commit(workspace, commit) for commit in commits]


def _commit(workspace: DemoWorkspace, commit: InjectedCommit) -> str:
    for change in commit.changes:
        _apply_change(workspace.project_dir, change)
    return workspace.git.commit_all(commit.message, commit.author)


def _apply_change(project_dir: Path, change: FileChange) -> None:
    path = project_dir / change.path
    content = path.read_text()
    if change.original not in content:
        raise FaultInjectionError(f"{change.path}: snippet to replace not found: {change.original!r}")
    logger.debug(f"write fault change {path}")
    path.write_text(content.replace(change.original, change.updated, 1))
