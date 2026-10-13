import os
import subprocess
import sys
from pathlib import Path
from typing import Literal

from loguru import logger
from pydantic import BaseModel

DbtCommand = Literal["build", "source freshness", "compile", "parse"]


class DbtInvocation(BaseModel):
    command: DbtCommand
    succeeded: bool
    output: str


class DbtRunner:
    """Runs the dbt CLI for a project against a DuckDB file."""

    def __init__(self, project_dir: Path, duckdb_path: Path) -> None:
        self._project_dir = project_dir.resolve()
        self._duckdb_path = duckdb_path.resolve()

    def run(self, command: DbtCommand) -> DbtInvocation:
        arguments = [
            str(Path(sys.executable).parent / "dbt"),
            *command.split(),
            "--project-dir",
            str(self._project_dir),
            "--profiles-dir",
            str(self._project_dir),
            "--no-use-colors",
            "--no-partial-parse",
        ]
        environment = {
            **os.environ,
            "PIPELINE_DOCTOR_DUCKDB_PATH": str(self._duckdb_path),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "DBT_LOG_PATH": str(self._project_dir / "logs"),
        }
        logger.debug(f"run dbt {command} {self._project_dir}")
        completed = subprocess.run(  # noqa: S603 - fixed argument vector, no shell
            arguments, cwd=self._project_dir, env=environment, capture_output=True, text=True, check=False
        )
        return DbtInvocation(
            command=command, succeeded=completed.returncode == 0, output=completed.stdout + completed.stderr
        )
