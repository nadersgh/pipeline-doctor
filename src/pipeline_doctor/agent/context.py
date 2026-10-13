import re
from typing import Any

from pydantic import BaseModel, Field

from pipeline_doctor.dbt_tools.artifacts import ResourceType
from pipeline_doctor.diagnostics.schema_drift import SchemaDrift
from pipeline_doctor.incidents.models import FailureEvent
from pipeline_doctor.memory.store import SimilarIncident
from pipeline_doctor.vcs.history import CommitChange

_ERROR_LINE = re.compile(r"(error|fail|exception|not found|warn)", re.IGNORECASE)
_MAX_LOG_CHARS = 3500


class FailedNode(BaseModel):
    unique_id: str
    name: str
    resource_type: ResourceType
    status: str
    message: str
    file_path: str = ""
    tested_relation: str | None = None
    tested_column: str | None = None
    test_name: str | None = Field(default=None, description="Generic test name; None for singular tests.")
    test_kwargs: dict[str, Any] = Field(default_factory=dict)
    compiled_code: str | None = None
    failures: int | None = None
    traced_source: str | None = Field(default=None, description="Source the tested column is read from.")
    traced_source_column: str | None = None
    parent_relation: str | None = Field(default=None, description="Referenced model of a relationships test.")
    parent_column: str | None = None
    parent_traced_source: str | None = None

    def describe(self) -> str:
        target = f" on {self.tested_relation}.{self.tested_column}" if self.tested_column else ""
        if self.traced_source:
            target += f" (read from {self.traced_source}.{self.traced_source_column})"
        test = f" ({self.test_name} test{target})" if self.resource_type is ResourceType.TEST else ""
        return f"{self.resource_type} {self.name}{test} -> {self.status}: {self.message}"


class IncidentContext(BaseModel):
    event: FailureEvent
    failures: list[FailedNode] = Field(default_factory=list)
    primary: FailedNode | None = None
    log_excerpt: str = ""
    upstream_sources: list[str] = Field(default_factory=list)
    upstream_models: list[str] = Field(default_factory=list)
    downstream_assets: list[str] = Field(default_factory=list)
    lineage_files: list[str] = Field(default_factory=list)
    schema_drift: list[SchemaDrift] = Field(default_factory=list)
    commits: list[CommitChange] = Field(default_factory=list)
    similar_incidents: list[SimilarIncident] = Field(default_factory=list)
    gathering_warnings: list[str] = Field(default_factory=list)
    source_loaded_at_columns: dict[str, str] = Field(default_factory=dict)
    source_consumer_files: dict[str, list[str]] = Field(
        default_factory=dict, description="Source short name -> files of models selecting from it directly."
    )
    project_files: dict[str, str] = Field(default_factory=dict, description="Current content of lineage files.")
    pre_change_files: dict[str, str] = Field(
        default_factory=dict, description="'<sha>:<path>' -> file content before that commit."
    )
    committed_files: dict[str, str] = Field(
        default_factory=dict, description="'<sha>:<path>' -> file content as of that commit."
    )

    def render_project_files(self) -> str:
        return "\n\n".join(f"### {path}\n```\n{content}\n```" for path, content in sorted(self.project_files.items()))

    def signature(self) -> str:
        if self.primary is None:
            return f"{self.event.task_id or ''} {self.log_excerpt[:400]}"
        node = self.primary
        return (
            f"{node.resource_type} {node.name} {node.test_name or ''} {node.tested_column or ''} {node.message[:300]}"
        )

    def render(self) -> str:
        """Compact, prompt-ready description of everything gathered about the incident."""
        sections = [
            f"## Trigger\n{self.event.describe()} at {self.event.occurred_at:%Y-%m-%d %H:%M UTC}",
            "## Failing dbt nodes\n"
            + _bullets([failure.describe() for failure in self.failures] or ["none parsed from artifacts"]),
            f"## Primary failure\n{self.primary.describe() if self.primary else 'unknown (see log)'}",
            _code_section(self.primary),
            f"## Task log excerpt\n```\n{self.log_excerpt or '(empty)'}\n```",
            "## Lineage\n"
            + _bullets(
                [
                    f"upstream sources: {self.upstream_sources}",
                    f"upstream models: {self.upstream_models}",
                    f"downstream assets: {self.downstream_assets}",
                ]
            ),
            "## Schema drift vs declared source columns\n"
            + _bullets([drift.describe() for drift in self.schema_drift] or ["no drift on upstream sources"]),
            "## Commits since last green run\n" + _render_commits(self.commits),
            "## Similar past incidents\n"
            + _bullets(
                [
                    f"{item.incident.category} on {item.incident.culprit} (similarity {item.similarity}): "
                    f"{item.incident.resolution}"
                    for item in self.similar_incidents
                ]
                or ["none"]
            ),
        ]
        if self.gathering_warnings:
            sections.append("## Context gathering warnings\n" + _bullets(self.gathering_warnings))
        return "\n\n".join(section for section in sections if section)


def extract_log_excerpt(log: str) -> str:
    lines = log.splitlines()
    keep: list[int] = []
    for index, line in enumerate(lines):
        if _ERROR_LINE.search(line) and "PASS=" not in line:
            keep.extend(range(index, min(index + 4, len(lines))))
    selected = [lines[index] for index in sorted(set(keep))] or lines[-25:]
    excerpt = "\n".join(selected)
    return excerpt[-_MAX_LOG_CHARS:]


def _bullets(items: list[str]) -> str:
    return "\n".join(f"- {item}" for item in items)


def _code_section(primary: FailedNode | None) -> str:
    if primary is None or not primary.compiled_code:
        return ""
    return f"## Compiled SQL of the primary failure\n```sql\n{primary.compiled_code[:2500]}\n```"


def _render_commits(commits: list[CommitChange]) -> str:
    if not commits:
        return "- no commits since the last green run"
    blocks = []
    for commit in commits:
        diff = f"\n```diff\n{commit.diff}\n```" if commit.diff else ""
        blocks.append(f"- {commit.describe()}{diff}")
    return "\n".join(blocks)
