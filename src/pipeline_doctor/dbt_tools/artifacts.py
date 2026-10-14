import json
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field


class ResourceType(StrEnum):
    MODEL = "model"
    TEST = "test"
    SOURCE = "source"
    SEED = "seed"
    SNAPSHOT = "snapshot"
    EXPOSURE = "exposure"


class NodeStatus(StrEnum):
    SUCCESS = "success"
    PASS = "pass"
    WARN = "warn"
    ERROR = "error"
    FAIL = "fail"
    SKIPPED = "skipped"
    RUNTIME_ERROR = "runtime error"
    NO_OP = "no-op"
    PARTIAL_SUCCESS = "partial success"

    @property
    def is_failure(self) -> bool:
        return self in {NodeStatus.ERROR, NodeStatus.FAIL, NodeStatus.RUNTIME_ERROR}


class _Artifact(BaseModel):
    model_config = ConfigDict(extra="ignore")


class DependsOn(_Artifact):
    nodes: list[str] = Field(default_factory=list)


class ColumnInfo(_Artifact):
    name: str
    data_type: str | None = None


class TestMetadata(_Artifact):  # type: ignore[no-redef] # pydantic.mypy __init__ also takes **kwargs
    name: str
    kwargs: dict[str, Any] = Field(default_factory=dict)


class ManifestNode(_Artifact):
    unique_id: str
    resource_type: ResourceType
    name: str
    original_file_path: str = ""
    patch_path: str | None = None
    package_name: str = ""
    raw_code: str = ""
    compiled_code: str | None = None
    relation_name: str | None = None
    depends_on: DependsOn = Field(default_factory=DependsOn)
    columns: dict[str, ColumnInfo] = Field(default_factory=dict)
    test_metadata: TestMetadata | None = None
    attached_node: str | None = None
    column_name: str | None = None
    source_name: str | None = None
    loaded_at_field: str | None = None
    schema_: str | None = Field(default=None, alias="schema")
    identifier: str | None = None
    label: str | None = None

    @property
    def short_name(self) -> str:
        if self.resource_type is ResourceType.SOURCE:
            return f"{self.source_name}.{self.name}"
        return self.name


class ManifestMetadata(_Artifact):
    project_name: str = ""


class Manifest(_Artifact):
    metadata: ManifestMetadata = Field(default_factory=ManifestMetadata)
    nodes: dict[str, ManifestNode] = Field(default_factory=dict)
    sources: dict[str, ManifestNode] = Field(default_factory=dict)
    exposures: dict[str, ManifestNode] = Field(default_factory=dict)
    parent_map: dict[str, list[str]] = Field(default_factory=dict)
    child_map: dict[str, list[str]] = Field(default_factory=dict)


class RunResult(_Artifact):
    unique_id: str
    status: NodeStatus
    message: str | None = None
    failures: int | None = None
    compiled_code: str | None = None
    relation_name: str | None = None


class ArtifactMetadata(_Artifact):
    generated_at: datetime
    invocation_id: str | None = None


class RunResults(_Artifact):
    metadata: ArtifactMetadata
    results: list[RunResult] = Field(default_factory=list)


class FreshnessResult(_Artifact):
    unique_id: str
    status: NodeStatus
    max_loaded_at: datetime | None = None
    snapshotted_at: datetime | None = None
    max_loaded_at_time_ago_in_s: float | None = None
    error: str | None = None


class SourceFreshnessResults(_Artifact):
    metadata: ArtifactMetadata
    results: list[FreshnessResult] = Field(default_factory=list)


class DbtArtifactMissingError(FileNotFoundError):
    pass


class DbtArtifacts:
    """Reads dbt artifacts from a target directory."""

    def __init__(self, target_dir: Path) -> None:
        self._target_dir = target_dir

    def load_manifest(self) -> Manifest:
        return Manifest.model_validate(self._read("manifest.json"))

    def load_run_results(self) -> RunResults | None:
        return RunResults.model_validate(self._read("run_results.json")) if self._exists("run_results.json") else None

    def load_source_freshness(self) -> SourceFreshnessResults | None:
        if not self._exists("sources.json"):
            return None
        return SourceFreshnessResults.model_validate(self._read("sources.json"))

    def _exists(self, name: str) -> bool:
        return (self._target_dir / name).exists()

    def _read(self, name: str) -> dict[str, Any]:
        path = self._target_dir / name
        if not path.exists():
            raise DbtArtifactMissingError(path)
        logger.debug(f"read dbt artifact {path}")
        data: dict[str, Any] = json.loads(path.read_text())
        return data
