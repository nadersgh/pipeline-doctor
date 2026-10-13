import re
from datetime import UTC, datetime
from functools import cached_property
from pathlib import Path

from loguru import logger

from pipeline_doctor.agent.context import FailedNode, IncidentContext, extract_log_excerpt
from pipeline_doctor.agent.fixes import read_project_file
from pipeline_doctor.airflow.logs import AirflowLogNotFoundError, TaskLogSource
from pipeline_doctor.dbt_tools.artifacts import (
    DbtArtifactMissingError,
    DbtArtifacts,
    ManifestNode,
    NodeStatus,
    ResourceType,
)
from pipeline_doctor.dbt_tools.lineage import CatalogSchema, DbtLineage
from pipeline_doctor.diagnostics.checks import CheckRequest, CheckResult, DiagnosticRunner
from pipeline_doctor.diagnostics.schema_drift import SchemaDrift, detect_schema_drift
from pipeline_doctor.incidents.models import FailureEvent
from pipeline_doctor.memory.store import IncidentMemory, SimilarIncident
from pipeline_doctor.observability.tracing import traced
from pipeline_doctor.security.untrusted import InjectionFinding, UntrustedSource, scan_for_injection
from pipeline_doctor.vcs.history import CommitChange, GitHistory
from pipeline_doctor.vcs.repository import GitCommandError
from pipeline_doctor.warehouse.base import ColumnSpec, Warehouse
from pipeline_doctor.warehouse.guard import GuardedWarehouse

_REF_TARGET = re.compile(r"ref\(\s*['\"](\w+)['\"]\s*\)")
_LOG_NODE_REFERENCE = re.compile(r"(?:Model|Test|Snapshot|Seed) '((?:model|test|snapshot|seed)\.[\w.]+)'")
_STATUS_PRIORITY = {ResourceType.MODEL: 0, ResourceType.SOURCE: 1, ResourceType.TEST: 2}


class DiagnosticToolkit:
    """Every read-only tool the agent can use: artifacts, lineage, logs, git, warehouse checks, and memory."""

    def __init__(
        self,
        *,
        project_dir: Path,
        warehouse: Warehouse,
        guarded_warehouse: GuardedWarehouse,
        history: GitHistory,
        logs: TaskLogSource,
        memory: IncidentMemory,
        allowed_schemas: list[str],
        target_dir: Path | None = None,
    ) -> None:
        self._project_dir = project_dir
        self._artifacts = DbtArtifacts(target_dir or project_dir / "target")
        self._warehouse = warehouse
        self._guarded = guarded_warehouse
        self._history = history
        self._logs = logs
        self._memory = memory
        self._allowed_schemas = allowed_schemas

    @property
    def project_dir(self) -> Path:
        return self._project_dir

    @property
    def guarded_warehouse(self) -> GuardedWarehouse:
        return self._guarded

    @cached_property
    def catalog(self) -> CatalogSchema:
        catalog: CatalogSchema = {}
        for column in self._warehouse.list_columns(self._allowed_schemas):
            tables = catalog.setdefault(column.database.lower(), {}).setdefault(column.schema_name.lower(), {})
            tables.setdefault(column.table.lower(), {})[column.name.lower()] = column.data_type
        return catalog

    @cached_property
    def lineage(self) -> DbtLineage:
        return DbtLineage(self._artifacts.load_manifest(), self.catalog)

    @cached_property
    def _diagnostics(self) -> DiagnosticRunner:
        return DiagnosticRunner(self._guarded, self.lineage, self.catalog)

    def read_task_log(self, event: FailureEvent) -> str:
        if not (event.dag_id and event.run_id and event.task_id):
            return event.log_excerpt
        with traced("tool.airflow.read_task_log", {"task_id": event.task_id}):
            return self._logs.get_task_log(event.dag_id, event.run_id, event.task_id, event.try_number)

    def load_failures(self, event: FailureEvent) -> list[FailedNode]:
        with traced("tool.dbt.load_failures"):
            failures = self._load_run_failures(event) + self._load_freshness_failures(event)
        if event.dbt_node_id:
            failures.sort(key=lambda failure: failure.unique_id != event.dbt_node_id)
        return failures

    def infer_failure_from_log(self, log: str) -> FailedNode | None:
        match = _LOG_NODE_REFERENCE.search(log)
        node = self.lineage.get(match.group(1)) if match else None
        if node is None:
            return None
        return FailedNode(
            unique_id=node.unique_id,
            name=node.short_name,
            resource_type=node.resource_type,
            status="error",
            message=extract_log_excerpt(log)[-800:],
            file_path=node.original_file_path,
        )

    def detect_schema_drift(self, source_names: list[str]) -> list[SchemaDrift]:
        with traced("tool.warehouse.detect_schema_drift"):
            drifts = []
            for name in source_names:
                source = self.lineage.find(name)
                if source is None or source.resource_type is not ResourceType.SOURCE:
                    continue
                drifts.append(detect_schema_drift(source, self._list_actual_columns(source)))
        return [drift for drift in drifts if drift.has_drift]

    def list_recent_changes(self, lineage_files: list[str], deployed_ref: str = "HEAD") -> list[CommitChange]:
        with traced("tool.git.list_recent_changes"):
            return self._history.list_recent_changes(lineage_files, deployed_ref)

    def run_check(self, request: CheckRequest) -> CheckResult:
        with traced("tool.warehouse.run_check", {"check": request.kind, "relation": request.relation}) as span:
            result = self._diagnostics.run(request)
            span.set_attribute("check.error", result.error or "")
            return result

    def search_similar_incidents(self, signature: str) -> list[SimilarIncident]:
        with traced("tool.memory.search"):
            return self._memory.search(signature)

    def read_project_file(self, path: str) -> str:
        return read_project_file(self._project_dir, path)

    def read_file_before_commit(self, sha: str, path: str) -> str | None:
        try:
            return self._history.read_file_before(sha, path)
        except GitCommandError:
            return None

    def read_file_at_commit(self, sha: str, path: str) -> str | None:
        try:
            return self._history.read_file_at(sha, path)
        except GitCommandError:
            return None

    def _list_actual_columns(self, source: ManifestNode) -> list[ColumnSpec]:
        parts = [part.strip('"').lower() for part in (source.relation_name or "").split(".")]
        if len(parts) != 3:
            return []
        columns = self.catalog.get(parts[0], {}).get(parts[1], {}).get(parts[2], {})
        return [
            ColumnSpec(database=parts[0], schema_name=parts[1], table=parts[2], name=name, data_type=data_type)
            for name, data_type in columns.items()
        ]

    def _load_run_failures(self, event: FailureEvent) -> list[FailedNode]:
        run_results = self._artifacts.load_run_results()
        if run_results is None or _is_stale(run_results.metadata.generated_at, event):
            return []
        failures = []
        for result in run_results.results:
            node = self.lineage.get(result.unique_id)
            if not result.status.is_failure or node is None:
                continue
            failures.append(self._to_failed_node(node, result.status, result.message or "", result.failures))
        return sorted(failures, key=self._failure_rank)

    def _load_freshness_failures(self, event: FailureEvent) -> list[FailedNode]:
        freshness = self._artifacts.load_source_freshness()
        if freshness is None or _is_stale(freshness.metadata.generated_at, event):
            return []
        failures = []
        for result in freshness.results:
            node = self.lineage.get(result.unique_id)
            if not result.status.is_failure or node is None:
                continue
            hours = (result.max_loaded_at_time_ago_in_s or 0) / 3600
            message = result.error or f"source freshness {result.status}: last loaded {hours:.1f} hours ago"
            failures.append(self._to_failed_node(node, result.status, message, None))
        return failures

    def _to_failed_node(self, node: ManifestNode, status: NodeStatus, message: str, failures: int | None) -> FailedNode:
        tested = self.lineage.get_tested_node(node) if node.resource_type is ResourceType.TEST else None
        metadata = node.test_metadata
        kwargs = metadata.kwargs if metadata else {}
        tested_column = node.column_name or kwargs.get("column_name")
        source_hit = self.lineage.trace_column_to_source(tested, tested_column) if tested and tested_column else None
        parent_match = _REF_TARGET.search(str(kwargs.get("to", "")))
        parent = self.lineage.find(parent_match.group(1)) if parent_match else None
        parent_field = str(kwargs.get("field", ""))
        parent_hit = self.lineage.trace_column_to_source(parent, parent_field) if parent and parent_field else None
        return FailedNode(
            unique_id=node.unique_id,
            name=node.short_name,
            resource_type=node.resource_type,
            status=status,
            message=message,
            file_path=node.original_file_path,
            tested_relation=tested.short_name if tested else None,
            tested_column=tested_column,
            test_name=metadata.name if metadata else None,
            test_kwargs={key: value for key, value in kwargs.items() if key != "model"},
            compiled_code=node.compiled_code,
            failures=failures,
            traced_source=source_hit[0].short_name if source_hit else None,
            traced_source_column=source_hit[1] if source_hit else None,
            parent_relation=parent.short_name if parent else None,
            parent_column=parent_field or None,
            parent_traced_source=parent_hit[0].short_name if parent_hit else None,
        )

    def _failure_rank(self, failure: FailedNode) -> tuple[int, int]:
        return _STATUS_PRIORITY.get(failure.resource_type, 3), len(self.lineage.list_upstream(failure.unique_id))


class ContextGatherer:
    """Collects logs, failing nodes, lineage, schema drift, recent commits, and similar incidents."""

    def __init__(self, toolkit: DiagnosticToolkit) -> None:
        self._toolkit = toolkit

    def gather(self, event: FailureEvent) -> IncidentContext:
        warnings: list[str] = []
        log = self._read_log(event, warnings)
        failures = self._load_failures(event, warnings)
        if not failures:
            inferred = self._toolkit.infer_failure_from_log(log)
            failures = [inferred] if inferred else []
        primary = failures[0] if failures else None
        anchor = self._lineage_anchor(primary)
        upstream = self._toolkit.lineage.list_upstream(anchor) if anchor else []
        downstream = self._toolkit.lineage.list_downstream(anchor) if anchor else []
        source_nodes = [node for node in upstream if node.resource_type is ResourceType.SOURCE]
        sources = [node.short_name for node in source_nodes]
        if primary is not None and primary.resource_type is ResourceType.SOURCE:
            source_nodes = [node for node in [self._toolkit.lineage.get(primary.unique_id)] if node is not None]
            sources = [primary.name]
        lineage_files = self._toolkit.lineage.list_lineage_files(primary.unique_id) if primary else []
        commits = self._list_commits(lineage_files, event.git_sha or "HEAD", warnings)
        context = IncidentContext(
            event=event,
            failures=failures,
            primary=primary,
            log_excerpt=extract_log_excerpt(log),
            upstream_sources=sources,
            upstream_models=[node.name for node in upstream if node.resource_type is ResourceType.MODEL],
            downstream_assets=[node.label or node.name for node in downstream],
            lineage_files=lineage_files,
            schema_drift=self._toolkit.detect_schema_drift(sources),
            commits=commits,
            gathering_warnings=warnings,
            source_loaded_at_columns={node.short_name: node.loaded_at_field or "" for node in source_nodes},
            source_consumer_files={
                node.short_name: [
                    model.original_file_path for model in self._toolkit.lineage.list_staging_models(node.unique_id)
                ]
                for node in source_nodes
            },
            project_files=self._read_project_files(lineage_files, source_nodes),
            pre_change_files=self._read_commit_files(commits, before=True),
            committed_files=self._read_commit_files(commits, before=False),
        )
        return context.model_copy(
            update={
                "similar_incidents": self._toolkit.search_similar_incidents(context.signature()),
                "injection_findings": _find_injections(context),
            }
        )

    def _read_log(self, event: FailureEvent, warnings: list[str]) -> str:
        try:
            return self._toolkit.read_task_log(event)
        except AirflowLogNotFoundError as error:
            warnings.append(f"task log unavailable: {error}")
            return event.log_excerpt

    def _load_failures(self, event: FailureEvent, warnings: list[str]) -> list[FailedNode]:
        try:
            return self._toolkit.load_failures(event)
        except DbtArtifactMissingError as error:
            warnings.append(f"dbt artifacts unavailable: {error}")
            return []

    def _list_commits(self, lineage_files: list[str], deployed_ref: str, warnings: list[str]) -> list[CommitChange]:
        try:
            return self._toolkit.list_recent_changes(lineage_files, deployed_ref)
        except GitCommandError as error:
            warnings.append(f"git history unavailable: {error}")
            return []

    def _read_project_files(self, lineage_files: list[str], sources: list[ManifestNode]) -> dict[str, str]:
        consumer_files = [
            model.original_file_path
            for source in sources
            for model in self._toolkit.lineage.list_staging_models(source.unique_id)
        ]
        return {path: self._toolkit.read_project_file(path) for path in sorted({*lineage_files, *consumer_files})}

    def _read_commit_files(self, commits: list[CommitChange], *, before: bool) -> dict[str, str]:
        files = {}
        for commit in commits:
            for path in commit.lineage_files:
                content = (
                    self._toolkit.read_file_before_commit(commit.sha, path)
                    if before
                    else self._toolkit.read_file_at_commit(commit.sha, path)
                )
                if content is not None:
                    files[f"{commit.sha}:{path}"] = content
        return files

    def _lineage_anchor(self, primary: FailedNode | None) -> str | None:
        if primary is None:
            return None
        if primary.resource_type is ResourceType.TEST:
            tested = self._toolkit.lineage.find(primary.tested_relation or "")
            return tested.unique_id if tested else primary.unique_id
        return primary.unique_id


def _find_injections(context: IncidentContext) -> list[InjectionFinding]:
    candidates = [
        (context.log_excerpt, UntrustedSource.TASK_LOG, "task log excerpt"),
        *((failure.message, UntrustedSource.DBT_RESULT, failure.unique_id) for failure in context.failures),
        *(
            (failure.compiled_code or "", UntrustedSource.COMPILED_SQL, failure.unique_id)
            for failure in context.failures
        ),
        *(
            (f"{commit.message}\n{commit.diff}", UntrustedSource.GIT_HISTORY, commit.sha[:8])
            for commit in context.commits
        ),
        *((drift.describe(), UntrustedSource.SCHEMA_DRIFT, drift.source) for drift in context.schema_drift),
    ]
    return [finding for candidate in candidates if (finding := scan_for_injection(*candidate))]


def _is_stale(generated_at: datetime, event: FailureEvent) -> bool:
    if event.run_started_at is None:
        return False
    logger.debug(f"artifact generated at {generated_at}, run started at {event.run_started_at}")
    return _as_utc(generated_at) < _as_utc(event.run_started_at)


def _as_utc(moment: datetime) -> datetime:
    return moment.replace(tzinfo=UTC) if moment.tzinfo is None else moment.astimezone(UTC)
