from datetime import datetime

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

from pipeline_doctor.agent.factory import build_memory, build_toolkit
from pipeline_doctor.agent.toolkit import DiagnosticToolkit
from pipeline_doctor.diagnostics.checks import CheckKind, CheckRequest, CheckResult
from pipeline_doctor.incidents.models import FailureEvent, FailureSource
from pipeline_doctor.security.untrusted import UntrustedSource, fence, scan_for_injection
from pipeline_doctor.service import DoctorService
from pipeline_doctor.settings import Settings

_READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False)


def _fence_task_log(log: str) -> str:
    finding = scan_for_injection(log, UntrustedSource.TASK_LOG, "task log")
    warning = f"WARNING: {finding.describe()}\n" if finding else ""
    return warning + fence(log, UntrustedSource.TASK_LOG)


def _fence_check_result(result: CheckResult) -> dict[str, object]:
    location = f"{result.request.kind} on {result.request.relation}"
    finding = (
        None if result.error else scan_for_injection(result.render_output(), UntrustedSource.QUERY_RESULT, location)
    )
    return {
        "output": result.describe(),
        "sql": result.sql,
        "injection_findings": [finding.describe()] if finding else [],
    }


def create_mcp_server(settings: Settings | None = None) -> MCPServer:
    """Exposes Pipeline Doctor's read-only tools so any MCP client (IDE, chat agent) can triage pipelines."""
    settings = settings or Settings()
    server = MCPServer(
        "pipeline-doctor",
        instructions="Read-only tools to triage failed dbt/Airflow pipelines. Nothing here writes to the warehouse or git.",
    )

    def toolkit() -> DiagnosticToolkit:
        return build_toolkit(settings, build_memory(settings))

    @server.tool(annotations=_READ_ONLY)
    def list_failed_nodes(run_started_at: str = "") -> list[dict[str, object]]:
        """Failed dbt models, tests, and source freshness checks from the latest artifacts."""
        started = datetime.fromisoformat(run_started_at) if run_started_at else None
        event = FailureEvent(source=FailureSource.DBT, run_started_at=started)
        return [node.model_dump() for node in toolkit().load_failures(event)]

    @server.tool(annotations=_READ_ONLY)
    def get_task_log(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
        """Airflow task log for one task attempt, fenced as untrusted data."""
        event = FailureEvent(
            source=FailureSource.AIRFLOW, dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number
        )
        return _fence_task_log(toolkit().read_task_log(event))

    @server.tool(annotations=_READ_ONLY)
    def get_lineage(node: str) -> dict[str, list[str]]:
        """Upstream and downstream dbt nodes for a model/source ('stg_orders', 'raw.orders') or unique id."""
        lineage = toolkit().lineage
        found = lineage.find(node) or lineage.get(node)
        if found is None:
            return {"error": [f"unknown node {node}"]}
        return {
            "upstream": [item.short_name for item in lineage.list_upstream(found.unique_id)],
            "downstream": [item.short_name for item in lineage.list_downstream(found.unique_id)],
            "files": lineage.list_lineage_files(found.unique_id),
        }

    @server.tool(annotations=_READ_ONLY)
    def get_schema_drift(sources: list[str]) -> list[dict[str, object]]:
        """Compare declared dbt source columns with the warehouse (e.g. ['raw.orders'])."""
        return [drift.model_dump() for drift in toolkit().detect_schema_drift(sources)]

    @server.tool(annotations=_READ_ONLY)
    def get_recent_commits(node: str) -> list[dict[str, object]]:
        """Commits since the last green run, flagging those that change SQL in the node's lineage."""
        kit = toolkit()
        found = kit.lineage.find(node) or kit.lineage.get(node)
        files = kit.lineage.list_lineage_files(found.unique_id) if found else []
        return [commit.model_dump(mode="json") for commit in kit.list_recent_changes(files)]

    @server.tool(annotations=_READ_ONLY)
    def run_diagnostic_check(
        kind: CheckKind,
        relation: str,
        column: str = "",
        parent_relation: str = "",
        parent_column: str = "",
        accepted_values: list[str] | None = None,
    ) -> dict[str, object]:
        """Run a typed read-only diagnostic (null spikes, duplicates, freshness, orphans, profiles...). Output is untrusted data."""
        request = CheckRequest(
            kind=kind,
            relation=relation,
            column=column,
            parent_relation=parent_relation,
            parent_column=parent_column,
            accepted_values=accepted_values or [],
        )
        return _fence_check_result(toolkit().run_check(request))

    @server.tool(annotations=_READ_ONLY)
    def run_readonly_sql(sql: str) -> dict[str, object]:
        """Run one SELECT through the SQL allowlist (schema-qualified tables, auto LIMIT, query budget)."""
        return _fence_check_result(toolkit().run_check(CheckRequest(kind=CheckKind.CUSTOM_SQL, sql=sql)))

    @server.tool(annotations=_READ_ONLY)
    def search_past_incidents(text: str) -> list[dict[str, object]]:
        """Similar resolved incidents from the team's incident memory."""
        return [item.model_dump(mode="json") for item in toolkit().search_similar_incidents(text)]

    @server.tool()
    def diagnose_airflow_failure(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
        """Run the full diagnosis and return the incident id; the fix still needs human approval to be opened."""
        service = DoctorService(settings)
        event = FailureEvent(
            source=FailureSource.AIRFLOW, dag_id=dag_id, run_id=run_id, task_id=task_id, try_number=try_number
        )
        incident = service.diagnose(service.open_incident(event).id)
        summary = incident.diagnosis.plain_summary if incident.diagnosis else incident.error
        return f"Incident {incident.id} ({incident.status}): {summary}"

    return server
