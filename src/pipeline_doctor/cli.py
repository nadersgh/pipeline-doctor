import sys
from pathlib import Path
from typing import Annotated

import typer
import uvicorn
from loguru import logger
from rich.console import Console
from rich.markdown import Markdown
from rich.table import Table

from pipeline_doctor.agent.fixes import FixApplicationError
from pipeline_doctor.demo.workspace import DemoWorkspace, create_workspace
from pipeline_doctor.evals.catalogue import EvalCaseNotFoundError, SplitFilter, get_case, list_all_cases, select_cases
from pipeline_doctor.evals.harness import EvalHarness, EvalPlan
from pipeline_doctor.evals.infra import run_case_pipeline
from pipeline_doctor.evals.injection import inject_fault
from pipeline_doctor.incidents.models import FailureEvent
from pipeline_doctor.incidents.report import render_incident_report
from pipeline_doctor.incidents.repository import IncidentNotFoundError
from pipeline_doctor.publishing.approval import IncidentApprovalError
from pipeline_doctor.service import DoctorService
from pipeline_doctor.settings import ReasonerKind, Settings

app = typer.Typer(help="Pipeline Doctor: diagnose and propose fixes for broken data pipelines.", no_args_is_help=True)
demo_app = typer.Typer(help="Demo dbt + Airflow workspace with injectable faults.", no_args_is_help=True)
app.add_typer(demo_app, name="demo")
console = Console()

_LAST_FAILURE_FILE = "last_failure.json"
WorkspaceOption = Annotated[Path, typer.Option(help="Demo workspace directory.")]


@app.callback()
def configure(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False) -> None:
    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if verbose else "WARNING")


@demo_app.command("setup")
def demo_setup(workspace: WorkspaceOption = Path("demo_workspace")) -> None:
    """Create the demo workspace: dbt project under git, DuckDB raw data, and a green baseline run."""
    created = create_workspace(workspace)
    console.print(f"[green]Demo workspace ready at {created.root}[/green] (baseline pipeline is green)")


@demo_app.command("faults")
def demo_faults() -> None:
    """List injectable faults (the eval cases)."""
    table = Table("case id", "split", "kind", "category", "title")
    for case in list_all_cases():
        table.add_row(case.case_id, case.split, case.kind, case.category, case.title)
    console.print(table)


@demo_app.command("break")
def demo_break(
    case_id: str,
    workspace: WorkspaceOption = Path("demo_workspace"),
    run_after: Annotated[bool, typer.Option("--run/--no-run", help="Run the pipeline after injecting.")] = True,
) -> None:
    """Inject a fault into the demo workspace and run the pipeline like Airflow would."""
    try:
        case = get_case(case_id)
    except EvalCaseNotFoundError as error:
        raise typer.BadParameter(f"unknown fault {case_id}; see `pipeline-doctor demo faults`") from error
    demo = DemoWorkspace(workspace)
    inject_fault(demo, case)
    if not run_after:
        console.print(f"[yellow]Injected {case_id}.[/yellow] Trigger the DAG in Airflow to see it fail.")
        return
    run = run_case_pipeline(demo, case, run_id=f"manual__{case_id}")
    if run.failure is None:
        console.print("[yellow]The pipeline still passed; no incident to diagnose.[/yellow]")
        return
    (workspace / _LAST_FAILURE_FILE).write_text(run.failure.model_dump_json(indent=2))
    console.print(f"[red]Task {run.failure.task_id} failed.[/red] Run `pipeline-doctor diagnose` next.")


@app.command()
def diagnose(
    event_file: Annotated[Path, typer.Option("--event", help="FailureEvent JSON.")] = Path("demo_workspace")
    / _LAST_FAILURE_FILE,
    reasoner: Annotated[ReasonerKind | None, typer.Option(help="Override the configured reasoner.")] = None,
    jev: Annotated[bool | None, typer.Option("--jev/--no-jev", help="Override PIPELINE_DOCTOR_JEV_ENABLED.")] = None,
) -> None:
    """Diagnose a failure event and store the incident for review."""
    settings = _settings(reasoner, jev)
    event = FailureEvent.model_validate_json(event_file.read_text())
    service = DoctorService(settings)
    incident = service.diagnose(service.open_incident(event).id)
    console.print(Markdown(render_incident_report(incident)))
    console.print(f"\nApprove with: [bold]pipeline-doctor approve {incident.id} --approver <name>[/bold]")


@app.command("incidents")
def list_incidents() -> None:
    """List stored incidents."""
    table = Table("id", "status", "category", "culprit", "created")
    for incident in DoctorService(Settings()).repository.list_all():
        diagnosis = incident.diagnosis
        table.add_row(
            incident.id,
            incident.status,
            diagnosis.category if diagnosis else "-",
            diagnosis.culprit.describe() if diagnosis else "-",
            f"{incident.created_at:%Y-%m-%d %H:%M}",
        )
    console.print(table)


@app.command()
def show(incident_id: str) -> None:
    """Print an incident report."""
    console.print(Markdown(render_incident_report(DoctorService(Settings()).repository.get(incident_id))))


@app.command()
def approve(incident_id: str, approver: Annotated[str, typer.Option(help="Who approves the fix.")]) -> None:
    """Approve a proposed fix: opens a draft pull request (or records the operational resolution)."""
    incident = DoctorService(Settings()).approvals.approve(incident_id, approver)
    console.print(f"[green]{incident.status}[/green] {incident.pull_request_url or ''}")


@app.command()
def reject(
    incident_id: str,
    reviewer: Annotated[str, typer.Option()],
    reason: Annotated[str, typer.Option()],
) -> None:
    """Reject a proposed fix."""
    incident = DoctorService(Settings()).approvals.reject(incident_id, reviewer, reason)
    console.print(f"[yellow]{incident.status}[/yellow]")


@app.command("eval")
def run_eval(
    reasoner: Annotated[ReasonerKind | None, typer.Option(help="Override the configured reasoner.")] = None,
    cases: Annotated[str, typer.Option(help="Comma-separated case ids; empty runs the whole split.")] = "",
    split: Annotated[
        SplitFilter, typer.Option(help="dev: regression set the heuristic was written against; holdout: untuned.")
    ] = SplitFilter.DEV,
    repeats: Annotated[int, typer.Option(min=1, help="Runs per case, each in a fresh workspace.")] = 1,
    workers: Annotated[int, typer.Option(min=1)] = 4,
    work_dir: Annotated[Path, typer.Option()] = Path(".pipeline_doctor/eval_workspaces"),
    output_dir: Annotated[Path, typer.Option()] = Path("evals/results"),
    jev: Annotated[bool | None, typer.Option("--jev/--no-jev", help="Override PIPELINE_DOCTOR_JEV_ENABLED.")] = None,
) -> None:
    """Inject every fault into an isolated workspace, diagnose it, and score the agent."""
    wanted = {item.strip() for item in cases.split(",") if item.strip()}
    plan = EvalPlan(cases=select_cases(split, wanted), repeats=repeats, split=split)
    run = EvalHarness(_settings(reasoner, jev), work_dir).run(plan, output_dir, workers)
    summary = (run.output_dir / "summary.md").read_text().split("## Cases")[0]
    console.print(Markdown(summary))
    console.print(f"Per-case results: {run.output_dir / 'summary.md'}")


@app.command()
def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Run the webhook + review API."""
    from pipeline_doctor.api.app import create_app  # noqa: PLC0415 - keeps CLI startup fast

    uvicorn.run(create_app(Settings()), host=host, port=port)


@app.command()
def mcp() -> None:
    """Serve the read-only diagnostic tools over MCP (stdio)."""
    from pipeline_doctor.mcp_server import create_mcp_server  # noqa: PLC0415

    create_mcp_server(Settings()).run()


def main() -> None:
    try:
        app()
    except (IncidentNotFoundError, IncidentApprovalError, FixApplicationError, EvalCaseNotFoundError) as error:
        console.print(f"[red]{type(error).__name__}: {error}[/red]")
        raise SystemExit(1) from error


def _settings(reasoner: ReasonerKind | None, jev: bool | None = None) -> Settings:
    overrides: dict[str, object] = {}
    if reasoner:
        overrides["reasoner"] = reasoner
    if jev is not None:
        overrides["jev_enabled"] = jev
    return Settings().model_copy(update=overrides)
