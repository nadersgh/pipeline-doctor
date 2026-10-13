import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

from loguru import logger
from pydantic import BaseModel

from pipeline_doctor.agent.factory import build_agent
from pipeline_doctor.agent.fixes import FixApplicationError, apply_edits, write_project_files
from pipeline_doctor.demo.workspace import DemoWorkspace, clone_workspace, create_workspace, run_pipeline
from pipeline_doctor.evals.cases import EvalCase
from pipeline_doctor.evals.injection import inject_fault
from pipeline_doctor.evals.report import CaseResult, EvalSummary, render_markdown, render_step_latency, summarize
from pipeline_doctor.incidents.models import FixKind, Incident
from pipeline_doctor.memory.store import IncidentMemory
from pipeline_doctor.observability.tracing import configure_tracing
from pipeline_doctor.settings import Settings


class EvalRun(BaseModel):
    summary: EvalSummary
    results: list[CaseResult]
    output_dir: Path


class EvalHarness:
    """Runs every case in an isolated copy of a green demo workspace and scores the agent against ground truth."""

    def __init__(self, settings: Settings, work_dir: Path) -> None:
        self._settings = settings
        self._work_dir = work_dir

    def run(self, cases: list[EvalCase], output_dir: Path, workers: int = 4) -> EvalRun:
        started_at = datetime.now(UTC)
        suffix = "" if self._settings.jev_enabled else "_nojev"
        run_dir = output_dir / f"{started_at:%Y%m%dT%H%M%S}_{self._settings.reasoner}{suffix}"
        run_dir.mkdir(parents=True, exist_ok=True)
        configure_tracing(run_dir / "spans.jsonl")
        template = create_workspace(self._work_dir / "_template")
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(lambda case: self._run_case(template, case), cases))
        summary = summarize(self._settings.reasoner, started_at, results, jev_enabled=self._settings.jev_enabled)
        logger.info(f"write eval results {run_dir}")
        (run_dir / "results.jsonl").write_text("".join(result.model_dump_json() + "\n" for result in results))
        (run_dir / "summary.json").write_text(summary.model_dump_json(indent=2))
        markdown = render_markdown(summary, results).replace(
            "## Cases", f"{render_step_latency(run_dir / 'spans.jsonl')}\n## Cases"
        )
        (run_dir / "summary.md").write_text(markdown)
        return EvalRun(summary=summary, results=results, output_dir=run_dir)

    def _run_case(self, template: DemoWorkspace, case: EvalCase) -> CaseResult:
        # Opaque names: the run id reaches the prompt and paths can reach logs, so neither may reveal the fault.
        opaque_id = uuid.uuid4().hex[:12]
        workspace = clone_workspace(template, self._work_dir / f"ws_{opaque_id}")
        base = CaseResult(
            case_id=case.case_id,
            title=case.title,
            expected_category=case.category,
            expected_culprit=case.culprit_relation,
            has_distractor=case.distractor is not None,
            fault_triggered=False,
        )
        inject_fault(workspace, case)
        pipeline = run_pipeline(workspace, run_id=f"scheduled__{opaque_id}")
        if pipeline.succeeded or pipeline.failure is None:
            logger.warning(f"case {case.case_id}: injected fault did not break the pipeline")
            return base
        agent = build_agent(self._settings_for(workspace), IncidentMemory(workspace.root / "memory.jsonl"))
        incident = agent.diagnose(pipeline.failure)
        scored = _score(base.model_copy(update={"fault_triggered": True}), case, incident)
        fix_valid, note = _validate_fix(workspace, case, incident, _count_tests(template))
        logger.info(
            f"case {case.case_id}: {scored.predicted_category} (correct={scored.category_correct}) fix_valid={fix_valid}"
        )
        return scored.model_copy(update={"fix_valid": fix_valid, "fix_note": note})

    def _settings_for(self, workspace: DemoWorkspace) -> Settings:
        return self._settings.model_copy(
            update={
                "dbt_project_dir": workspace.project_dir,
                "dbt_target_dir": workspace.target_dir,
                "duckdb_path": workspace.duckdb_path,
                "airflow_log_dir": workspace.airflow_log_dir,
                "airflow_base_url": "",
                "state_dir": workspace.root / ".pipeline_doctor",
            }
        )


def _score(base: CaseResult, case: EvalCase, incident: Incident) -> CaseResult:
    diagnosis = incident.diagnosis
    if diagnosis is None:
        return base.model_copy(update={"error": incident.error, **_metrics(incident)})
    predicted_relation = diagnosis.culprit.relation.strip().lower()
    expected_column = (case.culprit_column or "").lower()
    column_matches = not expected_column or (diagnosis.culprit.column or "").lower() == expected_column
    return base.model_copy(
        update={
            "predicted_category": diagnosis.category,
            "predicted_culprit": diagnosis.culprit.describe(),
            "confidence": diagnosis.confidence,
            "category_correct": diagnosis.category == case.category,
            "culprit_correct": _same_relation(predicted_relation, case.culprit_relation) and column_matches,
            "fix_kind": incident.fix.kind if incident.fix else None,
            "jev_decision": incident.verdict.decision if incident.verdict else None,
            "jev_score": incident.verdict.score if incident.verdict else None,
            "error": incident.error,
            **_metrics(incident),
        }
    )


def _validate_fix(
    workspace: DemoWorkspace, case: EvalCase, incident: Incident, baseline_tests: int
) -> tuple[bool, str]:
    fix = incident.fix
    if fix is None:
        return False, "no fix proposed"
    if fix.kind not in case.accepted_fix_kinds:
        return False, f"{fix.kind} is not an acceptable remedy for {case.category}"
    if fix.kind is FixKind.OPERATIONAL:
        return True, "correctly escalated to the data owner"
    try:
        write_project_files(workspace.project_dir, apply_edits(workspace.project_dir, fix.edits))
    except FixApplicationError as error:
        return False, f"patch did not apply: {error}"
    rerun = run_pipeline(workspace, run_id=f"scheduled__{uuid.uuid4().hex[:12]}")
    if not rerun.succeeded:
        return False, "pipeline still fails after the patch"
    tests_run = _count_tests(workspace)
    if tests_run < baseline_tests:
        return False, f"pipeline green only because tests were removed ({tests_run} < {baseline_tests})"
    return True, "pipeline green after applying the patch, with every baseline test still running"


def _count_tests(workspace: DemoWorkspace) -> int:
    run_results = json.loads((workspace.target_dir / "run_results.json").read_text())
    return sum(1 for result in run_results["results"] if result["unique_id"].startswith("test."))


def _same_relation(predicted: str, expected: str) -> bool:
    predicted, expected = predicted.replace('"', "").lower(), expected.lower()
    return predicted == expected or predicted.endswith(f".{expected}")


def _metrics(incident: Incident) -> dict[str, object]:
    metrics = incident.metrics
    return {
        "latency_seconds": metrics.latency_seconds,
        "cost_usd": metrics.cost_usd,
        "llm_calls": metrics.llm_calls,
        "queries_run": metrics.queries_run,
        "queries_blocked": metrics.queries_blocked,
        "injection_findings": len(incident.injection_findings),
    }


def load_results(path: Path) -> list[CaseResult]:
    return [CaseResult.model_validate(json.loads(line)) for line in path.read_text().splitlines() if line.strip()]
