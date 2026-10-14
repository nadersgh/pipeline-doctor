import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

from loguru import logger
from pydantic import BaseModel, Field

from pipeline_doctor.agent.factory import build_agent
from pipeline_doctor.agent.fixes import FixApplicationError, apply_edits, write_project_files
from pipeline_doctor.demo.workspace import DemoWorkspace, clone_workspace, create_workspace, run_pipeline
from pipeline_doctor.evals.cases import EvalCase, EvalCaseKind
from pipeline_doctor.evals.catalogue import SplitFilter
from pipeline_doctor.evals.infra import run_case_pipeline
from pipeline_doctor.evals.injection import inject_fault
from pipeline_doctor.evals.report import CaseResult, EvalSummary, render_markdown, render_step_latency, summarize
from pipeline_doctor.evals.scoring import IncidentScorer, is_code_change, match_cause
from pipeline_doctor.incidents.models import FixKind, Incident
from pipeline_doctor.memory.store import IncidentMemory
from pipeline_doctor.observability.tracing import configure_tracing
from pipeline_doctor.settings import Settings


class EvalPlan(BaseModel):
    cases: list[EvalCase]
    repeats: int = Field(default=1, ge=1)
    split: SplitFilter = SplitFilter.ALL


class EvalRun(BaseModel):
    summary: EvalSummary
    results: list[CaseResult]
    output_dir: Path


class EvalHarness:
    """Runs every case in an isolated copy of a green demo workspace and scores the agent against ground truth."""

    def __init__(self, settings: Settings, work_dir: Path) -> None:
        self._settings = settings
        self._work_dir = work_dir / f"run_{datetime.now(UTC):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
        self._scorer = IncidentScorer(settings.jev_min_confidence)

    def run(self, plan: EvalPlan, output_dir: Path, workers: int = 4) -> EvalRun:
        started_at = datetime.now(UTC)
        suffix = "" if self._settings.jev_enabled else "_nojev"
        run_dir = output_dir / f"{started_at:%Y%m%dT%H%M%S}_{self._settings.reasoner}{suffix}"
        run_dir.mkdir(parents=True, exist_ok=True)
        configure_tracing(run_dir / "spans.jsonl")
        template = create_workspace(self._work_dir / "_template")
        jobs = [(case, repeat) for case in plan.cases for repeat in range(plan.repeats)]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(lambda job: self._run_case(template, *job), jobs))
        summary = summarize(
            self._settings.reasoner, started_at, results, jev_enabled=self._settings.jev_enabled, split=plan.split
        )
        logger.info(f"write eval results {run_dir}")
        (run_dir / "results.jsonl").write_text("".join(result.model_dump_json() + "\n" for result in results))
        (run_dir / "summary.json").write_text(summary.model_dump_json(indent=2))
        markdown = render_markdown(summary, results).replace(
            "## Cases", f"{render_step_latency(run_dir / 'spans.jsonl')}\n## Cases"
        )
        (run_dir / "summary.md").write_text(markdown)
        return EvalRun(summary=summary, results=results, output_dir=run_dir)

    def _run_case(self, template: DemoWorkspace, case: EvalCase, repeat: int) -> CaseResult:
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
            split=case.split,
            kind=case.kind,
            repeat=repeat,
        )
        inject_fault(workspace, case)
        pipeline = run_case_pipeline(workspace, case, run_id=f"scheduled__{opaque_id}")
        if pipeline.succeeded or pipeline.failure is None:
            logger.warning(f"case {case.case_id}: injected fault did not break the pipeline")
            return base
        agent = build_agent(self._settings_for(workspace), IncidentMemory(workspace.root / "memory.jsonl"))
        incident = agent.diagnose(pipeline.failure)
        scored = self._scorer.score(base.model_copy(update={"fault_triggered": True}), case, incident)
        fix_valid, note = self._validate_fix(template, workspace, case, incident)
        logger.info(
            f"case {case.case_id}#{repeat}: {scored.predicted_category} (correct={scored.category_correct}) "
            f"fix_valid={fix_valid}"
        )
        return scored.model_copy(update={"fix_valid": fix_valid, "fix_note": note})

    def _validate_fix(
        self, template: DemoWorkspace, workspace: DemoWorkspace, case: EvalCase, incident: Incident
    ) -> tuple[bool, str]:
        fix = incident.fix
        if case.kind is EvalCaseKind.NO_FAULT:
            if is_code_change(fix):
                return False, "code change proposed for an infrastructure failure"
            return True, "no code change proposed"
        if fix is None:
            return False, "no fix proposed"
        cause = match_cause(case, incident.diagnosis) if incident.diagnosis else case
        if fix.kind not in cause.accepted_fix_kinds:
            return False, f"{fix.kind} is not an acceptable remedy for {cause.category}"
        if fix.kind is FixKind.OPERATIONAL:
            return True, "correctly escalated to the data owner"
        # A compound patch is checked against its matched fault alone; the other fault would keep the rerun red.
        target = workspace if case.kind is EvalCaseKind.SINGLE else self._inject_alone(template, cause)
        try:
            write_project_files(target.project_dir, apply_edits(target.project_dir, fix.edits))
        except FixApplicationError as error:
            return False, f"patch did not apply: {error}"
        rerun = run_pipeline(target, run_id=f"scheduled__{uuid.uuid4().hex[:12]}")
        if not rerun.succeeded:
            return False, "pipeline still fails after the patch"
        baseline_tests, tests_run = _count_tests(template), _count_tests(target)
        if tests_run < baseline_tests:
            return False, f"pipeline green only because tests were removed ({tests_run} < {baseline_tests})"
        return True, "pipeline green after applying the patch, with every baseline test still running"

    def _inject_alone(self, template: DemoWorkspace, cause: EvalCase) -> DemoWorkspace:
        workspace = clone_workspace(template, self._work_dir / f"ws_{uuid.uuid4().hex[:12]}")
        inject_fault(workspace, cause)
        return workspace

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


def _count_tests(workspace: DemoWorkspace) -> int:
    run_results = json.loads((workspace.target_dir / "run_results.json").read_text())
    return sum(1 for result in run_results["results"] if result["unique_id"].startswith("test."))


def load_results(path: Path) -> list[CaseResult]:
    return [CaseResult.model_validate(json.loads(line)) for line in path.read_text().splitlines() if line.strip()]
