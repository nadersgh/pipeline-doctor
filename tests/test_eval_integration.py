import json
from pathlib import Path

import pytest

from pipeline_doctor.airflow.logs import task_log_path
from pipeline_doctor.demo.workspace import DEMO_DAG_ID, DemoWorkspace
from pipeline_doctor.evals.cases import EvalCaseKind
from pipeline_doctor.evals.catalogue import get_case
from pipeline_doctor.evals.harness import EvalHarness, EvalPlan
from pipeline_doctor.evals.infra import run_case_pipeline
from pipeline_doctor.evals.injection import inject_fault
from pipeline_doctor.settings import Settings

pytestmark = pytest.mark.integration


def _task_log(workspace: DemoWorkspace, run_id: str, task_id: str) -> str:
    return task_log_path(workspace.airflow_log_dir, DEMO_DAG_ID, run_id, task_id, 1).read_text()


def _failed_tests(workspace: DemoWorkspace) -> set[str]:
    run_results = json.loads((workspace.target_dir / "run_results.json").read_text())
    return {result["unique_id"] for result in run_results["results"] if result["status"] in {"fail", "error"}}


@pytest.mark.parametrize(
    ("case_id", "task_id", "log_marker"),
    [
        ("nofault_warehouse_lock", "dbt_source_freshness", "Could not set lock on file"),
        ("nofault_worker_killed", "dbt_build", "Negsignal.SIGKILL"),
    ],
)
def test_no_fault_case_fails_the_run_without_touching_data_or_code(
    demo_workspace: DemoWorkspace, case_id: str, task_id: str, log_marker: str
) -> None:
    # Setup
    case = get_case(case_id)
    head_before = demo_workspace.git.resolve("HEAD")
    inject_fault(demo_workspace, case)

    # Act
    run = run_case_pipeline(demo_workspace, case, run_id=case_id)

    # Assert
    assert run.failure is not None
    assert run.failure.task_id == task_id
    assert log_marker in _task_log(demo_workspace, case_id, task_id)
    assert demo_workspace.git.resolve("HEAD") == head_before
    assert _failed_tests(demo_workspace) == set()


def test_compound_case_injects_both_faults(demo_workspace: DemoWorkspace) -> None:
    # Setup
    case = get_case("compound_null_customer_ids_and_cents")
    inject_fault(demo_workspace, case)

    # Act
    run = run_case_pipeline(demo_workspace, case, run_id=case.case_id)

    # Assert
    assert run.failure is not None
    failed = _failed_tests(demo_workspace)
    assert any("not_null_stg_orders_customer_id" in test for test in failed)
    assert any("assert_payment_amounts_reasonable" in test for test in failed)


def test_harness_scores_compound_and_no_fault_cases(demo_settings: Settings, tmp_path: Path) -> None:
    # Setup
    compound, no_fault = get_case("compound_stale_payments_and_bad_ref"), get_case("nofault_worker_killed")
    plan = EvalPlan(cases=[compound, no_fault], repeats=2)

    # Act
    run = EvalHarness(demo_settings, tmp_path / "work").run(plan, tmp_path / "results", workers=2)

    # Assert
    by_case = {(result.case_id, result.repeat): result for result in run.results}
    assert set(by_case) == {(case.case_id, repeat) for case in (compound, no_fault) for repeat in range(2)}
    assert all(result.fault_triggered for result in run.results)
    assert by_case[compound.case_id, 0].matched_cause in {cause.case_id for cause in compound.components}
    assert by_case[no_fault.case_id, 0].false_positive is not None
    assert set(run.summary.by_kind) == {EvalCaseKind.COMPOUND, EvalCaseKind.NO_FAULT}
    assert run.summary.overall.false_positive_rate is not None
    assert (run.output_dir / "summary.md").exists()
