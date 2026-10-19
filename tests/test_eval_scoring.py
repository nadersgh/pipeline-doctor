from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline_doctor.evals import harness
from pipeline_doctor.evals.cases import EvalCase, EvalCaseKind, EvalSplit, list_eval_cases
from pipeline_doctor.evals.catalogue import EvalCaseNotFoundError, SplitFilter, list_all_cases, select_cases
from pipeline_doctor.evals.compound import list_compound_cases
from pipeline_doctor.evals.harness import EvalHarness, EvalPlan, EvalReasonerFallbackError, check_no_fallbacks
from pipeline_doctor.evals.holdout import list_holdout_cases
from pipeline_doctor.evals.nofault import list_no_fault_cases
from pipeline_doctor.evals.report import (
    CaseResult,
    measure_consistency,
    measure_spread,
    render_markdown,
    score_card,
    summarize,
)
from pipeline_doctor.evals.scoring import IncidentScorer, match_cause
from pipeline_doctor.incidents.models import (
    Culprit,
    Diagnosis,
    FailureEvent,
    FailureSource,
    FileEdit,
    FixKind,
    FixProposal,
    Incident,
    RootCauseCategory,
)
from pipeline_doctor.settings import ReasonerKind, Settings

_MIN_CONFIDENCE = 0.6


def _result(case_id: str = "case", **fields: object) -> CaseResult:
    defaults: dict[str, object] = {
        "case_id": case_id,
        "title": case_id,
        "expected_category": RootCauseCategory.SOURCE_NULL_SPIKE,
        "expected_culprit": "raw.orders",
        "has_distractor": False,
        "fault_triggered": True,
    }
    return CaseResult.model_validate(defaults | fields)


def _incident(
    category: RootCauseCategory, relation: str, *, column: str | None = None, confidence: float = 0.9, fix: FixKind
) -> Incident:
    edits = [] if fix is FixKind.OPERATIONAL else [FileEdit(path="models/x.sql", original="a", updated="b")]
    return Incident(
        event=FailureEvent(source=FailureSource.AIRFLOW, dag_id="dag", task_id="dbt_build", run_id="run"),
        diagnosis=Diagnosis(
            category=category,
            confidence=confidence,
            culprit=Culprit(relation=relation, column=column),
            technical_summary="t",
            plain_summary="p",
        ),
        fix=FixProposal(kind=fix, title="fix", rationale="r", edits=edits),
    )


def _case(case_id: str) -> EvalCase:
    return next(case for case in list_all_cases() if case.case_id == case_id)


def _base(case: EvalCase) -> CaseResult:
    return _result(
        case.case_id,
        expected_category=case.category,
        expected_culprit=case.culprit_relation,
        split=case.split,
        kind=case.kind,
    )


@pytest.mark.parametrize(
    ("column", "category", "expected"),
    [
        ("status", RootCauseCategory.SOURCE_COLUMN_RENAMED, (True, True, True, True)),
        ("order_date", RootCauseCategory.SOURCE_COLUMN_RENAMED, (True, True, False, False)),
        ("status", RootCauseCategory.SOURCE_TYPE_CHANGED, (False, True, True, True)),
    ],
)
def test_scorer_credits_category_relation_and_column_separately(
    column: str, category: RootCauseCategory, expected: tuple[bool, bool, bool, bool]
) -> None:
    # Setup
    case = _case("renamed_orders_status")
    incident = _incident(category, "raw.orders", column=column, fix=FixKind.SCHEMA_PATCH)

    # Act
    result = IncidentScorer(_MIN_CONFIDENCE).score(_base(case), case, incident)

    # Assert
    assert (result.category_correct, result.relation_correct, result.column_correct, result.culprit_correct) == (
        expected
    )
    assert result.fix_kind_correct is True


def test_scorer_leaves_column_unscored_when_ground_truth_has_none() -> None:
    # Setup
    case = _case("code_bad_ref")
    incident = _incident(RootCauseCategory.CODE_SQL_ERROR, "int_order_payments", fix=FixKind.OPERATIONAL)

    # Act
    result = IncidentScorer(_MIN_CONFIDENCE).score(_base(case), case, incident)

    # Assert
    assert result.column_correct is None
    assert result.culprit_correct is True
    assert result.fix_kind_correct is False
    assert result.false_positive is None


@pytest.mark.parametrize(
    ("category", "relation", "primary_hit", "matched"),
    [
        (RootCauseCategory.SOURCE_STALE, "raw.payments", True, "stale_payments"),
        (RootCauseCategory.CODE_SQL_ERROR, "int_order_payments", True, "code_bad_ref"),
        (RootCauseCategory.CODE_SQL_ERROR, "raw.payments", False, "code_bad_ref"),
        (RootCauseCategory.SOURCE_NULL_SPIKE, "raw.orders", False, "stale_payments"),
    ],
)
def test_scorer_counts_either_compound_cause_as_primary_hit(
    category: RootCauseCategory, relation: str, primary_hit: bool, matched: str
) -> None:
    # Setup
    case = _case("compound_stale_payments_and_bad_ref")
    incident = _incident(category, relation, fix=FixKind.OPERATIONAL)

    # Act
    result = IncidentScorer(_MIN_CONFIDENCE).score(_base(case), case, incident)

    # Assert
    assert result.primary_hit is primary_hit
    assert result.matched_cause == matched


def test_match_cause_prefers_primary_on_tie() -> None:
    # Setup
    case = _case("compound_null_customer_ids_and_cents")
    diagnosis = _incident(RootCauseCategory.UNKNOWN, "unknown", fix=FixKind.OPERATIONAL).diagnosis
    assert diagnosis is not None

    # Act
    cause = match_cause(case, diagnosis)

    # Assert
    assert cause.case_id == "nulls_orders_customer_id"


@pytest.mark.parametrize(
    ("category", "confidence", "fix", "false_positive", "correct"),
    [
        (RootCauseCategory.UNKNOWN, 0.1, FixKind.OPERATIONAL, False, True),
        (RootCauseCategory.SOURCE_STALE, 0.4, FixKind.OPERATIONAL, False, False),
        (RootCauseCategory.SOURCE_STALE, 0.8, FixKind.OPERATIONAL, True, False),
        (RootCauseCategory.UNKNOWN, 0.1, FixKind.MODEL_CHANGE, True, True),
    ],
)
def test_scorer_flags_false_positive_on_no_fault_case(
    category: RootCauseCategory, confidence: float, fix: FixKind, false_positive: bool, correct: bool
) -> None:
    # Setup
    case = _case("nofault_warehouse_lock")
    incident = _incident(category, "raw.payments", confidence=confidence, fix=fix)

    # Act
    result = IncidentScorer(_MIN_CONFIDENCE).score(_base(case), case, incident)

    # Assert
    assert result.false_positive is false_positive
    assert result.category_correct is correct


def test_scorer_never_flags_false_positive_on_fault_case() -> None:
    # Setup
    case = _case("stale_orders")
    incident = _incident(RootCauseCategory.SOURCE_STALE, "raw.orders", fix=FixKind.MODEL_CHANGE)

    # Act
    result = IncidentScorer(_MIN_CONFIDENCE).score(_base(case), case, incident)

    # Assert
    assert result.false_positive is None


def test_score_card_rates_partial_credit_and_false_positives() -> None:
    # Setup
    results = [
        _result("a", category_correct=True, relation_correct=True, column_correct=True, fix_kind_correct=True),
        _result("b", relation_correct=True, column_correct=False),
        _result("c", relation_correct=True),
        _result("d", kind=EvalCaseKind.NO_FAULT, false_positive=True),
        _result("e", kind=EvalCaseKind.NO_FAULT, false_positive=False),
    ]

    # Act
    card = score_card(results)

    # Assert
    assert card.category_accuracy == 0.2
    assert card.relation_accuracy == 0.6
    assert card.column_accuracy == 0.5
    assert card.fix_kind_accuracy == 0.2
    assert card.false_positive_rate == 0.5


def test_score_card_reports_cost_latency_percentiles_and_totals() -> None:
    # Setup
    results = [_result(str(index), latency_seconds=float(index), cost_usd=0.01, llm_calls=2) for index in range(1, 21)]

    # Act
    card = score_card(results)

    # Assert
    assert card.p50_latency_seconds == 11.0
    assert card.p95_latency_seconds == 20.0
    assert card.total_latency_seconds == 210.0
    assert card.total_cost_usd == 0.2
    assert card.llm_calls == 40
    assert card.false_positive_rate is None
    assert card.column_accuracy is None


def test_measure_spread_reports_mean_std_min_max_across_repeats() -> None:
    # Setup
    cards = [
        score_card([_result("a", category_correct=True), _result("b", category_correct=True)]),
        score_card([_result("a", category_correct=True), _result("b")]),
    ]

    # Act
    spread = {row.metric: row for row in measure_spread(cards)}

    # Assert
    category = spread["category_accuracy"]
    assert (category.mean, category.min, category.max) == (0.75, 0.5, 1.0)
    assert category.std == pytest.approx(0.3536, abs=1e-4)
    assert spread["relation_accuracy"].std == 0.0
    assert "false_positive_rate" not in spread


def test_measure_spread_of_single_repeat_has_zero_std() -> None:
    # Act
    spread = measure_spread([score_card([_result(category_correct=True)])])

    # Assert
    assert all(row.std == 0.0 for row in spread)


def test_measure_consistency_reports_share_of_repeats_with_modal_outcome() -> None:
    # Setup
    stable = RootCauseCategory.SOURCE_STALE
    results = [
        *[_result("steady", repeat=index, predicted_category=stable, primary_hit=True) for index in range(3)],
        _result("flaky", repeat=0, predicted_category=stable, primary_hit=True),
        _result("flaky", repeat=1, predicted_category=RootCauseCategory.UNKNOWN),
        _result("flaky", repeat=2, predicted_category=stable, primary_hit=True),
    ]

    # Act
    rows = {row.case_id: row for row in measure_consistency(results)}

    # Assert
    assert rows["steady"].agreement == 1.0
    assert rows["flaky"].agreement == pytest.approx(0.6667, abs=1e-4)
    assert rows["flaky"].primary_hit_rate == pytest.approx(0.6667, abs=1e-4)


def test_summarize_reports_per_split_kind_and_repeat() -> None:
    # Setup
    results = [
        _result("dev_case", repeat=0, category_correct=True, predicted_category=RootCauseCategory.SOURCE_NULL_SPIKE),
        _result("dev_case", repeat=1, predicted_category=RootCauseCategory.UNKNOWN),
        _result("holdout_case", repeat=0, split=EvalSplit.HOLDOUT, kind=EvalCaseKind.COMPOUND, primary_hit=True),
        _result("holdout_case", repeat=1, split=EvalSplit.HOLDOUT, kind=EvalCaseKind.COMPOUND, primary_hit=True),
    ]

    # Act
    summary = summarize("heuristic", datetime.now(UTC), results)

    # Assert
    assert (summary.cases, summary.runs, summary.repeats) == (2, 4, 2)
    assert summary.by_split[EvalSplit.DEV].category_accuracy == 0.5
    assert summary.by_kind[EvalCaseKind.COMPOUND].primary_hit_rate == 1.0
    assert summary.mean_agreement == 0.75
    assert {row.metric for row in summary.spread} >= {"category_accuracy", "primary_hit_rate"}


def test_render_markdown_shows_partial_credit_cost_and_consistency() -> None:
    # Setup
    result = _result(
        "nofault", kind=EvalCaseKind.NO_FAULT, false_positive=True, cost_usd=0.0123, llm_calls=3, queries_run=4
    )
    summary = summarize("openai", datetime.now(UTC), [result])

    # Act
    markdown = render_markdown(summary, [result])

    # Assert
    assert "| False-positive rate (no-fault cases) | 100.0% |" in markdown
    assert "| no_fault | 1 | 1 |" in markdown
    assert "$0.0123" in markdown
    assert "## Consistency across repeats" in markdown


@pytest.mark.parametrize(
    ("split", "expected"),
    [(SplitFilter.DEV, {EvalSplit.DEV}), (SplitFilter.HOLDOUT, {EvalSplit.HOLDOUT}), (SplitFilter.ALL, set(EvalSplit))],
)
def test_select_cases_filters_by_split(split: SplitFilter, expected: set[EvalSplit]) -> None:
    # Act
    cases = select_cases(split, set())

    # Assert
    assert {case.split for case in cases} == expected


def test_select_cases_rejects_ids_outside_the_split() -> None:
    # Act / Assert
    with pytest.raises(EvalCaseNotFoundError, match="nofault_worker_killed"):
        select_cases(SplitFilter.DEV, {"nofault_worker_killed"})


def test_catalogue_meets_rigor_minimums() -> None:
    # Setup
    cases = list_all_cases()

    # Act
    ids = [case.case_id for case in cases]

    # Assert
    assert len(ids) == len(set(ids))
    assert all(case.split is EvalSplit.DEV for case in list_eval_cases())
    assert len(list_holdout_cases()) >= 6
    assert all(case.split is EvalSplit.HOLDOUT and case.kind is EvalCaseKind.SINGLE for case in list_holdout_cases())
    assert len(list_compound_cases()) >= 3
    assert all(len(case.acceptable_causes()) == 2 for case in list_compound_cases())
    assert len(list_no_fault_cases()) >= 2
    assert all(not case.data_sql and not case.commits for case in list_no_fault_cases())


def test_llm_eval_fails_when_steps_fell_back_to_the_heuristic(tmp_path: Path) -> None:
    # Setup
    results = [_result("a", llm_fallbacks=2), _result("b")]

    # Act / Assert
    with pytest.raises(EvalReasonerFallbackError, match="2 anthropic steps in 1 of 2 runs"):
        check_no_fallbacks(ReasonerKind.ANTHROPIC, results, tmp_path)


def test_llm_eval_passes_without_fallbacks_and_heuristic_eval_never_checks(tmp_path: Path) -> None:
    # Act / Assert
    check_no_fallbacks(ReasonerKind.ANTHROPIC, [_result("a")], tmp_path)
    check_no_fallbacks(ReasonerKind.HEURISTIC, [_result("a", llm_fallbacks=3)], tmp_path)


def test_concurrent_eval_runs_never_share_a_template_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Setup
    roots: list[Path] = []

    def record_root(root: Path) -> None:
        roots.append(root)
        raise RuntimeError("stop after recording the template location")

    monkeypatch.setattr(harness, "create_workspace", record_root)
    plan = EvalPlan(cases=[], repeats=1, split=SplitFilter.ALL)

    # Act
    for _ in range(2):
        with pytest.raises(RuntimeError, match="stop after recording"):
            EvalHarness(Settings(), tmp_path / "work").run(plan, tmp_path / "results")

    # Assert
    assert roots[0] != roots[1]
    assert all(root.is_relative_to(tmp_path / "work") for root in roots)


def _named(case_id: str, category: RootCauseCategory, confidence: float, *, correct: bool) -> CaseResult:
    return _result(case_id, predicted_category=category, confidence=confidence, category_correct=correct)


def test_score_card_separates_abstaining_from_naming_a_wrong_root_cause() -> None:
    # Setup
    results = [
        _named("right", RootCauseCategory.SOURCE_NULL_SPIKE, 0.9, correct=True),
        _named("confident-wrong", RootCauseCategory.CODE_JOIN_FANOUT, 0.8, correct=False),
        _named("hesitant-wrong", RootCauseCategory.SOURCE_STALE, 0.4, correct=False),
        _named("abstained-on-a-fault", RootCauseCategory.UNKNOWN, 0.5, correct=False),
    ]

    # Act
    card = score_card(results)

    # Assert
    assert card.abstention_rate == 0.25
    assert card.fault_abstention_rate == 0.25
    assert card.confident_wrong_rate == 0.25
    assert card.mean_confidence_right == 0.9
    assert card.mean_confidence_wrong == 0.6


def test_abstaining_on_a_no_fault_case_is_not_abstaining_on_a_fault() -> None:
    # Setup
    infra = _result(
        "infra",
        expected_category=RootCauseCategory.UNKNOWN,
        predicted_category=RootCauseCategory.UNKNOWN,
        confidence=0.8,
        category_correct=True,
    )

    # Act
    card = score_card([infra])

    # Assert
    assert card.abstention_rate == 1.0
    assert card.fault_abstention_rate is None
    assert card.mean_confidence_right is None


def test_render_markdown_reports_abstention_and_calibration_rows() -> None:
    # Setup
    results = [_named("a", RootCauseCategory.SOURCE_NULL_SPIKE, 0.9, correct=True)]
    summary = summarize("heuristic", datetime(2026, 10, 1, tzinfo=UTC), results, jev_enabled=True)

    # Act
    markdown = render_markdown(summary, results)

    # Assert
    assert "| Abstained on a real fault | 0.0% |" in markdown
    assert "| Mean confidence, right / wrong named answers | 0.90 / n/a |" in markdown
