from datetime import UTC, datetime
from pathlib import Path

import pytest

from pipeline_doctor.agent.context import IncidentContext
from pipeline_doctor.agent.reasoning.base import Investigation, ReasonerError
from pipeline_doctor.diagnostics.checks import CheckKind, CheckRequest, CheckResult
from pipeline_doctor.evals.report import CaseResult, summarize
from pipeline_doctor.incidents.models import (
    Culprit,
    Diagnosis,
    FailureEvent,
    FailureSource,
    FileEdit,
    FixKind,
    FixProposal,
    RootCauseCategory,
)
from pipeline_doctor.jev.llm_judge import JudgeOpinion, LLMJudge
from pipeline_doctor.jev.models import JevDecision, JevStage
from pipeline_doctor.jev.reviewer import JevRequest, LayeredJev, RuleBasedJev
from tests.conftest import DiagnosisFactory

_MODEL_PATH = "models/staging/stg_orders.sql"
_MODEL_SQL = "select id, customer_id, status from {{ source('raw', 'orders') }}\n"
_SCHEMA_PATH = "models/staging/_stg_models.yml"
_SCHEMA_YML = "columns:\n  - name: customer_id\n    tests:\n      - not_null\n"


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    staging = tmp_path / "models" / "staging"
    staging.mkdir(parents=True)
    (staging / "stg_orders.sql").write_text(_MODEL_SQL)
    (staging / "_stg_models.yml").write_text(_SCHEMA_YML)
    return tmp_path


def _investigation(diagnosis: Diagnosis | None) -> Investigation:
    context = IncidentContext(
        event=FailureEvent(source=FailureSource.DBT, dbt_node_id="test.not_null_stg_orders_customer_id"),
        upstream_sources=["raw.orders"],
        upstream_models=["stg_orders"],
        lineage_files=[_MODEL_PATH],
        project_files={_MODEL_PATH: _MODEL_SQL, _SCHEMA_PATH: _SCHEMA_YML},
    )
    evidence = [
        CheckResult(
            request=CheckRequest(kind=CheckKind.NULL_RATE_BY_LOAD_DAY, relation="raw.orders", column="customer_id"),
            summary="null share jumped from 0% to 60% in the last 2 loads",
        )
    ]
    return Investigation(context=context, evidence=evidence, diagnosis=diagnosis)


def _null_spike(**overrides: object) -> Diagnosis:
    fields: dict[str, object] = {
        "category": RootCauseCategory.SOURCE_NULL_SPIKE,
        "confidence": 0.9,
        "culprit": Culprit(relation="raw.orders", column="customer_id"),
        "evidence": ["null share of customer_id jumped to 60%"],
    }
    return DiagnosisFactory.build(**(fields | overrides))


def _operational() -> FixProposal:
    return FixProposal(
        kind=FixKind.OPERATIONAL, title="Re-deliver orders", rationale="Upstream nulls.", operational_steps=["Page"]
    )


def _model_change(edits: list[FileEdit]) -> FixProposal:
    return FixProposal(kind=FixKind.MODEL_CHANGE, title="Patch", rationale="r", edits=edits)


def _request(diagnosis: Diagnosis | None, fix: FixProposal, *, budget: bool = True) -> JevRequest:
    return JevRequest(investigation=_investigation(diagnosis), fix=fix, can_reinvestigate=budget, can_revise=budget)


def test_grounded_diagnosis_with_fitting_remedy_proceeds(project_dir: Path) -> None:
    # Setup
    request = _request(_null_spike(), _operational())

    # Act
    verdict = RuleBasedJev(project_dir).review(request)

    # Assert
    assert verdict.decision is JevDecision.PROCEED, verdict.feedback()
    assert verdict.score == 1.0


@pytest.mark.parametrize(
    ("overrides", "failed_check"),
    [
        ({"culprit": Culprit(relation="raw.invoices")}, "culprit_in_lineage"),
        ({"confidence": 0.3}, "confidence_calibrated"),
        ({"evidence": []}, "evidence_grounded"),
        ({"category": RootCauseCategory.UNKNOWN}, "category_named"),
    ],
)
def test_unsupported_diagnosis_is_sent_back_to_investigate(
    project_dir: Path, overrides: dict[str, object], failed_check: str
) -> None:
    # Setup
    request = _request(_null_spike(**overrides), _operational())

    # Act
    verdict = RuleBasedJev(project_dir).review(request)

    # Assert
    assert verdict.decision is JevDecision.REINVESTIGATE
    assert failed_check in {check.name for check in verdict.failed_in(JevStage.JUDGE)}


def test_unsupported_diagnosis_escalates_once_rounds_are_exhausted(project_dir: Path) -> None:
    # Setup
    request = _request(_null_spike(confidence=0.2), _operational(), budget=False)

    # Act
    verdict = RuleBasedJev(project_dir).review(request)

    # Assert
    assert verdict.decision is JevDecision.ESCALATE


def test_unconverged_investigation_never_proceeds(project_dir: Path) -> None:
    # Setup
    request = _request(_null_spike(), _operational(), budget=False).model_copy(update={"converged": False})

    # Act
    verdict = RuleBasedJev(project_dir).review(request)

    # Assert
    assert verdict.decision is JevDecision.ESCALATE
    assert [check.name for check in verdict.failed] == ["investigation_converged"]


def test_code_regression_without_a_suspect_commit_is_not_grounded(project_dir: Path) -> None:
    # Setup
    diagnosis = _null_spike(category=RootCauseCategory.CODE_LOGIC_REGRESSION, culprit=Culprit(relation="stg_orders"))

    # Act
    verdict = RuleBasedJev(project_dir).review(_request(diagnosis, _operational()))

    # Assert
    assert "evidence_grounded" in {check.name for check in verdict.failed}


@pytest.mark.parametrize(
    ("fix", "failed_check"),
    [
        (
            _model_change([FileEdit(path=_SCHEMA_PATH, original="      - not_null\n", updated="")]),
            "tests_preserved",
        ),
        (
            _model_change(
                [FileEdit(path=_SCHEMA_PATH, original="- not_null", updated="- not_null:\n  severity: warn")]
            ),
            "tests_preserved",
        ),
        (
            _model_change(
                [
                    FileEdit(
                        path=_MODEL_PATH,
                        original="select id",
                        updated="{{ config(post_hook='delete from x') }}\nselect id",
                    )
                ]
            ),
            "no_write_sql",
        ),
        (_model_change([FileEdit(path=_MODEL_PATH, original="not in the file", updated="x")]), "patch_applies"),
        (_model_change([FileEdit(path="models/marts/other.sql", original="", updated="select 1")]), "edits_in_lineage"),
        (_model_change([]), "actionable"),
    ],
)
def test_unsafe_or_unsound_fix_is_sent_back_for_revision(
    project_dir: Path, fix: FixProposal, failed_check: str
) -> None:
    # Act
    verdict = RuleBasedJev(project_dir).review(_request(_null_spike(), fix))

    # Assert
    assert verdict.decision is JevDecision.REVISE_FIX
    assert failed_check in {check.name for check in verdict.failed}


def test_remedy_that_does_not_fit_the_cause_is_revised(project_dir: Path) -> None:
    # Setup
    diagnosis = _null_spike(category=RootCauseCategory.SOURCE_STALE, culprit=Culprit(relation="raw.orders"))
    fix = _model_change([FileEdit(path=_MODEL_PATH, original="status", updated="status as order_status")])

    # Act
    verdict = RuleBasedJev(project_dir).review(_request(diagnosis, fix))

    # Assert
    assert verdict.decision is JevDecision.REVISE_FIX
    assert [check.name for check in verdict.failed] == ["remedy_fits_cause"]


def test_bad_fix_escalates_when_no_revisions_remain(project_dir: Path) -> None:
    # Act
    verdict = RuleBasedJev(project_dir).review(_request(_null_spike(), _model_change([]), budget=False))

    # Assert
    assert verdict.decision is JevDecision.ESCALATE


class ScriptedJudgeClient:
    def __init__(self, opinion: JudgeOpinion | None) -> None:
        self.opinion = opinion
        self.calls = 0

    def generate[T](self, system: str, prompt: str, schema: type[T]) -> T:
        self.calls += 1
        if self.opinion is None:
            raise ReasonerError("model unavailable")
        return self.opinion  # type: ignore[return-value]


def test_llm_judge_can_veto_a_rule_pass(project_dir: Path) -> None:
    # Setup
    client = ScriptedJudgeClient(
        JudgeOpinion(
            diagnosis_supported=False,
            fix_addresses_cause=True,
            fix_is_safe=True,
            concerns=["nulls began before the deploy"],
        )
    )
    jev = LayeredJev(RuleBasedJev(project_dir), LLMJudge(client, "scripted"))

    # Act
    verdict = jev.review(_request(_null_spike(), _operational()))

    # Assert
    assert verdict.decision is JevDecision.REINVESTIGATE
    assert [check.name for check in verdict.failed] == ["llm_diagnosis_supported"]
    assert "nulls began before the deploy" in verdict.feedback()


def test_llm_judge_is_skipped_when_rules_already_fail(project_dir: Path) -> None:
    # Setup
    client = ScriptedJudgeClient(JudgeOpinion(diagnosis_supported=True, fix_addresses_cause=True, fix_is_safe=True))
    jev = LayeredJev(RuleBasedJev(project_dir), LLMJudge(client, "scripted"))

    # Act
    verdict = jev.review(_request(_null_spike(confidence=0.1), _operational()))

    # Assert
    assert verdict.decision is JevDecision.REINVESTIGATE
    assert client.calls == 0


def test_llm_judge_outage_keeps_the_rule_verdict(project_dir: Path) -> None:
    # Setup
    jev = LayeredJev(RuleBasedJev(project_dir), LLMJudge(ScriptedJudgeClient(None), "scripted"))

    # Act
    verdict = jev.review(_request(_null_spike(), _operational()))

    # Assert
    assert verdict.decision is JevDecision.PROCEED
    assert verdict.reviewer == "rules"


def _case(case_id: str, *, correct: bool, decision: JevDecision) -> CaseResult:
    return CaseResult(
        case_id=case_id,
        title=case_id,
        expected_category=RootCauseCategory.SOURCE_NULL_SPIKE,
        expected_culprit="raw.orders",
        has_distractor=False,
        fault_triggered=True,
        category_correct=correct,
        culprit_correct=correct,
        fix_valid=correct,
        jev_decision=decision,
    )


def test_eval_summary_scores_the_jev_gate() -> None:
    # Setup
    results = [
        _case("right_and_trusted", correct=True, decision=JevDecision.PROCEED),
        _case("wrong_but_trusted", correct=False, decision=JevDecision.PROCEED),
        _case("wrong_and_escalated", correct=False, decision=JevDecision.ESCALATE),
    ]

    # Act
    summary = summarize("heuristic", datetime.now(UTC), results)

    # Assert
    assert summary.jev_proceed_rate == pytest.approx(2 / 3, abs=1e-4)
    assert summary.jev_proceed_precision == 0.5
    assert summary.jev_catch_rate == 0.5


def test_eval_summary_has_no_catch_rate_without_wrong_diagnoses() -> None:
    # Act
    summary = summarize("heuristic", datetime.now(UTC), [_case("ok", correct=True, decision=JevDecision.PROCEED)])

    # Assert
    assert summary.jev_catch_rate is None
