from pipeline_doctor.evals.cases import EvalCase, EvalCaseKind
from pipeline_doctor.evals.report import CaseResult
from pipeline_doctor.incidents.models import Diagnosis, FixKind, FixProposal, Incident, RootCauseCategory


class IncidentScorer:
    """Scores a diagnosis against a case's ground truth, crediting category, relation, column, and fix separately."""

    def __init__(self, min_confidence: float) -> None:
        self._min_confidence = min_confidence

    def score(self, base: CaseResult, case: EvalCase, incident: Incident) -> CaseResult:
        diagnosis = incident.diagnosis
        common = {
            "fix_kind": incident.fix.kind if incident.fix else None,
            "jev_decision": incident.verdict.decision if incident.verdict else None,
            "jev_score": incident.verdict.score if incident.verdict else None,
            "error": incident.error,
            **_metrics(incident),
        }
        if diagnosis is None:
            false_positive = is_code_change(incident.fix) if case.kind is EvalCaseKind.NO_FAULT else None
            return base.model_copy(update={**common, "false_positive": false_positive})
        common |= {
            "predicted_category": diagnosis.category,
            "predicted_culprit": diagnosis.culprit.describe(),
            "confidence": diagnosis.confidence,
        }
        if case.kind is EvalCaseKind.NO_FAULT:
            return base.model_copy(update={**common, **self._score_no_fault(diagnosis, incident.fix)})
        return base.model_copy(update={**common, **_score_fault(case, diagnosis, incident.fix)})

    def _score_no_fault(self, diagnosis: Diagnosis, fix: FixProposal | None) -> dict[str, object]:
        abstained = diagnosis.category is RootCauseCategory.UNKNOWN
        confident_culprit = not abstained and diagnosis.confidence >= self._min_confidence
        return {
            "category_correct": abstained,
            "relation_correct": abstained,
            "culprit_correct": abstained,
            "fix_kind_correct": not is_code_change(fix),
            "primary_hit": abstained,
            "false_positive": is_code_change(fix) or confident_culprit,
        }


def _score_fault(case: EvalCase, diagnosis: Diagnosis, fix: FixProposal | None) -> dict[str, object]:
    cause = match_cause(case, diagnosis)
    category_correct = diagnosis.category is cause.category
    relation_correct = same_relation(diagnosis.culprit.relation, cause.culprit_relation)
    column_correct = (
        None
        if cause.culprit_column is None
        else (diagnosis.culprit.column or "").lower() == cause.culprit_column.lower()
    )
    return {
        "matched_cause": cause.case_id if case.kind is EvalCaseKind.COMPOUND else None,
        "category_correct": category_correct,
        "relation_correct": relation_correct,
        "column_correct": column_correct,
        "culprit_correct": relation_correct and column_correct is not False,
        "fix_kind_correct": fix is not None and fix.kind in cause.accepted_fix_kinds,
        "primary_hit": category_correct and relation_correct,
    }


def match_cause(case: EvalCase, diagnosis: Diagnosis) -> EvalCase:
    """The acceptable root cause closest to the diagnosis; ties go to the primary (first) cause."""

    def closeness(cause: EvalCase) -> tuple[bool, bool, bool]:
        category = diagnosis.category is cause.category
        relation = same_relation(diagnosis.culprit.relation, cause.culprit_relation)
        return category and relation, category, relation

    return max(case.acceptable_causes(), key=closeness)


def is_code_change(fix: FixProposal | None) -> bool:
    return fix is not None and fix.kind is not FixKind.OPERATIONAL


def same_relation(predicted: str, expected: str) -> bool:
    predicted, expected = predicted.strip().replace('"', "").lower(), expected.lower()
    return predicted == expected or predicted.endswith(f".{expected}")


def _metrics(incident: Incident) -> dict[str, object]:
    metrics = incident.metrics
    return {
        "latency_seconds": metrics.latency_seconds,
        "cost_usd": metrics.cost_usd,
        "llm_calls": metrics.llm_calls,
        "queries_run": metrics.queries_run,
        "queries_blocked": metrics.queries_blocked,
    }
