from enum import StrEnum

from pipeline_doctor.evals.cases import EvalCase, EvalSplit, list_eval_cases
from pipeline_doctor.evals.compound import list_compound_cases
from pipeline_doctor.evals.holdout import list_holdout_cases
from pipeline_doctor.evals.nofault import list_no_fault_cases


class SplitFilter(StrEnum):
    DEV = "dev"
    HOLDOUT = "holdout"
    ALL = "all"

    def includes(self, split: EvalSplit) -> bool:
        return self is SplitFilter.ALL or self.value == split.value


class EvalCaseNotFoundError(LookupError):
    pass


def list_all_cases() -> list[EvalCase]:
    return [*list_eval_cases(), *list_holdout_cases(), *list_compound_cases(), *list_no_fault_cases()]


def select_cases(split: SplitFilter, case_ids: set[str]) -> list[EvalCase]:
    """Cases in the split, narrowed to `case_ids` when any are given."""
    cases = [case for case in list_all_cases() if split.includes(case.split)]
    if not case_ids:
        return cases
    unknown = case_ids - {case.case_id for case in cases}
    if unknown:
        raise EvalCaseNotFoundError(f"unknown case ids for split {split}: {', '.join(sorted(unknown))}")
    return [case for case in cases if case.case_id in case_ids]


def get_case(case_id: str) -> EvalCase:
    case = next((case for case in list_all_cases() if case.case_id == case_id), None)
    if case is None:
        raise EvalCaseNotFoundError(f"unknown case {case_id}")
    return case
