from pipeline_doctor.evals.cases import EvalCase, EvalSplit, list_eval_cases

# Two faults injected together. The first component is the primary ground truth; naming either one is a primary hit.
_COMPOUNDS: list[tuple[str, str, str]] = [
    ("compound_stale_payments_and_bad_ref", "stale_payments", "code_bad_ref"),
    ("compound_null_customer_ids_and_cents", "nulls_orders_customer_id", "code_cents_not_converted"),
    ("compound_renamed_first_name_and_duplicate_payments", "renamed_customers_first_name", "duplicates_payments"),
]


def _combine(case_id: str, first: EvalCase, second: EvalCase) -> EvalCase:
    return first.model_copy(
        update={
            "case_id": case_id,
            "title": f"{first.title} + {second.title}",
            "data_sql": [*first.data_sql, *second.data_sql],
            "commits": [*first.commits, *second.commits],
            "split": EvalSplit.HOLDOUT,
            "components": [first, second],
        }
    )


def list_compound_cases() -> list[EvalCase]:
    by_id = {case.case_id: case for case in list_eval_cases()}
    return [_combine(case_id, by_id[first], by_id[second]) for case_id, first, second in _COMPOUNDS]
