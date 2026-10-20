from pipeline_doctor.evals.cases import EvalCase, EvalSplit
from pipeline_doctor.incidents.models import FixKind, RootCauseCategory

# A data fault that heals by itself before the next run. At diagnosis time the data really is wrong, so naming the
# missing rows is right; any code change is a false fix, because the control reruns pass without it.
_TRANSIENT_CASES: list[EvalCase] = [
    EvalCase(
        case_id="transient_late_customers_land",
        title="Latest orders reference customers whose rows land before the next run",
        category=RootCauseCategory.SOURCE_MISSING_ROWS,
        culprit_relation="raw.customers",
        accepted_fix_kinds={FixKind.OPERATIONAL},
        split=EvalSplit.HOLDOUT,
        data_sql=[
            "insert into raw.orders select id + 100000, 401 + id % 6, order_date, status, _loaded_at "
            "from raw.orders where _loaded_at = (select max(_loaded_at) from raw.orders)"
        ],
        heal_sql=[
            "insert into raw.customers select 400 + n, 'Late', 'Arrival' || n, 'late' || n || '@example.com', "
            "now(), now() from range(1, 7) t(n)"
        ],
    ),
    EvalCase(
        case_id="transient_replayed_customers_restored",
        title="Customer rows disappear during a reload and are restored by the replay before the next run",
        category=RootCauseCategory.SOURCE_MISSING_ROWS,
        culprit_relation="raw.customers",
        accepted_fix_kinds={FixKind.OPERATIONAL},
        split=EvalSplit.HOLDOUT,
        data_sql=[
            "create table raw.customers_held as select * from raw.customers where id <= 8",
            "delete from raw.customers where id <= 8",
        ],
        heal_sql=["insert into raw.customers select * from raw.customers_held", "drop table raw.customers_held"],
    ),
]


def list_transient_cases() -> list[EvalCase]:
    return list(_TRANSIENT_CASES)
