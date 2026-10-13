# Held-out faults: new tables, columns, and mechanisms. Never tune a reasoner, prompt, or JEV rule against these
# cases; once a change is made to pass one, it belongs in the dev split and a fresh holdout case must replace it.
from pipeline_doctor.evals.cases import EvalCase, EvalSplit, FileChange, InjectedCommit
from pipeline_doctor.incidents.models import FixKind, RootCauseCategory


def _commit(message: str, change: FileChange) -> list[InjectedCommit]:
    return [InjectedCommit(message=message, author="Jordan Okafor <jordan.okafor@example.com>", changes=[change])]


_HOLDOUT_CASES: list[EvalCase] = [
    EvalCase(
        case_id="holdout_filter_drops_real_customers",
        title="stg_customers filter meant for test accounts drops real customers",
        category=RootCauseCategory.CODE_LOGIC_REGRESSION,
        culprit_relation="stg_customers",
        accepted_fix_kinds={FixKind.MODEL_CHANGE},
        commits=_commit(
            "chore: exclude internal test accounts from customer models",
            FileChange(
                path="models/staging/stg_customers.sql",
                original="    from source\n)",
                updated="    from source\n    where id > 25\n)",
            ),
        ),
    ),
    EvalCase(
        case_id="holdout_late_arriving_customers",
        title="Latest orders reference customers whose rows have not landed yet",
        category=RootCauseCategory.SOURCE_MISSING_ROWS,
        culprit_relation="raw.customers",
        accepted_fix_kinds={FixKind.OPERATIONAL},
        data_sql=[
            "insert into raw.orders select id + 100000, 401 + id % 6, order_date, status, _loaded_at "
            "from raw.orders where _loaded_at = (select max(_loaded_at) from raw.orders)"
        ],
    ),
    EvalCase(
        case_id="holdout_model_column_renamed",
        title="stg_orders renames order_date, fct_orders still selects it",
        category=RootCauseCategory.CODE_SQL_ERROR,
        culprit_relation="stg_orders",
        accepted_fix_kinds={FixKind.MODEL_CHANGE},
        commits=_commit(
            "refactor: align order date naming with the events schema",
            FileChange(
                path="models/staging/stg_orders.sql",
                original="cast(order_date as date) as order_date,",
                updated="cast(order_date as date) as ordered_on,",
            ),
        ),
    ),
    EvalCase(
        case_id="holdout_type_customers_id_prefixed",
        title="raw.customers.id now arrives as a prefixed string",
        category=RootCauseCategory.SOURCE_TYPE_CHANGED,
        culprit_relation="raw.customers",
        culprit_column="id",
        accepted_fix_kinds={FixKind.MODEL_CHANGE, FixKind.SCHEMA_PATCH},
        data_sql=["alter table raw.customers alter id type varchar using 'cus_' || cast(id as varchar)"],
    ),
    EvalCase(
        case_id="holdout_grain_change_order_payments",
        title="int_order_payments groups by order and method, changing its grain",
        category=RootCauseCategory.CODE_JOIN_FANOUT,
        culprit_relation="int_order_payments",
        accepted_fix_kinds={FixKind.MODEL_CHANGE},
        commits=_commit(
            "feat: break down order payments by method",
            FileChange(
                path="models/intermediate/int_order_payments.sql",
                original="    group by order_id\n",
                updated="    group by order_id, payment_method\n",
            ),
        ),
    ),
    EvalCase(
        case_id="holdout_cdc_updates_appended_customers",
        title="raw.customers receives CDC updates as appended rows",
        category=RootCauseCategory.SOURCE_DUPLICATE_ROWS,
        culprit_relation="raw.customers",
        culprit_column="id",
        accepted_fix_kinds={FixKind.MODEL_CHANGE, FixKind.SCHEMA_PATCH, FixKind.OPERATIONAL},
        data_sql=[
            "insert into raw.customers select id, first_name, last_name, replace(email, '@example.com', "
            "'@example.org'), created_at, (select max(_loaded_at) from raw.customers) + interval 1 hour "
            "from raw.customers where id % 17 = 0"
        ],
    ),
    EvalCase(
        case_id="holdout_payment_method_uppercased",
        title="raw.payments.payment_method arrives upper-cased in the latest batch",
        category=RootCauseCategory.SOURCE_NEW_CATEGORY,
        culprit_relation="raw.payments",
        culprit_column="payment_method",
        accepted_fix_kinds={FixKind.TEST_CHANGE, FixKind.MODEL_CHANGE, FixKind.OPERATIONAL},
        data_sql=[
            "update raw.payments set payment_method = upper(payment_method) "
            "where _loaded_at >= (select max(_loaded_at) from raw.payments) - interval 1 day"
        ],
    ),
]


def list_holdout_cases() -> list[EvalCase]:
    return [case.model_copy(update={"split": EvalSplit.HOLDOUT}) for case in _HOLDOUT_CASES]
