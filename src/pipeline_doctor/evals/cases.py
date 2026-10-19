from enum import StrEnum

from pydantic import BaseModel, Field

from pipeline_doctor.incidents.models import FixKind, RootCauseCategory

_RECENT = "_loaded_at >= (select max(_loaded_at) from {table}) - interval 1 day"
_CODE_FIX = {FixKind.MODEL_CHANGE, FixKind.SCHEMA_PATCH}
_OPERATIONAL = {FixKind.OPERATIONAL}


class FileChange(BaseModel):
    path: str
    original: str
    updated: str


class InjectedCommit(BaseModel):
    message: str
    author: str = "Sam Rivera <sam.rivera@example.com>"
    changes: list[FileChange]


class DistractorKind(StrEnum):
    DOCS = "docs"
    COMMENT = "comment"
    UNRELATED_SQL = "unrelated_sql"


class EvalSplit(StrEnum):
    DEV = "dev"
    HOLDOUT = "holdout"


class EvalCaseKind(StrEnum):
    SINGLE = "single"
    COMPOUND = "compound"
    NO_FAULT = "no_fault"


class InfraFailureKind(StrEnum):
    WAREHOUSE_LOCK = "warehouse_lock"
    WORKER_KILLED = "worker_killed"
    NETWORK_TIMEOUT = "network_timeout"
    HEARTBEAT_LOST = "heartbeat_lost"


class EvalCase(BaseModel):
    """One synthetic broken pipeline and the ground truth a correct diagnosis must match."""

    case_id: str
    title: str
    category: RootCauseCategory
    culprit_relation: str
    culprit_column: str | None = None
    accepted_fix_kinds: set[FixKind]
    data_sql: list[str] = Field(default_factory=list)
    commits: list[InjectedCommit] = Field(default_factory=list)
    distractor: DistractorKind | None = None
    split: EvalSplit = EvalSplit.DEV
    components: list["EvalCase"] = Field(
        default_factory=list, description="Simultaneous faults of a compound case; diagnosing any one is a hit."
    )
    infra_failure: InfraFailureKind | None = Field(
        default=None, description="Pipeline fails for an operational reason outside the project; no fault exists."
    )

    @property
    def kind(self) -> EvalCaseKind:
        if self.infra_failure is not None:
            return EvalCaseKind.NO_FAULT
        return EvalCaseKind.COMPOUND if self.components else EvalCaseKind.SINGLE

    def acceptable_causes(self) -> list["EvalCase"]:
        return self.components or [self]


_DISTRACTOR_COMMITS: dict[DistractorKind, InjectedCommit] = {
    DistractorKind.DOCS: InjectedCommit(
        message="docs: clarify staging model descriptions",
        author="Priya Shah <priya.shah@example.com>",
        changes=[
            FileChange(
                path="models/staging/_stg_models.yml",
                original="description: One row per order.",
                updated="description: One row per order placed on the storefront, including test orders.",
            ),
            FileChange(
                path="models/staging/_sources.yml",
                original="description: Orders from the storefront database.",
                updated="description: Orders from the storefront database (replicated hourly, batched daily).",
            ),
        ],
    ),
    DistractorKind.COMMENT: InjectedCommit(
        message="chore: add ownership headers to models",
        author="Priya Shah <priya.shah@example.com>",
        changes=[
            FileChange(
                path=f"models/{path}",
                original="with ",
                updated="-- owner: analytics-engineering, sla: 07:00 UTC\nwith ",
            )
            for path in [
                "staging/stg_orders.sql",
                "staging/stg_payments.sql",
                "staging/stg_customers.sql",
                "intermediate/int_order_payments.sql",
            ]
        ],
    ),
    DistractorKind.UNRELATED_SQL: InjectedCommit(
        message="feat: expose customer signup date in dim_customers",
        author="Priya Shah <priya.shah@example.com>",
        changes=[
            FileChange(
                path="models/marts/dim_customers.sql",
                original="        customers.email,\n",
                updated="        customers.email,\n        customers.created_at as signed_up_at,\n",
            )
        ],
    ),
}


def distractor_commit(kind: DistractorKind) -> InjectedCommit:
    return _DISTRACTOR_COMMITS[kind]


def _recent(table: str) -> str:
    return _RECENT.format(table=table)


def _renamed(case_id: str, table: str, old: str, new: str) -> EvalCase:
    return EvalCase(
        case_id=case_id,
        title=f"raw.{table}.{old} renamed to {new} upstream",
        category=RootCauseCategory.SOURCE_COLUMN_RENAMED,
        culprit_relation=f"raw.{table}",
        culprit_column=old,
        accepted_fix_kinds=_CODE_FIX,
        data_sql=[f"alter table raw.{table} rename column {old} to {new}"],
    )


def _null_spike(case_id: str, table: str, column: str) -> EvalCase:
    return EvalCase(
        case_id=case_id,
        title=f"Recent raw.{table} loads have null {column}",
        category=RootCauseCategory.SOURCE_NULL_SPIKE,
        culprit_relation=f"raw.{table}",
        culprit_column=column,
        accepted_fix_kinds=_OPERATIONAL | _CODE_FIX,
        data_sql=[f"update raw.{table} set {column} = null where {_recent(f'raw.{table}')} and id % 5 < 3"],
    )


def _duplicates(case_id: str, table: str, columns: str) -> EvalCase:
    return EvalCase(
        case_id=case_id,
        title=f"Latest raw.{table} batch replayed",
        category=RootCauseCategory.SOURCE_DUPLICATE_ROWS,
        culprit_relation=f"raw.{table}",
        culprit_column="id",
        accepted_fix_kinds=_CODE_FIX | _OPERATIONAL,
        data_sql=[
            f"insert into raw.{table} select {columns}, _loaded_at + interval 2 hour from raw.{table} "
            f"where {_recent(f'raw.{table}')}"
        ],
    )


def _stale(case_id: str, table: str) -> EvalCase:
    return EvalCase(
        case_id=case_id,
        title=f"raw.{table} ingestion stopped three days ago",
        category=RootCauseCategory.SOURCE_STALE,
        culprit_relation=f"raw.{table}",
        accepted_fix_kinds=_OPERATIONAL,
        data_sql=[f"update raw.{table} set _loaded_at = _loaded_at - interval 3 day"],
    )


def _new_category(case_id: str, table: str, column: str, value: str, where: str) -> EvalCase:
    return EvalCase(
        case_id=case_id,
        title=f"raw.{table}.{column} started emitting '{value}'",
        category=RootCauseCategory.SOURCE_NEW_CATEGORY,
        culprit_relation=f"raw.{table}",
        culprit_column=column,
        accepted_fix_kinds={FixKind.TEST_CHANGE, FixKind.MODEL_CHANGE, FixKind.OPERATIONAL},
        data_sql=[f"update raw.{table} set {column} = '{value}' where {where}"],
    )


def _missing_rows(case_id: str, table: str, where: str) -> EvalCase:
    return EvalCase(
        case_id=case_id,
        title=f"Rows deleted from raw.{table}",
        category=RootCauseCategory.SOURCE_MISSING_ROWS,
        culprit_relation=f"raw.{table}",
        accepted_fix_kinds=_OPERATIONAL,
        data_sql=[f"delete from raw.{table} where {where}"],
    )


def _code(
    case_id: str, title: str, category: RootCauseCategory, *, model: str, message: str, change: FileChange
) -> EvalCase:
    return EvalCase(
        case_id=case_id,
        title=title,
        category=category,
        culprit_relation=model,
        accepted_fix_kinds={FixKind.MODEL_CHANGE},
        commits=[InjectedCommit(message=message, changes=[change])],
    )


_BASE_CASES: list[EvalCase] = [
    _renamed("renamed_orders_status", "orders", "status", "order_status"),
    _renamed("renamed_payments_method", "payments", "payment_method", "method"),
    _renamed("renamed_customers_first_name", "customers", "first_name", "given_name"),
    EvalCase(
        case_id="type_payments_amount_formatted",
        title="raw.payments.amount now arrives as a formatted string",
        category=RootCauseCategory.SOURCE_TYPE_CHANGED,
        culprit_relation="raw.payments",
        culprit_column="amount",
        accepted_fix_kinds=_CODE_FIX,
        data_sql=["alter table raw.payments alter amount type varchar using format('{:,}', amount)"],
    ),
    EvalCase(
        case_id="type_orders_date_us_format",
        title="raw.orders.order_date now arrives as MM/DD/YYYY text",
        category=RootCauseCategory.SOURCE_TYPE_CHANGED,
        culprit_relation="raw.orders",
        culprit_column="order_date",
        accepted_fix_kinds=_CODE_FIX,
        data_sql=["alter table raw.orders alter order_date type varchar using strftime(order_date, '%m/%d/%Y')"],
    ),
    _null_spike("nulls_orders_customer_id", "orders", "customer_id"),
    _null_spike("nulls_payments_order_id", "payments", "order_id"),
    _null_spike("nulls_customers_email", "customers", "email"),
    _duplicates("duplicates_orders", "orders", "id, customer_id, order_date, status"),
    _duplicates("duplicates_payments", "payments", "id, order_id, payment_method, amount"),
    _duplicates("duplicates_customers", "customers", "id, first_name, last_name, email, created_at"),
    _stale("stale_orders", "orders"),
    _stale("stale_payments", "payments"),
    _stale("stale_customers", "customers"),
    _new_category(
        "new_status_refund_pending", "orders", "status", "refund_pending", "status = 'returned' and id % 3 = 0"
    ),
    _new_category(
        "new_payment_method_crypto", "payments", "payment_method", "crypto", f"{_recent('raw.payments')} and id % 4 = 0"
    ),
    _new_category(
        "new_status_on_hold", "orders", "status", "on_hold", f"{_recent('raw.orders')} and status = 'placed'"
    ),
    _missing_rows("missing_customers", "customers", "id % 9 = 0 and id < 300"),
    _missing_rows("missing_orders", "orders", "id % 13 = 0"),
    _code(
        "code_typo_column",
        "Typo in a column name in stg_payments",
        RootCauseCategory.CODE_SQL_ERROR,
        model="stg_payments",
        message="fix: tidy up payment casting",
        change=FileChange(
            path="models/staging/stg_payments.sql",
            original="cast(amount as integer)",
            updated="cast(ammount as integer)",
        ),
    ),
    _code(
        "code_missing_comma",
        "Missing comma in fct_orders",
        RootCauseCategory.CODE_SQL_ERROR,
        model="fct_orders",
        message="refactor: reorder fct_orders columns",
        change=FileChange(path="models/marts/fct_orders.sql", original="orders.status,\n", updated="orders.status\n"),
    ),
    _code(
        "code_bad_ref",
        "int_order_payments references a model that does not exist",
        RootCauseCategory.CODE_SQL_ERROR,
        model="int_order_payments",
        message="refactor: point intermediate model at renamed staging model",
        change=FileChange(
            path="models/intermediate/int_order_payments.sql",
            original="ref('stg_payments')",
            updated="ref('stg_payment')",
        ),
    ),
    _code(
        "code_fanout_fct_orders",
        "fct_orders joins raw payments instead of the per-order aggregate",
        RootCauseCategory.CODE_JOIN_FANOUT,
        model="fct_orders",
        message="perf: read payments directly instead of the intermediate view",
        change=FileChange(
            path="models/marts/fct_orders.sql",
            original="    select * from {{ ref('int_order_payments') }}\n",
            updated=(
                "    select\n"
                "        order_id,\n"
                "        amount as total_amount,\n"
                "        1 as payment_count,\n"
                "        case when payment_method = 'credit_card' then amount else 0 end as credit_card_amount,\n"
                "        case when payment_method = 'coupon' then amount else 0 end as coupon_amount\n"
                "    from {{ ref('stg_payments') }}\n"
            ),
        ),
    ),
    _code(
        "code_fanout_dim_customers",
        "dim_customers stops aggregating orders per customer",
        RootCauseCategory.CODE_JOIN_FANOUT,
        model="dim_customers",
        message="refactor: simplify customer order metrics",
        change=FileChange(
            path="models/marts/dim_customers.sql",
            original=(
                "        min(order_date) as first_order_date,\n"
                "        max(order_date) as most_recent_order_date,\n"
                "        count(order_id) as number_of_orders,\n"
                "        sum(amount) as lifetime_value\n"
                "    from orders\n"
                "    group by customer_id\n"
            ),
            updated=(
                "        order_date as first_order_date,\n"
                "        order_date as most_recent_order_date,\n"
                "        1 as number_of_orders,\n"
                "        amount as lifetime_value\n"
                "    from orders\n"
            ),
        ),
    ),
    _code(
        "code_cents_not_converted",
        "stg_payments stops converting cents to dollars",
        RootCauseCategory.CODE_LOGIC_REGRESSION,
        model="stg_payments",
        message="chore: keep payment amounts in cents for precision",
        change=FileChange(
            path="models/staging/stg_payments.sql",
            original="cast(amount as integer) / 100.0 as amount",
            updated="cast(amount as integer) as amount",
        ),
    ),
    _code(
        "code_dropped_coalesce",
        "fct_orders drops the coalesce on order amount",
        RootCauseCategory.CODE_LOGIC_REGRESSION,
        model="fct_orders",
        message="refactor: drop redundant coalesce",
        change=FileChange(
            path="models/marts/fct_orders.sql",
            original="coalesce(order_payments.total_amount, 0) as amount",
            updated="order_payments.total_amount as amount",
        ),
    ),
]

_HARD_VARIANTS: dict[str, DistractorKind] = {
    "renamed_orders_status": DistractorKind.COMMENT,
    "type_payments_amount_formatted": DistractorKind.DOCS,
    "nulls_orders_customer_id": DistractorKind.COMMENT,
    "duplicates_payments": DistractorKind.UNRELATED_SQL,
    "stale_orders": DistractorKind.DOCS,
    "new_payment_method_crypto": DistractorKind.COMMENT,
    "missing_customers": DistractorKind.UNRELATED_SQL,
    "code_typo_column": DistractorKind.DOCS,
    "code_fanout_fct_orders": DistractorKind.COMMENT,
    "code_dropped_coalesce": DistractorKind.UNRELATED_SQL,
}


def list_eval_cases() -> list[EvalCase]:
    """The 10 fault categories as 26 base cases, 10 hard variants that add a misleading recent commit, and
    3 adversarial variants that carry a prompt-injection payload."""
    from pipeline_doctor.evals.adversarial import list_adversarial_cases  # noqa: PLC0415 - it imports this module

    by_id = {case.case_id: case for case in _BASE_CASES}
    hard = [
        by_id[case_id].model_copy(
            update={
                "case_id": f"{case_id}__{kind}",
                "title": f"{by_id[case_id].title} (+{kind} distractor)",
                "distractor": kind,
            }
        )
        for case_id, kind in _HARD_VARIANTS.items()
    ]
    return [*_BASE_CASES, *hard, *list_adversarial_cases(by_id)]
