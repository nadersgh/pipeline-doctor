import pytest

from pipeline_doctor.warehouse.base import ColumnSpec, QueryResult
from pipeline_doctor.warehouse.guard import (
    GuardedWarehouse,
    QueryBudget,
    QueryBudgetExceededError,
    SqlGuard,
    SqlGuardViolationError,
)


class RecordingWarehouse:
    def __init__(self, scanned_bytes: int | None = None) -> None:
        self.executed: list[str] = []
        self._scanned_bytes = scanned_bytes

    @property
    def dialect(self) -> str:
        return "duckdb"

    @property
    def database(self) -> str:
        return "warehouse"

    def execute(self, sql: str, max_rows: int, timeout_seconds: int) -> QueryResult:
        self.executed.append(sql)
        return QueryResult(columns=["n"], rows=[[1]], elapsed_seconds=0.0)

    def list_columns(self, schemas: list[str]) -> list[ColumnSpec]:
        return []

    def estimate_scanned_bytes(self, sql: str) -> int | None:
        return self._scanned_bytes


@pytest.fixture
def guard() -> SqlGuard:
    return SqlGuard("duckdb", allowed_databases=["warehouse"], allowed_schemas=["raw", "analytics"], max_rows=50)


@pytest.mark.parametrize(
    "sql",
    [
        "select count(*) from raw.orders",
        "with recent as (select * from raw.orders) select status, count(*) from recent group by 1",
        'select * from "warehouse"."raw"."orders" o join analytics.stg_orders s on o.id = s.order_id',
        "select 1 as a union all select 2",
        "select column_name from information_schema.columns",
    ],
)
def test_sql_guard_allows_read_only_queries_and_enforces_limit(guard: SqlGuard, sql: str) -> None:
    # Act
    safe = guard.enforce(sql)

    # Assert
    assert safe.endswith("LIMIT 50")


@pytest.mark.parametrize(
    "sql",
    [
        "delete from raw.orders",
        "update raw.orders set status = 'x'",
        "insert into raw.orders select * from raw.orders",
        "drop table raw.orders",
        "create table raw.copy as select * from raw.orders",
        "select * from raw.orders; drop table raw.orders",
        "select * into raw.copy from raw.orders",
        "copy raw.orders to 'orders.csv'",
        "attach 'other.duckdb'",
        "pragma database_list",
        "select * from read_csv('/etc/passwd')",
        "select read_text('/etc/passwd')",
        "select * from glob('*')",
        "select * from secrets.api_keys",
        "select * from orders",
        "select * from other_db.raw.orders",
        "select raw.my_udf(id) from raw.orders",
        "select exfiltrate(id) from raw.orders",
        "select * from parquet_scan('data.parquet')",
        "select iceberg_scan('table')",
        "select sqlite_scan('app.db', 'users')",
        "select http_get('http://internal.example')",
        "select current_setting('s3_secret_access_key')",
        "select * from duckdb_settings()",
        "select * from duckdb_secrets()",
        "select md5(getenv('HOME'))",
        "select load_extension('httpfs')",
        "select * from 'orders.csv'",
        "not even sql (",
    ],
)
def test_sql_guard_blocks_writes_file_access_and_unlisted_schemas(guard: SqlGuard, sql: str) -> None:
    with pytest.raises(SqlGuardViolationError):
        guard.enforce(sql)


def test_guarded_warehouse_blocks_queries_once_budget_is_spent(guard: SqlGuard) -> None:
    # Setup
    warehouse = RecordingWarehouse()
    guarded = GuardedWarehouse(
        warehouse, guard, QueryBudget(max_queries=2, max_bytes_scanned=10, timeout_seconds=5), 50
    )
    guarded.query("select 1 from raw.orders")
    guarded.query("select 2 from raw.orders")

    # Act
    with pytest.raises(QueryBudgetExceededError):
        guarded.query("select 3 from raw.orders")

    # Assert
    assert len(warehouse.executed) == 2
    assert guarded.budget.queries_blocked == 1


def test_guarded_warehouse_rejects_queries_over_the_scan_budget(guard: SqlGuard) -> None:
    # Setup
    warehouse = RecordingWarehouse(scanned_bytes=5_000)
    guarded = GuardedWarehouse(
        warehouse, guard, QueryBudget(max_queries=10, max_bytes_scanned=1_000, timeout_seconds=5), 50
    )

    # Act
    with pytest.raises(QueryBudgetExceededError):
        guarded.query("select * from raw.orders")

    # Assert
    assert warehouse.executed == []


def test_guarded_warehouse_never_forwards_rejected_sql(guard: SqlGuard) -> None:
    # Setup
    warehouse = RecordingWarehouse()
    guarded = GuardedWarehouse(
        warehouse, guard, QueryBudget(max_queries=5, max_bytes_scanned=10, timeout_seconds=5), 50
    )

    # Act
    with pytest.raises(SqlGuardViolationError):
        guarded.query("delete from raw.orders")

    # Assert
    assert warehouse.executed == []
    assert guarded.budget.queries_run == 0


@pytest.mark.parametrize(
    "sql",
    [
        "select get_presigned_url(@raw.stage, 'file.csv')",
        "select * from @raw.stage",
        "select other_db.raw.seq.nextval",
        "select raw.seq.nextval from raw.orders",
        "select * from other_db.raw.orders",
    ],
)
def test_sql_guard_blocks_snowflake_stages_sequences_and_other_databases(sql: str) -> None:
    guard = SqlGuard("snowflake", allowed_databases=["analytics_db"], allowed_schemas=["raw"], max_rows=10)
    with pytest.raises(SqlGuardViolationError):
        guard.enforce(sql)


def test_sql_guard_allows_the_configured_database() -> None:
    guard = SqlGuard("snowflake", allowed_databases=["analytics_db"], allowed_schemas=["raw"], max_rows=10)
    assert guard.enforce("select count(*) from analytics_db.raw.orders").endswith("LIMIT 10")
