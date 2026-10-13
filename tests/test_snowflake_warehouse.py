import json
import sys
import types
from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest
import sqlglot
from pydantic import SecretStr, ValidationError
from sqlglot import exp

from pipeline_doctor.agent.factory import build_warehouse
from pipeline_doctor.dbt_tools.artifacts import ColumnInfo, Manifest, ManifestNode, ResourceType
from pipeline_doctor.dbt_tools.lineage import CatalogSchema, DbtLineage
from pipeline_doctor.diagnostics.checks import CheckKind, CheckRequest, DiagnosticRunner
from pipeline_doctor.diagnostics.schema_drift import detect_schema_drift
from pipeline_doctor.settings import Settings, WarehouseKind
from pipeline_doctor.warehouse.base import WarehouseQueryError
from pipeline_doctor.warehouse.guard import GuardedWarehouse, QueryBudget, QueryBudgetExceededError, SqlGuard
from pipeline_doctor.warehouse.snowflake_warehouse import SnowflakeConnectionSettings, SnowflakeWarehouse

type Row = tuple[Any, ...]

_INFORMATION_SCHEMA_COLUMNS = [
    "TABLE_CATALOG",
    "TABLE_SCHEMA",
    "TABLE_NAME",
    "COLUMN_NAME",
    "DATA_TYPE",
    "NUMERIC_PRECISION",
    "NUMERIC_SCALE",
]
_TIMESTAMP_COLUMNS = {"max_loaded_at", "first_seen", "last_loaded_at", "latest_duplicate_load"}


class FakeSnowflakeError(Exception):
    """Stands in for snowflake.connector.errors.ProgrammingError."""


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self._connection = connection
        self._rows: list[Row] = []
        self.description: list[tuple[str, str]] | None = None
        self.closed = False

    def execute(self, sql: str, params: list[str] | None = None) -> "FakeCursor":
        self._connection.statements.append(sql)
        self._connection.params.append(params)
        columns, self._rows = self._connection.respond(sql)
        self.description = [(column, "TEXT") for column in columns] or None
        return self

    def fetchmany(self, size: int) -> list[Row]:
        self._connection.fetch_sizes.append(size)
        return self._rows[:size]

    def fetchall(self) -> list[Row]:
        return list(self._rows)

    def fetchone(self) -> Row | None:
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    """Answers like Snowflake: upper-case column names, EXPLAIN USING JSON plans, information_schema rows.

    Without canned rows it synthesises one row per query from the guarded projection, which also proves the
    statement parses in the snowflake dialect.
    """

    def __init__(
        self,
        *,
        columns: list[str] | None = None,
        rows: list[Row] | None = None,
        plan: dict[str, Any] | None = None,
        information_schema_rows: list[Row] | None = None,
        error: Exception | None = None,
    ) -> None:
        self._columns = columns or []
        self._rows = rows
        self._plan = plan if plan is not None else {"GlobalStats": {"partitionsTotal": 1, "bytesAssigned": 0}}
        self._information_schema_rows = information_schema_rows or []
        self._error = error
        self.statements: list[str] = []
        self.params: list[list[str] | None] = []
        self.fetch_sizes: list[int] = []
        self.cursors: list[FakeCursor] = []

    @property
    def query_statements(self) -> list[str]:
        return [sql for sql in self.statements if not sql.lower().startswith(("alter session", "explain"))]

    def cursor(self) -> FakeCursor:
        cursor = FakeCursor(self)
        self.cursors.append(cursor)
        return cursor

    def respond(self, sql: str) -> tuple[list[str], list[Row]]:
        lowered = sql.lower()
        if lowered.startswith("alter session"):
            return [], []
        if self._error is not None:
            raise self._error
        if lowered.startswith("explain using json"):
            return ["content"], [(json.dumps(self._plan),)]
        if "information_schema.columns" in lowered:
            return _INFORMATION_SCHEMA_COLUMNS, self._information_schema_rows
        if self._rows is not None:
            return self._columns, self._rows
        return _synthesize_result(sql)


class FakeConnector:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.connection = FakeConnection(columns=["N"], rows=[(1,)])

    def connect(self, **kwargs: Any) -> FakeConnection:
        self.calls.append(kwargs)
        return self.connection


def _synthesize_result(sql: str) -> tuple[list[str], list[Row]]:
    statement = sqlglot.parse_one(sql, read="snowflake")
    guarded = next(subquery for subquery in statement.find_all(exp.Subquery) if subquery.alias == "guarded_query")
    names = [name.lower() for name in guarded.this.named_selects]
    return [name.upper() for name in names], [tuple(_synthesize_value(name) for name in names)]


def _synthesize_value(column: str) -> Any:
    if column == "load_day":
        return date(2026, 10, 12)
    if column in _TIMESTAMP_COLUMNS:
        return datetime(2026, 10, 12, 6, 0)
    if column == "value":
        return "shipped"
    return 1


@pytest.fixture
def connection_settings() -> SnowflakeConnectionSettings:
    return SnowflakeConnectionSettings(
        account="acme-xy12345",
        user="pipeline_doctor",
        password=SecretStr("not-a-real-password"),
        role="PIPELINE_DOCTOR_READONLY",
        warehouse="DOCTOR_WH",
        database="ANALYTICS_DB",
        statement_timeout_seconds=45,
    )


@pytest.fixture
def fake_connector(monkeypatch: pytest.MonkeyPatch) -> FakeConnector:
    connector = FakeConnector()
    module = types.ModuleType("snowflake.connector")
    module.connect = connector.connect  # type: ignore[attr-defined]
    package = types.ModuleType("snowflake")
    package.connector = module  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "snowflake", package)
    monkeypatch.setitem(sys.modules, "snowflake.connector", module)
    return connector


@pytest.fixture
def snowflake_guard() -> SqlGuard:
    return SqlGuard("snowflake", allowed_databases=["ANALYTICS_DB"], allowed_schemas=["raw"], max_rows=50)


def test_snowflake_connect_uses_the_read_only_role_and_session_parameters(
    fake_connector: FakeConnector, connection_settings: SnowflakeConnectionSettings
) -> None:
    # Setup
    warehouse = SnowflakeWarehouse(connection_settings)

    # Act
    warehouse.execute("select 1 as n", max_rows=10, timeout_seconds=5)

    # Assert
    [call] = fake_connector.calls
    assert call["role"] == "PIPELINE_DOCTOR_READONLY"
    assert (call["account"], call["user"], call["warehouse"], call["database"]) == (
        "acme-xy12345",
        "pipeline_doctor",
        "DOCTOR_WH",
        "ANALYTICS_DB",
    )
    assert call["password"] == "not-a-real-password"
    assert call["session_parameters"] == {
        "QUERY_TAG": "pipeline-doctor",
        "AUTOCOMMIT": False,
        "STATEMENT_TIMEOUT_IN_SECONDS": 45,
    }


def test_snowflake_connects_once_and_reuses_the_session(
    fake_connector: FakeConnector, connection_settings: SnowflakeConnectionSettings
) -> None:
    # Setup
    warehouse = SnowflakeWarehouse(connection_settings)

    # Act
    warehouse.execute("select 1 as n", max_rows=10, timeout_seconds=5)
    warehouse.estimate_scanned_bytes("select 1 as n")
    warehouse.list_columns(["raw"])

    # Assert
    assert len(fake_connector.calls) == 1


def test_build_warehouse_connects_to_snowflake_with_the_default_read_only_role(
    fake_connector: FakeConnector, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Setup
    monkeypatch.delenv("PIPELINE_DOCTOR_SNOWFLAKE_ROLE", raising=False)
    settings = Settings(
        warehouse=WarehouseKind.SNOWFLAKE,
        snowflake_account="acme-xy12345",
        snowflake_user="pipeline_doctor",
        snowflake_password=SecretStr("not-a-real-password"),
        snowflake_warehouse="DOCTOR_WH",
        snowflake_database="ANALYTICS_DB",
        query_timeout_seconds=12,
        _env_file=None,
    )

    # Act
    warehouse = build_warehouse(settings)
    warehouse.execute("select 1 as n", max_rows=10, timeout_seconds=12)

    # Assert
    assert (warehouse.dialect, warehouse.database) == ("snowflake", "ANALYTICS_DB")
    [call] = fake_connector.calls
    assert call["role"] == "PIPELINE_DOCTOR_READONLY"
    assert call["session_parameters"]["STATEMENT_TIMEOUT_IN_SECONDS"] == 12


def test_snowflake_settings_reject_an_empty_role(connection_settings: SnowflakeConnectionSettings) -> None:
    with pytest.raises(ValidationError):
        SnowflakeConnectionSettings.model_validate({**connection_settings.model_dump(), "role": ""})


def test_snowflake_execute_sets_the_statement_timeout_before_each_query(
    connection_settings: SnowflakeConnectionSettings,
) -> None:
    # Setup
    connection = FakeConnection(columns=["N"], rows=[(1,)])
    warehouse = SnowflakeWarehouse(connection_settings, connection)

    # Act
    warehouse.execute("select 1 as n", max_rows=10, timeout_seconds=7)

    # Assert
    assert connection.statements == ["alter session set statement_timeout_in_seconds = 7", "select 1 as n"]
    assert all(cursor.closed for cursor in connection.cursors)


@pytest.mark.parametrize(
    ("available_rows", "expected_rows", "expected_truncated"),
    [(5, 3, True), (3, 3, False), (0, 0, False)],
)
def test_snowflake_execute_limits_rows_and_flags_truncation(
    connection_settings: SnowflakeConnectionSettings,
    available_rows: int,
    expected_rows: int,
    expected_truncated: bool,
) -> None:
    # Setup
    connection = FakeConnection(columns=["N"], rows=[(index,) for index in range(available_rows)])
    warehouse = SnowflakeWarehouse(connection_settings, connection)

    # Act
    result = warehouse.execute("select n from raw.numbers", max_rows=3, timeout_seconds=5)

    # Assert
    assert len(result.rows) == expected_rows
    assert result.truncated is expected_truncated
    assert connection.fetch_sizes == [4]


def test_snowflake_execute_lowercases_column_names_and_converts_values(
    connection_settings: SnowflakeConnectionSettings,
) -> None:
    # Setup
    connection = FakeConnection(
        columns=["ORDER_ID", "AMOUNT", "LOADED_AT", "Quoted_Alias"],
        rows=[(7, Decimal("9.50"), datetime(2026, 10, 12, 6, 0), "x")],
    )
    warehouse = SnowflakeWarehouse(connection_settings, connection)

    # Act
    result = warehouse.execute("select * from raw.orders", max_rows=10, timeout_seconds=5)

    # Assert
    assert result.as_records() == [
        {"order_id": 7, "amount": 9.5, "loaded_at": "2026-10-12T06:00:00", "quoted_alias": "x"}
    ]


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(lambda warehouse: warehouse.execute("select 1", 10, 5), id="execute"),
        pytest.param(lambda warehouse: warehouse.estimate_scanned_bytes("select 1"), id="explain"),
        pytest.param(lambda warehouse: warehouse.list_columns(["raw"]), id="list_columns"),
    ],
)
def test_snowflake_errors_are_mapped_to_warehouse_query_error(
    connection_settings: SnowflakeConnectionSettings, operation: Callable[[SnowflakeWarehouse], object]
) -> None:
    # Setup
    connection = FakeConnection(error=FakeSnowflakeError("002003 (42S02): SQL compilation error: does not exist"))
    warehouse = SnowflakeWarehouse(connection_settings, connection)

    # Act
    with pytest.raises(WarehouseQueryError, match="SQL compilation error") as raised:
        operation(warehouse)

    # Assert
    assert isinstance(raised.value.__cause__, FakeSnowflakeError)
    assert all(cursor.closed for cursor in connection.cursors)


def test_snowflake_estimate_reads_bytes_assigned_from_the_json_plan(
    connection_settings: SnowflakeConnectionSettings,
) -> None:
    # Setup
    connection = FakeConnection(plan={"GlobalStats": {"partitionsTotal": 8, "bytesAssigned": 123_456}})
    warehouse = SnowflakeWarehouse(connection_settings, connection)

    # Act
    estimated = warehouse.estimate_scanned_bytes("select * from raw.orders")

    # Assert
    assert estimated == 123_456
    assert connection.statements == ["explain using json select * from raw.orders"]


def test_snowflake_estimate_defaults_to_zero_without_global_stats(
    connection_settings: SnowflakeConnectionSettings,
) -> None:
    # Setup
    warehouse = SnowflakeWarehouse(connection_settings, FakeConnection(plan={}))

    # Act
    estimated = warehouse.estimate_scanned_bytes("select 1")

    # Assert
    assert estimated == 0


def test_guarded_snowflake_query_over_the_scan_budget_never_runs(
    connection_settings: SnowflakeConnectionSettings, snowflake_guard: SqlGuard
) -> None:
    # Setup
    connection = FakeConnection(plan={"GlobalStats": {"bytesAssigned": 5 * 1024**3}})
    budget = QueryBudget(max_queries=10, max_bytes_scanned=1024**3, timeout_seconds=30)
    guarded = GuardedWarehouse(SnowflakeWarehouse(connection_settings, connection), snowflake_guard, budget, 50)

    # Act
    with pytest.raises(QueryBudgetExceededError, match="byte budget"):
        guarded.query("select * from raw.orders")

    # Assert
    assert connection.query_statements == []
    assert (budget.queries_run, budget.queries_blocked, budget.bytes_scanned) == (0, 1, 0)


def test_guarded_snowflake_query_within_budget_runs_the_limited_sql(
    connection_settings: SnowflakeConnectionSettings, snowflake_guard: SqlGuard
) -> None:
    # Setup
    connection = FakeConnection(plan={"GlobalStats": {"bytesAssigned": 1_000}})
    budget = QueryBudget(max_queries=10, max_bytes_scanned=1024**3, timeout_seconds=30)
    guarded = GuardedWarehouse(SnowflakeWarehouse(connection_settings, connection), snowflake_guard, budget, 50)

    # Act
    guarded.query("select order_id from raw.orders")

    # Assert
    [executed] = connection.query_statements
    assert executed == "SELECT * FROM (SELECT order_id FROM raw.orders) AS guarded_query LIMIT 50"
    assert f"explain using json {executed}" in connection.statements
    assert "alter session set statement_timeout_in_seconds = 30" in connection.statements
    assert (budget.queries_run, budget.bytes_scanned) == (1, 1_000)


def test_snowflake_list_columns_reads_information_schema_and_normalises_case(
    connection_settings: SnowflakeConnectionSettings,
) -> None:
    # Setup
    connection = FakeConnection(
        information_schema_rows=[
            ("ANALYTICS_DB", "RAW", "ORDERS", "ORDER_ID", "NUMBER", 38, 0),
            ("ANALYTICS_DB", "RAW", "ORDERS", "AMOUNT", "NUMBER", 10, 2),
            ("ANALYTICS_DB", "RAW", "ORDERS", "STATUS", "TEXT", None, None),
        ]
    )
    warehouse = SnowflakeWarehouse(connection_settings, connection)

    # Act
    columns = warehouse.list_columns(["RAW", "analytics"])

    # Assert
    assert "from information_schema.columns where lower(table_schema) in (%s, %s)" in connection.statements[0]
    assert connection.params == [["raw", "analytics"]]
    assert [
        (column.database, column.schema_name, column.table, column.name, column.data_type) for column in columns
    ] == [
        ("analytics_db", "raw", "orders", "order_id", "NUMBER(38,0)"),
        ("analytics_db", "raw", "orders", "amount", "NUMBER(10,2)"),
        ("analytics_db", "raw", "orders", "status", "TEXT"),
    ]


def test_snowflake_columns_drive_schema_drift_without_false_type_changes(
    connection_settings: SnowflakeConnectionSettings,
) -> None:
    # Setup
    source = ManifestNode(
        unique_id="source.shop.raw.orders",
        resource_type=ResourceType.SOURCE,
        name="orders",
        source_name="raw",
        columns={
            "order_id": ColumnInfo(name="order_id", data_type="integer"),
            "amount": ColumnInfo(name="amount", data_type="numeric(10,2)"),
            "status": ColumnInfo(name="status", data_type="varchar"),
            "_loaded_at": ColumnInfo(name="_loaded_at", data_type="timestamp"),
        },
    )
    connection = FakeConnection(
        information_schema_rows=[
            ("ANALYTICS_DB", "RAW", "ORDERS", "ORDER_ID", "NUMBER", 38, 0),
            ("ANALYTICS_DB", "RAW", "ORDERS", "AMOUNT", "NUMBER", 10, 2),
            ("ANALYTICS_DB", "RAW", "ORDERS", "ORDER_STATUS", "TEXT", None, None),
            ("ANALYTICS_DB", "RAW", "ORDERS", "_LOADED_AT", "TIMESTAMP_NTZ", None, None),
        ]
    )
    warehouse = SnowflakeWarehouse(connection_settings, connection)

    # Act
    drift = detect_schema_drift(source, warehouse.list_columns(["raw"]))

    # Assert
    assert drift.missing_columns == ["status"]
    assert drift.added_columns == ["order_status"]
    assert drift.type_changes == []


_ORDERS_COLUMNS = {
    "order_id": "NUMBER(38,0)",
    "customer_id": "NUMBER(38,0)",
    "status": "TEXT",
    "amount": "NUMBER(10,2)",
    "_loaded_at": "TIMESTAMP_NTZ",
}
_CHECK_REQUESTS = {
    CheckKind.ROW_COUNT: CheckRequest(kind=CheckKind.ROW_COUNT, relation="raw.orders"),
    CheckKind.VOLUME_BY_LOAD_DAY: CheckRequest(kind=CheckKind.VOLUME_BY_LOAD_DAY, relation="raw.orders"),
    CheckKind.NULL_RATE_BY_LOAD_DAY: CheckRequest(
        kind=CheckKind.NULL_RATE_BY_LOAD_DAY, relation="raw.orders", column="customer_id"
    ),
    CheckKind.DUPLICATE_KEYS: CheckRequest(kind=CheckKind.DUPLICATE_KEYS, relation="raw.orders", column="order_id"),
    CheckKind.FRESHNESS: CheckRequest(kind=CheckKind.FRESHNESS, relation="raw.orders"),
    CheckKind.UNEXPECTED_VALUES: CheckRequest(
        kind=CheckKind.UNEXPECTED_VALUES, relation="raw.orders", column="status", accepted_values=["placed", "o'k"]
    ),
    CheckKind.ORPHAN_KEYS: CheckRequest(
        kind=CheckKind.ORPHAN_KEYS,
        relation="raw.orders",
        column="customer_id",
        parent_relation="raw.customers",
        parent_column="customer_id",
    ),
    CheckKind.COLUMN_PROFILE: CheckRequest(kind=CheckKind.COLUMN_PROFILE, relation="raw.orders", column="amount"),
    CheckKind.CUSTOM_SQL: CheckRequest(
        kind=CheckKind.CUSTOM_SQL,
        sql="select status, count(*) as row_count from ANALYTICS_DB.RAW.ORDERS group by 1 qualify count(*) > 0",
    ),
}


def _source(name: str, columns: dict[str, str], loaded_at_field: str | None = None) -> ManifestNode:
    return ManifestNode(
        unique_id=f"source.shop.raw.{name}",
        resource_type=ResourceType.SOURCE,
        name=name,
        source_name="raw",
        relation_name=f"ANALYTICS_DB.RAW.{name.upper()}",
        loaded_at_field=loaded_at_field,
        columns={column: ColumnInfo(name=column, data_type=data_type) for column, data_type in columns.items()},
    )


@pytest.fixture
def snowflake_runner(
    connection_settings: SnowflakeConnectionSettings, snowflake_guard: SqlGuard
) -> tuple[DiagnosticRunner, FakeConnection]:
    orders = _source("orders", _ORDERS_COLUMNS, loaded_at_field="_loaded_at")
    customers = _source("customers", {"customer_id": "NUMBER(38,0)"})
    manifest = Manifest(sources={orders.unique_id: orders, customers.unique_id: customers})
    catalog: CatalogSchema = {
        "analytics_db": {"raw": {"orders": _ORDERS_COLUMNS, "customers": {"customer_id": "NUMBER"}}}
    }
    connection = FakeConnection(plan={"GlobalStats": {"bytesAssigned": 2_048}})
    budget = QueryBudget(max_queries=10, max_bytes_scanned=1024**3, timeout_seconds=30)
    guarded = GuardedWarehouse(SnowflakeWarehouse(connection_settings, connection), snowflake_guard, budget, 50)
    return DiagnosticRunner(guarded, DbtLineage(manifest, catalog), catalog), connection


@pytest.mark.parametrize("kind", list(CheckKind))
def test_diagnostic_check_runs_through_the_snowflake_guard_and_adapter(
    snowflake_runner: tuple[DiagnosticRunner, FakeConnection], kind: CheckKind
) -> None:
    # Setup
    runner, connection = snowflake_runner

    # Act
    result = runner.run(_CHECK_REQUESTS[kind])

    # Assert
    assert result.error is None
    assert len(connection.query_statements) == len(result.sql)
    assert all(statement.endswith("LIMIT 50") for statement in connection.query_statements)


@pytest.mark.parametrize("kind", list(CheckKind))
def test_diagnostic_check_sql_transpiled_to_snowflake_passes_the_guard(
    snowflake_runner: tuple[DiagnosticRunner, FakeConnection], snowflake_guard: SqlGuard, kind: CheckKind
) -> None:
    # Setup
    runner, _ = snowflake_runner
    result = runner.run(_CHECK_REQUESTS[kind])

    # Act
    guarded = [
        snowflake_guard.enforce(sqlglot.transpile(sql, read="duckdb", write="snowflake")[0]) for sql in result.sql
    ]

    # Assert
    assert result.sql
    assert all(isinstance(sqlglot.parse_one(sql, read="snowflake"), exp.Select) for sql in guarded)
