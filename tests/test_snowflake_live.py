"""Opt-in read-only pass against a real Snowflake account, using the shared SNOWFLAKE_SAMPLE_DATA database.

Run with `uv run pytest -m snowflake_live` once PIPELINE_DOCTOR_SNOWFLAKE_ACCOUNT, _USER and _PASSWORD are set.
See docs/snowflake.md.
"""

import os

import pytest

from pipeline_doctor.dbt_tools.artifacts import Manifest, ManifestNode, ResourceType
from pipeline_doctor.dbt_tools.lineage import CatalogSchema, DbtLineage
from pipeline_doctor.diagnostics.checks import CheckKind, CheckRequest, DiagnosticRunner
from pipeline_doctor.warehouse.guard import (
    GuardedWarehouse,
    QueryBudget,
    QueryBudgetExceededError,
    SqlGuard,
    SqlGuardViolationError,
)
from pipeline_doctor.warehouse.snowflake_warehouse import SnowflakeConnectionSettings, SnowflakeWarehouse

_REQUIRED_ENV = (
    "PIPELINE_DOCTOR_SNOWFLAKE_ACCOUNT",
    "PIPELINE_DOCTOR_SNOWFLAKE_USER",
    "PIPELINE_DOCTOR_SNOWFLAKE_PASSWORD",
)
_DATABASE = "SNOWFLAKE_SAMPLE_DATA"
_SCHEMA = "TPCH_SF1"
_TIMEOUT_SECONDS = 60
# A bare trial user only has ACCOUNTADMIN/SYSADMIN; after running snowflake/setup_readonly_role.sql set
# PIPELINE_DOCTOR_SNOWFLAKE_ROLE=PIPELINE_DOCTOR_READONLY to exercise the read-only role as well.
_TRIAL_ROLE = "ACCOUNTADMIN"
_TRIAL_WAREHOUSE = "COMPUTE_WH"

pytestmark = [
    pytest.mark.snowflake_live,
    pytest.mark.skipif(
        not all(os.environ.get(name) for name in _REQUIRED_ENV), reason=f"set {', '.join(_REQUIRED_ENV)}"
    ),
]

_CHECK_REQUESTS = {
    CheckKind.ROW_COUNT: CheckRequest(kind=CheckKind.ROW_COUNT, relation="tpch.orders"),
    CheckKind.VOLUME_BY_LOAD_DAY: CheckRequest(kind=CheckKind.VOLUME_BY_LOAD_DAY, relation="tpch.orders"),
    CheckKind.NULL_RATE_BY_LOAD_DAY: CheckRequest(
        kind=CheckKind.NULL_RATE_BY_LOAD_DAY, relation="tpch.orders", column="o_clerk"
    ),
    CheckKind.DUPLICATE_KEYS: CheckRequest(kind=CheckKind.DUPLICATE_KEYS, relation="tpch.orders", column="o_orderkey"),
    CheckKind.FRESHNESS: CheckRequest(kind=CheckKind.FRESHNESS, relation="tpch.orders"),
    CheckKind.UNEXPECTED_VALUES: CheckRequest(
        kind=CheckKind.UNEXPECTED_VALUES, relation="tpch.orders", column="o_orderstatus", accepted_values=["F", "O"]
    ),
    CheckKind.ORPHAN_KEYS: CheckRequest(
        kind=CheckKind.ORPHAN_KEYS,
        relation="tpch.orders",
        column="o_custkey",
        parent_relation="tpch.customer",
        parent_column="c_custkey",
    ),
    CheckKind.COLUMN_PROFILE: CheckRequest(
        kind=CheckKind.COLUMN_PROFILE, relation="tpch.orders", column="o_orderstatus"
    ),
    CheckKind.CUSTOM_SQL: CheckRequest(
        kind=CheckKind.CUSTOM_SQL,
        sql=f"select o_orderpriority, count(*) as row_count from {_DATABASE}.{_SCHEMA}.orders group by 1",
    ),
}


@pytest.fixture(scope="module")
def connection_settings() -> SnowflakeConnectionSettings:
    return SnowflakeConnectionSettings.model_validate(
        {
            "account": os.environ["PIPELINE_DOCTOR_SNOWFLAKE_ACCOUNT"],
            "user": os.environ["PIPELINE_DOCTOR_SNOWFLAKE_USER"],
            "password": os.environ["PIPELINE_DOCTOR_SNOWFLAKE_PASSWORD"],
            "role": os.environ.get("PIPELINE_DOCTOR_SNOWFLAKE_ROLE") or _TRIAL_ROLE,
            "warehouse": os.environ.get("PIPELINE_DOCTOR_SNOWFLAKE_WAREHOUSE") or _TRIAL_WAREHOUSE,
            "database": _DATABASE,
            "statement_timeout_seconds": _TIMEOUT_SECONDS,
        }
    )


@pytest.fixture(scope="module")
def warehouse(connection_settings: SnowflakeConnectionSettings) -> SnowflakeWarehouse:
    return SnowflakeWarehouse(connection_settings)


@pytest.fixture(scope="module")
def catalog(warehouse: SnowflakeWarehouse) -> CatalogSchema:
    catalog: CatalogSchema = {}
    for column in warehouse.list_columns([_SCHEMA]):
        tables = catalog.setdefault(column.database, {}).setdefault(column.schema_name, {})
        tables.setdefault(column.table, {})[column.name] = column.data_type
    return catalog


@pytest.fixture
def guard() -> SqlGuard:
    return SqlGuard("snowflake", allowed_databases=[_DATABASE], allowed_schemas=[_SCHEMA], max_rows=50)


def _guarded(warehouse: SnowflakeWarehouse, guard: SqlGuard, max_bytes_scanned: int) -> GuardedWarehouse:
    budget = QueryBudget(max_queries=25, max_bytes_scanned=max_bytes_scanned, timeout_seconds=_TIMEOUT_SECONDS)
    return GuardedWarehouse(warehouse, guard, budget, 50)


def _tpch_source(name: str, loaded_at_field: str | None = None) -> ManifestNode:
    return ManifestNode(
        unique_id=f"source.tpch.tpch.{name}",
        resource_type=ResourceType.SOURCE,
        name=name,
        source_name="tpch",
        relation_name=f"{_DATABASE}.{_SCHEMA}.{name.upper()}",
        loaded_at_field=loaded_at_field,
    )


@pytest.fixture
def runner(warehouse: SnowflakeWarehouse, guard: SqlGuard, catalog: CatalogSchema) -> DiagnosticRunner:
    orders = _tpch_source("orders", loaded_at_field="o_orderdate")
    customer = _tpch_source("customer")
    manifest = Manifest(sources={orders.unique_id: orders, customer.unique_id: customer})
    return DiagnosticRunner(_guarded(warehouse, guard, 10 * 1024**3), DbtLineage(manifest, catalog), catalog)


def test_live_session_uses_the_configured_role_tag_and_timeout(
    warehouse: SnowflakeWarehouse, connection_settings: SnowflakeConnectionSettings
) -> None:
    # Act
    role = warehouse.execute("select current_role() as role", max_rows=1, timeout_seconds=45)
    parameters = warehouse.execute("show parameters in session", max_rows=1_000, timeout_seconds=45)

    # Assert
    values = {record["key"]: record["value"] for record in parameters.as_records()}
    assert role.rows == [[connection_settings.role.upper()]]
    assert values["QUERY_TAG"] == "pipeline-doctor"
    assert values["STATEMENT_TIMEOUT_IN_SECONDS"] == "45"


def test_live_list_columns_reads_information_schema(catalog: CatalogSchema) -> None:
    # Act
    orders = catalog["snowflake_sample_data"]["tpch_sf1"]["orders"]

    # Assert
    assert orders["o_orderkey"] == "NUMBER(38,0)"
    assert orders["o_totalprice"] == "NUMBER(12,2)"
    assert orders["o_orderdate"] == "DATE"


def test_live_explain_estimates_bytes_scanned(warehouse: SnowflakeWarehouse, guard: SqlGuard) -> None:
    # Act
    estimated = warehouse.estimate_scanned_bytes(guard.enforce(f"select * from {_SCHEMA}.orders"))

    # Assert
    assert estimated is not None
    assert estimated > 0


@pytest.mark.parametrize("kind", list(CheckKind))
def test_live_diagnostic_check_runs_read_only(runner: DiagnosticRunner, kind: CheckKind) -> None:
    # Act
    result = runner.run(_CHECK_REQUESTS[kind])

    # Assert
    assert result.error is None


@pytest.mark.parametrize(
    ("kind", "metric", "expected"),
    [
        (CheckKind.ROW_COUNT, "row_count", 1_500_000),
        (CheckKind.FRESHNESS, "max_loaded_at", "1998-08-02"),
        (CheckKind.DUPLICATE_KEYS, "duplicated_keys", 0),
        (CheckKind.ORPHAN_KEYS, "orphan_keys", 0),
        (CheckKind.UNEXPECTED_VALUES, "unexpected_values", ["P"]),
        (CheckKind.COLUMN_PROFILE, "data_type", "TEXT"),
    ],
)
def test_live_diagnostic_check_reports_known_tpch_facts(
    runner: DiagnosticRunner, kind: CheckKind, metric: str, expected: object
) -> None:
    # Act
    result = runner.run(_CHECK_REQUESTS[kind])

    # Assert
    assert result.metrics[metric] == expected


def test_live_guard_blocks_a_write_before_it_reaches_snowflake(warehouse: SnowflakeWarehouse, guard: SqlGuard) -> None:
    # Setup
    guarded = _guarded(warehouse, guard, 10 * 1024**3)

    # Act
    with pytest.raises(SqlGuardViolationError):
        guarded.query(f"delete from {_DATABASE}.{_SCHEMA}.orders")

    # Assert
    assert (guarded.budget.queries_run, guarded.budget.queries_blocked) == (0, 1)


def test_live_budget_blocks_a_scan_over_the_byte_limit(warehouse: SnowflakeWarehouse, guard: SqlGuard) -> None:
    # Setup
    guarded = _guarded(warehouse, guard, max_bytes_scanned=1)

    # Act
    with pytest.raises(QueryBudgetExceededError):
        guarded.query(f"select * from {_SCHEMA}.orders")

    # Assert
    assert (guarded.budget.queries_run, guarded.budget.queries_blocked) == (0, 1)
