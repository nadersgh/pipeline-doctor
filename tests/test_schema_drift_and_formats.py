import pytest

from pipeline_doctor.dbt_tools.artifacts import ColumnInfo, ManifestNode, ResourceType
from pipeline_doctor.diagnostics.checks import ValueFormat, detect_value_format
from pipeline_doctor.diagnostics.schema_drift import detect_schema_drift, normalize_type
from pipeline_doctor.warehouse.base import ColumnSpec


@pytest.fixture
def orders_source() -> ManifestNode:
    return ManifestNode(
        unique_id="source.shop.raw.orders",
        resource_type=ResourceType.SOURCE,
        name="orders",
        source_name="raw",
        columns={
            "id": ColumnInfo(name="id", data_type="INTEGER"),
            "status": ColumnInfo(name="status", data_type="VARCHAR"),
            "order_date": ColumnInfo(name="order_date", data_type="DATE"),
        },
    )


def _columns(**types: str) -> list[ColumnSpec]:
    return [
        ColumnSpec(database="wh", schema_name="raw", table="orders", name=name, data_type=data_type)
        for name, data_type in types.items()
    ]


def test_detect_schema_drift_reports_renamed_column(orders_source: ManifestNode) -> None:
    # Act
    drift = detect_schema_drift(orders_source, _columns(id="INTEGER", order_status="VARCHAR", order_date="DATE"))

    # Assert
    assert drift.missing_columns == ["status"]
    assert drift.added_columns == ["order_status"]
    assert drift.type_changes == []


def test_detect_schema_drift_reports_type_change(orders_source: ManifestNode) -> None:
    # Act
    drift = detect_schema_drift(orders_source, _columns(id="INTEGER", status="VARCHAR", order_date="VARCHAR"))

    # Assert
    assert [(change.column, change.actual_type) for change in drift.type_changes] == [("order_date", "VARCHAR")]


def test_detect_schema_drift_ignores_equivalent_type_spellings(orders_source: ManifestNode) -> None:
    # Act
    drift = detect_schema_drift(orders_source, _columns(id="NUMBER(38,0)", status="TEXT", order_date="DATE"))

    # Assert
    assert not drift.has_drift


@pytest.mark.parametrize(
    ("declared", "normalized"),
    [
        ("int", "INTEGER"),
        ("NUMBER(38,0)", "INTEGER"),
        ("numeric(12, 0)", "INTEGER"),
        ("NUMBER(10,2)", "DECIMAL"),
        ("numeric", "DECIMAL"),
        ("string", "VARCHAR"),
        ("TIMESTAMP_NTZ(9)", "TIMESTAMP"),
    ],
)
def test_normalize_type_maps_dialect_aliases(declared: str, normalized: str) -> None:
    assert normalize_type(declared) == normalized


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (["01/15/2026", "12/31/2025"], ValueFormat.US_DATE),
        (["1,234", "987", "12,000"], ValueFormat.NUMBER_WITH_SEPARATORS),
        (["$12.50", "$1,200.00"], ValueFormat.CURRENCY),
        (["2026-01-15", "2025-12-31"], ValueFormat.ISO_DATE),
        (["42", "7"], ValueFormat.INTEGER),
        (["placed", "shipped"], ValueFormat.TEXT),
        ([], ValueFormat.TEXT),
    ],
)
def test_detect_value_format(values: list[str], expected: ValueFormat) -> None:
    assert detect_value_format(values) is expected
