import re

from pydantic import BaseModel, Field

from pipeline_doctor.dbt_tools.artifacts import ManifestNode
from pipeline_doctor.warehouse.base import ColumnSpec

_TYPE_ALIASES = {
    "INT": "INTEGER",
    "INT4": "INTEGER",
    "INT32": "INTEGER",
    "BIGINT": "INTEGER",
    "INT8": "INTEGER",
    "INT64": "INTEGER",
    "NUMBER": "INTEGER",
    "NUMERIC": "DECIMAL",
    "TEXT": "VARCHAR",
    "STRING": "VARCHAR",
    "CHAR": "VARCHAR",
    "TIMESTAMP_NTZ": "TIMESTAMP",
    "DATETIME": "TIMESTAMP",
    "FLOAT8": "DOUBLE",
    "FLOAT": "DOUBLE",
}
_EXACT_NUMERIC = re.compile(r"^(NUMBER|NUMERIC|DECIMAL)\(\s*\d+\s*,\s*(\d+)\s*\)$")


class TypeChange(BaseModel):
    column: str
    declared_type: str
    actual_type: str


class SchemaDrift(BaseModel):
    source: str = Field(description="Source short name such as 'raw.orders'.")
    missing_columns: list[str] = Field(default_factory=list)
    added_columns: list[str] = Field(default_factory=list)
    type_changes: list[TypeChange] = Field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.missing_columns or self.added_columns or self.type_changes)

    def describe(self) -> str:
        parts = []
        if self.missing_columns:
            parts.append(f"missing declared columns {self.missing_columns}")
        if self.added_columns:
            parts.append(f"new undeclared columns {self.added_columns}")
        parts.extend(
            f"{change.column} declared {change.declared_type} but is now {change.actual_type}"
            for change in self.type_changes
        )
        return f"{self.source}: " + "; ".join(parts) if parts else f"{self.source}: no drift"


def detect_schema_drift(source: ManifestNode, actual_columns: list[ColumnSpec]) -> SchemaDrift:
    """Compares the columns a dbt source declares with the columns the warehouse actually has."""
    actual = {column.name.lower(): column.data_type for column in actual_columns}
    declared = {name.lower(): info.data_type for name, info in source.columns.items()}
    type_changes = [
        TypeChange(column=name, declared_type=declared_type, actual_type=actual[name])
        for name, declared_type in declared.items()
        if declared_type and name in actual and normalize_type(declared_type) != normalize_type(actual[name])
    ]
    return SchemaDrift(
        source=source.short_name,
        missing_columns=[name for name in declared if name not in actual] if actual else [],
        added_columns=[name for name in actual if name not in declared] if declared else [],
        type_changes=type_changes,
    )


def normalize_type(data_type: str) -> str:
    upper = data_type.strip().upper()
    exact_numeric = _EXACT_NUMERIC.match(upper)
    if exact_numeric:
        # Snowflake stores every integer type as NUMBER(38,0); only the scale tells integers from decimals.
        return "INTEGER" if int(exact_numeric.group(2)) == 0 else "DECIMAL"
    base = re.sub(r"\(.*\)", "", upper).strip()
    return _TYPE_ALIASES.get(base, base)
