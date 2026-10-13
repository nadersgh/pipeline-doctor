from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Protocol

from pydantic import BaseModel

type CellValue = str | int | float | bool | None


class ColumnSpec(BaseModel):
    database: str
    schema_name: str
    table: str
    name: str
    data_type: str


class QueryResult(BaseModel):
    columns: list[str]
    rows: list[list[CellValue]]
    elapsed_seconds: float
    truncated: bool = False

    def as_records(self) -> list[dict[str, CellValue]]:
        return [dict(zip(self.columns, row, strict=True)) for row in self.rows]

    def to_markdown(self, max_rows: int = 15) -> str:
        if not self.columns:
            return "(no columns)"
        header = "| " + " | ".join(self.columns) + " |"
        divider = "| " + " | ".join("---" for _ in self.columns) + " |"
        body = [
            "| " + " | ".join("" if value is None else str(value) for value in row) + " |"
            for row in self.rows[:max_rows]
        ]
        suffix = [f"... {len(self.rows) - max_rows} more rows"] if len(self.rows) > max_rows else []
        return "\n".join([header, divider, *body, *suffix])


class Warehouse(Protocol):
    """Read-only access to a SQL warehouse. Implementations must connect with read-only credentials."""

    @property
    def dialect(self) -> str: ...

    @property
    def database(self) -> str: ...

    def execute(self, sql: str, max_rows: int, timeout_seconds: int) -> QueryResult: ...

    def list_columns(self, schemas: list[str]) -> list[ColumnSpec]: ...

    def estimate_scanned_bytes(self, sql: str) -> int | None: ...


class WarehouseQueryError(RuntimeError):
    pass


def to_cell(value: Any) -> CellValue:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    return str(value)
