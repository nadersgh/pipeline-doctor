import threading
import time
from pathlib import Path

import duckdb
from loguru import logger

from pipeline_doctor.warehouse.base import ColumnSpec, QueryResult, WarehouseQueryError, to_cell


class DuckDBWarehouse:
    """DuckDB warehouse opened read-only with external file and network access disabled."""

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def dialect(self) -> str:
        return "duckdb"

    @property
    def database(self) -> str:
        return self._path.stem

    def execute(self, sql: str, max_rows: int, timeout_seconds: int) -> QueryResult:
        logger.debug(f"query duckdb {self._path.name}")
        started = time.perf_counter()
        with self._connect() as connection:
            timer = threading.Timer(timeout_seconds, connection.interrupt)
            timer.start()
            try:
                cursor = connection.execute(sql)
                columns = [description[0] for description in cursor.description or []]
                fetched = cursor.fetchmany(max_rows + 1)
            except duckdb.Error as error:
                raise WarehouseQueryError(str(error)) from error
            finally:
                timer.cancel()
        rows = [[to_cell(value) for value in row] for row in fetched[:max_rows]]
        return QueryResult(
            columns=columns,
            rows=rows,
            elapsed_seconds=time.perf_counter() - started,
            truncated=len(fetched) > max_rows,
        )

    def list_columns(self, schemas: list[str]) -> list[ColumnSpec]:
        placeholders = ", ".join("?" for _ in schemas)
        sql = (
            "select table_catalog, table_schema, table_name, column_name, data_type "
            f"from information_schema.columns where lower(table_schema) in ({placeholders}) "
            "order by table_schema, table_name, ordinal_position"
        )
        logger.debug(f"query duckdb information_schema {self._path.name}")
        with self._connect() as connection:
            rows = connection.execute(sql, [schema.lower() for schema in schemas]).fetchall()
        return [
            ColumnSpec(database=database, schema_name=schema, table=table, name=name, data_type=data_type)
            for database, schema, table, name, data_type in rows
        ]

    def estimate_scanned_bytes(self, sql: str) -> int | None:
        return None

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self._path), read_only=True, config={"enable_external_access": False})
