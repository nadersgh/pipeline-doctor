import json
import time
from typing import Any

from loguru import logger
from pydantic import BaseModel, Field, SecretStr

from pipeline_doctor.warehouse.base import ColumnSpec, QueryResult, WarehouseQueryError, to_cell

_QUERY_TAG = "pipeline-doctor"


class SnowflakeConnectionSettings(BaseModel):
    account: str
    user: str
    password: SecretStr
    # An empty role would silently fall back to the user's default role, which may be able to write.
    role: str = Field(min_length=1)
    warehouse: str
    database: str
    statement_timeout_seconds: int = Field(default=30, gt=0)


class SnowflakeWarehouse:
    """Snowflake access through a dedicated read-only role, tagged and time-boxed per statement."""

    def __init__(self, settings: SnowflakeConnectionSettings, connection: Any | None = None) -> None:
        self._settings = settings
        self._connection = connection

    @property
    def dialect(self) -> str:
        return "snowflake"

    @property
    def database(self) -> str:
        return self._settings.database

    def execute(self, sql: str, max_rows: int, timeout_seconds: int) -> QueryResult:
        connection = self._get_connection()
        started = time.perf_counter()
        logger.debug(f"query snowflake {self._settings.database}")
        cursor = connection.cursor()
        try:
            cursor.execute(f"alter session set statement_timeout_in_seconds = {int(timeout_seconds)}")
            cursor.execute(sql)
            columns = [description[0].lower() for description in cursor.description or []]
            fetched = cursor.fetchmany(max_rows + 1)
        except Exception as error:
            raise WarehouseQueryError(str(error)) from error
        finally:
            cursor.close()
        return QueryResult(
            columns=columns,
            rows=[[to_cell(value) for value in row] for row in fetched[:max_rows]],
            elapsed_seconds=time.perf_counter() - started,
            truncated=len(fetched) > max_rows,
        )

    def list_columns(self, schemas: list[str]) -> list[ColumnSpec]:
        placeholders = ", ".join(["%s"] * len(schemas))
        sql = (
            "select table_catalog, table_schema, table_name, column_name, data_type, numeric_precision, numeric_scale "
            f"from information_schema.columns where lower(table_schema) in ({placeholders}) "
            "order by table_schema, table_name, ordinal_position"
        )
        logger.debug(f"query snowflake information_schema {self._settings.database}")
        cursor = self._get_connection().cursor()
        try:
            cursor.execute(sql, [schema.lower() for schema in schemas])
            rows = cursor.fetchall()
        except Exception as error:
            raise WarehouseQueryError(str(error)) from error
        finally:
            cursor.close()
        return [
            ColumnSpec(
                database=database.lower(),
                schema_name=schema.lower(),
                table=table.lower(),
                name=name.lower(),
                data_type=_render_data_type(data_type, precision, scale),
            )
            for database, schema, table, name, data_type, precision, scale in rows
        ]

    def estimate_scanned_bytes(self, sql: str) -> int | None:
        cursor = self._get_connection().cursor()
        try:
            logger.debug(f"explain snowflake {self._settings.database}")
            cursor.execute(f"explain using json {sql}")
            plan = json.loads(cursor.fetchone()[0])
        except Exception as error:
            raise WarehouseQueryError(str(error)) from error
        finally:
            cursor.close()
        return int(plan.get("GlobalStats", {}).get("bytesAssigned", 0))

    def _get_connection(self) -> Any:
        if self._connection is None:
            import snowflake.connector  # noqa: PLC0415 - optional dependency

            logger.info(f"connect snowflake {self._settings.account}")
            self._connection = snowflake.connector.connect(
                account=self._settings.account,
                user=self._settings.user,
                password=self._settings.password.get_secret_value(),
                role=self._settings.role,
                warehouse=self._settings.warehouse,
                database=self._settings.database,
                session_parameters={
                    "QUERY_TAG": _QUERY_TAG,
                    "AUTOCOMMIT": False,
                    "STATEMENT_TIMEOUT_IN_SECONDS": self._settings.statement_timeout_seconds,
                },
            )
        return self._connection


def _render_data_type(data_type: str, precision: int | None, scale: int | None) -> str:
    # information_schema reports every exact numeric as NUMBER; the scale separates integers from decimals.
    if data_type.upper() == "NUMBER" and precision is not None and scale is not None:
        return f"NUMBER({precision},{scale})"
    return data_type
