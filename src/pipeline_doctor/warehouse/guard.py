import time

import sqlglot
from loguru import logger
from pydantic import BaseModel
from sqlglot import exp
from sqlglot.errors import ParseError

from pipeline_doctor.warehouse.base import QueryResult, Warehouse

_FORBIDDEN_NODE_NAMES = (
    "Insert", "Update", "Delete", "Merge", "Create", "Drop", "Alter", "AlterTable", "Command", "Into", "Copy",
    "Pragma", "Set", "Use", "Transaction", "Commit", "Rollback", "Grant", "Revoke", "TruncateTable", "Attach",
    "Detach", "Export", "Install", "Load", "Describe", "Cache", "Uncache", "Refresh", "LoadData",
)  # fmt: skip
_FORBIDDEN_NODES = tuple(getattr(exp, name) for name in _FORBIDDEN_NODE_NAMES if hasattr(exp, name))
_FORBIDDEN_FUNCTION_PREFIXES = (
    "read_",
    "glob",
    "system$",
    "parquet_",
    "csv_",
    "json_scan",
    "sniff_",
    "query",
    "getenv",
)
_INFORMATION_SCHEMA = "information_schema"
_SEQUENCE_ACCESSORS = {"nextval", "currval"}
# Functions sqlglot does not model natively; anything else unknown (UDFs, stage/file helpers) is rejected.
_ALLOWED_UNKNOWN_FUNCTIONS = {
    "approx_count_distinct",
    "date_diff",
    "datediff",
    "date_part",
    "date_trunc",
    "epoch",
    "list",
    "median",
    "mode",
    "quantile_cont",
    "regexp_matches",
    "strftime",
    "strptime",
    "try_cast",
    "try_to_date",
    "try_to_number",
    "try_to_timestamp",
}


class SqlGuardViolationError(ValueError):
    pass


class QueryBudgetExceededError(RuntimeError):
    pass


class SqlGuard:
    """Allowlist for agent-issued SQL: a single read-only SELECT over allowed databases and schemas, row-limited.

    Defence in depth only: the warehouse credentials themselves must be read-only (DuckDB read_only connection,
    Snowflake role without stage, sequence, or external-function privileges).
    """

    def __init__(
        self, dialect: str, *, allowed_databases: list[str], allowed_schemas: list[str], max_rows: int
    ) -> None:
        self._dialect = dialect
        self._allowed_databases = {database.lower() for database in allowed_databases}
        self._allowed_schemas = {schema.lower() for schema in [*allowed_schemas, _INFORMATION_SCHEMA]}
        self._max_rows = max_rows

    def enforce(self, sql: str) -> str:
        statement = self._parse_single(sql)
        if not isinstance(statement, exp.Query):
            raise SqlGuardViolationError(f"only SELECT queries are allowed, got {type(statement).__name__}")
        forbidden = next(statement.find_all(*_FORBIDDEN_NODES), None)
        if forbidden is not None:
            raise SqlGuardViolationError(f"forbidden operation: {type(forbidden).__name__}")
        self._check_functions(statement)
        self._check_stages_and_sequences(statement)
        self._check_tables(statement)
        limited = exp.select("*").from_(statement.subquery("guarded_query")).limit(self._max_rows)
        return limited.sql(dialect=self._dialect)

    def _parse_single(self, sql: str) -> exp.Expr:
        try:
            statements = [statement for statement in sqlglot.parse(sql, read=self._dialect) if statement is not None]
        except ParseError as error:
            raise SqlGuardViolationError(f"unparseable SQL: {error}") from error
        if len(statements) != 1:
            raise SqlGuardViolationError(f"exactly one statement is allowed, got {len(statements)}")
        return statements[0]

    def _check_functions(self, statement: exp.Expr) -> None:
        for function in statement.find_all(exp.Func):
            is_unknown = isinstance(function, exp.Anonymous)
            name = (function.name if is_unknown else function.sql_name()).lower()
            if name.startswith(_FORBIDDEN_FUNCTION_PREFIXES) or (is_unknown and name not in _ALLOWED_UNKNOWN_FUNCTIONS):
                raise SqlGuardViolationError(f"function not allowed: {name}")
            if isinstance(function.parent, exp.Dot) and function.parent.expression is function:
                raise SqlGuardViolationError(f"schema-qualified (user-defined) functions are not allowed: {name}")

    def _check_stages_and_sequences(self, statement: exp.Expr) -> None:
        if next(statement.find_all(exp.Parameter), None) is not None:
            raise SqlGuardViolationError("stage references and parameters are not allowed")
        for column in statement.find_all(exp.Column):
            if column.name.lower() in _SEQUENCE_ACCESSORS:
                raise SqlGuardViolationError("sequence access is not allowed")

    def _check_tables(self, statement: exp.Expr) -> None:
        cte_names = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}
        for table in statement.find_all(exp.Table):
            if not isinstance(table.this, exp.Identifier):
                raise SqlGuardViolationError(f"table functions are not allowed: {table.sql(dialect=self._dialect)}")
            if not table.db and table.name.lower() in cte_names:
                continue
            if table.catalog and table.catalog.lower() not in self._allowed_databases:
                raise SqlGuardViolationError(
                    f"table {table.sql(dialect=self._dialect)} is outside the allowed databases {sorted(self._allowed_databases)}"
                )
            if table.db.lower() not in self._allowed_schemas:
                raise SqlGuardViolationError(
                    f"table {table.sql(dialect=self._dialect)} is outside the allowed schemas "
                    f"{sorted(self._allowed_schemas)}; qualify tables as schema.table"
                )


class QueryBudget(BaseModel):
    max_queries: int
    max_bytes_scanned: int
    timeout_seconds: int
    queries_run: int = 0
    queries_blocked: int = 0
    bytes_scanned: int = 0

    def reserve(self, estimated_bytes: int | None) -> None:
        if self.queries_run >= self.max_queries:
            raise QueryBudgetExceededError(f"query budget of {self.max_queries} queries exhausted")
        if estimated_bytes is not None and self.bytes_scanned + estimated_bytes > self.max_bytes_scanned:
            raise QueryBudgetExceededError(
                f"query would scan {estimated_bytes} bytes, exceeding the {self.max_bytes_scanned} byte budget"
            )
        self.queries_run += 1
        self.bytes_scanned += estimated_bytes or 0


class GuardedWarehouse:
    """The only warehouse handle the agent sees: every query passes the SQL guard and the budget."""

    def __init__(self, warehouse: Warehouse, guard: SqlGuard, budget: QueryBudget, max_rows: int) -> None:
        self._warehouse = warehouse
        self._guard = guard
        self._budget = budget
        self._max_rows = max_rows

    @property
    def dialect(self) -> str:
        return self._warehouse.dialect

    @property
    def budget(self) -> QueryBudget:
        return self._budget

    def query(self, sql: str) -> QueryResult:
        try:
            safe_sql = self._guard.enforce(sql)
            self._budget.reserve(self._warehouse.estimate_scanned_bytes(safe_sql))
        except (SqlGuardViolationError, QueryBudgetExceededError):
            self._budget.queries_blocked += 1
            raise
        logger.info(f"query warehouse {self.dialect}")
        started = time.perf_counter()
        result = self._warehouse.execute(safe_sql, self._max_rows, self._budget.timeout_seconds)
        logger.debug(f"query finished in {time.perf_counter() - started:.3f}s")
        return result
