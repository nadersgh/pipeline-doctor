import re
import statistics
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum

from loguru import logger
from pydantic import BaseModel, Field

from pipeline_doctor.dbt_tools.artifacts import ManifestNode, ResourceType
from pipeline_doctor.dbt_tools.lineage import CatalogSchema, DbtLineage
from pipeline_doctor.security.untrusted import UntrustedSource, fence
from pipeline_doctor.warehouse.base import CellValue, QueryResult, WarehouseQueryError
from pipeline_doctor.warehouse.guard import GuardedWarehouse, QueryBudgetExceededError, SqlGuardViolationError

type MetricValue = CellValue | list[str]

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
_DEFAULT_LOADED_AT_COLUMN = "_loaded_at"
_RECENT_LOAD_DAYS = 2
_HISTORY_DAYS = 14


class CheckKind(StrEnum):
    ROW_COUNT = "row_count"
    VOLUME_BY_LOAD_DAY = "volume_by_load_day"
    NULL_RATE_BY_LOAD_DAY = "null_rate_by_load_day"
    DUPLICATE_KEYS = "duplicate_keys"
    FRESHNESS = "freshness"
    UNEXPECTED_VALUES = "unexpected_values"
    ORPHAN_KEYS = "orphan_keys"
    COLUMN_PROFILE = "column_profile"
    CUSTOM_SQL = "custom_sql"


CHECK_DESCRIPTIONS: dict[CheckKind, str] = {
    CheckKind.ROW_COUNT: "Row count of `relation`.",
    CheckKind.VOLUME_BY_LOAD_DAY: "Rows per load day for `relation` over the last 14 load days.",
    CheckKind.NULL_RATE_BY_LOAD_DAY: "Null rate of `column` per load day in `relation`; flags recent spikes.",
    CheckKind.DUPLICATE_KEYS: "Keys of `column` appearing more than once in `relation`, and across how many loads.",
    CheckKind.FRESHNESS: "Hours since the latest load of `relation`.",
    CheckKind.UNEXPECTED_VALUES: "Values of `column` in `relation` outside `accepted_values`, with first-seen time.",
    CheckKind.ORPHAN_KEYS: "Keys in `relation`.`column` with no match in `parent_relation`.`parent_column`.",
    CheckKind.COLUMN_PROFILE: "Warehouse type, null share, and sample values of `column` in `relation`.",
    CheckKind.CUSTOM_SQL: "A single read-only SELECT in `sql` (schema-qualified tables only, auto-limited).",
}


class ValueFormat(StrEnum):
    US_DATE = "us_date"
    ISO_DATE = "iso_date"
    NUMBER_WITH_SEPARATORS = "number_with_separators"
    CURRENCY = "currency"
    INTEGER = "integer"
    DECIMAL = "decimal"
    TEXT = "text"


_FORMAT_PATTERNS: list[tuple[ValueFormat, re.Pattern[str]]] = [
    (ValueFormat.US_DATE, re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")),
    (ValueFormat.ISO_DATE, re.compile(r"^\d{4}-\d{2}-\d{2}")),
    (ValueFormat.CURRENCY, re.compile(r"^-?[$€£]\s?[\d,]+(\.\d+)?$")),
    (ValueFormat.NUMBER_WITH_SEPARATORS, re.compile(r"^-?\d{1,3}(,\d{3})+(\.\d+)?$")),
    (ValueFormat.INTEGER, re.compile(r"^-?\d+$")),
    (ValueFormat.DECIMAL, re.compile(r"^-?\d+\.\d+$")),
]


class CheckRequest(BaseModel):
    kind: CheckKind
    relation: str = Field(default="", description="Source as 'raw.orders' or model name as 'stg_orders'.")
    column: str = ""
    parent_relation: str = ""
    parent_column: str = ""
    accepted_values: list[str] = Field(default_factory=list)
    sql: str = ""
    rationale: str = ""

    def key(self) -> str:
        return "|".join(
            [self.kind, self.relation.lower(), self.column.lower(), self.parent_relation.lower(), self.sql.strip()]
        )


class CheckResult(BaseModel):
    request: CheckRequest
    sql: list[str] = Field(default_factory=list)
    table: QueryResult | None = None
    metrics: dict[str, MetricValue] = Field(default_factory=dict)
    summary: str = ""
    error: str | None = None

    def describe(self) -> str:
        target = ".".join(part for part in [self.request.relation, self.request.column] if part)
        header = f"[{self.request.kind} {target}]".replace(" ]", "]")
        return f"{header}{' ERROR:' if self.error else ''}\n{fence(self.render_output(), UntrustedSource.QUERY_RESULT)}"

    def render_output(self) -> str:
        """What the warehouse returned: summary and rows, or the error. May echo source data."""
        if self.error:
            return self.error
        table = f"\n{self.table.to_markdown()}" if self.table and self.table.rows else ""
        return f"{self.summary}{table}"


class DiagnosticCheckError(ValueError):
    pass


class DiagnosticRunner:
    """Turns typed check requests into guarded read-only queries and interprets the results."""

    def __init__(self, warehouse: GuardedWarehouse, lineage: DbtLineage, catalog: CatalogSchema) -> None:
        self._warehouse = warehouse
        self._lineage = lineage
        self._catalog = catalog
        self._handlers: dict[CheckKind, Callable[[CheckRequest], CheckResult]] = {
            CheckKind.ROW_COUNT: self._row_count,
            CheckKind.VOLUME_BY_LOAD_DAY: self._volume_by_load_day,
            CheckKind.NULL_RATE_BY_LOAD_DAY: self._null_rate_by_load_day,
            CheckKind.DUPLICATE_KEYS: self._duplicate_keys,
            CheckKind.FRESHNESS: self._freshness,
            CheckKind.UNEXPECTED_VALUES: self._unexpected_values,
            CheckKind.ORPHAN_KEYS: self._orphan_keys,
            CheckKind.COLUMN_PROFILE: self._column_profile,
            CheckKind.CUSTOM_SQL: self._custom_sql,
        }

    def run(self, request: CheckRequest) -> CheckResult:
        try:
            return self._handlers[request.kind](request)
        except (
            DiagnosticCheckError,
            SqlGuardViolationError,
            QueryBudgetExceededError,
            WarehouseQueryError,
        ) as error:
            logger.info(f"diagnostic check {request.kind} on {request.relation} recorded an error: {error}")
            return CheckResult(request=request, error=str(error))

    def _row_count(self, request: CheckRequest) -> CheckResult:
        relation, _ = self._resolve(request.relation)
        sql = f"select count(*) as row_count from {relation}"
        table = self._warehouse.query(sql)
        count = table.rows[0][0]
        return CheckResult(
            request=request, sql=[sql], table=table, metrics={"row_count": count}, summary=f"{count} rows."
        )

    def _volume_by_load_day(self, request: CheckRequest) -> CheckResult:
        relation, node = self._resolve(request.relation)
        loaded_at = self._loaded_at_column(node)
        sql = (
            f"select cast({loaded_at} as date) as load_day, count(*) as row_count from {relation} "
            f"group by 1 order by 1 desc limit {_HISTORY_DAYS}"
        )
        table = self._warehouse.query(sql)
        counts = [int(row[1] or 0) for row in table.rows]
        if not counts:
            return CheckResult(request=request, sql=[sql], table=table, summary="The relation is empty.")
        latest_day = str(table.rows[0][0])
        median = statistics.median(counts)
        days_since = (datetime.now(UTC).date() - datetime.fromisoformat(latest_day).date()).days
        metrics: dict[str, MetricValue] = {
            "latest_load_day": latest_day,
            "latest_rows": counts[0],
            "median_rows": median,
            "latest_vs_median": round(counts[0] / median, 3) if median else None,
            "days_since_latest_load": days_since,
        }
        summary = (
            f"Latest load day {latest_day} ({days_since} days ago) has {counts[0]} rows "
            f"vs a median of {median} over the last {len(counts)} load days."
        )
        return CheckResult(request=request, sql=[sql], table=table, metrics=metrics, summary=summary)

    def _null_rate_by_load_day(self, request: CheckRequest) -> CheckResult:
        relation, node = self._resolve(request.relation)
        column = _identifier(request.column)
        loaded_at = self._loaded_at_column(node)
        sql = (
            f"select cast({loaded_at} as date) as load_day, count(*) as total_rows, "
            f"sum(case when {column} is null then 1 else 0 end) as null_rows "
            f"from {relation} group by 1 order by 1 desc limit {_HISTORY_DAYS}"
        )
        table = self._warehouse.query(sql)
        recent, baseline = table.rows[:_RECENT_LOAD_DAYS], table.rows[_RECENT_LOAD_DAYS:]
        recent_rate, baseline_rate = _null_rate(recent), _null_rate(baseline)
        spike = recent_rate >= 0.02 and recent_rate > 3 * baseline_rate + 0.01
        metrics: dict[str, MetricValue] = {
            "recent_null_rate": round(recent_rate, 4),
            "baseline_null_rate": round(baseline_rate, 4),
            "spike": spike,
            "total_null_rows": sum(int(row[2] or 0) for row in table.rows),
        }
        summary = (
            f"Null rate of {request.column} is {recent_rate:.1%} in the last {_RECENT_LOAD_DAYS} load days "
            f"vs {baseline_rate:.1%} before ({'SPIKE' if spike else 'no spike'})."
        )
        return CheckResult(request=request, sql=[sql], table=table, metrics=metrics, summary=summary)

    def _duplicate_keys(self, request: CheckRequest) -> CheckResult:
        relation, node = self._resolve(request.relation)
        column = _identifier(request.column)
        loaded_at = self._loaded_at_column_or_none(node)
        load_columns = (
            f", count(distinct {loaded_at}) as load_batches, max({loaded_at}) as last_loaded_at" if loaded_at else ""
        )
        load_metrics = ", max(load_batches) as max_load_batches, max(last_loaded_at) as latest_duplicate_load"
        sql = (
            f"with keyed as (select {column} as key_value, count(*) as copies{load_columns} from {relation} "
            f"where {column} is not null group by 1 having count(*) > 1) "
            f"select count(*) as duplicated_keys, coalesce(sum(copies - 1), 0) as extra_rows, "
            f"max(copies) as max_copies{load_metrics if loaded_at else ''} from keyed"
        )
        table = self._warehouse.query(sql)
        record = table.as_records()[0]
        duplicated = int(record["duplicated_keys"] or 0)
        replayed = bool(loaded_at) and int(record.get("max_load_batches") or 0) > 1
        metrics: dict[str, MetricValue] = {
            "duplicated_keys": duplicated,
            "extra_rows": int(record["extra_rows"] or 0),
            "max_copies": record["max_copies"],
            "duplicates_span_multiple_loads": replayed,
            "latest_duplicate_load": record.get("latest_duplicate_load"),
        }
        summary = f"{duplicated} values of {request.column} are duplicated ({metrics['extra_rows']} extra rows)" + (
            "; duplicates were loaded in separate batches (replayed load)." if replayed else "."
        )
        return CheckResult(request=request, sql=[sql], table=table, metrics=metrics, summary=summary)

    def _freshness(self, request: CheckRequest) -> CheckResult:
        relation, node = self._resolve(request.relation)
        loaded_at = self._loaded_at_column(node)
        sql = f"select max({loaded_at}) as max_loaded_at, count(*) as total_rows from {relation}"
        table = self._warehouse.query(sql)
        max_loaded_at = table.rows[0][0]
        if max_loaded_at is None:
            return CheckResult(request=request, sql=[sql], table=table, summary="No rows have ever been loaded.")
        latest = datetime.fromisoformat(str(max_loaded_at))
        latest = latest if latest.tzinfo else latest.replace(tzinfo=UTC)
        hours = round((datetime.now(UTC) - latest).total_seconds() / 3600, 1)
        metrics: dict[str, MetricValue] = {"max_loaded_at": str(max_loaded_at), "hours_since_last_load": hours}
        summary = f"Latest row loaded at {max_loaded_at}, {hours} hours ago."
        return CheckResult(request=request, sql=[sql], table=table, metrics=metrics, summary=summary)

    def _unexpected_values(self, request: CheckRequest) -> CheckResult:
        relation, node = self._resolve(request.relation)
        column = _identifier(request.column)
        loaded_at = self._loaded_at_column_or_none(node)
        first_seen = f", min({loaded_at}) as first_seen" if loaded_at else ""
        accepted = ", ".join(_literal(value) for value in request.accepted_values)
        where = f"and cast({column} as varchar) not in ({accepted})" if accepted else ""
        sql = (
            f"select cast({column} as varchar) as value, count(*) as row_count{first_seen} from {relation} "
            f"where {column} is not null {where} group by 1 order by 2 desc limit 20"
        )
        table = self._warehouse.query(sql)
        values = [str(row[0]) for row in table.rows]
        metrics: dict[str, MetricValue] = {
            "unexpected_values": values if accepted else [],
            "unexpected_rows": sum(int(row[1] or 0) for row in table.rows) if accepted else 0,
            "first_seen": min(str(row[2]) for row in table.rows) if (accepted and loaded_at and table.rows) else None,
        }
        summary = (
            f"Values outside the accepted set: {values or 'none'}."
            if accepted
            else f"Top values of {request.column}: {values}."
        )
        return CheckResult(request=request, sql=[sql], table=table, metrics=metrics, summary=summary)

    def _orphan_keys(self, request: CheckRequest) -> CheckResult:
        child, _ = self._resolve(request.relation)
        parent, _ = self._resolve(request.parent_relation)
        child_column, parent_column = _identifier(request.column), _identifier(request.parent_column)
        sql = (
            f"with child_keys as (select distinct {child_column} as key_value from {child} "
            f"where {child_column} is not null), "
            f"parent_keys as (select distinct {parent_column} as key_value from {parent} "
            f"where {parent_column} is not null), "
            "orphans as (select key_value from child_keys where key_value not in "
            "(select key_value from parent_keys)) "
            "select (select count(*) from orphans) as orphan_keys, "
            "(select min(key_value) from orphans) as min_orphan, (select max(key_value) from orphans) as max_orphan, "
            "(select count(*) from parent_keys) as parent_keys, "
            "(select min(key_value) from parent_keys) as parent_min, "
            "(select max(key_value) from parent_keys) as parent_max"
        )
        table = self._warehouse.query(sql)
        record = table.as_records()[0]
        orphans = int(record["orphan_keys"] or 0)
        within_range = _is_within_range(
            record["min_orphan"], record["max_orphan"], record["parent_min"], record["parent_max"]
        )
        metrics: dict[str, MetricValue] = {
            "orphan_keys": orphans,
            "parent_keys": record["parent_keys"],
            "orphans_within_parent_key_range": within_range,
        }
        if orphans == 0:
            summary = "No orphaned keys."
        elif within_range:
            summary = f"{orphans} orphaned keys fall inside the parent key range: parent rows were deleted or lost."
        else:
            summary = f"{orphans} orphaned keys lie beyond the parent key range: the parent load is behind."
        return CheckResult(request=request, sql=[sql], table=table, metrics=metrics, summary=summary)

    def _column_profile(self, request: CheckRequest) -> CheckResult:
        relation, node = self._resolve(request.relation)
        column = _identifier(request.column)
        data_type = self._column_type(node, request.column)
        stats_sql = (
            f"select count(*) as total_rows, count({column}) as non_null_rows, "
            f"count(distinct {column}) as distinct_values from {relation}"
        )
        sample_sql = (
            f"select cast({column} as varchar) as value, count(*) as row_count from {relation} "
            f"where {column} is not null group by 1 order by 2 desc limit 10"
        )
        stats = self._warehouse.query(stats_sql).as_records()[0]
        samples = self._warehouse.query(sample_sql)
        values = [str(row[0]) for row in samples.rows]
        total = int(stats["total_rows"] or 0)
        value_format = detect_value_format(values)
        metrics: dict[str, MetricValue] = {
            "data_type": data_type,
            "null_fraction": round(1 - int(stats["non_null_rows"] or 0) / total, 4) if total else None,
            "distinct_values": stats["distinct_values"],
            "sample_values": values,
            "value_format": value_format,
        }
        summary = f"{request.column} is {data_type or 'unknown type'}; samples {values[:5]} look like {value_format}."
        return CheckResult(
            request=request, sql=[stats_sql, sample_sql], table=samples, metrics=metrics, summary=summary
        )

    def _custom_sql(self, request: CheckRequest) -> CheckResult:
        if not request.sql.strip():
            raise DiagnosticCheckError("custom_sql requires `sql`")
        table = self._warehouse.query(request.sql)
        summary = f"Returned {len(table.rows)} rows{' (truncated)' if table.truncated else ''}."
        return CheckResult(request=request, sql=[request.sql], table=table, summary=summary)

    def _resolve(self, name: str) -> tuple[str, ManifestNode]:
        node = self._lineage.find(name)
        if node is None or not node.relation_name:
            raise DiagnosticCheckError(f"unknown relation {name!r}; use 'source_name.table' or a model name")
        return node.relation_name, node

    def _loaded_at_column_or_none(self, node: ManifestNode) -> str | None:
        if node.resource_type is ResourceType.SOURCE and node.loaded_at_field:
            return _identifier(node.loaded_at_field)
        has_default = self._column_type(node, _DEFAULT_LOADED_AT_COLUMN) is not None
        return _DEFAULT_LOADED_AT_COLUMN if has_default else None

    def _loaded_at_column(self, node: ManifestNode) -> str:
        column = self._loaded_at_column_or_none(node)
        if column is None:
            raise DiagnosticCheckError(f"{node.short_name} has no load timestamp column")
        return column

    def _column_type(self, node: ManifestNode, column: str) -> str | None:
        parts = [part.strip('"').lower() for part in (node.relation_name or "").split(".")]
        if len(parts) != 3:
            return None
        database, schema, table = parts
        return self._catalog.get(database, {}).get(schema, {}).get(table, {}).get(column.lower())


def detect_value_format(values: list[str]) -> ValueFormat:
    votes = Counter(
        next(
            (value_format for value_format, pattern in _FORMAT_PATTERNS if pattern.match(value.strip())),
            ValueFormat.TEXT,
        )
        for value in values
    )
    if not votes:
        return ValueFormat.TEXT
    if ValueFormat.NUMBER_WITH_SEPARATORS in votes and set(votes) <= {
        ValueFormat.NUMBER_WITH_SEPARATORS,
        ValueFormat.INTEGER,
    }:
        return ValueFormat.NUMBER_WITH_SEPARATORS
    return votes.most_common(1)[0][0]


def _identifier(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise DiagnosticCheckError(f"invalid column name {name!r}")
    return name


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _null_rate(rows: list[list[CellValue]]) -> float:
    total = sum(int(row[1] or 0) for row in rows)
    nulls = sum(int(row[2] or 0) for row in rows)
    return nulls / total if total else 0.0


def _is_within_range(low: CellValue, high: CellValue, parent_low: CellValue, parent_high: CellValue) -> bool:
    bounds = [low, high, parent_low, parent_high]
    numbers = [value for value in bounds if isinstance(value, int | float) and not isinstance(value, bool)]
    if len(numbers) != len(bounds):
        return False
    orphan_low, orphan_high, key_low, key_high = numbers
    return key_low <= orphan_low and orphan_high <= key_high
