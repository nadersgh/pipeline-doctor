import pytest
import sqlglot
from sqlglot import exp

from pipeline_doctor.warehouse.guard import SqlGuard, SqlGuardViolationError


@pytest.fixture
def guard() -> SqlGuard:
    return SqlGuard("snowflake", allowed_databases=["ANALYTICS_DB"], allowed_schemas=["raw"], max_rows=10)


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("select * from @raw.stage", id="stage-scan"),
        pytest.param("select $1 from @raw.stage/file.csv", id="stage-file"),
        pytest.param("select t.$1 from @~/staged t", id="user-stage"),
        pytest.param("select * from @%orders", id="table-stage"),
        pytest.param("select metadata$filename from @raw.stage", id="stage-metadata"),
        pytest.param("list @raw.stage", id="list-stage"),
        pytest.param("select raw.seq.nextval", id="nextval"),
        pytest.param("select raw.seq.nextval as id from raw.orders", id="nextval-in-query"),
        pytest.param("copy into raw.orders from @raw.stage", id="copy-into-table"),
        pytest.param("copy into @raw.stage from raw.orders", id="copy-into-stage"),
        pytest.param("copy into 's3://bucket/x' from raw.orders", id="copy-into-url"),
        pytest.param("put file:///etc/passwd @raw.stage", id="put"),
        pytest.param("get @raw.stage file:///home/", id="get"),
        pytest.param("remove @raw.stage", id="remove"),
        pytest.param("select * from table(generator(rowcount => 1000000000))", id="table-generator"),
        pytest.param("select * from table(flatten(input => parse_json('[1]')))", id="table-flatten"),
        pytest.param("select * from table(split_to_table('a,b', ','))", id="table-split"),
        pytest.param("select * from table(information_schema.query_history())", id="table-query-history"),
        pytest.param("select * from table(result_scan(last_query_id()))", id="table-result-scan"),
        pytest.param("select * from table(infer_schema(location => '@raw.stage'))", id="table-infer-schema"),
        pytest.param("select * from table(raw.my_udtf(1))", id="table-udtf"),
        pytest.param("select * from identifier('raw.orders')", id="identifier-function"),
        pytest.param("select * into raw.copy from raw.orders", id="select-into"),
        pytest.param("select 1 from raw.orders; select 2 from raw.orders", id="two-selects"),
        pytest.param("select 1 from raw.orders; drop table raw.orders", id="select-then-drop"),
        pytest.param("begin; select 1 from raw.orders; commit", id="transaction"),
        pytest.param("create or replace table raw.x as select 1", id="ctas"),
        pytest.param("insert overwrite into raw.orders select * from raw.orders", id="insert-overwrite"),
        pytest.param("merge into raw.orders t using raw.o2 s on t.id = s.id when matched then delete", id="merge"),
        pytest.param("truncate table raw.orders", id="truncate"),
        pytest.param("undrop table raw.orders", id="undrop"),
        pytest.param("call raw.my_proc()", id="call"),
        pytest.param("execute immediate 'drop table raw.orders'", id="execute-immediate"),
        pytest.param("use role accountadmin", id="use-role"),
        pytest.param("alter session set query_tag = 'x'", id="alter-session"),
        pytest.param("show tables", id="show"),
        pytest.param("describe table raw.orders", id="describe"),
        pytest.param("grant select on raw.orders to role public", id="grant"),
        pytest.param("select system$whitelist()", id="system-function"),
        pytest.param("select get_presigned_url(@raw.stage, 'f.csv')", id="presigned-url"),
        pytest.param("select build_scoped_file_url(@raw.stage, 'f')", id="scoped-file-url"),
        pytest.param("select raw.ext_fn(id) from raw.orders", id="external-function"),
        pytest.param("select * from snowflake.account_usage.query_history", id="account-usage"),
        pytest.param("select * from other_db.raw.orders", id="other-database"),
        pytest.param("select * from analytics_db.marts.revenue", id="unlisted-schema"),
        pytest.param("select * from orders", id="unqualified-table"),
    ],
)
def test_snowflake_guard_blocks_writes_stages_and_escape_hatches(guard: SqlGuard, sql: str) -> None:
    with pytest.raises(SqlGuardViolationError):
        guard.enforce(sql)


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param("select count(*) from analytics_db.raw.orders", id="lowercase"),
        pytest.param("select count(*) from ANALYTICS_DB.RAW.ORDERS", id="uppercase"),
        pytest.param('select count(*) from "ANALYTICS_DB"."RAW"."ORDERS"', id="quoted-uppercase"),
        pytest.param("select column_name from information_schema.columns where table_schema = 'RAW'", id="info-schema"),
        pytest.param(
            "select o_orderkey from raw.orders qualify row_number() over (partition by o_custkey order by o_orderdate) = 1",
            id="qualify",
        ),
        pytest.param(
            "select iff(amount > 0, 1, 0), datediff('day', order_date, current_date()) from raw.orders",
            id="iff-datediff",
        ),
        pytest.param("select try_to_date(raw_date), try_to_number(raw_amount) from raw.orders", id="try-functions"),
        pytest.param("select payload:items[0].sku::string from raw.orders", id="variant-path"),
        pytest.param(
            "select f.value from raw.orders, lateral flatten(input => raw.orders.items) f", id="lateral-flatten"
        ),
        pytest.param("select * from raw.orders sample (10)", id="sample"),
    ],
)
def test_snowflake_guard_allows_read_only_snowflake_sql_and_limits_it(guard: SqlGuard, sql: str) -> None:
    # Act
    safe = guard.enforce(sql)

    # Assert
    assert safe.endswith("LIMIT 10")
    assert isinstance(sqlglot.parse_one(safe, read="snowflake"), exp.Select)
