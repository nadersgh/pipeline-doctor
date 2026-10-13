# Snowflake

## Status

| | |
| --- | --- |
| Verified in CI | `SnowflakeWarehouse` against a fake `snowflake.connector` ([tests/test_snowflake_warehouse.py](../tests/test_snowflake_warehouse.py)): connect parameters, read-only role, `QUERY_TAG`, `STATEMENT_TIMEOUT_IN_SECONDS`, row limiting, error mapping to `WarehouseQueryError`, `EXPLAIN USING JSON` bytes feeding `QueryBudget`, `information_schema.columns` feeding schema drift, upper-case identifiers. Every `CheckKind` runs through the snowflake-dialect guard and the adapter, and its SQL transpiled to Snowflake passes the guard. `SqlGuard` dialect cases (stages, `.nextval`, `COPY INTO`, `PUT`/`GET`, `TABLE(...)` functions, `SELECT ... INTO`, multi-statement) are in [tests/test_snowflake_guard.py](../tests/test_snowflake_guard.py). |
| Not verified | No live run yet: there is no CI Snowflake account. The provisioning script below has not been run. The demo and evals run on DuckDB only. |
| Known gaps | Password auth only (no key-pair, OAuth, or SSO). Column names in checks are unquoted, so quoted lower- or mixed-case Snowflake columns are unreachable. The bytes estimate is the compile-time `bytesAssigned` from `EXPLAIN`, not the bytes actually scanned. |

## Provision a read-only role

Run [snowflake/setup_readonly_role.sql](../snowflake/setup_readonly_role.sql) as `ACCOUNTADMIN` after replacing the database, schemas, and password. It creates:

- `PIPELINE_DOCTOR_READONLY`: `USAGE` on the warehouse, database, and allowed schemas, `SELECT` on current and future tables and views. No `OWNERSHIP`, `CREATE`, stage, sequence, or external-function grants.
- `PIPELINE_DOCTOR_WH`: x-small, 120 s statement timeout, behind a 10-credit monthly resource monitor.
- `PIPELINE_DOCTOR`: a `LEGACY_SERVICE` user with that default role and warehouse and a 120 s user-level statement timeout.

Then point the agent at it:

```bash
export PIPELINE_DOCTOR_WAREHOUSE=snowflake
export PIPELINE_DOCTOR_SNOWFLAKE_ACCOUNT=<org>-<account>
export PIPELINE_DOCTOR_SNOWFLAKE_USER=PIPELINE_DOCTOR
export PIPELINE_DOCTOR_SNOWFLAKE_PASSWORD=...
export PIPELINE_DOCTOR_SNOWFLAKE_WAREHOUSE=PIPELINE_DOCTOR_WH
export PIPELINE_DOCTOR_SNOWFLAKE_DATABASE=ANALYTICS_DB
```

## Live pass on a trial account

[tests/test_snowflake_live.py](../tests/test_snowflake_live.py) only reads the shared `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1` tables, so a fresh trial account is enough:

```bash
uv sync --all-extras
export PIPELINE_DOCTOR_SNOWFLAKE_ACCOUNT=<org>-<account>
export PIPELINE_DOCTOR_SNOWFLAKE_USER=<trial user>
export PIPELINE_DOCTOR_SNOWFLAKE_PASSWORD=...
uv run pytest -m snowflake_live -v
```

It checks the session role, `QUERY_TAG`, and statement timeout; `information_schema` columns; the `EXPLAIN` bytes estimate; every diagnostic check against `ORDERS`/`CUSTOMER` (row count 1,500,000, latest `O_ORDERDATE` 1998-08-02, no duplicate or orphan keys, `O_ORDERSTATUS` profile); that the guard blocks a `DELETE`; and that a 1-byte budget blocks a scan.

On a bare trial it connects as `ACCOUNTADMIN` on `COMPUTE_WH`, so it tests the guard and budget, not the role. After running the provisioning script, including its optional sample-data grant, add `PIPELINE_DOCTOR_SNOWFLAKE_ROLE=PIPELINE_DOCTOR_READONLY` and `PIPELINE_DOCTOR_SNOWFLAKE_WAREHOUSE=PIPELINE_DOCTOR_WH` to run it as the read-only role.

The default `uv run pytest` deselects these tests; they also skip when the three variables are unset.
