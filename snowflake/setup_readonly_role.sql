-- Pipeline Doctor: read-only role, capped warehouse, and service user.
-- Run as ACCOUNTADMIN (resource monitors need it). Replace ANALYTICS_DB, the RAW/ANALYTICS schemas, and the
-- password before running. Repeat the schema block for every schema in PIPELINE_DOCTOR_ALLOWED_SCHEMAS.
-- Not yet run against a live account; see docs/snowflake.md.

-- 1. Role. It gets USAGE and SELECT only: no OWNERSHIP, CREATE, stage, sequence, or external-function grants.
use role securityadmin;
create role if not exists PIPELINE_DOCTOR_READONLY comment = 'Pipeline Doctor: SELECT-only diagnostics';
grant role PIPELINE_DOCTOR_READONLY to role sysadmin;

-- 2. Small warehouse with a hard statement timeout and a credit cap.
use role sysadmin;
create warehouse if not exists PIPELINE_DOCTOR_WH
  warehouse_size = xsmall
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
  statement_timeout_in_seconds = 120
  statement_queued_timeout_in_seconds = 60
  comment = 'Pipeline Doctor diagnostics';

use role accountadmin;
create resource monitor if not exists PIPELINE_DOCTOR_MONITOR
  with credit_quota = 10
  frequency = monthly
  start_timestamp = immediately
  triggers on 80 percent do notify
           on 100 percent do suspend
           on 110 percent do suspend_immediate;
alter warehouse PIPELINE_DOCTOR_WH set resource_monitor = PIPELINE_DOCTOR_MONITOR;

-- 3. Read access to the dbt database: USAGE on the database and allowed schemas, SELECT on tables and views,
--    including ones created later.
use role securityadmin;
grant usage on warehouse PIPELINE_DOCTOR_WH to role PIPELINE_DOCTOR_READONLY;
grant usage on database ANALYTICS_DB to role PIPELINE_DOCTOR_READONLY;

grant usage on schema ANALYTICS_DB.RAW to role PIPELINE_DOCTOR_READONLY;
grant select on all tables in schema ANALYTICS_DB.RAW to role PIPELINE_DOCTOR_READONLY;
grant select on all views in schema ANALYTICS_DB.RAW to role PIPELINE_DOCTOR_READONLY;
grant select on future tables in schema ANALYTICS_DB.RAW to role PIPELINE_DOCTOR_READONLY;
grant select on future views in schema ANALYTICS_DB.RAW to role PIPELINE_DOCTOR_READONLY;

grant usage on schema ANALYTICS_DB.ANALYTICS to role PIPELINE_DOCTOR_READONLY;
grant select on all tables in schema ANALYTICS_DB.ANALYTICS to role PIPELINE_DOCTOR_READONLY;
grant select on all views in schema ANALYTICS_DB.ANALYTICS to role PIPELINE_DOCTOR_READONLY;
grant select on future tables in schema ANALYTICS_DB.ANALYTICS to role PIPELINE_DOCTOR_READONLY;
grant select on future views in schema ANALYTICS_DB.ANALYTICS to role PIPELINE_DOCTOR_READONLY;

-- 4. Service user. The adapter authenticates with a password today, so it needs a LEGACY_SERVICE user.
use role useradmin;
create user if not exists PIPELINE_DOCTOR
  type = legacy_service
  password = '<replace-with-a-generated-secret>'
  default_role = PIPELINE_DOCTOR_READONLY
  default_warehouse = PIPELINE_DOCTOR_WH
  comment = 'Pipeline Doctor diagnostics';

use role securityadmin;
grant role PIPELINE_DOCTOR_READONLY to user PIPELINE_DOCTOR;
alter user PIPELINE_DOCTOR set statement_timeout_in_seconds = 120;

-- 5. Optional: lets the live test (tests/test_snowflake_live.py) run as this role on the shared sample data.
use role accountadmin;
grant imported privileges on database SNOWFLAKE_SAMPLE_DATA to role PIPELINE_DOCTOR_READONLY;

-- 6. Check: expect only USAGE (warehouse, database, schemas) and SELECT (tables, views), plus IMPORTED PRIVILEGES
--    if step 5 ran.
show grants to role PIPELINE_DOCTOR_READONLY;
show future grants in schema ANALYTICS_DB.RAW;
