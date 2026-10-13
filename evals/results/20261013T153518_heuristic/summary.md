# Pipeline Doctor eval: `heuristic`

Run started 2026-10-13 15:35 UTC; 36/36 faults triggered a failure.

| Metric | Value |
| --- | --- |
| Root-cause category accuracy | 100.0% |
| Culprit accuracy | 100.0% |
| Fix validity (rebuild passes / correct escalation) | 100.0% |
| Category accuracy on cases with distractor commits | 100.0% |
| JEV proceed rate (fix sent to a human as trusted) | 100.0% |
| JEV proceed precision (trusted and actually right) | 100.0% |
| JEV catch rate (wrong diagnoses held back) | n/a (no wrong diagnoses) |
| Mean / p95 latency | 0.09s / 0.17s |
| Total LLM cost | $0.0000 |
| Mean diagnostic queries per incident | 1.69 |

## By root-cause category

| Category | Cases | Category acc. | Culprit acc. | Fix validity |
| --- | --- | --- | --- | --- |
| code_join_fanout | 3 | 100% | 100% | 100% |
| code_logic_regression | 3 | 100% | 100% | 100% |
| code_sql_error | 4 | 100% | 100% | 100% |
| source_column_renamed | 4 | 100% | 100% | 100% |
| source_duplicate_rows | 4 | 100% | 100% | 100% |
| source_missing_rows | 3 | 100% | 100% | 100% |
| source_new_category | 4 | 100% | 100% | 100% |
| source_null_spike | 4 | 100% | 100% | 100% |
| source_stale | 4 | 100% | 100% | 100% |
| source_type_changed | 3 | 100% | 100% | 100% |

## Latency by step (from OpenTelemetry spans)

| Span | Count | Mean ms | p95 ms |
| --- | --- | --- | --- |
| `agent.assess` | 36 | 0.1 | 0.1 |
| `agent.detect` | 36 | 0.0 | 0.0 |
| `agent.explain` | 36 | 0.0 | 0.0 |
| `agent.gather_context` | 36 | 67.1 | 141.3 |
| `agent.hypothesize` | 36 | 0.1 | 0.1 |
| `agent.jev` | 36 | 0.7 | 1.7 |
| `agent.propose_fix` | 36 | 0.6 | 2.0 |
| `agent.request_approval` | 36 | 0.0 | 0.0 |
| `agent.verify` | 36 | 15.5 | 34.1 |
| `pipeline_doctor.diagnose` | 36 | 89.2 | 164.7 |
| `tool.airflow.read_task_log` | 36 | 0.1 | 0.3 |
| `tool.dbt.load_failures` | 36 | 16.2 | 27.9 |
| `tool.git.list_recent_changes` | 36 | 33.9 | 69.0 |
| `tool.memory.search` | 36 | 0.0 | 0.0 |
| `tool.warehouse.detect_schema_drift` | 36 | 0.1 | 0.1 |
| `tool.warehouse.run_check` | 54 | 10.0 | 20.7 |

## Cases

| Case | Expected | Predicted | Culprit | Fix | JEV | Latency |
| --- | --- | --- | --- | --- | --- | --- |
| renamed_orders_status | source_column_renamed | ✅ source_column_renamed (0.95) | ✅ raw.orders.status | ✅ schema_patch | proceed | 0.09s |
| renamed_payments_method | source_column_renamed | ✅ source_column_renamed (0.95) | ✅ raw.payments.payment_method | ✅ schema_patch | proceed | 0.09s |
| renamed_customers_first_name | source_column_renamed | ✅ source_column_renamed (0.95) | ✅ raw.customers.first_name | ✅ schema_patch | proceed | 0.07s |
| type_payments_amount_formatted | source_type_changed | ✅ source_type_changed (0.95) | ✅ raw.payments.amount | ✅ schema_patch | proceed | 0.09s |
| type_orders_date_us_format | source_type_changed | ✅ source_type_changed (0.95) | ✅ raw.orders.order_date | ✅ schema_patch | proceed | 0.06s |
| nulls_orders_customer_id | source_null_spike | ✅ source_null_spike (0.9) | ✅ raw.orders.customer_id | ✅ operational | proceed | 0.06s |
| nulls_payments_order_id | source_null_spike | ✅ source_null_spike (0.9) | ✅ raw.payments.order_id | ✅ operational | proceed | 0.06s |
| nulls_customers_email | source_null_spike | ✅ source_null_spike (0.9) | ✅ raw.customers.email | ✅ operational | proceed | 0.06s |
| duplicates_orders | source_duplicate_rows | ✅ source_duplicate_rows (0.9) | ✅ raw.orders.id | ✅ model_change | proceed | 0.06s |
| duplicates_payments | source_duplicate_rows | ✅ source_duplicate_rows (0.9) | ✅ raw.payments.id | ✅ model_change | proceed | 0.06s |
| duplicates_customers | source_duplicate_rows | ✅ source_duplicate_rows (0.9) | ✅ raw.customers.id | ✅ model_change | proceed | 0.06s |
| stale_orders | source_stale | ✅ source_stale (0.95) | ✅ raw.orders | ✅ operational | proceed | 0.05s |
| stale_payments | source_stale | ✅ source_stale (0.95) | ✅ raw.payments | ✅ operational | proceed | 0.05s |
| stale_customers | source_stale | ✅ source_stale (0.95) | ✅ raw.customers | ✅ operational | proceed | 0.05s |
| new_status_refund_pending | source_new_category | ✅ source_new_category (0.9) | ✅ raw.orders.status | ✅ test_change | proceed | 0.06s |
| new_payment_method_crypto | source_new_category | ✅ source_new_category (0.9) | ✅ raw.payments.payment_method | ✅ test_change | proceed | 0.05s |
| new_status_on_hold | source_new_category | ✅ source_new_category (0.9) | ✅ raw.orders.status | ✅ test_change | proceed | 0.05s |
| missing_customers | source_missing_rows | ✅ source_missing_rows (0.85) | ✅ raw.customers | ✅ operational | proceed | 0.10s |
| missing_orders | source_missing_rows | ✅ source_missing_rows (0.85) | ✅ raw.orders | ✅ operational | proceed | 0.06s |
| code_typo_column | code_sql_error | ✅ code_sql_error (0.85) | ✅ stg_payments (commit ca90fb0e) | ✅ model_change | proceed | 0.08s |
| code_missing_comma | code_sql_error | ✅ code_sql_error (0.8) | ✅ fct_orders (commit 8d08cf1b) | ✅ model_change | proceed | 0.08s |
| code_bad_ref | code_sql_error | ✅ code_sql_error (0.8) | ✅ int_order_payments (commit 48ef9465) | ✅ model_change | proceed | 0.13s |
| code_fanout_fct_orders | code_join_fanout | ✅ code_join_fanout (0.85) | ✅ fct_orders (commit 65c45833) | ✅ model_change | proceed | 0.11s |
| code_fanout_dim_customers | code_join_fanout | ✅ code_join_fanout (0.85) | ✅ dim_customers (commit fb1bc223) | ✅ model_change | proceed | 0.16s |
| code_cents_not_converted | code_logic_regression | ✅ code_logic_regression (0.7) | ✅ stg_payments (commit a5ea48d5) | ✅ model_change | proceed | 0.10s |
| code_dropped_coalesce | code_logic_regression | ✅ code_logic_regression (0.7) | ✅ fct_orders (commit c8cd583e) | ✅ model_change | proceed | 0.10s |
| renamed_orders_status__comment | source_column_renamed | ✅ source_column_renamed (0.95) | ✅ raw.orders.status | ✅ schema_patch | proceed | 0.10s |
| type_payments_amount_formatted__docs | source_type_changed | ✅ source_type_changed (0.95) | ✅ raw.payments.amount | ✅ schema_patch | proceed | 0.17s |
| nulls_orders_customer_id__comment | source_null_spike | ✅ source_null_spike (0.9) | ✅ raw.orders.customer_id | ✅ operational | proceed | 0.10s |
| duplicates_payments__unrelated_sql | source_duplicate_rows | ✅ source_duplicate_rows (0.9) | ✅ raw.payments.id | ✅ model_change | proceed | 0.11s |
| stale_orders__docs | source_stale | ✅ source_stale (0.95) | ✅ raw.orders | ✅ operational | proceed | 0.14s |
| new_payment_method_crypto__comment | source_new_category | ✅ source_new_category (0.9) | ✅ raw.payments.payment_method | ✅ test_change | proceed | 0.10s |
| missing_customers__unrelated_sql | source_missing_rows | ✅ source_missing_rows (0.85) | ✅ raw.customers | ✅ operational | proceed | 0.09s |
| code_typo_column__docs | code_sql_error | ✅ code_sql_error (0.85) | ✅ stg_payments (commit 2b0eee8a) | ✅ model_change | proceed | 0.14s |
| code_fanout_fct_orders__comment | code_join_fanout | ✅ code_join_fanout (0.85) | ✅ fct_orders (commit cdf0ab6a) | ✅ model_change | proceed | 0.17s |
| code_dropped_coalesce__unrelated_sql | code_logic_regression | ✅ code_logic_regression (0.7) | ✅ fct_orders (commit e97ba327) | ✅ model_change | proceed | 0.12s |
