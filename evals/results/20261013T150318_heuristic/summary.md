# Pipeline Doctor eval: `heuristic`

Run started 2026-10-13 15:03 UTC; 36/36 faults triggered a failure.

| Metric | Value |
| --- | --- |
| Root-cause category accuracy | 100.0% |
| Culprit accuracy | 100.0% |
| Fix validity (rebuild passes / correct escalation) | 100.0% |
| Category accuracy on cases with distractor commits | 100.0% |
| Mean / p95 latency | 0.09s / 0.15s |
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
| `agent.gather_context` | 36 | 68.3 | 137.7 |
| `agent.hypothesize` | 36 | 0.1 | 0.1 |
| `agent.propose_fix` | 36 | 0.7 | 1.6 |
| `agent.request_approval` | 36 | 0.0 | 0.0 |
| `agent.verify` | 36 | 17.1 | 33.9 |
| `pipeline_doctor.diagnose` | 36 | 91.4 | 153.8 |
| `tool.airflow.read_task_log` | 36 | 0.2 | 1.0 |
| `tool.dbt.load_failures` | 36 | 18.0 | 29.7 |
| `tool.git.list_recent_changes` | 36 | 33.3 | 60.7 |
| `tool.memory.search` | 36 | 0.0 | 0.1 |
| `tool.warehouse.detect_schema_drift` | 36 | 0.1 | 0.1 |
| `tool.warehouse.run_check` | 54 | 10.9 | 26.6 |

## Cases

| Case | Expected | Predicted | Culprit | Fix | Latency |
| --- | --- | --- | --- | --- | --- |
| renamed_orders_status | source_column_renamed | ✅ source_column_renamed (0.95) | ✅ raw.orders.status | ✅ schema_patch | 0.08s |
| renamed_payments_method | source_column_renamed | ✅ source_column_renamed (0.95) | ✅ raw.payments.payment_method | ✅ schema_patch | 0.06s |
| renamed_customers_first_name | source_column_renamed | ✅ source_column_renamed (0.95) | ✅ raw.customers.first_name | ✅ schema_patch | 0.09s |
| type_payments_amount_formatted | source_type_changed | ✅ source_type_changed (0.95) | ✅ raw.payments.amount | ✅ schema_patch | 0.10s |
| type_orders_date_us_format | source_type_changed | ✅ source_type_changed (0.95) | ✅ raw.orders.order_date | ✅ schema_patch | 0.09s |
| nulls_orders_customer_id | source_null_spike | ✅ source_null_spike (0.9) | ✅ raw.orders.customer_id | ✅ operational | 0.07s |
| nulls_payments_order_id | source_null_spike | ✅ source_null_spike (0.9) | ✅ raw.payments.order_id | ✅ operational | 0.06s |
| nulls_customers_email | source_null_spike | ✅ source_null_spike (0.9) | ✅ raw.customers.email | ✅ operational | 0.06s |
| duplicates_orders | source_duplicate_rows | ✅ source_duplicate_rows (0.9) | ✅ raw.orders.id | ✅ model_change | 0.08s |
| duplicates_payments | source_duplicate_rows | ✅ source_duplicate_rows (0.9) | ✅ raw.payments.id | ✅ model_change | 0.07s |
| duplicates_customers | source_duplicate_rows | ✅ source_duplicate_rows (0.9) | ✅ raw.customers.id | ✅ model_change | 0.07s |
| stale_orders | source_stale | ✅ source_stale (0.95) | ✅ raw.orders | ✅ operational | 0.06s |
| stale_payments | source_stale | ✅ source_stale (0.95) | ✅ raw.payments | ✅ operational | 0.06s |
| stale_customers | source_stale | ✅ source_stale (0.95) | ✅ raw.customers | ✅ operational | 0.06s |
| new_status_refund_pending | source_new_category | ✅ source_new_category (0.9) | ✅ raw.orders.status | ✅ test_change | 0.05s |
| new_payment_method_crypto | source_new_category | ✅ source_new_category (0.9) | ✅ raw.payments.payment_method | ✅ test_change | 0.05s |
| new_status_on_hold | source_new_category | ✅ source_new_category (0.9) | ✅ raw.orders.status | ✅ test_change | 0.05s |
| missing_customers | source_missing_rows | ✅ source_missing_rows (0.85) | ✅ raw.customers | ✅ operational | 0.07s |
| missing_orders | source_missing_rows | ✅ source_missing_rows (0.85) | ✅ raw.orders | ✅ operational | 0.10s |
| code_typo_column | code_sql_error | ✅ code_sql_error (0.85) | ✅ stg_payments (commit 84de0c90) | ✅ model_change | 0.12s |
| code_missing_comma | code_sql_error | ✅ code_sql_error (0.8) | ✅ fct_orders (commit 915703a4) | ✅ model_change | 0.08s |
| code_bad_ref | code_sql_error | ✅ code_sql_error (0.8) | ✅ int_order_payments (commit d72f40b3) | ✅ model_change | 0.08s |
| code_fanout_fct_orders | code_join_fanout | ✅ code_join_fanout (0.85) | ✅ fct_orders (commit 2f552576) | ✅ model_change | 0.12s |
| code_fanout_dim_customers | code_join_fanout | ✅ code_join_fanout (0.85) | ✅ dim_customers (commit 86ab4144) | ✅ model_change | 0.12s |
| code_cents_not_converted | code_logic_regression | ✅ code_logic_regression (0.7) | ✅ stg_payments (commit 36a212b1) | ✅ model_change | 0.11s |
| code_dropped_coalesce | code_logic_regression | ✅ code_logic_regression (0.7) | ✅ fct_orders (commit a3af75f6) | ✅ model_change | 0.15s |
| renamed_orders_status__comment | source_column_renamed | ✅ source_column_renamed (0.95) | ✅ raw.orders.status | ✅ schema_patch | 0.10s |
| type_payments_amount_formatted__docs | source_type_changed | ✅ source_type_changed (0.95) | ✅ raw.payments.amount | ✅ schema_patch | 0.14s |
| nulls_orders_customer_id__comment | source_null_spike | ✅ source_null_spike (0.9) | ✅ raw.orders.customer_id | ✅ operational | 0.11s |
| duplicates_payments__unrelated_sql | source_duplicate_rows | ✅ source_duplicate_rows (0.9) | ✅ raw.payments.id | ✅ model_change | 0.13s |
| stale_orders__docs | source_stale | ✅ source_stale (0.95) | ✅ raw.orders | ✅ operational | 0.10s |
| new_payment_method_crypto__comment | source_new_category | ✅ source_new_category (0.9) | ✅ raw.payments.payment_method | ✅ test_change | 0.12s |
| missing_customers__unrelated_sql | source_missing_rows | ✅ source_missing_rows (0.85) | ✅ raw.customers | ✅ operational | 0.08s |
| code_typo_column__docs | code_sql_error | ✅ code_sql_error (0.85) | ✅ stg_payments (commit 7f33c8ab) | ✅ model_change | 0.15s |
| code_fanout_fct_orders__comment | code_join_fanout | ✅ code_join_fanout (0.85) | ✅ fct_orders (commit 2cdbb3b9) | ✅ model_change | 0.16s |
| code_dropped_coalesce__unrelated_sql | code_logic_regression | ✅ code_logic_regression (0.7) | ✅ fct_orders (commit 05b5ed49) | ✅ model_change | 0.11s |
