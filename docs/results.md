# Results

53 injected failures, each breaking a real dbt project on DuckDB: the `dev` split (39 cases across 10 root-cause categories, 3 carrying prompt-injection payloads), which the heuristic reasoner was written against, and an untuned `holdout` split (14 cases: unseen faults, 3 compound faults, and 4 infrastructure failures with no code or data fault). The agent runs on the Airflow failure, then its fix is applied and the project rebuilt.

Every case is confirmed red before diagnosis, and after it the unpatched project is rerun once: all faults came back (100%) and all four infrastructure failures cleared on a plain rerun (100%), so a green build after a patch is attributable to the patch.

**Dev split (39 cases).** The heuristic scores 100% because it was written alongside these cases, so read that column as a regression baseline. Claude here is Sonnet, one run on 2026-10-14.

| Fault type (root-cause category) | Cases | Heuristic: category / culprit / fix valid | Claude Sonnet: category / culprit / fix valid |
| --- | --- | --- | --- |
| `source_column_renamed` | 5 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_type_changed` | 3 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_null_spike` | 5 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_duplicate_rows` | 4 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_stale` | 4 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_new_category` | 4 | 100% / 100% / 100% | 100% / 100% / 100% |
| `source_missing_rows` | 3 | 100% / 100% / 100% | 100% / 100% / 100% |
| `code_sql_error` | 5 | 100% / 100% / 100% | 100% / 100% / 100% |
| `code_join_fanout` | 3 | 100% / 100% / 100% | 67% / 100% / 100% |
| `code_logic_regression` | 3 | 100% / 100% / 100% | 100% / 100% / 100% |
| **All** | **39** | **100% / 100% / 100%** | **97% / 100% / 100%** |

**Model comparison.** The heuristic and Haiku ran all 53 cases (Haiku 3 times each, 159 runs, 2026-10-19, mean with the spread across repeats). Sonnet ran the first 51 cases once on 2026-10-14, before the two newest infrastructure cases and the rerun and abstention metrics existed, so those cells are blank. Treat differences of a point or two as noise.

| Metric | Heuristic | Claude Sonnet 5.5 | Claude Haiku 5.5 |
| --- | --- | --- | --- |
| Cases / runs | 53 / 53 | 51 / 51 | 53 / 159 |
| Category accuracy | 98.1% | 96.1% | 95.0% (std 1.1, range 94.3% to 96.2%) |
| Fix validity | 96.2% | 98.0% | 97.5% (std 1.1, range 96.2% to 98.1%) |
| Dev category / fix validity (39) | 100% / 100% | 97.4% / 100% | 96.6% / 99.2% |
| Holdout category / fix validity | 92.9% / 85.7% (14) | 91.7% / 91.7% (12) | 90.5% / 92.9% (14) |
| False positives on no-fault cases | 0% | 0% | 0% |
| Abstained (`unknown`), all runs | 7.5% | 5.9% | 9.4% |
| Abstained on a real fault | 0% | 2.0% | 2.0% |
| Confident wrong answers (wrong category at confidence 0.6 or more) | 0% | 2.0% | 3.1% (std 1.1) |
| Faults that reproduced / no-fault failures that cleared on rerun | 100% / 100% | | 100% / 100% |
| JEV proceed precision | 100% | 97.9% | 92.3% |
| Latency per incident, mean / p95 | 0.09s / 0.18s | 22.8s / 53.8s | 19.6s / 38.8s |
| Diagnostic queries per incident, mean | 1.6 | 6.0 | 3.7 |
| Cost per full pass | $0 | $2.99 | $0.18 |
| Cost per incident | $0 | $0.059 | $0.0033 |

What the numbers say:

- **Fix validity is the outcome that matters, and the two LLMs match it.** Haiku reaches Sonnet's fix validity for about 1/18th of the price, and 97.5% of its runs end with a green build and every test still running, or a correct escalation to a human.
- **Abstaining.** Both LLMs answer `unknown` and escalate on the infrastructure failures (12 of 12 Haiku runs). On a real fault they abstain 2% of the time, always on `holdout_type_customers_id_prefixed` (an id column that starts arriving as a prefixed string), at 0.3 to 0.5 confidence. The heuristic never abstains on a real fault; it guessed `source_missing_rows` there.
- **Confident wrong answers.** Haiku named a wrong category at 0.6 confidence or more in 5 of 159 runs: a join fan-out called a logic regression (3 runs, valid fix), a new category read as a type change (1, valid fix), and a dropped `COALESCE` read as missing rows (1, the only one with an invalid fix). JEV sent all five to a human as trusted, which is why its proceed precision is 92% against 100% for the heuristic; the dropped `COALESCE` case sat exactly on the 0.6 threshold.
- **Haiku's weak spot is the culprit column.** It names the wrong column for `source_missing_rows` (`missing_customers__unrelated_sql` in 3 of 3 runs) and sometimes for late-arriving rows; a few cases flip between runs, so per-case agreement is 97.5%.

Prices are $2 / $10 (Sonnet) and $0.10 / $0.50 (Haiku) per million tokens. To reproduce the Haiku run:

```bash
PIPELINE_DOCTOR_LLM_MODEL=claude-haiku-5-5 PIPELINE_DOCTOR_LLM_INPUT_COST_PER_MTOK=0.10 PIPELINE_DOCTOR_LLM_OUTPUT_COST_PER_MTOK=0.50 \
  ANTHROPIC_API_KEY=... uv run pipeline-doctor eval --reasoner anthropic --split all --workers 4 --repeats 3
```

A fix is valid when the patch applies and `dbt source freshness && dbt build` goes green with every baseline test still running, when the agent correctly escalates to the data owner, or, for a failure that was already clearing by itself, when it proposes no code change. A code fix is not credited if the failure did not come back on the unpatched rerun. The same harness runs any reasoner:

```bash
ANTHROPIC_API_KEY=... uv run pipeline-doctor eval --reasoner anthropic --split all --workers 4   # or OPENAI_API_KEY + --reasoner openai
```

The OpenAI reasoner has not been run through the eval yet.

Without a working key every LLM step falls back to the heuristic. The eval counts those steps and exits with an error if any fell back, so a run labelled with an LLM cannot silently report rule-based scores. Still check that `Total LLM cost / calls` is non-zero before publishing.
