# Evals

The catalogue has two splits:

- `dev`: [`evals/cases.py`](../src/pipeline_doctor/evals/cases.py), 26 faults across all 10 categories plus 10 hard variants that add a misleading recent commit (a docs edit, a comment-only SQL change, or a real SQL change outside the failing lineage). [`evals/adversarial.py`](../src/pipeline_doctor/evals/adversarial.py) adds 3 prompt-injection variants: a real fault plus a payload in a commit message, a source column value, or a dbt error. The heuristic reasoner was written against the 36 non-adversarial cases.
- `holdout`: 16 cases nobody tuned against. [`holdout.py`](../src/pipeline_doctor/evals/holdout.py) has 7 single faults on new tables, columns, and mechanisms: an over-broad filter in a staging model, late-arriving customers, a column renamed in a model, a prefixed string id, an aggregate grain change, CDC updates appended as rows, and upper-cased enum values. [`compound.py`](../src/pipeline_doctor/evals/compound.py) has 3 cases with two simultaneous faults. [`nofault.py`](../src/pipeline_doctor/evals/nofault.py) has 4 runs that fail for infrastructure reasons with green data and no suspect commits, and clear on a plain rerun: a held DuckDB write lock, an OOM-killed worker, a warehouse connection reset, and a zombie task after a lost heartbeat. [`transient.py`](../src/pipeline_doctor/evals/transient.py) has 2 data faults that heal by themselves before the next run: late customer rows that land after the first failure, and customer rows that a replay restores. At diagnosis time the data really is wrong, so naming the missing rows or answering `unknown` are both correct; any code change is a false fix, because the control reruns pass without it. `holdout_late_arriving_customers` is the same fault without the healing, so the pair shows the control rerun telling a persistent fault from a transient one. Keep holdout untuned: once a reasoner, prompt, or JEV rule is changed to pass a holdout case, move that case to `dev` and add a fresh one.

For each case and repeat the harness:

1. clones a green workspace (dbt project, git history, DuckDB)
2. injects the fault: mutates raw data, commits a code change as a developer would, or reproduces the declared infrastructure failure
3. runs the pipeline exactly like the DAG, and checks that the run actually fails
4. runs the agent on the resulting Airflow failure event
5. scores each part of the answer separately: **category**, affected **relation**, culprit **column** (when the ground truth names one), **fix kind** (acceptable for the case), and **fix validity**. A code fix only counts if the patch applies, `dbt source freshness && dbt build` goes green afterwards, and every baseline test still runs, so deleting a test does not count as a fix. An operational fix only counts when escalation is the right answer, such as a stale feed or deleted parent rows.

A compound case accepts either fault as the root cause. **Primary hit** means category and relation match one of them, and the patch is validated against that fault alone. A no-fault case is correct when the agent answers `unknown` and proposes no code change. **False-positive rate** is the share of no-fault runs where the agent proposed a code change or named a culprit with confidence at or above `jev_min_confidence`.

Run ids and workspace paths are random, so nothing in the agent's context names the fault. A fault that fails to break the pipeline counts as a miss.

```bash
uv run pipeline-doctor eval --split holdout          # dev (default, the regression set) | holdout | all
uv run pipeline-doctor eval --repeats 3 --workers 6  # each run in a fresh workspace
```

With `--repeats N` the report adds mean, std dev, min, and max of every metric across repeats, plus per-case agreement (share of repeats with the same outcome). Each split and case kind gets accuracy next to cost, LLM calls, and p50/p95 latency, and the per-case table lists cost, latency, LLM calls, and queries. Results go to `evals/results/<run>/` as `summary.md`, `summary.json`, `results.jsonl`, and `spans.jsonl`, with a per-step latency table built from the traces.

Current numbers for the heuristic and the LLM reasoners are in [results.md](results.md).

After each diagnosis the harness reruns the unpatched project three times (`--control-reruns`). A failure is *reproduced* when every rerun fails, *cleared* when none does, and *flaky* when some do. A code fix is credited only for a reproduced failure, so a green build can only be credited to the patch. Besides accuracy, the report shows the abstention rate (`unknown` answers, overall and on real faults), the confident wrong rate (a wrong category named at confidence 0.6 or more), and mean confidence for right and for wrong answers.
