# JEV decision layer

JEV ([`jev/`](../src/pipeline_doctor/jev/)) sits between `propose_fix` and the human. The reasoner proposes. JEV decides whether the proposal deserves a reviewer's time.

| Stage | Question | Checks |
| --- | --- | --- |
| Judge | Does the evidence support this diagnosis? | category named, confidence above `jev_min_confidence`, culprit in the failing lineage, grounded in a successful query, schema drift, or a SQL-changing commit, reasoner converged |
| Evaluate | Does the fix address the cause? | remedy fits the category ([`REMEDY_POLICY`](../src/pipeline_doctor/jev/policy.py)), fix is actionable, edits stay in the lineage, no test removed or loosened (`severity: warn`, `enabled: false`, `where:`) |
| Verify | Is the patch safe to show? | patch re-applies and changes something, no DDL/DML or `pre_hook`/`post_hook`/`run_query` added |

Each check is weighted; the verdict carries a score and the failed checks. The decision routes the graph:

| Decision | When | Next step |
| --- | --- | --- |
| `proceed` | every check passes | `request_approval` |
| `reinvestigate` | a Judge check fails and rounds remain | `hypothesize`, with the critique added to the LLM prompt |
| `revise_fix` | an Evaluate or Verify check fails and `jev_max_fix_revisions` remain | `propose_fix`, with the failed checks as feedback |
| `escalate` | budget exhausted | code fix replaced by an operational handoff to the on-call engineer |

JEV is on by default. Turn it off with `PIPELINE_DOCTOR_JEV_ENABLED=false`, or per run with `--no-jev` on `diagnose` and `eval`. When off, `propose_fix` goes straight to `request_approval`, incidents carry no verdict, and the human reviewer is the only gate. Eval runs without JEV are saved as `<run>_nojev` so you can compare both.

The rule reviewer is deterministic and free. Set `PIPELINE_DOCTOR_JEV_LLM_JUDGE=true` with an LLM reasoner to add an LLM-as-judge behind it. The judge runs only after the rules pass, can veto but never overturn a rule failure, and falls back to the rule verdict if the model call fails. When JEV is on, `ApprovalService` refuses to open a PR for any code fix whose verdict is not `proceed`. The verdict appears in the incident report, the Slack message, the `agent.jev` span, and the eval table (proceed rate, proceed precision, catch rate).
