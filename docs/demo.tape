# Demo GIF: a broken dbt pipeline becomes a draft PR.
# Render from the repo root after `uv sync --all-extras`:  vhs docs/demo.tape
# Uses a scratch workspace under .pipeline_doctor/ (gitignored) and the heuristic reasoner.
# Only repo-relative paths appear on screen; `head -1` drops approve's absolute file:// PR URL.

Output docs/demo.gif

Require bash
Set Shell "bash"
Set FontSize 16
Set Width 1300
Set Height 780
Set Padding 20
Set Framerate 20
Set TypingSpeed 30ms
Set WaitTimeout 90s
Set Theme "Catppuccin Mocha"

Hide
Type "source .venv/bin/activate && export WS=.pipeline_doctor/demo-gif && rm -rf $WS" Enter
Type "export PIPELINE_DOCTOR_DBT_PROJECT_DIR=$WS/project PIPELINE_DOCTOR_DUCKDB_PATH=$WS/warehouse.duckdb" Enter
Type "export PIPELINE_DOCTOR_AIRFLOW_LOG_DIR=$WS/airflow_logs PIPELINE_DOCTOR_STATE_DIR=$WS/state" Enter
Type "export PIPELINE_DOCTOR_REASONER=heuristic FORCE_COLOR=1 COLUMNS=120 LESS=-R GIT_PAGER=cat" Enter
Type "PS1='\[\e[1;35m\]$\[\e[0m\] ' && clear" Enter
Sleep 500ms
Show

Type "# Build the demo: dbt project under git, DuckDB raw data, green baseline run" Enter
Sleep 300ms
Type "pipeline-doctor demo setup --workspace $WS" Enter
Wait+Screen /baseline pipeline is green/
Sleep 1.5s

Type "# Upstream renames raw.orders.status; the Airflow dbt_build task fails" Enter
Sleep 300ms
Type "pipeline-doctor demo break renamed_orders_status --workspace $WS" Enter
Wait+Screen /failed\./
Sleep 2s

Type "pipeline-doctor diagnose --event $WS/last_failure.json | less" Enter
Wait+Screen /Root cause/
Sleep 9s
Type " "
Sleep 9s
Type "q"
Sleep 500ms

Type "pipeline-doctor incidents" Enter
Wait+Screen /awaiting/
Sleep 2.5s
Hide
Type "ID=$(basename $PIPELINE_DOCTOR_STATE_DIR/incidents/*.json .json) && clear" Enter
Sleep 300ms
Show

Type "# A named human approves; the fix lands on a branch as a draft PR" Enter
Sleep 300ms
Type "pipeline-doctor approve $ID --approver you | head -1" Enter
Wait+Screen /pr_opened/
Sleep 2.5s
Type "git -C $WS/project log --oneline --all -3" Enter
Sleep 2.5s
Type "git -C $WS/project show pipeline-doctor/$ID" Enter
Sleep 7s
