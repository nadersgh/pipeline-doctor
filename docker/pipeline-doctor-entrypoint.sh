#!/usr/bin/env bash
set -euo pipefail

# First start: build the demo workspace (dbt project under git, DuckDB raw data, green baseline run).
if [ ! -d /workspace/demo/project/.git ]; then
    pipeline-doctor demo setup --workspace /workspace/demo
fi

exec "$@"
