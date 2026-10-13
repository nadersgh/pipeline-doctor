FROM apache/airflow:2.10.5-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# dbt lives in its own venv so it cannot conflict with Airflow's pinned dependencies.
# Versions match uv.lock: the DuckDB file written here is read by the Pipeline Doctor container.
RUN python -m venv /home/airflow/dbt \
    && /home/airflow/dbt/bin/pip install --no-cache-dir dbt-core==1.10.23 dbt-duckdb==1.11.0 duckdb==1.5.6 \
    && mkdir -p /home/airflow/.local/bin \
    && ln -s /home/airflow/dbt/bin/dbt /home/airflow/.local/bin/dbt
