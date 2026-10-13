FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.9.14 /uv /usr/local/bin/uv

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    # uid 50000 matches the Airflow image, so both containers own the shared workspace and its git repo
    && useradd --uid 50000 --gid 0 --create-home doctor \
    && mkdir /workspace && chown 50000:0 /workspace

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --all-extras --no-install-project
COPY src ./src
COPY demo_project ./demo_project
RUN uv sync --frozen --no-dev --all-extras

ENV PATH="/app/.venv/bin:${PATH}"
COPY --chmod=755 docker/pipeline-doctor-entrypoint.sh /usr/local/bin/entrypoint.sh
USER 50000
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["pipeline-doctor", "serve", "--host", "0.0.0.0", "--port", "8000"]
