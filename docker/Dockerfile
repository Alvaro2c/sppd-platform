# Airflow + dbt + DuckDB orchestrator
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc build-essential git curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv

ARG BINARY_VERSION=v1.2.2
RUN curl -sSL -o /tmp/sppd-cli.tar.gz \
    "https://github.com/Alvaro2c/sppd-cli/releases/download/${BINARY_VERSION}/sppd-cli-linux-x86_64-${BINARY_VERSION}.tar.gz" \
    && tar -xzf /tmp/sppd-cli.tar.gz -C /tmp \
    && mv /tmp/sppd-cli /usr/local/bin/sppd-cli \
    && chmod +x /usr/local/bin/sppd-cli \
    && rm -rf /tmp/sppd-cli*

WORKDIR /opt/sppd
RUN useradd -m -u 1000 appuser

COPY pyproject.toml README.md ./
COPY airflow ./airflow
COPY dbt ./dbt
RUN uv pip install --system .

RUN mkdir -p /opt/sppd/data/parquet/mc /opt/sppd/data/parquet/pt \
    && chown -R appuser:appuser /opt/sppd
USER appuser

EXPOSE 8080
ENV AIRFLOW_HOME=/opt/sppd/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/sppd/airflow/dags
ENV DBT_PROJECT_DIR=/opt/sppd/dbt
ENV DUCKDB_DATABASE=/opt/sppd/data/sppd.duckdb

ENTRYPOINT ["airflow", "standalone"]
