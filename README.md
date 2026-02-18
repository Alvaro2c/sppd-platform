# SPPD Platform

ETL platform for **Spanish Public Procurement Data (SPPD)**. Orchestrates Airflow 3.x, dbt, and DuckDB to ingest, convert, and load procurement records from Spain's open data sources into an analytical database.

## ETL flow

| Stage | Tool | Description |
|-------|------|-------------|
| **Extract** | [sppd-cli](https://github.com/Alvaro2c/sppd-cli) | Downloads XML from the Spanish procurement API (minor contracts and public tenders) |
| **Transform** | sppd-cli | Converts XML to Parquet; stored under `data/parquet/{mc,pt}/{period}/` |
| **Load** | dbt | Loads Parquet into DuckDB's `bronze` schema as `{type}_{period}` tables (e.g. `bronze.mc_202602`, `bronze.pt_2023`) |

The `sppd_pipeline` DAG runs both data types in parallel (`sppd_cli_mc`, `sppd_cli_pt`), then a single `bronze_load` task discovers periods from config and parquet directories and loads them via a dbt macro.

## Quick start

1. Copy `.env.example` to `.env` and set required values (PostgreSQL, JWT secret).
2. `docker compose -f docker/docker-compose.yml up -d`
3. Visit `http://localhost:8080` once the orchestrator is healthy (Airflow auto-generates credentials).
4. Trigger `sppd_pipeline` manually for an immediate run, or rely on the daily schedule.

## Configuration

- **`config/sppd_mc.toml`** — minor contracts: `start`/`end` period, download and parquet paths.
- **`config/sppd_pt.toml`** — public tenders: same structure.

Periods are `YYYY` or `YYYYMM`. The bronze loader only ingests periods that exist both in config range and on disk.

## Layout

```
config/          # sppd-cli TOML per data type (mc, pt)
airflow/         # DAGs + generated state (logs, DB — ignored in git)
dbt/             # dbt project; bronze_load macro loads parquet → DuckDB
docker/          # Orchestrator + PostgreSQL
pyproject.toml   # Dependencies for the orchestrator image
.env.example     # Template for secrets/credentials
```
