import json
import os
import subprocess
import tomllib
from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.sdk import dag, task

PARQUET_BASE = Path("/opt/sppd/data/parquet")
CONFIG_BASE = Path("/opt/sppd/config")
DBT_DIR = Path("/opt/sppd/dbt")
TYPE_TO_DIR = {"minor-contracts": "mc", "public-tenders": "pt"}


def _period_in_range(period: str, start: str, end: str) -> bool:
    """Check if a period (YYYY or YYYYMM) falls within config start/end."""
    if len(period) == 4:  # YYYY
        period_lo = period + "01"
        period_hi = period + "12"
    else:  # YYYYMM
        period_lo = period_hi = period
    return start <= period_hi and period_lo <= end


def _discover_periods() -> list[dict]:
    """Read configs, scan parquet dirs, return list of {type, period} within range."""
    periods = []
    for config_name in ("sppd_mc.toml", "sppd_pt.toml"):
        config_path = CONFIG_BASE / config_name
        if not config_path.exists():
            continue
        with open(config_path, "rb") as f:
            cfg = tomllib.load(f)
        proc_type = cfg.get("type", "")
        dir_name = TYPE_TO_DIR.get(proc_type)
        if not dir_name:
            continue
        start = str(cfg.get("start", ""))
        end = str(cfg.get("end", ""))
        if not start or not end:
            continue
        type_path = PARQUET_BASE / dir_name
        if not type_path.exists():
            continue
        for sub in type_path.iterdir():
            if not sub.is_dir() or not sub.name.isdigit():
                continue
            if _period_in_range(sub.name, start, end):
                periods.append({"type": dir_name, "period": sub.name})
    return periods


@dag(
    dag_id="sppd_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "owner": "sppd",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["sppd"],
)
def sppd_pipeline():
    sppd_mc = BashOperator(
        task_id="sppd_cli_mc",
        bash_command="cd /opt/sppd && sppd-cli toml config/sppd_mc.toml",
        env={
            **os.environ,
            "DUCKDB_DATABASE": "/opt/sppd/data/sppd.duckdb",
        },
    )
    sppd_pt = BashOperator(
        task_id="sppd_cli_pt",
        bash_command="cd /opt/sppd && sppd-cli toml config/sppd_pt.toml",
        env={
            **os.environ,
            "DUCKDB_DATABASE": "/opt/sppd/data/sppd.duckdb",
        },
    )

    @task
    def bronze_load():
        periods = _discover_periods()
        if not periods:
            return
        args_json = json.dumps({"periods": periods})
        env = {
            **os.environ,
            "DBT_PROJECT_DIR": str(DBT_DIR),
            "DUCKDB_DATABASE": "/opt/sppd/data/sppd.duckdb",
        }
        subprocess.run(
            ["dbt", "run-operation", "bronze_load", "--args", args_json],
            cwd=str(DBT_DIR),
            check=True,
            env=env,
        )

    [sppd_mc, sppd_pt] >> bronze_load()


sppd_pipeline()
