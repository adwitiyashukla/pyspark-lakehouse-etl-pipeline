from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

HOME = os.environ.get("SPILLWAY_HOME", os.getcwd())
CONFIG = os.environ.get("SPILLWAY_CONFIG", "config.toml")
PYTHON = os.environ.get("SPILLWAY_PYTHON", "python")
STAGES = (
    ("ingest", "ingest"),
    ("bronze", "bronze"),
    ("silver", "silver"),
    ("gold", "gold"),
    ("quality", "quality"),
    ("iceberg", "iceberg"),
)
STREAM_STAGES = (
    ("stream_produce", "stream-produce"),
    ("stream_run", "stream-run --reset"),
)
DEFAULT_ARGS = {
    "owner": "spillway",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


def command(stage: str) -> str:
    return f"cd {HOME} && {PYTHON} -m spillway.cli --config {CONFIG} {stage}"


with DAG(
    dag_id="spillway_pipeline",
    description="NYC trip record lakehouse: ingest, bronze, silver, gold, quality, iceberg",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["spark", "etl", "lakehouse"],
) as dag:
    tasks = {}
    for name, stage in STAGES:
        tasks[name] = BashOperator(task_id=name, bash_command=command(stage))
    for name, stage in STREAM_STAGES:
        tasks[name] = BashOperator(task_id=name, bash_command=command(stage))
    order = [item[0] for item in STAGES]
    for index in range(len(order) - 1):
        tasks[order[index]] >> tasks[order[index + 1]]
    tasks["gold"] >> tasks["stream_produce"] >> tasks["stream_run"]
