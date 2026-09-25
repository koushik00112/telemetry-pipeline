"""Daily telemetry pipeline.

Each run processes one UTC day ({{ ds }}): land raw readings, check them, aggregate
hourly, check again, then rebuild and test the dbt models. Catchup is on, so setting an
earlier start date (or `airflow dags backfill`) replays history day by day.

Airflow only orchestrates. The work runs in the `pipeline` CLI, installed in its own
virtualenv, so the pipeline's dependencies (DuckDB, PyArrow, Spark, dbt) never conflict
with Airflow's (ADR 0001).
"""

import os
from datetime import UTC, datetime, timedelta

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG, Param
except ImportError:  # Airflow 2.x
    from airflow import DAG  # type: ignore[no-redef]
    from airflow.models.param import Param  # type: ignore[no-redef]
    from airflow.operators.bash import BashOperator  # type: ignore[no-redef]

PIPELINE = os.environ.get("PIPELINE_BIN", "pipeline")
DAY = "--date {{ ds }}"

with DAG(
    dag_id="telemetry_daily",
    description="Raw telemetry -> hourly aggregates -> dbt marts, with quality gates",
    schedule="@daily",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=True,
    # One day at a time: the dbt step writes a single DuckDB file, which allows one writer.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "source": Param(
            "generate",
            enum=["generate", "platform"],
            description="generate = synthetic data; platform = Project 1's DB",
        ),
        "engine": Param("duckdb", enum=["duckdb", "spark"]),
        "scale": Param(1, type="integer", minimum=1, description="synthetic data only"),
    },
    tags=["telemetry", "portfolio"],
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        bash_command=f"{PIPELINE} ingest {DAY} --source {{{{ params.source }}}} "
        "--scale {{ params.scale }}",
    )
    quality_raw = BashOperator(
        task_id="quality_raw", bash_command=f"{PIPELINE} quality {DAY} --layer raw"
    )
    aggregate = BashOperator(
        task_id="transform",
        bash_command=f"{PIPELINE} transform {DAY} --engine {{{{ params.engine }}}}",
    )
    quality_curated = BashOperator(
        task_id="quality_curated", bash_command=f"{PIPELINE} quality {DAY} --layer curated"
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{PIPELINE} dbt",
        retries=0,  # a failing dbt test is a data problem; retrying won't fix it
    )

    ingest >> quality_raw >> aggregate >> quality_curated >> dbt_build
