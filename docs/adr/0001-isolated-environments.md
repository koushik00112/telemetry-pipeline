# ADR 0001: Airflow orchestrates a CLI; the pipeline and dbt live in their own virtualenv

Date: 2026-09-25
Status: Accepted

## Context
Airflow pins many dependencies (SQLAlchemy, Pydantic and others). dbt-core pins its own,
and the pipeline needs DuckDB, PyArrow and Spark. Installing everything into Airflow's
environment is the most common source of broken Airflow images.

## Decision
- The DAG contains only `BashOperator` calls to the `pipeline` CLI. It never imports
  pipeline code.
- In the image, the pipeline and dbt are installed in `/opt/pipeline/venv`, separate from
  Airflow.
- `pipeline dbt` runs dbt as a subprocess and summarises `run_results.json`.

## Consequences
- Airflow can be upgraded without touching the pipeline, and the reverse.
- Every step can be run and debugged by hand with the same command Airflow runs.
- There's a small process start-up cost per task; negligible for a daily job.
- Checked: the DAG loads and runs end to end under Airflow 3.3.2 (`airflow dags test`),
  locally on 2026-09-25 and in CI.
