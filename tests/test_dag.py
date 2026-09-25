"""DAG integrity: loads the DAG with a real Airflow install. Skipped when Airflow isn't installed
(it's kept out of the pipeline's own environment on purpose; CI installs it separately)."""

from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow.sdk")  # Airflow 3

DAGS = Path(__file__).parent.parent / "dags"


@pytest.fixture(scope="module")
def dag():
    try:
        from airflow.dag_processing.dagbag import DagBag
    except ImportError:
        from airflow.models.dagbag import DagBag
    try:
        bag = DagBag(dag_folder=str(DAGS), include_examples=False)  # Airflow 2 / early 3
    except TypeError:
        bag = DagBag(dag_folder=str(DAGS))  # Airflow 3.3+: examples controlled by config
    assert bag.import_errors == {}
    return bag.dags["telemetry_daily"]


def test_task_order(dag):
    order = ["ingest", "quality_raw", "transform", "quality_curated", "dbt_build"]
    assert {t.task_id for t in dag.tasks} == set(order)
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert dag.get_task(downstream).upstream_task_ids == {upstream}


def test_schedule_supports_backfill_and_single_writer(dag):
    assert dag.catchup is True
    assert dag.max_active_runs == 1
    assert dag.default_args["retries"] == 2
    assert dag.default_args["retry_delay"] == timedelta(minutes=2)
    assert dag.get_task("dbt_build").retries == 0


def test_every_task_works_on_the_runs_own_day(dag):
    for task in dag.tasks:
        if task.task_id != "dbt_build":
            assert "--date {{ ds }}" in task.bash_command
    assert "--engine {{ params.engine }}" in dag.get_task("transform").bash_command
