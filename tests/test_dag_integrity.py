"""Import-time checks on the DAGs.

Cheap and worth having: a DAG that does not parse is a pipeline that silently
stops running, and the scheduler will not tell you loudly.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def dagbag():
    os.environ.setdefault("AIRFLOW_HOME", str(ROOT / ".airflow"))
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    from airflow.models import DagBag

    return DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)


def test_dags_import_without_errors(dagbag):
    assert not dagbag.import_errors, dagbag.import_errors


def test_both_dags_are_registered(dagbag):
    assert set(dagbag.dag_ids) == {"orders_ingest", "reliability_layer"}


def test_ingest_dag_hands_failures_to_the_reliability_layer(dagbag):
    task = dagbag.dags["orders_ingest"].get_task("validate_and_load")
    assert task.on_failure_callback, "a failure with no callback is a failure nobody sees"


def test_reliability_layer_is_event_driven_not_scheduled(dagbag):
    assert dagbag.dags["reliability_layer"].schedule_interval is None
