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

    folder = str(ROOT / "dags")
    try:  # Airflow 2
        return DagBag(dag_folder=folder, include_examples=False)
    except TypeError:  # Airflow 3 dropped the argument
        return DagBag(dag_folder=folder)


def test_dags_import_without_errors(dagbag):
    assert not dagbag.import_errors, dagbag.import_errors


def test_both_dags_are_registered(dagbag):
    assert {"orders_ingest", "reliability_layer"} <= set(dagbag.dag_ids)


def test_pipeline_knows_nothing_about_the_reliability_layer():
    """The queue is the interface. If this fails, the two have grown a coupling.

    Checked against imports and calls rather than the whole file, because the
    module docstring is allowed to explain the arrangement it is part of.
    """
    import ast

    source = (ROOT / "dags" / "orders_ingest.py").read_text(encoding="utf-8")
    tree = ast.parse(source)

    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module)

    assert not {m for m in imported if m.endswith(("healer", "queue", "escalation"))}
    assert "trigger_dag" not in source, "the pipeline must not reach into another DAG"


def test_reliability_layer_waits_drains_and_can_rerun(dagbag):
    tasks = {t.task_id for t in dagbag.dags["reliability_layer"].tasks}
    assert {
        "wait_for_an_incident",
        "drain_incident_queue",
        "anything_repaired",
        "rerun_orders_ingest",
    } == tasks


def test_the_queue_is_drained_only_after_the_sensor_has_found_something(dagbag):
    """Caught by running it, not by reading it.

    `wait >> anything_repaired(drain_incident_queue())` binds the sensor to the
    short circuit, because that expression evaluates to the short circuit task.
    The drain then runs immediately, in parallel with the wait, and the sensor
    becomes decoration. Every task is still present and correctly named, so a
    test that checks the task list passes happily.
    """
    drain = dagbag.dags["reliability_layer"].get_task("drain_incident_queue")
    assert "wait_for_an_incident" in drain.upstream_task_ids


def test_the_sensor_gives_up_before_the_next_run_is_due(dagbag):
    """A quiet queue should time out before the next scheduled run is due."""
    dag = dagbag.dags["reliability_layer"]
    sensor = dag.get_task("wait_for_an_incident")

    assert type(sensor).__name__ == "IncidentSensor", "waiting must not hold a worker slot"
    # Airflow 3 uses DeltaTriggerTimetable with a public delta attribute;
    # Airflow 2 exposes its data-interval schedule through serialize().
    timetable = dag.timetable
    interval_seconds = (
        timetable.delta.total_seconds()
        if hasattr(timetable, "delta")
        else timetable.serialize()["delta"]
    )
    assert 0 < sensor.timeout < interval_seconds


def test_the_cli_and_the_dags_call_the_same_functions():
    """The demo runs the CLI while telling the viewer Airflow runs the same code.

    That claim is load-bearing: it is the reason a loop demonstrated in one
    terminal is evidence about the thing that runs on a scheduler. If the two
    ever drift apart, the demo is lying to whoever is watching it, politely and
    in print.
    """
    import ast

    def called_names(path: Path) -> set[str]:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        return {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }

    cli = called_names(ROOT / "src" / "dag_healer" / "cli.py")
    assert "run_ingest" in cli & called_names(ROOT / "dags" / "orders_ingest.py")
    assert "heal" in cli & called_names(ROOT / "dags" / "reliability_layer.py")
