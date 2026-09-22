# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""The reliability layer DAG.

Drains the incident queue. For each pending incident it produces a diagnosis,
puts that diagnosis through the gates in `dag_healer.healer`, and either
applies a repair that passes the configured checks or files an escalation with evidence
assembled.

It waits on the queue rather than being triggered by the failing pipeline,
because the queue is the interface. The producer does not call this DAG;
the consumer currently uses one entity's settings and re-triggers
`orders_ingest` by name. Supporting other pipelines requires routing changes.
The sensor defers to the triggerer, which polls the shared directory every
five seconds while releasing the worker slot. This is local filesystem
polling, not a distributed event bus. See `dag_healer.triggers`.
"""

from __future__ import annotations

import logging
import sys
from datetime import timedelta
from pathlib import Path

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # pragma: no cover - Airflow 2
    from airflow.decorators import dag, task

try:  # Airflow 3 moved the standard operators into their own provider
    from airflow.providers.standard.operators.trigger_dagrun import (
        TriggerDagRunOperator,
    )
except ImportError:  # pragma: no cover - Airflow 2
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from dag_healer import queue as queue_mod  # noqa: E402
from dag_healer.config import Settings, load_policy  # noqa: E402
from dag_healer.healer import heal  # noqa: E402
from dag_healer.triggers import IncidentSensor  # noqa: E402

DAG_ID = "reliability_layer"

log = logging.getLogger(__name__)

# Each scheduled run arms a deferred sensor that polls until an incident
# appears or its timeout expires. The timeout leaves thirty seconds before
# the next scheduled run; scheduling delays can extend that gap. Detection
# latency therefore also depends on whether a sensor is currently waiting.
SCHEDULE_SECONDS = 5 * 60
SENSOR_GIVE_UP_AFTER = SCHEDULE_SECONDS - 30


def _backend():
    """Claude Code if it is on PATH, otherwise the deterministic stand-in.

    The Airflow image installs Claude Code, so the container diagnoses with
    the model. CI has no model and falls back rather than failing, because
    what the tests exercise there is the gates.
    """
    from dag_healer.backends.claude_code import ClaudeCodeBackend
    from dag_healer.backends.mock import MockBackend

    claude = ClaudeCodeBackend()
    return claude if claude.available() else MockBackend()


@dag(
    dag_id=DAG_ID,
    schedule=timedelta(seconds=SCHEDULE_SECONDS),
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["reliability"],
    doc_md=__doc__,
)
def reliability_layer():
    wait = IncidentSensor(
        task_id="wait_for_an_incident",
        incidents_dir=str(Settings(root=REPO_ROOT).incidents_dir),
        poll_seconds=5,
        timeout=SENSOR_GIVE_UP_AFTER,
        # An empty queue is the normal state, not a failure. Skipping leaves a
        # run history that reads honestly: nothing happened because nothing
        # broke.
        soft_fail=True,
    )

    @task(task_id="drain_incident_queue")
    def drain_incident_queue() -> dict:
        settings = Settings(root=REPO_ROOT)
        policy = load_policy(settings.policy_path)
        backend = _backend()

        outcomes = {"repaired": 0, "escalated": 0, "changed": 0}
        for incident_path in queue_mod.pending(settings.incidents_dir):
            resolution = heal(incident_path, settings, backend, policy)
            outcomes[resolution.outcome] = outcomes.get(resolution.outcome, 0) + 1
            outcomes["changed"] += int(resolution.changed)
            # Every gate, not only the objections: a task log that records what
            # was verified is the difference between an audit trail and a claim.
            # Logged rather than printed: Airflow stamps captured stdout after
            # the task's own "Done" line, and its log view then folds the last
            # checks into the collapsed post-execute section.
            log.info("[%s] %s", resolution.incident_id, resolution.summary())
            for check in resolution.checks:
                log.info("    %s", check)

        return outcomes

    @task.short_circuit(task_id="anything_repaired")
    def anything_repaired(outcomes: dict) -> bool:
        """Re-run the pipeline only if a repair survived verification and changed something.

        A duplicate incident resolves as repaired without touching the mapping.
        Re-running on that would start a pipeline for a fix that was already in
        place on the previous pass.
        """
        return outcomes.get("changed", 0) > 0

    rerun = TriggerDagRunOperator(
        task_id="rerun_orders_ingest",
        trigger_dag_id="orders_ingest",
        # One pipeline in this demo. With several, this becomes a dynamic
        # mapping over the origin DAGs named in the resolutions.
        conf={"triggered_by": DAG_ID},
        reset_dag_run=True,
        wait_for_completion=False,
    )

    # Bound to the drain, not to the short circuit: `wait >> anything_repaired(
    # drain_incident_queue())` chains the sensor to the wrong end of the
    # expression and leaves the queue being drained in parallel with the wait.
    drained = drain_incident_queue()
    wait >> drained
    anything_repaired(drained) >> rerun


dag_object = reliability_layer()
