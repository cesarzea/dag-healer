"""The reliability layer DAG.

Triggered by a failing pipeline, never scheduled. It diagnoses one incident,
applies a repair only if the repair survives verification, and re-runs the
origin pipeline if it did. Everything else becomes an escalation with the
evidence already assembled.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from dag_healer.config import Settings, load_policy  # noqa: E402
from dag_healer.healer import heal  # noqa: E402

DAG_ID = "reliability_layer"


def _backend():
    """Claude Code if it is on PATH, otherwise the deterministic stand-in.

    Falling back rather than failing keeps the DAG runnable in CI, where there
    is no model and no need for one: the tests are about the gates.
    """
    from dag_healer.backends.claude_code import ClaudeCodeBackend
    from dag_healer.backends.mock import MockBackend

    claude = ClaudeCodeBackend()
    return claude if claude.available() else MockBackend()


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["reliability"],
    doc_md=__doc__,
)
def reliability_layer():
    @task(task_id="diagnose_and_verify")
    def diagnose_and_verify(**context) -> dict:
        conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
        incident_path = conf.get("incident_path")
        if not incident_path:
            raise ValueError("this DAG must be triggered with conf.incident_path")

        settings = Settings(root=REPO_ROOT)
        resolution = heal(
            incident_path, settings, _backend(), load_policy(settings.policy_path)
        )

        print(f"[{resolution.incident_id}] {resolution.summary()}")
        for reason in resolution.reasons:
            print(f"  - {reason}")

        return {
            "outcome": resolution.outcome,
            "incident_id": resolution.incident_id,
            "origin_dag": conf.get("origin_dag"),
            "report": str(resolution.report_path) if resolution.report_path else None,
        }

    @task.short_circuit(task_id="was_repaired")
    def was_repaired(resolution: dict) -> bool:
        return resolution["outcome"] == "repaired"

    @task(task_id="rerun_origin_pipeline")
    def rerun_origin_pipeline(resolution: dict) -> str:
        from airflow.api.common.trigger_dag import trigger_dag

        origin = resolution.get("origin_dag") or "orders_ingest"
        trigger_dag(dag_id=origin, conf={"triggered_by": DAG_ID}, replace_microseconds=False)
        return origin

    resolution = diagnose_and_verify()
    rerun_origin_pipeline(resolution).set_upstream(was_repaired(resolution))


dag_object = reliability_layer()
