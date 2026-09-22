"""The pipeline DAG.

Ordinary, on purpose. The only unusual thing here is that failure is treated as
an event worth capturing rather than as a log line: `on_failure_callback` hands
the incident to a separate DAG that owns reliability. Keeping the two apart is
the point. A pipeline that repairs itself is a pipeline nobody can reason
about; a reliability layer that sits beside many pipelines is a thing you can
give a policy to.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from dag_healer.config import Settings  # noqa: E402
from dag_healer.pipeline import ContractViolationWithIncident, run_ingest  # noqa: E402

DAG_ID = "orders_ingest"


def hand_incident_to_reliability_layer(context) -> None:
    """Trigger the reliability layer with a pointer to the filed evidence."""
    exception = context.get("exception")
    incident_path = getattr(exception, "incident_path", None)
    if incident_path is None:
        # Not every failure is diagnosable; the layer only handles the ones
        # that arrive with evidence attached.
        return

    from airflow.api.common.trigger_dag import trigger_dag

    trigger_dag(
        dag_id="reliability_layer",
        conf={"incident_path": str(incident_path), "origin_dag": DAG_ID},
        replace_microseconds=False,
    )


@dag(
    dag_id=DAG_ID,
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": hand_incident_to_reliability_layer,
    },
    tags=["ingest", "orders"],
    doc_md=__doc__,
)
def orders_ingest():
    @task(task_id="validate_and_load")
    def validate_and_load() -> dict:
        settings = Settings(root=REPO_ROOT)
        try:
            result = run_ingest(settings, dag_id=DAG_ID, task_id="validate_and_load")
        except ContractViolationWithIncident as exc:
            # Re-raised so the task fails honestly; the callback picks up the
            # incident path from the exception.
            raise
        return {"rows": result.rows, "mapping_version": result.mapping_version}

    validate_and_load()


dag_object = orders_ingest()
