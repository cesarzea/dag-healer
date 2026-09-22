# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""The pipeline DAG.

`run_ingest` files a structured incident when the data contract fails, then
raises. Exhausted HTTP retries currently raise without filing an incident.
This DAG has no knowledge of the reliability layer, no callback into it and
no import from it.

The shared incident directory separates ingestion from diagnosis and repair.
The current consumer is configured for this orders pipeline; serving several
pipelines would require additional routing and coordination there.
"""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # pragma: no cover - Airflow 2
    from airflow.decorators import dag, task

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "src"))

from dag_healer.config import Settings  # noqa: E402
from dag_healer.pipeline import run_ingest  # noqa: E402

DAG_ID = "orders_ingest"


@dag(
    dag_id=DAG_ID,
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # Airflow's default delay. A retry only helps if something can change
    # before it runs; five minutes leaves time for a diagnosis and repair,
    # which take one to two minutes with a live model.
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["ingest", "orders"],
    doc_md=__doc__,
)
def orders_ingest():
    @task(task_id="validate_and_load")
    def validate_and_load() -> dict:
        settings = Settings(root=REPO_ROOT)
        result = run_ingest(settings, dag_id=DAG_ID, task_id="validate_and_load")
        return {"rows": result.rows, "mapping_version": result.mapping_version}

    validate_and_load()


dag_object = orders_ingest()
