# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""Poll the incident directory without occupying an Airflow worker slot.

`IncidentSensor` defers to the triggerer. An asynchronous loop checks for
unresolved incidents and wakes the task when it finds one. This still costs
directory reads and triggerer resources; it is filesystem polling, not push
delivery. The DAG configures a five-second interval and a bounded wait.

Checking only for file existence would be insufficient because resolved
incidents remain on disk for inspection. The queue predicate also checks
whether a resolution exists. Both producer and consumer need access to the
same directory, and distributed coordination is outside this POC.
"""

from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent

try:  # Airflow 3
    from airflow.sdk.bases.sensor import BaseSensorOperator
except ImportError:  # pragma: no cover - Airflow 2
    from airflow.sensors.base import BaseSensorOperator

from . import queue as queue_mod


class IncidentQueueTrigger(BaseTrigger):
    """Fires when a poll finds an unresolved incident."""

    def __init__(self, incidents_dir: str, poll_seconds: float = 5.0) -> None:
        super().__init__()
        self.incidents_dir = incidents_dir
        self.poll_seconds = poll_seconds

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "dag_healer.triggers.IncidentQueueTrigger",
            {"incidents_dir": self.incidents_dir, "poll_seconds": self.poll_seconds},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        while True:
            pending = queue_mod.pending(self.incidents_dir)
            if pending:
                yield TriggerEvent({"pending": [str(p) for p in pending]})
                return
            # Cheap: this runs in the triggerer's event loop beside every other
            # deferred task in the deployment, not in a worker slot of its own.
            await asyncio.sleep(self.poll_seconds)


class IncidentSensor(BaseSensorOperator):
    """Waits for an unresolved incident, holding no worker while it waits."""

    def __init__(self, *, incidents_dir: str, poll_seconds: float = 5.0, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.incidents_dir = incidents_dir
        self.poll_seconds = poll_seconds

    def execute(self, context: Any) -> Any:
        # Checked once here so an incident that is already waiting does not pay
        # a round trip through the triggerer.
        if queue_mod.pending(self.incidents_dir):
            return {"pending": [str(p) for p in queue_mod.pending(self.incidents_dir)]}
        self.defer(
            trigger=IncidentQueueTrigger(self.incidents_dir, self.poll_seconds),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Any, event: dict[str, Any] | None = None) -> Any:
        return event
