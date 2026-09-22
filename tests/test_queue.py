"""The incident queue: files in a directory, drained by the layer."""

from __future__ import annotations

import pytest

from dag_healer import queue as queue_mod
from dag_healer.backends.mock import MockBackend
from dag_healer.config import load_policy
from dag_healer.healer import heal
from dag_healer.pipeline import ContractViolationWithIncident, run_ingest


@pytest.fixture()
def two_incidents(settings, clean_baseline, api):
    api.set(rename_total_price=True)
    paths = []
    for _ in range(2):
        with pytest.raises(ContractViolationWithIncident) as excinfo:
            run_ingest(settings)
        paths.append(excinfo.value.incident_path)
    return paths


def test_empty_directory_has_nothing_pending(settings):
    assert queue_mod.pending(settings.incidents_dir) == []


def test_new_incidents_are_pending(settings, two_incidents):
    assert len(queue_mod.pending(settings.incidents_dir)) == 2


def test_a_resolved_incident_leaves_the_queue(settings, two_incidents):
    heal(two_incidents[0], settings, MockBackend(), load_policy(settings.policy_path))
    pending = queue_mod.pending(settings.incidents_dir)
    assert two_incidents[0] not in pending
    assert two_incidents[1] in pending


def test_an_escalated_incident_also_leaves_the_queue(settings, two_incidents):
    """Escalation is an outcome, not a retryable state. Re-diagnosing it forever
    would turn one unresolved problem into an unbounded stream of them."""
    wrong = {
        "cause_class": "semantic_change",
        "ownership": "customer",
        "confidence": 0.9,
        "summary": "needs a human",
        "proposed_action": {"type": "escalate"},
    }
    heal(two_incidents[0], settings, MockBackend(wrong), load_policy(settings.policy_path))
    assert two_incidents[0] not in queue_mod.pending(settings.incidents_dir)


def test_resolution_record_carries_the_outcome_and_the_reasons(settings, two_incidents):
    import json

    resolution = heal(
        two_incidents[0], settings, MockBackend(), load_policy(settings.policy_path)
    )
    path = queue_mod.resolution_path(settings.incidents_dir, resolution.incident_id)
    recorded = json.loads(path.read_text(encoding="utf-8"))
    assert recorded["outcome"] == "repaired"
    assert recorded["mapping_version"] == 2
    assert recorded["diagnosis"]["cause_class"] == "schema_drift_renamed_field"


def test_resolution_files_are_not_mistaken_for_incidents(settings, two_incidents):
    """Regression: `inc_*.json` also matches `inc_*.resolution.json`.

    The bug did not raise anything. It just quietly reported twice as many
    incidents as existed, and offered a resolution file to the healer as if it
    were an incident. Exactly the class of failure this project is about.
    """
    heal(two_incidents[0], settings, MockBackend(), load_policy(settings.policy_path))

    assert len(queue_mod.incidents(settings.incidents_dir)) == 2
    assert len(queue_mod.pending(settings.incidents_dir)) == 1
    assert all(
        not p.name.endswith(".resolution.json")
        for p in queue_mod.incidents(settings.incidents_dir)
    )
