"""What the layer refuses to do, and why.

These are the tests that matter. Repairing a renamed field is the easy half;
the half worth writing down is everything the layer declines to touch.
"""

from __future__ import annotations

import pytest

from dag_healer.backends.mock import MockBackend
from dag_healer.config import load_policy
from dag_healer.healer import heal
from dag_healer.mapping import load_mapping
from dag_healer.pipeline import ContractViolationWithIncident, run_ingest


@pytest.fixture()
def incident_path(settings, clean_baseline, api):
    """A real incident, produced by upstream renaming a field mid-flight."""
    api.set(rename_total_price=True)
    with pytest.raises(ContractViolationWithIncident) as excinfo:
        run_ingest(settings)
    return excinfo.value.incident_path


def diagnosis(**overrides):
    payload = {
        "cause_class": "schema_drift_renamed_field",
        "ownership": "customer",
        "confidence": 0.9,
        "summary": "upstream renamed the field",
        "proposed_action": {
            "type": "remap_field",
            "canonical_field": "total_amount",
            "new_source_field": "order_total",
            "rationale": "same type and range as before",
        },
    }
    action = overrides.pop("proposed_action", None)
    payload.update(overrides)
    if action:
        payload["proposed_action"] = {**payload["proposed_action"], **action}
    return payload


def run(incident_path, settings, response=None):
    return heal(
        incident_path,
        settings,
        MockBackend(response),
        load_policy(settings.policy_path),
    )


def test_correct_repair_is_applied_and_the_pipeline_recovers(incident_path, settings):
    resolution = run(incident_path, settings, diagnosis())

    assert resolution.outcome == "repaired"
    assert resolution.review_required, "an automated schema change is held for review"
    assert load_mapping(settings.mapping_path).fields["total_amount"] == "order_total"
    assert run_ingest(settings).ok


def test_plausible_but_wrong_field_is_rejected_by_the_baseline(incident_path, settings):
    """The decoy.

    `shipping_price` is a number, in range, never null, and passes the contract
    without complaint. Only the comparison against the last known-good profile
    shows that it is the wrong column, which is the entire argument for not
    trusting a diagnosis that merely type-checks.
    """
    resolution = run(
        incident_path,
        settings,
        diagnosis(proposed_action={"new_source_field": "shipping_price"}),
    )

    assert resolution.outcome == "escalated"
    assert any("mean moved" in r for r in resolution.reasons)
    assert load_mapping(settings.mapping_path).fields["total_amount"] == "total_price"
    assert resolution.report_path and resolution.report_path.exists()


def test_action_outside_the_allowlist_is_refused(incident_path, settings):
    resolution = run(
        incident_path,
        settings,
        diagnosis(proposed_action={"type": "rewrite_dag"}),
    )
    assert resolution.outcome == "escalated"
    assert any("allowlist" in r for r in resolution.reasons)


def test_low_confidence_is_refused_however_sensible_the_action(incident_path, settings):
    resolution = run(incident_path, settings, diagnosis(confidence=0.2))
    assert resolution.outcome == "escalated"
    assert any("confidence" in r for r in resolution.reasons)


def test_cause_class_marked_escalate_in_policy_is_never_automated(incident_path, settings):
    resolution = run(incident_path, settings, diagnosis(cause_class="semantic_change"))
    assert resolution.outcome == "escalated"
    assert any("never repaired automatically" in r for r in resolution.reasons)


def test_unknown_cause_class_defaults_to_escalation(incident_path, settings):
    resolution = run(incident_path, settings, diagnosis(cause_class="vibes"))
    assert resolution.outcome == "escalated"


def test_layer_cannot_invent_a_canonical_field(incident_path, settings):
    resolution = run(
        incident_path,
        settings,
        diagnosis(proposed_action={"canonical_field": "profit_margin"}),
    )
    assert resolution.outcome == "escalated"
    assert any("not a canonical field" in r for r in resolution.reasons)


def test_source_field_that_upstream_never_sent_is_refused(incident_path, settings):
    resolution = run(
        incident_path,
        settings,
        diagnosis(proposed_action={"new_source_field": "grand_total"}),
    )
    assert resolution.outcome == "escalated"
    assert any("not present in the upstream payload" in r for r in resolution.reasons)


def test_layer_will_not_steal_a_source_field_from_another_canonical_field(incident_path, settings):
    resolution = run(
        incident_path,
        settings,
        diagnosis(proposed_action={"new_source_field": "line_items_count"}),
    )
    assert resolution.outcome == "escalated"
    assert any("already the source" in r for r in resolution.reasons)


def test_repair_is_refused_when_there_is_no_known_good_baseline(settings, api):
    """With nothing to compare against, a repair cannot be verified, so it is not made."""
    api.set(rename_total_price=True)
    with pytest.raises(ContractViolationWithIncident) as excinfo:
        run_ingest(settings)

    resolution = run(excinfo.value.incident_path, settings, diagnosis())
    assert resolution.outcome == "escalated"
    assert any("no known-good baseline" in r for r in resolution.reasons)


def test_transient_class_resolves_as_a_retry_without_touching_the_mapping(incident_path, settings):
    resolution = run(
        incident_path,
        settings,
        diagnosis(cause_class="transient_source_error", proposed_action={"type": "retry"}),
    )
    assert resolution.outcome == "repaired"
    assert not resolution.review_required
    assert load_mapping(settings.mapping_path).version == 1


def test_escalation_report_contains_the_evidence_a_human_needs(incident_path, settings):
    resolution = run(
        incident_path,
        settings,
        diagnosis(proposed_action={"new_source_field": "shipping_price"}),
    )
    report = resolution.report_path.read_text(encoding="utf-8")
    assert "missing_source_field" in report
    assert "order_total" in report
    assert "shipping_price" in report


def test_default_mock_picks_the_only_sensible_candidate(incident_path, settings):
    """Sanity check on the fixture itself, not on the layer."""
    resolution = run(incident_path, settings)
    assert resolution.outcome == "repaired"
