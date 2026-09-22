"""Parsing whatever the CLI wraps the answer in.

Worth its own tests because this is the seam where a working demo turns into a
confusing one: the model is right, the JSON is there, and the run still fails
because it arrived inside a fenced block or an envelope.
"""

from __future__ import annotations

import json

import pytest

from dag_healer.backends.base import Diagnosis
from dag_healer.backends.claude_code import ClaudeCodeBackend, _parse

DIAGNOSIS = {
    "cause_class": "schema_drift_renamed_field",
    "ownership": "customer",
    "confidence": 0.82,
    "summary": "upstream renamed total_price",
    "proposed_action": {
        "type": "remap_field",
        "canonical_field": "total_amount",
        "new_source_field": "order_total",
        "rationale": "same type and distribution",
    },
}


def test_parses_a_bare_object():
    assert _parse(json.dumps(DIAGNOSIS))["cause_class"] == DIAGNOSIS["cause_class"]


def test_parses_the_cli_json_envelope_with_a_string_result():
    envelope = {"type": "result", "result": json.dumps(DIAGNOSIS), "is_error": False}
    assert _parse(json.dumps(envelope))["confidence"] == 0.82


def test_parses_the_cli_json_envelope_with_an_object_result():
    envelope = {"type": "result", "result": DIAGNOSIS}
    assert _parse(json.dumps(envelope))["ownership"] == "customer"


def test_parses_a_fenced_block_with_prose_around_it():
    text = "Here is what I found.\n\n```json\n" + json.dumps(DIAGNOSIS) + "\n```\n\nHope that helps."
    assert _parse(text)["summary"] == DIAGNOSIS["summary"]


def test_unparseable_output_raises_rather_than_guessing():
    with pytest.raises(ValueError):
        _parse("I was unable to determine the cause.")


def test_diagnosis_defaults_to_escalation_when_fields_are_absent():
    d = Diagnosis.from_dict({})
    assert d.cause_class == "unknown"
    assert d.confidence == 0.0
    assert d.action.type == "escalate"


def test_prompt_carries_the_evidence_and_warns_about_untrusted_input(project, settings, api):
    from dag_healer.pipeline import ContractViolationWithIncident, run_ingest
    from dag_healer.incident import Incident

    run_ingest(settings)
    api.set(rename_total_price=True)
    with pytest.raises(ContractViolationWithIncident) as excinfo:
        run_ingest(settings)

    prompt = ClaudeCodeBackend().build_prompt(Incident.load(excinfo.value.incident_path))
    assert "order_total" in prompt
    assert "untrusted" in prompt.lower()
    assert "escalate" in prompt


def test_missing_binary_is_reported_clearly():
    backend = ClaudeCodeBackend(binary="definitely-not-installed-xyz")
    assert not backend.available()
    with pytest.raises(RuntimeError, match="not found on PATH"):
        backend.diagnose(incident=None)  # type: ignore[arg-type]
