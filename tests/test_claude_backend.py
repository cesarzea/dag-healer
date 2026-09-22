"""Parsing whatever the CLI wraps the answer in.

Worth its own tests because this is the seam where a working demo turns into a
confusing one: the model is right, the JSON is there, and the run still fails
because it arrived inside a fenced block or an envelope.
"""

from __future__ import annotations

import json
import logging
import sys
import threading

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


def test_the_model_is_invoked_with_no_tools_at_all():
    """The prompt says the model may only answer; this is what makes that true.

    A prompt is a request, not a boundary. The diagnosis step runs with the
    code-running tools removed, the built-in tools disabled, MCP servers
    ignored and local settings files bypassed, so that "it cannot act on its
    own answer" is a property of the process rather than of its good manners.
    """
    from dag_healer.backends.claude_code import _NO_TOOLS

    assert "--restricted" in _NO_TOOLS
    assert "--strict-mcp-config" in _NO_TOOLS
    assert _NO_TOOLS[_NO_TOOLS.index("--tools") + 1] == "", "an empty tool list, not a subset"


def test_slow_cli_reports_the_wait_without_changing_its_captured_answer(monkeypatch, caplog):
    from dag_healer.backends import claude_code

    monkeypatch.setattr(claude_code, "_WAIT_UPDATE_SECONDS", 0.02)
    caplog.set_level(logging.INFO, logger=claude_code.__name__)
    previous_threads = set(threading.enumerate())
    result = claude_code._run_cli(
        [sys.executable, "-c", 'import time; time.sleep(0.12); print(\'{"result": "answer"}\')'],
        timeout=5,
    )
    assert json.loads(result.stdout) == {"result": "answer"}
    updates = [record for record in caplog.records if "has not returned yet" in record.getMessage()]
    assert len(updates) >= 2, "the caller must see repeated updates while the CLI is silent"
    assert all(record.args[0] > 0 and record.args[1] == 5 for record in updates)
    assert "No input is needed" in caplog.text and "Ctrl+C cancels" in caplog.text
    assert caplog.records[-1].getMessage().startswith("RESPONSE RECEIVED")
    assert not [t for t in threading.enumerate() if t not in previous_threads and t.name == "claude-code-wait"]


def test_cli_timeout_stops_wait_updates_and_reports_a_readable_error(monkeypatch, caplog):
    from dag_healer.backends import claude_code

    monkeypatch.setattr(claude_code, "_WAIT_UPDATE_SECONDS", 0.02)
    caplog.set_level(logging.INFO, logger=claude_code.__name__)
    previous_threads = set(threading.enumerate())
    with pytest.raises(RuntimeError, match="did not return within 0.1s") as error:
        claude_code._run_cli(
            [sys.executable, "-c", "import time; time.sleep(30)", "private evidence placeholder"],
            timeout=0.1,
        )
    assert "private evidence placeholder" not in str(error.value)
    assert caplog.records[-1].getMessage().startswith("TIMED OUT")
    assert "RESPONSE RECEIVED" not in caplog.text
    assert not [t for t in threading.enumerate() if t not in previous_threads and t.name == "claude-code-wait"]


def test_cli_failure_stops_wait_updates_and_preserves_the_process_error(caplog):
    from dag_healer.backends import claude_code

    caplog.set_level(logging.INFO, logger=claude_code.__name__)
    previous_threads = set(threading.enumerate())
    with pytest.raises(RuntimeError, match="exited 7: sign-in required"):
        claude_code._run_cli(
            [sys.executable, "-c", 'import sys; sys.stderr.write("sign-in required"); sys.exit(7)'],
            timeout=5,
        )
    assert caplog.records[-1].getMessage().startswith("FAILED")
    assert "RESPONSE RECEIVED" not in caplog.text
    assert not [t for t in threading.enumerate() if t not in previous_threads and t.name == "claude-code-wait"]


def test_cancelling_cli_stops_wait_updates_before_propagating_interrupt(monkeypatch, caplog):
    from dag_healer.backends import claude_code

    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt

    monkeypatch.setattr(claude_code.subprocess, "run", interrupt)
    caplog.set_level(logging.INFO, logger=claude_code.__name__)
    previous_threads = set(threading.enumerate())
    with pytest.raises(KeyboardInterrupt):
        claude_code._run_cli(["claude"], timeout=5)
    assert caplog.records[-1].getMessage().startswith("CANCELLED")
    assert "RESPONSE RECEIVED" not in caplog.text
    assert not [t for t in threading.enumerate() if t not in previous_threads and t.name == "claude-code-wait"]
