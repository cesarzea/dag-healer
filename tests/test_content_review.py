"""Exercise historical content evidence and the healer's shared response boundary.

CLI responses here are controlled. These tests do not measure a live model's
ability to distinguish product descriptions from other kinds of text.
"""

from __future__ import annotations

import json
import subprocess

import pytest
import yaml

from dag_healer import baseline, pipeline
from dag_healer.backends.claude_code import ClaudeCodeBackend
from dag_healer.backends.base import Diagnosis, LLMBackend
from dag_healer.backends.mock import MockBackend
from dag_healer.cli import _backend
from dag_healer.config import Settings
from dag_healer.healer import heal
from dag_healer.incident import Incident
from dag_healer.mapping import load_mapping


DESCRIPTIONS = [
    "A wool scarf with a ribbed texture for cold weather.",
    "A stainless steel bottle with an insulated double wall.",
    "A lightweight cotton shirt with short sleeves.",
]
REVIEWS = [
    "I loved it, five stars. Would buy again.",
    "Arrived late and the packaging was damaged.",
    "Disappointed with the service. One star.",
]


@pytest.fixture()
def product_case(project, monkeypatch):
    def build(current=DESCRIPTIONS, *, sampling=True):
        contract = {
            "entity": "products", "version": 1, "primary_key": "product_id",
            "fields": [
                {"name": "product_id", "type": "string", "unique": True},
                {"name": "product_description", "type": "string",
                 "description": "Product features, materials and intended use.",
                 "sample_for_diagnosis": sampling},
                {"name": "contact", "type": "string", "sample_for_diagnosis": True},
            ],
            "row_expectations": {"min_rows": 1},
        }
        mapping = {
            "entity": "products", "version": 1, "source": "fixture",
            "endpoint": "/products", "records_path": "products",
            "fields": {"product_id": "id", "product_description": "body", "contact": "email"},
            "history": [],
        }
        settings = Settings(root=project, entity="products")
        settings.contract_path.write_text(yaml.safe_dump(contract))
        settings.mapping_path.write_text(yaml.safe_dump(mapping))
        original = [{"id": f"p{i}", "body": text, "email": "private@example.test"} for i, text in enumerate(DESCRIPTIONS)]
        changed = [{"id": f"p{i}", "details": text, "email": "private@example.test"} for i, text in enumerate(current)]
        monkeypatch.setattr(pipeline, "fetch_raw", lambda *args: original)
        pipeline.run_ingest(settings)
        saved_baseline = settings.baseline_path.read_bytes()
        monkeypatch.setattr(pipeline, "fetch_raw", lambda *args: changed)
        with pytest.raises(pipeline.ContractViolationWithIncident) as failure:
            pipeline.run_ingest(settings)
        assert settings.baseline_path.read_bytes() == saved_baseline
        return settings, failure.value.incident_path, Incident.load(failure.value.incident_path)
    return build


def answer(verdict="equivalent"):
    return {
        "cause_class": "schema_drift_renamed_field", "ownership": "provider",
        "confidence": 0.95, "summary": "The API appears to have renamed body to details.",
        "proposed_action": {
            "type": "remap_field", "canonical_field": "product_description",
            "new_source_field": "details", "rationale": "Proposed description source.",
        },
        "content_check": {
            "canonical_field": "product_description", "new_source_field": "details",
            "verdict": verdict,
            "reference_summary": "Descriptions of product features and materials.",
            "candidate_summary": "Descriptions of product features and materials." if verdict == "equivalent" else "Customer opinions and delivery complaints.",
            "reason": "Both samples describe product properties." if verdict == "equivalent" else "The current text no longer describes product properties.",
        },
    }


def controlled_claude(monkeypatch, response):
    backend = ClaudeCodeBackend()
    monkeypatch.setattr(backend, "available", lambda: True)
    calls = []

    def run(command, **kwargs):
        calls.append(command)
        assert command[1] == "-p"
        assert command[command.index("--model") + 1] == "claude-opus-5-5"
        assert command[command.index("--effort") + 1] == "max"
        assert "--restricted" in command and command[command.index("--tools") + 1] == ""
        return subprocess.CompletedProcess(command, 0, json.dumps({"result": response}), "")

    monkeypatch.setattr("dag_healer.backends.claude_code.subprocess.run", run)
    return backend, calls


SOURCES = ["claude-code", "mock", "inline-json", "file-json", "custom-backend"]


def supplied_backend(source, response, monkeypatch, tmp_path):
    if source == "claude-code":
        return controlled_claude(monkeypatch, response)[0]
    if source == "mock":
        return MockBackend(response)
    if source == "inline-json":
        return _backend("claude-code", json.dumps(response))
    if source == "file-json":
        path = tmp_path / "diagnosis.json"
        path.write_text(json.dumps(response))
        return _backend("claude-code", str(path))

    class IndependentBackend(LLMBackend):
        name = "independent-test-backend"

        def diagnose(self, incident):
            return Diagnosis.from_dict(response)

    return IndependentBackend()


def test_incident_carries_real_old_and_new_content_with_field_meaning(product_case):
    settings, _, incident = product_case(REVIEWS)
    prompt = ClaudeCodeBackend().build_prompt(incident)
    evidence = json.loads(prompt.split("\nEVIDENCE\n--------\n", 1)[1])
    assert evidence["baseline"]["product_description"]["examples"] == DESCRIPTIONS
    assert [row["details"] for row in evidence["upstream_samples"]] == REVIEWS
    spec = next(field for field in evidence["contract_fields"] if field["name"] == "product_description")
    assert spec["description"] == "Product features, materials and intended use."
    assert "customer reviews" in prompt and "unpaired samples" in prompt
    # An opted-in canonical alias must not bypass the source-field redaction.
    assert baseline.load_baseline(settings.baseline_path)["contact"].examples == []
    assert "private@example.test" not in prompt
    assert baseline.load_baseline(settings.baseline_path)["product_id"].examples == []


def test_equivalent_content_assessment_reaches_the_healer_and_is_recorded(product_case, monkeypatch):
    settings, path, _ = product_case()
    backend, calls = controlled_claude(monkeypatch, answer())
    resolution = heal(path, settings, backend)
    assert resolution.outcome == "repaired"
    assert load_mapping(settings.mapping_path).fields["product_description"] == "details"
    assert resolution.diagnosis.content_check.verdict == "equivalent"
    record = json.loads(path.with_suffix(".resolution.json").read_text())
    assert record["diagnosis"]["content_check"]["reference_summary"] == answer()["content_check"]["reference_summary"]
    assert len(calls) == 1
    assert DESCRIPTIONS[0] in calls[0][2]


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("verdict", ["different", "insufficient_evidence"])
def test_unfavorable_content_review_blocks_a_remap_even_when_types_and_profiles_pass(
    product_case, monkeypatch, tmp_path, verdict, source
):
    settings, path, _ = product_case(REVIEWS)
    mapping = load_mapping(settings.mapping_path)
    candidate = mapping.with_remap("product_description", "details", reason="test candidate")
    trial = pipeline.dry_run(candidate, settings)
    assert not trial.violations, "both descriptions and reviews satisfy the string contract"
    assert not baseline.compare("product_description", trial.profiles["product_description"], baseline.load_baseline(settings.baseline_path)["product_description"])
    before = settings.mapping_path.read_bytes()
    warehouse = settings.warehouse_path.read_bytes()
    backend = supplied_backend(source, answer(verdict), monkeypatch, tmp_path)
    resolution = heal(path, settings, backend)
    assert resolution.outcome == "escalated"
    assert resolution.diagnosis.cause_class == "schema_drift_renamed_field"
    assert resolution.diagnosis.action.type == "remap_field", "the healer must not rewrite the original diagnosis"
    assert not resolution.changed
    assert [c.gate for c in resolution.checks if not c.passed] == ["content"]
    assert not any(c.gate == "evidence" for c in resolution.checks)
    assert resolution.diagnosis.raw["proposed_action"]["type"] == "remap_field", "retain the original answer for inspection"
    assert settings.mapping_path.read_bytes() == before and settings.warehouse_path.read_bytes() == warehouse
    assert "Content assessment" in resolution.report_path.read_text()


@pytest.mark.parametrize("source", SOURCES)
@pytest.mark.parametrize("defect", ["missing", "malformed", "wrong_field", "blank_reason", "unknown_verdict", "no_old_samples", "no_current_samples"])
def test_content_gate_blocks_every_diagnosis_source(product_case, monkeypatch, tmp_path, defect, source):
    settings, path, incident = product_case()
    response = answer()
    if defect == "missing":
        del response["content_check"]
    elif defect == "malformed":
        response["content_check"] = "equivalent"
    elif defect == "wrong_field":
        response["content_check"]["new_source_field"] = "some_other_field"
    elif defect == "blank_reason":
        response["content_check"]["reason"] = " "
    elif defect == "unknown_verdict":
        response["content_check"]["verdict"] = "probably"
    elif defect == "no_old_samples":
        incident.baseline["product_description"].pop("examples")
    else:
        for row in incident.upstream_samples:
            row["details"] = "<redacted>"
    incident.save(settings.incidents_dir)
    protected = {p: p.read_bytes() for p in (settings.mapping_path, settings.warehouse_path, settings.baseline_path)}
    backend = supplied_backend(source, response, monkeypatch, tmp_path)
    resolution = heal(path, settings, backend)
    assert resolution.outcome == "escalated" and not resolution.changed
    assert [c.gate for c in resolution.checks if not c.passed] == ["content"]
    assert not any(c.gate == "evidence" for c in resolution.checks)
    assert resolution.diagnosis.raw == response
    assert all(p.read_bytes() == content for p, content in protected.items())
    recorded = json.loads(path.with_suffix(".resolution.json").read_text())
    assert any(c["gate"] == "content" and not c["passed"] for c in recorded["checks"])
    assert "content" in resolution.report_path.read_text()


@pytest.mark.parametrize("source", SOURCES)
def test_complete_assessment_is_checked_for_every_source(product_case, monkeypatch, tmp_path, source):
    settings, path, _ = product_case()
    resolution = heal(path, settings, supplied_backend(source, answer(), monkeypatch, tmp_path))
    assert resolution.outcome == "repaired"
    assert [c.gate for c in resolution.checks] == ["policy", "policy", "allowlist", "structure", "structure", "structure", "content", "evidence", "evidence"]
    assert all(c.passed for c in resolution.checks)


def test_confident_but_wrong_content_claim_still_does_not_prove_meaning(product_case):
    settings, path, _ = product_case(REVIEWS)
    resolution = heal(path, settings, MockBackend(answer("equivalent")))
    assert resolution.outcome == "repaired", "this deliberately false claim exposes the stated limit"
    assert any(c.gate == "content" and c.passed for c in resolution.checks)
    assert pipeline.run_ingest(settings).ok


def test_previous_sensitive_source_is_not_eligible_for_content_review(product_case):
    settings, path, _ = product_case()
    response = answer()
    response["proposed_action"]["canonical_field"] = "contact"
    response["content_check"]["canonical_field"] = "contact"
    resolution = heal(path, settings, MockBackend(response))
    assert resolution.outcome == "escalated"
    assert any(c.gate == "content" and not c.passed and "excluded from content sampling" in c.detail for c in resolution.checks)


@pytest.mark.parametrize("sampling", [False, "true"])
def test_historical_sampling_requires_explicit_boolean_opt_in(product_case, sampling):
    settings, _, incident = product_case(sampling=sampling)
    assert baseline.load_baseline(settings.baseline_path)["product_description"].examples == []
    assert incident.baseline["product_description"]["examples"] == []


def test_samples_are_bounded_and_older_baselines_remain_readable(tmp_path):
    samples = baseline.sample_values([None, "", "<redacted>", "a" * 1000, "short", "short", 42, "ignored"])
    assert len(samples) == 3 and len(samples[0]) == 240
    assert samples[0].endswith("[truncated]") and samples[1:] == ["short", 42]
    old = baseline.profile_field(["description"])
    payload = old.as_dict()
    del payload["examples"]
    path = tmp_path / "old.baseline.json"
    path.write_text(json.dumps({"description": payload}))
    assert baseline.load_baseline(path)["description"].examples == []
