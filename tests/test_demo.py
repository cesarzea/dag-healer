# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""The walkthrough must show real results and stop at its promised boundaries."""

from __future__ import annotations

import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest
import httpx
import yaml

from dag_healer import demo, queue
from dag_healer.backends.mock import MockBackend
from dag_healer.mapping import load_mapping


@pytest.fixture()
def demo_settings(settings):
    root = Path(__file__).resolve().parents[1]
    shutil.copytree(root / "scripts", settings.root / "scripts")
    return settings


def test_return_boundaries_and_one_diagnosis_cover_the_real_repair_loop(
    demo_settings, api, monkeypatch, capsys
):
    # A different batch size proves the narration reads this run's results.
    from fake_shop_api import main as shop

    monkeypatch.setitem(shop.DEFAULT_STATE, "order_count", 8)
    api.set(rename_to_subtotal=True)
    calls = []

    class CountingBackend(MockBackend):
        def diagnose(self, incident):
            calls.append(incident.incident_id)
            return super().diagnose(incident)

    prompts = []

    def press_return(prompt):
        prompts.append(prompt)
        if "phase 5:" in prompt:
            assert len(calls) == 1, "the proposal must be produced before this pause"
            assert load_mapping(demo_settings.mapping_path).version == 1
            assert not list(demo_settings.incidents_dir.glob("*.resolution.json"))
        if "phase 6:" in prompt:
            assert load_mapping(demo_settings.mapping_path).version == 2
        return ""

    monkeypatch.setattr("builtins.input", press_return)
    demo.run_demo(demo_settings, CountingBackend(), demo.Console())

    assert len(prompts) == 9, "pause before starting, after each phase, then show the recap"
    assert len(calls) == 1, "verification must check the exact proposal shown, without another model call"
    mapping = load_mapping(demo_settings.mapping_path)
    assert mapping.fields["total_amount"] == "order_total"
    assert mapping.version == 2 and len(mapping.history) == 1
    assert mapping.history[0]["review"] == "pending"
    incident_path, = queue.incidents(demo_settings.incidents_dir)
    real = json.loads(queue.resolution_path(demo_settings.incidents_dir, incident_path.stem).read_text())
    rejected = json.loads((demo_settings.incidents_dir / "safety-check.resolution.json").read_text())
    assert real["outcome"] == "repaired"
    assert rejected["outcome"] == "escalated"
    assert demo.warehouse_snapshot(demo_settings)[0] == 8
    experiment_root, = (demo_settings.root / "data").glob("semantic-check-*")
    experiment = replace(demo_settings, root=experiment_root)
    resolutions = [json.loads(path.read_text()) for path in experiment.incidents_dir.glob("*.resolution.json")]
    by_cause = {record["diagnosis"]["cause_class"]: record for record in resolutions}
    assert set(by_cause) == {"semantic_change", "schema_drift_renamed_field"}
    assert by_cause["semantic_change"]["outcome"] == "escalated"
    wrong = by_cause["schema_drift_renamed_field"]
    assert wrong["outcome"] == "repaired" and wrong["review_required"]
    assert all(check["passed"] for check in wrong["checks"])
    assert {check["gate"] for check in wrong["checks"]} == {"policy", "allowlist", "structure", "content", "evidence"}
    assert load_mapping(experiment.mapping_path).fields["total_amount"] == "subtotal"
    assert load_mapping(experiment.mapping_path).history[-1]["review"] == "pending"
    main_rows, main_total = demo.warehouse_snapshot(demo_settings)
    copied_rows, copied_total = demo.warehouse_snapshot(experiment)
    assert copied_rows == main_rows == 8
    assert copied_total / main_total == pytest.approx(0.90, abs=0.001)
    copied_baseline = json.loads(experiment.baseline_path.read_text())
    main_baseline = json.loads(demo_settings.baseline_path.read_text())
    assert copied_baseline["total_amount"]["mean"] == pytest.approx(copied_total / copied_rows)
    assert main_baseline["total_amount"]["mean"] == pytest.approx(main_total / main_rows)
    state = httpx.get(f"{api.url}/admin/state").json()
    assert state["rename_total_price"] is True and state["rename_to_subtotal"] is False
    output = capsys.readouterr().out
    assert "8 orders saved" in output and "120 orders" not in output
    assert "auto_then_review" in output and "record the change for human review" in output
    assert "RESULT: FALSE ACCEPT" in output
    assert "PHASE 8/8" in output
    assert "DEMO COMPLETE" in output


def test_a_refused_live_proposal_stops_before_claiming_recovery(
    demo_settings, monkeypatch, capsys
):
    refusal = MockBackend({
        "cause_class": "unknown", "confidence": 0,
        "summary": "The evidence is ambiguous.",
        "proposed_action": {"type": "escalate"},
    })
    monkeypatch.setattr(demo, "Settings", lambda: demo_settings)
    monkeypatch.setattr(demo, "MockBackend", lambda: refusal)
    assert demo.main(["mock", "--no-pause"]) == 1
    output = capsys.readouterr().out
    assert "DEMO STOPPED" in output
    assert "PHASE 6/8" not in output and "DEMO COMPLETE" not in output
    assert load_mapping(demo_settings.mapping_path).version == 1


def test_eof_at_a_pause_does_not_silently_continue_or_reset_the_project(
    demo_settings, monkeypatch, capsys
):
    original = demo_settings.mapping_path.read_bytes()

    def no_input(prompt):
        raise EOFError

    monkeypatch.setattr("builtins.input", no_input)
    monkeypatch.setattr(demo, "Settings", lambda: demo_settings)
    assert demo.main(["mock"]) == 1
    assert demo_settings.mapping_path.read_bytes() == original
    assert not demo_settings.warehouse_path.exists()
    output = capsys.readouterr().out
    assert "--no-pause" in output
    assert "PHASE 1/8" not in output and "DEMO COMPLETE" not in output


def test_unattended_mode_never_requests_input(monkeypatch):
    def unexpected_input(prompt):
        pytest.fail("--no-pause must not read input")

    monkeypatch.setattr("builtins.input", unexpected_input)
    demo.Console(no_pause=True).pause("the next phase")


def test_default_output_traces_real_operations_before_reporting_success(
    demo_settings, monkeypatch, capsys
):
    logger = logging.getLogger("dag_healer")
    prior_state = (list(logger.handlers), logger.level, logger.propagate)
    monkeypatch.setattr(demo, "Settings", lambda: demo_settings)
    assert demo.main(["mock", "--no-pause"]) == 0
    output = " ".join(capsys.readouterr().out.split())
    assert (
        output.index("START run_ingest: healthy import")
        < output.index("SQL COMMIT")
        < output.index("END run_ingest: healthy import")
        < output.index("RESULT: LOADED")
    )
    assert "dag_healer.extract.fetch_raw GET http://127.0.0.1:" in output
    assert f"WRITE{demo_settings.baseline_path}" in "".join(output.split())
    assert "ContractViolationWithIncident after" in output
    assert "outcome=escalated" in output
    assert (
        output.index("START heal: misclassified subtotal proposal")
        < output.index("RESULT: FALSE ACCEPT")
        < output.index("START run_ingest: load incorrect subtotal amounts into the copy")
        < output.index("Loaded evidence")
        < output.index("restore source settings after experiment")
        < output.index("DEMO COMPLETE")
    )
    assert (list(logger.handlers), logger.level, logger.propagate) == prior_state


def test_unexpected_runtime_failure_has_a_trace_and_stops_the_walkthrough(
    demo_settings, monkeypatch, capsys
):
    from dag_healer.errors import SourceUnavailable

    def failed_import(settings):
        raise SourceUnavailable("The API became unavailable during the import.")

    monkeypatch.setattr(demo, "Settings", lambda: demo_settings)
    monkeypatch.setattr(demo, "run_ingest", failed_import)
    assert demo.main(["mock", "--no-pause"]) == 1
    output = " ".join(capsys.readouterr().out.split())
    assert "END run_ingest: healthy import -> SourceUnavailable after" in output
    assert "DEMO STOPPED: The API became unavailable" in output
    assert "RESULT: LOADED" not in output and "PHASE 2/8" not in output


def test_shell_entry_point_shows_call_durations_with_return_pauses(demo_settings):
    # The shell uses -m, where __name__ is __main__. An import-only test can
    # miss a logger name that drops the demo's own START/END records.
    source = Path(__file__).resolve().parents[1]
    shutil.copytree(source / "src", demo_settings.root / "src", ignore=shutil.ignore_patterns("__pycache__"))
    venv_bin = demo_settings.root / ".venv/bin"
    venv_bin.mkdir(parents=True)
    # Preserve the interpreter's environment; a relocated binary symlink can
    # lose its pyvenv.cfg and consequently its installed dependencies.
    launcher = venv_bin / "python"
    launcher.write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} "$@"\n')
    launcher.chmod(0o755)
    result = subprocess.run(
        ["bash", "./scripts/demo.sh", "mock"],
        cwd=demo_settings.root,
        env={**os.environ, "SHOP_API_URL": demo_settings.base_url, "NO_PAUSE": ""},
        input="\n" * 9,
        capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.count("Press Return to continue") == 9
    assert "START run_ingest: healthy import" in result.stdout
    assert "END run_ingest: healthy import -> completed in" in result.stdout
    assert "REUSE captured diagnosis" in result.stdout
    assert "SQL READ" in result.stdout
    assert "PHASE 8/8" in result.stdout and "RESULT: FALSE ACCEPT" in result.stdout


def test_tighter_policy_stops_instead_of_claiming_a_false_acceptance(
    demo_settings, api, monkeypatch, capsys
):
    policy = yaml.safe_load(demo_settings.policy_path.read_text())
    policy["verification"]["baseline_tolerance"]["mean_relative_delta"] = 0.05
    demo_settings.policy_path.write_text(yaml.safe_dump(policy))
    monkeypatch.setattr(demo, "Settings", lambda: demo_settings)
    assert demo.main(["mock", "--no-pause"]) == 1
    output = capsys.readouterr().out
    assert "The incorrect subtotal proposal was not accepted" in output
    assert "RESULT: FALSE ACCEPT" not in output and "DEMO COMPLETE" not in output
    assert load_mapping(demo_settings.mapping_path).fields["total_amount"] == "order_total"
    root, = (demo_settings.root / "data").glob("semantic-check-*")
    experiment = replace(demo_settings, root=root)
    assert demo.warehouse_snapshot(experiment) == demo.warehouse_snapshot(demo_settings)
    assert experiment.mapping_path.read_bytes() == demo_settings.mapping_path.read_bytes()
    state = httpx.get(f"{api.url}/admin/state").json()
    assert state["rename_to_subtotal"] is False and state["rename_total_price"] is True


def test_experiment_load_failure_restores_api_and_preserves_main_state(
    demo_settings, api, monkeypatch, capsys
):
    original_ingest = demo.run_ingest
    main_state = {}

    def fail_experiment_load(settings):
        if settings.root != demo_settings.root:
            if not main_state:
                main_state.update({
                    path: path.read_bytes()
                    for path in (demo_settings.mapping_path, demo_settings.baseline_path, demo_settings.warehouse_path)
                })
            if load_mapping(settings.mapping_path).fields["total_amount"] == "subtotal":
                raise RuntimeError("Experiment warehouse unavailable")
        return original_ingest(settings)

    monkeypatch.setattr(demo, "run_ingest", fail_experiment_load)
    monkeypatch.setattr(demo, "Settings", lambda: demo_settings)
    assert demo.main(["mock", "--no-pause"]) == 1
    assert main_state and all(path.read_bytes() == saved for path, saved in main_state.items())
    state = httpx.get(f"{api.url}/admin/state").json()
    assert state["rename_to_subtotal"] is False and state["rename_total_price"] is True
    output = capsys.readouterr().out
    assert "DEMO STOPPED: Experiment warehouse unavailable" in output
    assert "Loaded evidence" not in output and "DEMO COMPLETE" not in output
