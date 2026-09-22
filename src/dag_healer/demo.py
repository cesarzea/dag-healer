"""A guided demonstration of the real pipeline and its repair checks."""

from __future__ import annotations

import argparse
import copy
import logging
import os
import shutil
import sqlite3
import subprocess
import tempfile
import textwrap
import time
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Callable, Iterator, TypeVar

import httpx

from .backends.base import Diagnosis, LLMBackend
from .backends.claude_code import ClaudeCodeBackend
from .backends.mock import MockBackend
from .config import Policy, Settings, load_policy
from .extract import fetch_raw
from .healer import Resolution, heal
from .incident import Incident
from .mapping import load_mapping
from .pipeline import ContractViolationWithIncident, run_ingest
from .startup import StartupError, ensure_services

# -m executes this file as __main__; keep its records under the same handler
# as the library modules when the actual shell entry point is used.
log = logging.getLogger("dag_healer.demo")
T = TypeVar("T")
PHASE_COUNT = 8


class DemoTraceHandler(logging.Handler):
    """Send runtime records through the same layout as the narration."""

    def __init__(self, out: Console) -> None:
        super().__init__()
        self.out = out

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.out.trace(self.format(record))
        except Exception:
            self.handleError(record)


@contextmanager
def execution_traces(out: Console, *, debug: bool) -> Iterator[None]:
    """Stream runtime records without leaving handlers attached to a caller's logger."""
    project_log = logging.getLogger("dag_healer")
    previous_level, previous_propagate = project_log.level, project_log.propagate
    handler = DemoTraceHandler(out)
    handler.setFormatter(logging.Formatter(
        "TRACE %(asctime)s.%(msecs)03d %(name)s.%(funcName)s\n%(message)s",
        datefmt="%H:%M:%S",
    ))
    # The normal trace shows execution. --debug additionally prints the long
    # model instructions, which otherwise interrupt the explanation mid-phase.
    handler.addFilter(lambda record: debug or not (
        record.getMessage().startswith("  |")
        or record.getMessage() == "the instructions it is given, verbatim:"
    ))
    project_log.addHandler(handler)
    project_log.setLevel(logging.DEBUG)
    project_log.propagate = False
    try:
        yield
    finally:
        out.end_traces()
        project_log.removeHandler(handler)
        handler.close()
        project_log.setLevel(previous_level)
        project_log.propagate = previous_propagate


def execute(label: str, function: Callable[..., T], *args, **kwargs) -> T:
    """Bracket a real call so delays and failures are visible before the narration resumes."""
    started = time.perf_counter()
    log.info("START %s", label)
    try:
        result = function(*args, **kwargs)
    except Exception as exc:
        log.info("END %s -> %s after %.1f ms", label, type(exc).__name__, (time.perf_counter() - started) * 1000)
        raise
    log.info("END %s -> completed in %.1f ms", label, (time.perf_counter() - started) * 1000)
    return result


class DemoError(Exception):
    """Stop on an unexpected result instead of narrating it as a success."""


class Console:
    def __init__(self, *, no_pause: bool = False) -> None:
        self.no_pause = no_pause
        self._blank = False
        self._tracing = False

    def _write(self, text: str) -> None:
        print(text, flush=True)
        self._blank = False

    def _space(self) -> None:
        if not self._blank:
            print(flush=True)
            self._blank = True

    def _paragraph(self, text: str, *, indent: int = 2) -> None:
        margin = " " * indent
        self._write(textwrap.fill(text, width=76, initial_indent=margin, subsequent_indent=margin))

    def _next_block(self) -> None:
        self.end_traces()
        self._space()

    def say(self, text: str = "") -> None:
        self._next_block()
        if text:
            self._paragraph(text)

    def fact(self, label: str, value: object) -> None:
        self._next_block()
        self._paragraph(label if ":" in label else f"{label}:")
        self._paragraph(str(value), indent=4)

    def trace(self, record: str) -> None:
        if not self._tracing:
            self._space()
            self._paragraph("Execution trace | live Python output")
            self._paragraph("-" * 72)
            self._tracing = True
        header, *message = record.splitlines()
        self._paragraph(header)
        for line in message:
            self._paragraph(line, indent=4)

    def end_traces(self) -> None:
        if self._tracing:
            self._paragraph("-" * 72)
            self._tracing = False
            self._space()

    def phase(self, number: int, title: str, actor: str) -> None:
        self._next_block()
        self._paragraph("=" * 72)
        self._paragraph(f"PHASE {number}/{PHASE_COUNT} | {title}")
        self._paragraph("=" * 72)
        self.fact("Who acts", actor)

    def pause(self, next_phase: str) -> None:
        if self.no_pause:
            return
        self._next_block()
        try:
            input(f"  Press Return to continue -> {next_phase}\n  ")
            self._blank = False
        except EOFError as exc:
            raise DemoError(
                "No input is available at the pause. Run in a terminal to advance "
                "with Return, or add --no-pause for an unattended run."
            ) from exc


class CapturedDiagnosis(LLMBackend):
    """Check the answer the audience just read, without making a second AI call."""

    def __init__(self, diagnosis: Diagnosis, name: str) -> None:
        self.diagnosis = diagnosis
        self.name = name

    def diagnose(self, incident: Incident) -> Diagnosis:
        log.info("REUSE captured diagnosis for %s: cause=%s, action=%s; no new model call", incident.incident_id, self.diagnosis.cause_class, self.diagnosis.action.type)
        return self.diagnosis


def warehouse_snapshot(settings: Settings) -> tuple[int, float]:
    with sqlite3.connect(settings.warehouse_path) as conn:
        rows, total = conn.execute(
            'SELECT COUNT(*), SUM(CAST(total_amount AS REAL)) FROM orders'
        ).fetchone()
    log.info("SQL READ %s -> orders=%d, SUM(total_amount)=%.2f", settings.warehouse_path, rows, total or 0.0)
    return rows, total or 0.0


def show_proposal(out: Console, diagnosis: Diagnosis) -> None:
    causes = {
        "schema_drift_renamed_field": "An API field appears to have been renamed.",
        "schema_drift_removed_field": "An API field appears to have been removed.",
        "semantic_change": "The meaning of the data appears to have changed.",
        "unknown": "The cause could not be identified.",
    }
    out.fact("Failure class", diagnosis.cause_class)
    out.fact("Meaning", causes.get(diagnosis.cause_class, diagnosis.cause_class.replace("_", " ")))
    out.fact("Explanation", diagnosis.summary or "No explanation was supplied.")
    out.fact("Ownership", f"{diagnosis.ownership}: the diagnosis's suggested responsible party, not a verified assignment.")
    if diagnosis.action.type == "remap_field":
        out.fact("Action", "remap_field: change where one existing reporting field reads its value.")
        out.fact("Proposed rule", f"Read reporting field '{diagnosis.action.canonical_field}' from API field '{diagnosis.action.new_source_field}'.")
    else:
        out.fact("Proposed action", diagnosis.action.type.replace("_", " "))
    if diagnosis.action.rationale:
        out.fact("Reason", diagnosis.action.rationale)
    out.fact("Confidence", f"{diagnosis.confidence:.0%}, reported by the diagnosis source. It is a claim, not a test result.")
    if diagnosis.content_check:
        content = diagnosis.content_check
        out.fact("Content verdict", f"{content.verdict}: an assessment supplied by the diagnosis source, not an independent semantic test.")
        out.fact("Previous content", content.reference_summary or "No historical content summary supplied.")
        out.fact("Current content", content.candidate_summary or "No candidate content summary supplied.")
        out.fact("Content reason", content.reason or "No explanation supplied.")
    else:
        out.fact("Content review", "No structured content assessment was supplied. A remap with this omission will be refused by the healer's content gate, regardless of the diagnosis source.")


def show_checks(out: Console, resolution: Resolution, policy: Policy) -> None:
    diagnosis = resolution.diagnosis
    assert diagnosis is not None
    action = diagnosis.action
    groups = (
        ("policy", "1. policy | May this kind of failure be repaired automatically?"),
        ("allowlist", "2. allowlist | Is this action in the list of permitted operations?"),
        ("structure", "3. structure | Do the field names support the proposed mapping?"),
        ("content", "4. content | Is the assessment complete and supported by available samples?"),
        ("evidence", "5. evidence | Does a trial import pass BOTH data checks?"),
    )
    for gate, question in groups:
        out.say(question)
        checks = [c for c in resolution.checks if c.gate == gate]
        if not checks:
            out.fact("NOT RUN", "An earlier decision ended verification.")
        for index, check in enumerate(checks):
            detail = check.detail
            if gate == "policy" and index == 0:
                if not check.passed:
                    detail = f"policy.yml assigns {diagnosis.cause_class} to 'escalate': a person must handle this failure. Automatic repair is forbidden."
                elif policy.level_for(diagnosis.cause_class) == "auto_then_review":
                    detail = f"policy.yml assigns {diagnosis.cause_class} to 'auto_then_review': verify the repair, apply it, then record the change for human review."
                else:
                    detail = f"policy.yml assigns {diagnosis.cause_class} to 'auto': handle this failure automatically."
            elif gate == "policy":
                detail = f"The proposal reports {diagnosis.confidence:.0%} confidence; the required minimum is {policy.min_confidence:.0%}. Confidence only permits evaluation to continue."
            elif gate == "allowlist":
                meanings = {
                    "remap_field": "repoint one existing reporting field",
                    "retry": "try the import again",
                    "escalate": "refer the problem to a person",
                }
                allowed = "; ".join(f"{name} ({meanings.get(name, 'a configured operation')})" for name in policy.allowed_actions)
                detail = (
                    f"{action.type} is allowed. The permitted actions are: {allowed}."
                    if check.passed
                    else f"The proposal cannot be applied automatically. Permitted actions: {allowed}."
                )
            elif gate == "structure" and len(checks) == 3:
                field, source = action.canonical_field, action.new_source_field
                detail = (
                    f"'{field}' {'already exists' if check.passed else 'does not exist'} in our reporting format.",
                    f"The shop {'did' if check.passed else 'did not'} send a field called '{source}'.",
                    f"'{source}' {'is free to use: no other reporting field reads from it' if check.passed else 'already supplies another reporting field'}.",
                )[index]
            else:
                for original, replacement in {
                    "canonical field": "reporting field",
                    "satisfies the contract": "passes the required-field, type and value rules",
                    "contract still violated": "data rules still broken",
                    "violations": "broken rules",
                    "null rate": "missing-value rate",
                    "average": "mean order amount",
                    "known-good baseline": "saved reference from the healthy import",
                }.items():
                    detail = detail.replace(original, replacement)
            out.fact("PASS" if check.passed else "STOP", detail)
        out.say()


def reset_demo(settings: Settings) -> None:
    # The reset command remains the single definition of the starting fixture.
    subprocess.run(
        ["bash", str(settings.root / "scripts/reset.sh")],
        capture_output=True, text=True, check=True,
    )
    response = httpx.post(
        f"{settings.base_url}/admin/state",
        json={"rename_total_price": False, "rename_to_subtotal": False, "fail_next": 0, "rate_limit": False},
        timeout=5,
    )
    response.raise_for_status()


def run_demo(settings: Settings, backend: LLMBackend, out: Console) -> None:
    out.say()
    out.say("-" * 72)
    out.say()
    out.say("DAG-HEALER | Pipeline repair: diagnosis, checks and limits")
    out.say()
    out.say("Investigating and resolving data pipeline problems takes engineering time: understanding what failed, gathering evidence and deciding what to change. This proof of concept (POC) demonstrates how AI can help with that process in pipelines orchestrated by Apache Airflow. An Airflow DAG defines a workflow's tasks and the dependencies between them. The AI uses failure evidence to diagnose a problem and propose a repair that code can evaluate.")
    out.say()
    out.say("To make that idea concrete, this demo implements one limited example: a simulated orders API renames a field, the import used by an Airflow DAG fails, and a separate repair workflow evaluates a proposed field mapping. This is one example of AI-assisted problem resolution; the implementation covers only this narrow scenario.")
    out.say()
    out.say("The workflow, verification rules and infrastructure are built for this POC. They are not designed or validated as a production solution. You will see a successful recovery, a rejected wrong proposal and a wrong mapping that the implemented checks accept, making both the potential and the limits visible.")
    out.say()
    out.fact("The data", "One simulated shop with an orders API and a local SQLite warehouse. The import translates API fields into a stable reporting format.")
    out.fact("Why orders", "Order totals and delivery charges are both numbers, but they mean different things. Confusing them would distort revenue reports even if the import finished successfully.")
    out.fact("Diagnosis backend", f"Claude Code supplies one live AI diagnosis using {backend.model} with effort={backend.effort} (Opus 5.5, maximum reasoning effort)." if backend.name == "claude-code" else "A scripted stand-in. No AI is called in this run; the repair checks are the real ones.")
    out.fact("The execution", "This script calls the same Python functions as the two Airflow DAGs in the repository, advancing on Return. It does not start an Airflow scheduler.")
    out.fact("The traces", "Blocks labeled 'Execution trace' contain live output from the running Python code. Each TRACE header identifies the time and function; the indented message below it describes the operation. START/END records show real calls and elapsed time. The closing line of dashes separates these records from the explanation or result that follows.")
    out.say()
    out.say("The walkthrough has eight phases, in this order. Press Return after each phase to continue.")
    out.fact("Phase 1: establish the healthy reference", "Reset the demo's mapping, incidents, reference statistics and database. Import healthy orders and save their statistics and selected content examples for later comparison.")
    out.fact("Phase 2: change the source API", "Rename the order amount field in the simulated API while leaving its values unchanged. The import's mapping still expects the old name.")
    out.fact("Phase 3: observe the failed import", "Run the import again. It stops before loading the invalid batch and saves an incident containing evidence of the failure.")
    out.fact("Phase 4: diagnose and propose", "Claude Code interprets the evidence, assesses the field's content and proposes an action. Inspect its explanation before continuing; the mapping has not changed yet." if backend.name == "claude-code" else "A scripted stand-in supplies a diagnosis and proposed action. Inspect the proposal before continuing; the mapping has not changed yet. This backend does not assess content semantics.")
    out.fact("Phase 5: verify and apply", "Python evaluates the proposal against policy and data checks. It applies the mapping change only if the required checks pass. Passing these POC checks does not prove equivalent meaning.")
    out.fact("Phase 6: verify the recovered import", "Run the import with the accepted mapping and inspect the loaded data. The change remains pending human review.")
    out.fact("Phase 7: reject a wrong proposal", "Supply a deliberately wrong mapping to shipping_price, the delivery charge. Show how the historical comparison rejects it despite valid types and ranges.")
    out.fact("Phase 8: expose a verification limit", "Use two supplied diagnoses on an isolated copy: classifying a subtotal as a semantic change blocks the repair, but misclassifying it as a rename lets wrong amounts pass the implemented checks and load.")
    out.pause("phase 1: import healthy orders")

    out.phase(1, "Run the orders_ingest task on healthy data", "The demo executes the function used by orders_ingest.validate_and_load.")
    out.fact("Import DAG", "orders_ingest: validate_and_load (@task) calls run_ingest to extract, map, validate and load orders.")
    out.fact("Airflow settings", "Scheduled every 15 minutes; max_active_runs=1 prevents overlapping runs of this DAG. A failed task has one retry after one minute.")
    out.say("First we reset the local demo data and inspect the healthy API response. The next trace block shows this preparation: reset_demo restores the starting state, then fetch_raw reads the orders. We will run the import after explaining how those source fields map to the reporting data.")
    execute("reset_demo", reset_demo, settings)
    before = load_mapping(settings.mapping_path)
    raw_before = execute("fetch_raw: inspect the healthy API", fetch_raw, before, settings.base_url)
    if not raw_before:
        raise DemoError("The shop returned no orders. This demonstration needs a non-empty order batch.")
    sample_before = raw_before[0]
    out.say("The shop calls the order amount 'total_price'. Our reporting model calls it 'total_amount'. That stable name is the canonical field: shared SQL reports can use it across merchants. A mapping connects each source field to that name.")
    out.fact("Mapping rule", "Read total_price from the shop and save it as total_amount.")
    out.fact("Data contract", "Rules every import must meet. For example: the order amount must be present, numeric and between 0 and 1,000,000.")
    out.fact("Example order", f"{sample_before['id']}: total_price = {sample_before['total_price']:.2f}")
    out.fact("Inside the task", "fetch_raw issues an HTTP GET; Mapping.apply projects source keys onto canonical names; contracts.validate checks required values, types, ranges and uniqueness before any load.")
    out.say()
    out.say("Running the import: fetch orders, translate field names, check the rules, then save valid data...")
    healthy = execute("run_ingest: healthy import", run_ingest, settings)
    original_warehouse = warehouse_snapshot(settings)
    reference = healthy.profiles["total_amount"]
    out.fact("RESULT: LOADED", f"{healthy.rows} orders saved in the reporting database.")
    out.fact("Reference mean", f"{reference.mean:.2f} per order; {reference.null_rate:.0%} missing amounts.")
    out.fact("Saved examples", f"total_amount: {reference.examples}. The contract enables sample_for_diagnosis for this field so a later diagnosis can inspect previous values.")
    out.say("These saved statistics are the baseline: our reference for what valid data looked like. Later, a proposed replacement must resemble this column.")
    out.pause("phase 2: change the shop's API")

    out.phase(2, "The shop renames the amount field", "The demo changes the fake shop, simulating a provider update.")
    out.say("We rename 'total_price' to 'order_total' in the API response. The order amounts stay the same. The pipeline's mapping still uses the old name.")
    out.fact("Fault injection", "POST /admin/state sets rename_total_price=true. The fake API then emits order_total instead of total_price on each GET /admin/api/orders.json. This switch belongs to the demo, not to the repair system.")
    response = execute("POST /admin/state: rename_total_price=true", httpx.post, f"{settings.base_url}/admin/state", json={"rename_total_price": True}, timeout=5)
    response.raise_for_status()
    log.info("API state update -> HTTP %s", response.status_code)
    raw_after = execute("fetch_raw: inspect the changed API", fetch_raw, before, settings.base_url)
    sample_after = next((r for r in raw_after if r.get("id") == sample_before["id"]), None)
    if sample_after is None or "total_price" in sample_after or "order_total" not in sample_after:
        raise DemoError("The API did not produce the expected field rename for the example order.")
    out.say()
    out.fact("Same order", sample_before["id"])
    out.fact("API before", f"total_price = {sample_before['total_price']:.2f}")
    out.fact("API now", f"order_total = {sample_after['order_total']:.2f}; total_price is absent.")
    out.fact("Also present", f"shipping_price = {sample_after['shipping_price']:.2f}: delivery charges, a different amount.")
    out.fact("RESULT: MISMATCH", "The job will ask for total_price, but the shop now sends order_total.")
    out.pause("phase 3: import from the changed API")

    out.phase(3, "The import task raises a contract failure", "orders_ingest's task function detects the problem and records evidence.")
    out.say("Running the same import again. It still reads the amount from the old API field, so the reporting amount becomes empty.")
    try:
        execute("run_ingest: import after the API rename", run_ingest, settings)
    except ContractViolationWithIncident as exc:
        incident_path = exc.incident_path
    else:
        raise DemoError("The import unexpectedly succeeded after the rename. The expected failure was not demonstrated.")
    incident = Incident.load(incident_path)
    if not any(v["kind"] == "missing_source_field" and v["field"] == "total_amount" for v in incident.violations):
        raise DemoError("The import failed for a different reason than the renamed amount field. Inspect the incident before continuing.")
    if warehouse_snapshot(settings) != original_warehouse:
        raise DemoError("The failed import changed the reporting data.")
    out.fact("RESULT: STOPPED", "The required order amount is missing. The import refuses this batch.")
    out.fact("Task identity", f"dag_id={incident.dag_id}; task_id={incident.task_id}; mapping_version={incident.mapping_version}.")
    out.fact("Failure labels", ", ".join(v["kind"] for v in incident.violations))
    out.say("missing_source_field means the API no longer sends the configured source. null_in_required_field means that absence left a required reporting value empty. Retrying the unchanged mapping cannot restore the missing field.")
    out.fact("Database", f"Still holds the {healthy.rows} orders from phase 1; the failed batch was not loaded.")
    out.say("The pipeline saves an incident: a file of evidence another process can read to diagnose the failure. Here is what it captured:")
    out.fact("Missing source", incident.mapping_fields["total_amount"])
    out.fact("Fields received", ", ".join(incident.upstream_fields_present))
    out.fact("More evidence", f"{len(incident.upstream_samples)} sample orders, the failed rules, the mapping and the reference statistics.")
    out.fact("Inside the task", "incident.build captures evidence while the response is in memory; Incident.save writes JSON; ContractViolationWithIncident is then raised. Selected top-level sensitive sample fields are redacted by name.")
    out.say("The incident directory connects the two DAGs. orders_ingest records the failure and raises an exception; it does not import the healer or call the repair DAG. Airflow can retry the task, while the separate reliability_layer reads the evidence. Saving an incident does not itself send an alert or open a ticket.")
    out.pause("phase 4: ask for a diagnosis")

    out.phase(4, "reliability_layer requests a diagnosis", "The repair workflow uses Claude Code." if backend.name == "claude-code" else "The repair workflow uses a scripted stand-in for the diagnosis.")
    out.fact("Repair DAG", "wait_for_an_incident -> drain_incident_queue -> anything_repaired -> rerun_orders_ingest.")
    out.say("wait_for_an_incident uses IncidentSensor. When the queue is empty, it defers to Airflow's triggerer, whose asynchronous loop checks for unresolved files every five seconds. Waiting does not occupy a worker slot. drain_incident_queue then processes the evidence; the LLM is called for diagnosis, not to poll for failures.")
    out.say("The diagnosis receives the evidence saved in phase 3: what failed, which fields the shop sent and what the amounts looked like before. It is asked to explain the failure and propose at most one change.")
    out.say("Claude Code must compare historical content examples with the candidate's current values and the contract's description. Product descriptions should still describe products; a subtotal must not be mistaken for an order total just because both are numbers. Its answer includes a content_check verdict, two content summaries and a reason.")
    out.say("In phase 5, the healer requires every remap to carry a complete equivalent-content assessment for the proposed fields, with available old and new examples. This content gate applies to Claude Code, the mock, supplied diagnoses and future backends. The backend only returns the answer; the healer records a PASS or STOP without rewriting what the diagnosis source said.")
    out.say("This requirement blocks missing evidence and an assessment that admits uncertainty, but a confident, incorrect equivalent verdict can pass. Requiring the assessment is a rule for accepting a proposal, not independent proof of its meaning. The mock supplies a clearly labeled scripted claim; it does not perform semantic analysis.")
    out.fact("Backend contract", "LLMBackend.diagnose returns a Diagnosis: cause_class, ownership, confidence and proposed_action. Both the Claude Code backend and the mock implement this interface.")
    if backend.name == "claude-code":
        out.say(f'ClaudeCodeBackend starts a subprocess: claude -p <evidence prompt> --model {backend.model} --effort {backend.effort} --output-format json --restricted --tools "" --strict-mcp-config. The model and reasoning effort are selected explicitly. The CLI is configured without tools; its JSON answer is parsed into a Diagnosis. These CLI restrictions are not an operating-system sandbox.')
        out.say("Calling Claude Code now to classify the failure, compare the content examples and propose an action. The demo waits for the CLI's complete JSON answer, so no partial answer is displayed. A WAITING message appears every five seconds with elapsed time and the timeout; it reports the wait, not how much of the model's work is complete. No input is needed. Press Ctrl+C to cancel.")
    else:
        out.say("The stand-in chooses the first unused API field in alphabetical order. It is deliberately simple: the independent checks must judge its answer. Run without 'mock' to use Claude Code instead.")
    diagnosis = execute(f"{backend.name}.diagnose: {incident.incident_id}", backend.diagnose, incident)
    out.say()
    show_proposal(out, diagnosis)
    out.say()
    out.fact("RESULT: PROPOSED", "The answer has been received. The field mapping has not changed yet.")
    out.say("Return only advances the demonstration. It does not approve the repair; the next phase runs the checks that make that decision.")
    out.pause("phase 5: verify the proposal before applying it")

    out.phase(5, "Verify the repair within drain_incident_queue", "The healer's Python checks evaluate the diagnosis before applying it.")
    policy = load_policy(settings.policy_path)
    out.say("An engineer defines the automation rules in advance. They are stored in policy.yml, a configuration file specifying which failures may be repaired, which actions are allowed and how a repair must be checked.")
    out.say("The policy uses the cause_class supplied by the diagnosis. semantic_change forces escalation, but Python does not independently establish that classification. A semantic change mislabeled as a rename can reach the data checks; phase 8 demonstrates that gap.")
    out.say("Five gates run in order: policy, allowlist, structure, content and evidence. The content gate checks the assessment's completeness, field names, equivalent verdict and available samples before the trial import. A STOP escalates the proposal and leaves the mapping unchanged.")
    out.say("The trial import fetches fresh orders using the proposed mapping without saving them to the reporting database. It checks the data contract, then compares the proposed amounts with the healthy reference from phase 1.")
    out.fact("Inside the healer", "Mapping.with_remap creates a candidate in memory. dry_run fetches and validates with that candidate; baseline.compare checks the remapped column. Mapping.save writes the change only after the required checks pass.")
    out.say(f"The replacement must still be numeric. Its mean may move by at most {policy.mean_relative_delta:.0%}, and its missing-value rate by at most {policy.null_rate_delta * 100:g} percentage points. A failed check stops the repair.")
    out.say("These statistics can reject some wrong substitutions. Similar statistics do not establish equivalent meaning, and a fixed tolerance can also reject legitimate variation. This POC has no measured false-acceptance or false-escalation rate.")
    out.say()
    out.say("Running the checks now...")
    resolution = execute(f"heal: {incident.incident_id}", heal, incident_path, settings, CapturedDiagnosis(diagnosis, backend.name), policy)
    show_checks(out, resolution, policy)
    if resolution.outcome != "repaired" or diagnosis.action.type != "remap_field" or not resolution.changed:
        if resolution.report_path:
            out.fact("Report", str(resolution.report_path))
        raise DemoError("The proposal did not produce a verified field repair. The demo stops here; it cannot claim that the import recovered.")
    after = load_mapping(settings.mapping_path)
    log.info("READ %s -> version=%d, fields=%s", settings.mapping_path, after.version, after.fields)
    out.fact("RESULT: APPLIED", "Every required check passed. The repair layer saved the new mapping.")
    field = diagnosis.action.canonical_field
    out.fact("Before", f"Read {field} from API field {before.fields[field]}.")
    out.fact("After", f"Read {field} from API field {after.fields[field]}.")
    out.fact("Change record", f"Mapping version {before.version} -> {after.version}; history records the old source, new source, time and reason.")
    out.say("The edit changes where an existing reporting field gets its value. The reporting format and its validation rules stay the same.")
    out.pause("phase 6: rerun the import with the repaired mapping")

    out.phase(6, "Rerun orders_ingest with the accepted mapping", "The demo calls the import function again to check recovery.")
    out.say("In Airflow, rerun_orders_ingest uses TriggerDagRunOperator to request another run of orders_ingest. That run loads the repaired data; a verification trial alone does not update the warehouse. A new DAG run also preserves the earlier attempt's execution history.")
    out.say("The shop still sends 'order_total'. Now the mapping knows where to read the amount, so we run a real import that saves the data.")
    out.fact("Inside the load", "load_to_warehouse replaces the orders table contents in a SQLite transaction. After a successful load, the column profiles replace the saved baseline. This is a local full-refresh load: every successful import replaces the entire orders snapshot.")
    recovered = execute("run_ingest: recovery import", run_ingest, settings)
    recovered_warehouse = warehouse_snapshot(settings)
    out.fact("RESULT: RECOVERED", f"{recovered.rows} orders loaded using mapping version {recovered.mapping_version}.")
    out.fact("Mean amount", f"{recovered.profiles['total_amount'].mean:.2f}, compared with {reference.mean:.2f} in the healthy run.")
    reviews = [h for h in after.history if h.get("review") == "pending"]
    out.fact("Human review", f"{len(reviews)} applied change(s) recorded as awaiting review.")
    out.say("Pending review means an engineer can inspect what was changed and why. This demo records that obligation; it does not pause data loading until approval or provide an approval screen.")
    out.say("If an incorrect mapping passes the checks, auto_then_review permits wrong values to enter the warehouse before a person reviews them. The subsequent profile refresh can also make those values the next baseline. Review afterwards does not prevent this exposure or implement rollback.")
    out.pause("phase 7: challenge the checks with a wrong proposal")

    _wrong_proposal(settings, out, incident, policy, recovered_warehouse)
    out.pause("phase 8: expose a semantic mistake the checks accept")
    semantic_root = _semantic_blind_spot(settings, out, policy, recovered_warehouse)
    out.pause("the final recap")
    out.say()
    out.say("DEMO COMPLETE")
    out.say("The renamed field was repaired and the import recovered. The shipping_price proposal was rejected by the baseline. In the isolated subtotal experiment, semantic_change forced escalation; misclassifying the same incident as a rename let an incorrect mapping pass every check and load wrong amounts.")
    out.say("The LLM contributes diagnosis; deterministic checks constrain actions and reject some wrong proposals. Neither the classification nor a passing statistical comparison proves semantic equivalence. The controlled answers in phases 7 and 8 test the checks, not the model's accuracy.")
    out.say("This POC demonstrates how AI can assist pipeline problem resolution through one limited example. Its validation rules are demonstration rules, not a finished production standard. Engineers would configure and extend them to meet the application's data requirements and acceptable risk, including refusing automatic repair where the evidence is insufficient.")
    out.say("The operational benefit to evaluate is fewer repeated investigations, with evidence for every applied or refused repair. This run demonstrates one orders pipeline and one repairable field rename. Measuring recovery time, review effort and incorrect repairs across more pipelines would be the next step.")
    out.fact("Production work", "Durable incident delivery, concurrent-update protection, a review interface, rolling baselines and failure-path hardening would be needed beyond this local demonstration.")
    out.say("The local YAML mapping and file queue do not provide coordination across distributed workers. Better statistical references can help with variability, but establishing field meaning needs additional evidence, such as provider definitions, business invariants or human review before use.")
    out.fact("Watch the DAGs", "Run docker compose up and open http://127.0.0.1:8080. The container uses the mock diagnosis backend; the scheduler, task dependencies and deferred sensor run in Airflow.")
    out.say()
    out.say("Files from this run, if you want to inspect the evidence:")
    out.fact("Field rules", "contracts/orders.contract.yml")
    out.fact("Automation rules", "policy.yml")
    out.fact("Applied mapping", "mappings/orders.mapping.yml (includes the change history)")
    out.fact("Reference data", "baselines/orders.baseline.json")
    out.fact("Failure evidence", str(incident_path.relative_to(settings.root)))
    out.fact("Repair checks", str(incident_path.with_suffix('.resolution.json').relative_to(settings.root)))
    out.fact("Rejected proposal", "incidents/safety-check.escalation.md")
    out.fact("Accepted mistake", f"{semantic_root.relative_to(settings.root)}/ (isolated mapping, incidents, warehouse and refreshed baseline)")
    out.fact("Design context", "docs/design-rationale.md (the orders example, Airflow architecture and production boundaries)")


def _wrong_proposal(
    settings: Settings,
    out: Console,
    incident: Incident,
    policy: Policy,
    recovered_warehouse: tuple[int, float],
) -> None:
    out.phase(7, "A plausible answer can still be wrong", "The demo supplies a deliberately wrong diagnosis; Python checks it.")
    out.say("The API also contains 'shipping_price', the delivery charge. It is numeric, non-null and within the amount's allowed range. Using it as the order amount could pass all those data rules while making revenue reports wrong.")
    out.say("A successful DAG state tells us that its tasks completed. To trust the resulting revenue report, we also need evidence that the amount field still represents order totals.")
    out.say("We now test that exact mistake. This answer is written into the demo, not produced by AI. It includes a deliberately false equivalent-content claim so the content gate passes and we can test whether the numeric evidence catches the error. We use a copy of the original incident, so the real repair record is preserved.")
    wrong = Diagnosis.from_dict({
        "cause_class": "schema_drift_renamed_field",
        "ownership": "customer",
        "confidence": 0.95,
        "summary": "The order amount appears to have moved to shipping_price.",
        "content_check": {
            "canonical_field": "total_amount",
            "new_source_field": "shipping_price",
            "verdict": "equivalent",
            "reference_summary": "Numeric order totals in the historical examples.",
            "candidate_summary": "Numeric delivery charges in the current examples.",
            "reason": "Deliberately false equivalence claim supplied to test whether the remaining evidence checks reject the proposal.",
        },
        "proposed_action": {
            "type": "remap_field",
            "canonical_field": "total_amount",
            "new_source_field": "shipping_price",
            "rationale": "It is a number, within the permitted range, and never empty.",
        },
    })
    out.say()
    show_proposal(out, wrong)
    out.say()
    out.say("Running the same repair checks against that alternative. Watch the two evidence results: satisfying the contract is only the first one.")
    safety_incident = copy.deepcopy(incident)
    safety_incident.incident_id = "safety-check"
    safety_path = safety_incident.save(settings.incidents_dir)
    saved_mapping = settings.mapping_path.read_bytes()
    resolution = execute("heal: deliberately wrong shipping_price proposal", heal, safety_path, settings, CapturedDiagnosis(wrong, "scripted-wrong-answer"), policy)
    show_checks(out, resolution, policy)
    failed = [c for c in resolution.checks if not c.passed]
    contract_passed = any(c.gate == "evidence" and c.passed and "satisfies the contract" in c.detail for c in resolution.checks)
    if resolution.outcome != "escalated" or not contract_passed or not any("average moved" in c.detail for c in failed):
        raise DemoError("The wrong proposal did not produce the expected result: contract passes, historical comparison rejects it. Inspect the recorded checks.")
    if settings.mapping_path.read_bytes() != saved_mapping or warehouse_snapshot(settings) != recovered_warehouse:
        raise DemoError("The rejected proposal unexpectedly changed the mapping or reporting data.")
    out.fact("RESULT: REJECTED", "escalated: automatic repair was refused and an explanation was saved for a person.")
    out.say("The required-field, type and range checks passed. The mean amount was too far from the healthy reference, so the historical comparison stopped the change.")
    out.fact("Mapping and data", "The repair from phase 5 and the loaded orders from phase 6 are unchanged.")
    out.fact("Escalation report", "A local Markdown file records the rejected proposal, passed checks and reason for refusal. No message is sent to anyone.")
    out.fact("Inside the refusal", "_escalate writes the Markdown report and a .resolution.json record. The demo then reads the mapping and database again to verify that rejecting this proposal left them unchanged.")


def _semantic_blind_spot(
    settings: Settings,
    out: Console,
    policy: Policy,
    recovered_warehouse: tuple[int, float],
) -> Path:
    out.phase(8, "A semantic mistake can pass every check", "The demo supplies two controlled diagnoses; the real healer and import run on an isolated copy.")
    out.say("The next API change replaces the total with subtotal, defined by this fixture as 90% of the original amount. The name and the quantity change. Its mean remains close enough to pass the default tolerance.")
    out.say("We copy the recovered mapping, contract, policy, baseline and warehouse into a separate experiment directory. Both diagnoses below are supplied test answers, even when phase 4 used Claude Code. They also falsely claim equivalent content, simulating an incorrect semantic assessment. This comparison tests policy enforcement and its limits; it does not demonstrate that a model will classify this case correctly.")
    out.say("Two separate recorded live checks of this incident returned semantic_change and escalate. The earlier check suggested subtotal + shipping_price as a reconstruction, which this fixture does not support: subtotal is 90% of the original total and shipping is generated independently.")
    out.say("A repeat with Opus 5.5 and maximum effort explicitly noticed that this sum still falls short and that historical and current samples are unpaired. It still inferred that shipping was excluded and speculated about tax or other charges, which the fixture does not establish. The healer refused the change and preserved the copied mapping, baseline and warehouse. These are two observations, not an accuracy guarantee or a controlled model comparison. This phase uses supplied answers so the checks remain reproducible.")
    out.fact("Recorded live checks", "docs/observations/subtotal-claude-code.json and docs/observations/subtotal-opus-5-5-max.json preserve the evidence and original responses, including their limitations.")
    protected = {
        path: path.read_bytes()
        for path in (settings.mapping_path, settings.baseline_path, settings.warehouse_path)
    }
    root = Path(tempfile.mkdtemp(prefix="semantic-check-", dir=settings.root / "data"))
    experiment = replace(settings, root=root)
    for source, target in (
        (settings.mapping_path, experiment.mapping_path),
        (settings.contract_path, experiment.contract_path),
        (settings.policy_path, experiment.policy_path),
        (settings.baseline_path, experiment.baseline_path),
        (settings.warehouse_path, experiment.warehouse_path),
    ):
        target.parent.mkdir(parents=True, exist_ok=True)
        execute(f"copy experiment input: {source.name}", shutil.copy2, source, target)
    out.fact("Experiment files", str(root.relative_to(settings.root)))
    state_response = execute("GET /admin/state: remember source settings", httpx.get, f"{settings.base_url}/admin/state", timeout=5)
    state_response.raise_for_status()
    state = state_response.json()
    restore = {name: state[name] for name in ("rename_total_price", "rename_to_subtotal")}
    try:
        response = execute("POST /admin/state: rename_to_subtotal=true", httpx.post, f"{settings.base_url}/admin/state", json={"rename_total_price": False, "rename_to_subtotal": True}, timeout=5)
        response.raise_for_status()
        out.fact("Fault injection", "The fake API now emits subtotal = round(original total * 0.90, 2). The copied mapping still requests order_total.")
        try:
            execute("run_ingest: subtotal breaks the copied mapping", run_ingest, experiment)
        except ContractViolationWithIncident as exc:
            incident_path = exc.incident_path
        else:
            raise DemoError("The subtotal experiment did not produce the expected contract failure.")
        incident = Incident.load(incident_path)
        if "subtotal" not in incident.upstream_fields_present or not any(
            v["kind"] == "missing_source_field" and v["field"] == "total_amount"
            for v in incident.violations
        ):
            raise DemoError("The experiment failed for a different reason than the subtotal replacement.")
        if warehouse_snapshot(experiment) != recovered_warehouse:
            raise DemoError("The failed experiment import changed the copied warehouse.")
        out.fact("Captured evidence", f"{len(incident.upstream_samples)} samples; received fields: {', '.join(incident.upstream_fields_present)}.")
        response_data = {
            "cause_class": "semantic_change",
            "ownership": "provider",
            "confidence": 0.95,
            "summary": "Controlled classification comparison for the subtotal replacement.",
            "content_check": {
                "canonical_field": "total_amount",
                "new_source_field": "subtotal",
                "verdict": "equivalent",
                "reference_summary": "Numeric order totals from the previous successful import.",
                "candidate_summary": "Numeric subtotal values in the current response.",
                "reason": "Deliberately false assessment: treat these two monetary quantities as equivalent to test the remaining checks.",
            },
            "proposed_action": {
                "type": "remap_field",
                "canonical_field": "total_amount",
                "new_source_field": "subtotal",
                "rationale": "A deliberately incorrect remap supplied to test the policy and evidence gates.",
            },
        }
        out.say("Case A: classify the incident as semantic_change. We deliberately keep the wrong remap in the answer to show that policy overrides it. Confidence and proposed action will be identical in case B.")
        recognized = Diagnosis.from_dict(response_data)
        show_proposal(out, recognized)
        saved_mapping = experiment.mapping_path.read_bytes()
        saved_baseline = experiment.baseline_path.read_bytes()
        refused = execute("heal: semantic_change classification", heal, incident_path, experiment, CapturedDiagnosis(recognized, "controlled-semantic-diagnosis"), policy)
        show_checks(out, refused, policy)
        if refused.outcome != "escalated" or not any(c.gate == "policy" and not c.passed for c in refused.checks):
            raise DemoError("The configured policy did not refuse the semantic_change diagnosis.")
        if experiment.mapping_path.read_bytes() != saved_mapping or experiment.baseline_path.read_bytes() != saved_baseline or warehouse_snapshot(experiment) != recovered_warehouse:
            raise DemoError("The refused semantic diagnosis changed the experiment's mapping, baseline or data.")
        out.fact("RESULT: ESCALATED", "Policy blocked the remap because the supplied cause_class was semantic_change. The copied mapping and warehouse are unchanged.")

        out.say("Case B: use the same evidence, confidence and proposed remap, but change cause_class to schema_drift_renamed_field. This deliberately simulates a classification error. No model is called.")
        misclassified_incident = copy.deepcopy(incident)
        misclassified_incident.incident_id = "semantic-misclassified"
        wrong_path = misclassified_incident.save(experiment.incidents_dir)
        wrong = Diagnosis.from_dict({**response_data, "cause_class": "schema_drift_renamed_field"})
        show_proposal(out, wrong)
        accepted = execute("heal: misclassified subtotal proposal", heal, wrong_path, experiment, CapturedDiagnosis(wrong, "controlled-wrong-classification"), policy)
        show_checks(out, accepted, policy)
        if accepted.outcome != "repaired" or not accepted.changed or not accepted.checks or not all(c.passed for c in accepted.checks):
            raise DemoError("The incorrect subtotal proposal was not accepted. This counterexample requires the demo policy's permissive mean tolerance; it cannot claim a false acceptance with these settings.")
        if load_mapping(experiment.mapping_path).fields["total_amount"] != "subtotal":
            raise DemoError("The accepted experiment mapping does not point at subtotal.")
        out.fact("RESULT: FALSE ACCEPT", "Every implemented check passed, and the wrong mapping was applied in the experiment. This is a demonstrated limitation, not a successful repair.")
        out.say("This is a POC with deliberately limited verification rules. The wrong result shows a gap in these rules; it does not set the level of validation a real system should accept. Engineers can make the acceptance criteria as strict as their use case requires, through configuration and additional implementation.")
        out.fact("Configurable today", "In policy.yml, setting mean_relative_delta to 0.05 (5%) would reject this 10% difference. Setting schema_drift_renamed_field to escalate would refuse automatic repair and leave the incident for a person.")
        out.fact("Additional engineering", "Checks against business rules, authoritative field definitions or human approval before applying a repair would require further implementation. Tighter thresholds can also reject valid changes, and similar statistics still do not establish equivalent meaning.")
        out.say("Run the import with that mapping now. The writes below go to the copied SQLite warehouse, showing the downstream consequence of accepting this proposal.")
        loaded = execute("run_ingest: load incorrect subtotal amounts into the copy", run_ingest, experiment)
        rows, total = warehouse_snapshot(experiment)
        original_rows, original_total = recovered_warehouse
        if rows != original_rows or rows == 0 or original_total <= 0:
            raise DemoError("The experiment no longer has a comparable non-empty order batch.")
        difference = abs(total - original_total) / original_total
        if abs(difference - 0.10) > 0.001:
            raise DemoError("The loaded experiment totals did not show the expected 10% error.")
        out.fact("Loaded evidence", f"{rows} orders; mean {original_total / rows:.2f} -> {total / rows:.2f}; total {original_total:.2f} -> {total:.2f} ({difference:.0%} lower).")
        out.fact("Review state", f"review_required={accepted.review_required}; mapping history records review=pending. The copied warehouse already contains the wrong values.")
        out.fact("Baseline effect", f"The successful import also saved {loaded.profiles['total_amount'].mean:.2f} as the next reference mean, despite the semantic error.")
        out.say("The classification was the decisive difference. The policy rejects a semantic_change label; it does not independently recognize every semantic change. Similar numeric profiles cannot establish that two fields have the same meaning.")
    finally:
        response = execute("POST /admin/state: restore source settings after experiment", httpx.post, f"{settings.base_url}/admin/state", json=restore, timeout=5)
        response.raise_for_status()
    if any(path.read_bytes() != saved for path, saved in protected.items()) or warehouse_snapshot(settings) != recovered_warehouse:
        raise DemoError("The isolated experiment changed the main demo's recovered state.")
    out.fact("Main demo state", "The recovered mapping, baseline and warehouse are unchanged. The API's original rename settings have been restored.")
    out.fact("Evidence retained", f"{root.relative_to(settings.root)}/ contains both resolutions, the wrong mapping and the copied warehouse for inspection. It is separate from Airflow's incident queue.")
    return root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="./scripts/demo.sh", description="Walk through a pipeline repair and its verification limits, pressing Return after each phase.")
    parser.add_argument("backend", nargs="?", choices=("claude-code", "mock"), default="claude-code", help="Use Claude Code for a live diagnosis, or mock for a scripted answer with the same real checks.")
    parser.add_argument("--no-pause", action="store_true", default=bool(os.environ.get("NO_PAUSE")), help="Run unattended without waiting for Return.")
    parser.add_argument("--start-services", action="store_true", help="Authorize starting Docker and the local API if needed, without startup questions. Phase pauses still apply unless --no-pause is used.")
    debug = parser.add_mutually_exclusive_group()
    debug.add_argument("--debug", action="store_true", help="Expand the runtime trace to include model instructions and exception tracebacks.")
    debug.add_argument("--no-debug", dest="debug", action="store_false", help="Use the standard explanation and live execution traces (the default).")
    parser.set_defaults(debug=False)
    args = parser.parse_args(argv)
    out = Console(no_pause=args.no_pause)
    settings = Settings()
    backend = ClaudeCodeBackend() if args.backend == "claude-code" else MockBackend()
    with execution_traces(out, debug=args.debug):
        return _run(settings, backend, out, debug=args.debug, start_services=args.start_services)


def _run(settings: Settings, backend: LLMBackend, out: Console, *, debug: bool, start_services: bool = False) -> int:
    try:
        if isinstance(backend, ClaudeCodeBackend) and not backend.available():
            raise DemoError("Claude Code was not found. Install and sign in to Claude Code, or run './scripts/demo.sh mock' to use the scripted diagnosis.")
        ensure_services(settings, out, start_services=start_services, no_pause=out.no_pause)
        run_demo(settings, backend, out)
    except KeyboardInterrupt:
        out.say("Demo stopped. Changes from completed phases remain on disk; rerunning the demo resets its state.")
        return 130
    except StartupError as exc:
        out.say()
        out.say(f"DEMO NOT STARTED: {exc}")
        out.say("The walkthrough has not reset or changed its mapping, incidents, baseline or warehouse.")
        return 1
    except Exception as exc:  # The presentation boundary must explain unexpected failures too.
        out.say()
        out.say(f"DEMO STOPPED: {exc}")
        out.say("Later phases were not run. Completed phases may have left data and evidence on disk. Use --debug for additional execution details.")
        if debug:
            log.exception("Demo failed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
