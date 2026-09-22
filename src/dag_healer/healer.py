# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""The reliability layer.

The whole design rests on one rule: a diagnosis is a hypothesis, and a
hypothesis is worthless until something other than its author has tried to
falsify it. So a mapping proposal is put through five gates before it is
allowed to change anything.

  1. Policy      is this class of failure one we have agreed to automate?
  2. Allowlist   is the proposed action one of the small number we can perform?
  3. Structure   does the action even make sense against the current mapping
                 and the fields the upstream actually returned?
  4. Content     is there a complete equivalent-content assessment, with actual
                 old and new examples, regardless of the diagnosis source?
  5. Evidence    with the change applied, does a fresh run satisfy the contract
                 *and* still look like the last known-good run?

The content gate checks the supplied assessment and evidence availability;
it does not establish that the assessment is true. The evidence gate rejects
some plausible wrong substitutions, but similar statistics do not prove meaning.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import baseline as baseline_mod
from . import escalation
from . import queue as queue_mod
from .backends.base import ContentCheck, Diagnosis, LLMBackend
from .config import Policy, Settings, load_policy
from .incident import Incident, is_sensitive_field
from .mapping import Mapping, load_mapping
from .pipeline import dry_run

log = logging.getLogger(__name__)


@dataclass
class Check:
    """One gate's verdict, recorded whether it passed or not.

    Kept because a layer that only reports its objections is impossible to
    trust: you cannot tell a repair that survived five gates from one that was
    never really examined. The passing checks are the audit trail.
    """

    gate: str  # policy | allowlist | structure | content | evidence
    passed: bool
    detail: str

    def __str__(self) -> str:
        return f"{'ok  ' if self.passed else 'STOP'} {self.gate:<9} {self.detail}"


@dataclass
class Resolution:
    outcome: str  # repaired | escalated
    incident_id: str
    reasons: list[str] = field(default_factory=list)
    diagnosis: Diagnosis | None = None
    mapping_version: int | None = None
    report_path: Path | None = None
    review_required: bool = True
    checks: list[Check] = field(default_factory=list)
    changed: bool = True

    def summary(self) -> str:
        if self.outcome == "repaired":
            action = self.diagnosis.action if self.diagnosis else None
            what = (
                f"{action.canonical_field} -> {action.new_source_field}"
                if action and action.type == "remap_field"
                else "change"
            )
            if not self.changed:
                return f"already repaired ({what}), mapping v{self.mapping_version} untouched"
            state = "pending review" if self.review_required else "applied"
            return f"repaired ({what}), mapping v{self.mapping_version}, {state}"
        return "escalated: " + ("; ".join(self.reasons) or "no reason recorded")


def heal(
    incident_path: str | Path,
    settings: Settings,
    backend: LLMBackend,
    policy: Policy | None = None,
) -> Resolution:
    incident = Incident.load(incident_path)
    log.debug("incident %s loaded", incident.incident_id)
    policy = policy or load_policy(settings.policy_path)
    mapping = load_mapping(settings.mapping_path)

    log.debug("asking backend '%s' for a diagnosis; its answer is a hypothesis, not an instruction", backend.name)
    diagnosis = backend.diagnose(incident)
    log.debug("diagnosis received; mapping proposals must pass five gates")
    checks: list[Check] = []

    log.debug("gate 1 of 5, policy: is this class of failure one we agreed to automate?")
    level = policy.level_for(diagnosis.cause_class)
    checks.append(
        _check(
            "policy",
            level != "escalate",
            f"'{diagnosis.cause_class}' is '{level}' in policy.yml",
            f"policy says '{diagnosis.cause_class}' is never repaired automatically",
        )
    )
    checks.append(
        _check(
            "policy",
            diagnosis.confidence >= policy.min_confidence,
            f"confidence {diagnosis.confidence:.2f} clears the "
            f"{policy.min_confidence:.2f} threshold",
            f"confidence {diagnosis.confidence:.2f} is below the "
            f"{policy.min_confidence:.2f} threshold",
        )
    )

    log.debug("gate 2 of 5, allowlist: is the proposed action one of the few we can perform?")
    action = diagnosis.action
    if action.type not in policy.allowed_actions:
        checks.append(
            Check(
                "allowlist",
                False,
                f"action '{action.type}' is not in the allowlist {policy.allowed_actions}",
            )
        )
    elif action.type == "escalate":
        checks.append(Check("allowlist", False, "the diagnosis itself asked for a human"))
    else:
        checks.append(
            Check("allowlist", True, f"'{action.type}' is one of {policy.allowed_actions}")
        )

    if _failed(checks):
        return _escalate(incident, diagnosis, checks, settings)

    if action.type == "retry":
        # A retry needs no repair: re-running the pipeline is the verification.
        checks.append(
            Check("evidence", True, "retry needs no verification of its own: the next run is the test")
        )
        resolution = Resolution(
            outcome="repaired",
            incident_id=incident.incident_id,
            reasons=["transient failure; retry is self-verifying"],
            diagnosis=diagnosis,
            mapping_version=mapping.version,
            review_required=False,
            checks=checks,
        )
        queue_mod.record(settings.incidents_dir, resolution)
        return resolution

    log.debug("gate 3 of 5, structure: does the action make sense against the mapping and the payload?")
    checks += _structural_checks(action, mapping, incident)
    if _failed(checks):
        return _escalate(incident, diagnosis, checks, settings)

    log.debug("gate 4 of 5, content: does this remap have a complete assessment and actual content examples?")
    checks += _content_checks(diagnosis, incident)
    if _failed(checks):
        return _escalate(incident, diagnosis, checks, settings)

    if mapping.fields.get(action.canonical_field) == action.new_source_field:
        # A duplicate incident for a failure that is already fixed. One contract
        # failure files one incident per task attempt, so Airflow's own retry
        # produces two; so do a manual re-trigger, a backfill, and two
        # schedulers. Applying the change again would pass every gate, because
        # the candidate mapping is the live one: the run satisfies the contract
        # and the column matches the baseline exactly. Nothing would look wrong,
        # and the human review queue would carry two entries for one real
        # change, one of them a remap from a field to itself.
        checks.append(
            Check(
                "structure",
                True,
                f"'{action.canonical_field}' already points at '{action.new_source_field}'; "
                "this incident duplicates one that was already repaired",
            )
        )
        resolution = Resolution(
            outcome="repaired",
            incident_id=incident.incident_id,
            reasons=["the mapping already points there; nothing to change"],
            diagnosis=diagnosis,
            mapping_version=mapping.version,
            review_required=False,
            checks=checks,
            changed=False,
        )
        queue_mod.record(settings.incidents_dir, resolution)
        return resolution

    assert action.canonical_field and action.new_source_field
    candidate = mapping.with_remap(
        action.canonical_field,
        action.new_source_field,
        reason=f"{diagnosis.cause_class}: {diagnosis.summary}"[:300],
    )

    log.debug("gate 5 of 5, evidence: running the whole pipeline with the candidate mapping to try to disprove it")
    checks += _evidence_checks(candidate, action.canonical_field, policy, settings)
    if _failed(checks):
        return _escalate(incident, diagnosis, checks, settings)

    log.debug("every gate passed; writing mapping v%d", candidate.version)
    candidate.save(settings.mapping_path)
    resolution = Resolution(
        outcome="repaired",
        incident_id=incident.incident_id,
        reasons=[
            "contract satisfied after the change",
            "remapped column matches the last known-good profile",
        ],
        diagnosis=diagnosis,
        mapping_version=candidate.version,
        review_required=policy.level_for(diagnosis.cause_class) == "auto_then_review",
        checks=checks,
    )
    queue_mod.record(settings.incidents_dir, resolution)
    return resolution


def _check(gate: str, passed: bool, ok: str, objection: str) -> Check:
    """A gate's verdict, phrased for whichever way it went."""
    return Check(gate, passed, ok if passed else objection)


def _failed(checks: list[Check]) -> list[str]:
    return [c.detail for c in checks if not c.passed]


def _structural_checks(action: Any, mapping: Mapping, incident: Incident) -> list[Check]:
    if not action.canonical_field or not action.new_source_field:
        return [Check("structure", False, "remap_field was proposed without naming both fields")]

    taken = {
        src: canon
        for canon, src in mapping.fields.items()
        if canon != action.canonical_field
    }

    return [
        _check(
            "structure",
            action.canonical_field in mapping.fields,
            f"'{action.canonical_field}' is an existing canonical field",
            f"'{action.canonical_field}' is not a canonical field; the layer cannot "
            "invent one, that would be a contract change",
        ),
        _check(
            "structure",
            action.new_source_field in incident.upstream_fields_present,
            f"'{action.new_source_field}' really was in the upstream payload",
            f"'{action.new_source_field}' was not present in the upstream payload "
            f"(saw: {', '.join(incident.upstream_fields_present) or 'nothing'})",
        ),
        _check(
            "structure",
            action.new_source_field not in taken,
            f"'{action.new_source_field}' is not already the source for another field",
            f"'{action.new_source_field}' is already the source for "
            f"'{taken.get(action.new_source_field)}'; stealing it would break that field",
        ),
    ]


def _content_checks(diagnosis: Diagnosis, incident: Incident) -> list[Check]:
    """Check the assessment for every remap source, preserving the original answer."""
    action, content = diagnosis.action, diagnosis.content_check
    reason = ""
    if not isinstance(content, ContentCheck):
        reason = "the proposed remap has no structured content_check assessment"
    elif content.canonical_field != action.canonical_field or content.new_source_field != action.new_source_field:
        reason = "the content assessment does not refer to the proposed field mapping"
    elif any(is_sensitive_field(name) for name in (
        content.canonical_field, content.new_source_field,
        incident.mapping_fields.get(content.canonical_field, ""),
    )):
        reason = "the proposed fields are excluded from content sampling"
    else:
        reference = incident.baseline.get(content.canonical_field, {}).get("examples", [])
        candidate = [row.get(content.new_source_field) for row in incident.upstream_samples]
        if not isinstance(reference, list) or not baseline_mod.sample_values(reference):
            reason = f"no known-good baseline content examples for '{content.canonical_field}'; statistics alone are insufficient"
        elif not baseline_mod.sample_values(candidate):
            reason = f"no readable current content examples for '{content.new_source_field}'"
        elif content.verdict != "equivalent":
            reason = f"content verdict is '{content.verdict}'; a remap requires 'equivalent'"
        elif not all(isinstance(value, str) and value.strip() for value in (
            content.reference_summary, content.candidate_summary, content.reason,
        )):
            reason = "the content assessment is incomplete: both sample summaries and a reason are required"
    check = Check(
        "content", not reason,
        reason or "complete equivalent-content assessment matches the proposed fields and has old and new examples; its semantic truth is not independently verified",
    )
    log.info("CONTENT CHECK %s | %s", "PASS" if check.passed else "STOP", check.detail)
    return [check]


def _evidence_checks(
    candidate: Mapping,
    canonical_field: str,
    policy: Policy,
    settings: Settings,
) -> list[Check]:
    """Run the pipeline with the candidate mapping and try to disprove the repair."""
    checks: list[Check] = []

    try:
        result = dry_run(candidate, settings)
    except Exception as exc:  # noqa: BLE001 - any failure here is a reason to stop
        return [Check("evidence", False, f"verification run failed: {type(exc).__name__}: {exc}")]

    if policy.require_contract_pass:
        checks.append(
            _check(
                "evidence",
                not result.violations,
                f"a run with the candidate mapping satisfies the contract "
                f"({len(result.records)} rows, 0 violations)",
                "contract still violated after the change: "
                + "; ".join(f"{v.kind} on {v.field}" for v in result.violations[:3]),
            )
        )

    if policy.require_baseline_match:
        reference = baseline_mod.load_baseline(settings.baseline_path)
        if canonical_field not in reference:
            checks.append(
                Check(
                    "evidence",
                    False,
                    f"no known-good baseline for '{canonical_field}', so the repair cannot be verified",
                )
            )
        else:
            observed = result.profiles[canonical_field]
            known_good = reference[canonical_field]
            log.debug(
                "comparing '%s' against the last known-good profile: this is what "
                "catches a repair that passes the contract and is still wrong",
                canonical_field,
            )
            problems = baseline_mod.compare(
                canonical_field,
                observed,
                known_good,
                null_rate_delta=policy.null_rate_delta,
                mean_relative_delta=policy.mean_relative_delta,
            )
            # The measurement is stated either way: a verification that only
            # speaks when it objects is indistinguishable from one that never ran.
            checks += [Check("evidence", False, p) for p in problems] or [
                Check(
                    "evidence",
                    True,
                    baseline_mod.describe(canonical_field, observed, known_good)
                    + f", inside a {policy.mean_relative_delta:.0%} tolerance",
                )
            ]

    return checks


def _escalate(
    incident: Incident,
    diagnosis: Diagnosis | None,
    checks: list[Check],
    settings: Settings,
) -> Resolution:
    reasons = _failed(checks)
    log.debug("not repairing; writing an escalation with the evidence already gathered")
    report = escalation.write_report(
        incident, diagnosis, reasons, settings.incidents_dir, checks=checks
    )
    resolution = Resolution(
        outcome="escalated",
        incident_id=incident.incident_id,
        reasons=reasons,
        diagnosis=diagnosis,
        report_path=report,
        checks=checks,
        changed=False,
    )
    queue_mod.record(settings.incidents_dir, resolution)
    return resolution
