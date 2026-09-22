"""The reliability layer.

The whole design rests on one rule: a diagnosis is a hypothesis, and a
hypothesis is worthless until something other than its author has tried to
falsify it. So the model's proposal is put through four gates before it is
allowed to change anything.

  1. Policy      is this class of failure one we have agreed to automate?
  2. Allowlist   is the proposed action one of the small number we can perform?
  3. Structure   does the action even make sense against the current mapping
                 and the fields the upstream actually returned?
  4. Evidence    with the change applied, does a fresh run satisfy the contract
                 *and* still look like the last known-good run?

Gate 4 is the one that earns its keep. A renamed `total_price` and an unrelated
`shipping_price` are both numbers in a plausible range, so a repair that picks
the wrong one passes the contract cleanly and silently corrupts every downstream
number. The baseline comparison is what catches that, and it is the reason this
is a reliability layer rather than a way to automate being wrong faster.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import baseline as baseline_mod
from . import escalation
from .backends.base import Diagnosis, LLMBackend
from .config import Policy, Settings, load_policy
from .incident import Incident
from .mapping import Mapping, load_mapping
from .pipeline import dry_run


@dataclass
class Resolution:
    outcome: str  # repaired | escalated
    incident_id: str
    reasons: list[str] = field(default_factory=list)
    diagnosis: Diagnosis | None = None
    mapping_version: int | None = None
    report_path: Path | None = None
    review_required: bool = True

    def summary(self) -> str:
        if self.outcome == "repaired":
            action = self.diagnosis.action if self.diagnosis else None
            what = (
                f"{action.canonical_field} -> {action.new_source_field}"
                if action and action.type == "remap_field"
                else "change"
            )
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
    policy = policy or load_policy(settings.policy_path)
    mapping = load_mapping(settings.mapping_path)

    diagnosis = backend.diagnose(incident)
    reasons: list[str] = []

    # Gate 1: policy.
    level = policy.level_for(diagnosis.cause_class)
    if level == "escalate":
        reasons.append(
            f"policy says '{diagnosis.cause_class}' is never repaired automatically"
        )
    if diagnosis.confidence < policy.min_confidence:
        reasons.append(
            f"confidence {diagnosis.confidence:.2f} is below the {policy.min_confidence:.2f} threshold"
        )

    # Gate 2: allowlist.
    action = diagnosis.action
    if action.type not in policy.allowed_actions:
        reasons.append(f"action '{action.type}' is not in the allowlist {policy.allowed_actions}")
    elif action.type == "escalate":
        reasons.append("the diagnosis itself asked for a human")

    if reasons:
        return _escalate(incident, diagnosis, reasons, settings)

    if action.type == "retry":
        # A retry needs no repair: re-running the pipeline is the verification.
        return Resolution(
            outcome="repaired",
            incident_id=incident.incident_id,
            reasons=["transient failure; retry is self-verifying"],
            diagnosis=diagnosis,
            mapping_version=mapping.version,
            review_required=False,
        )

    # Gate 3: structure.
    reasons += _structural_objections(action, mapping, incident)
    if reasons:
        return _escalate(incident, diagnosis, reasons, settings)

    assert action.canonical_field and action.new_source_field
    candidate = mapping.with_remap(
        action.canonical_field,
        action.new_source_field,
        reason=f"{diagnosis.cause_class}: {diagnosis.summary}"[:300],
    )

    # Gate 4: evidence.
    reasons += _evidence_objections(candidate, action.canonical_field, policy, settings)
    if reasons:
        return _escalate(incident, diagnosis, reasons, settings)

    candidate.save(settings.mapping_path)
    return Resolution(
        outcome="repaired",
        incident_id=incident.incident_id,
        reasons=[
            "contract satisfied after the change",
            "remapped column matches the last known-good profile",
        ],
        diagnosis=diagnosis,
        mapping_version=candidate.version,
        review_required=policy.level_for(diagnosis.cause_class) == "auto_then_review",
    )


def _structural_objections(action: Any, mapping: Mapping, incident: Incident) -> list[str]:
    problems: list[str] = []

    if not action.canonical_field or not action.new_source_field:
        problems.append("remap_field was proposed without naming both fields")
        return problems

    if action.canonical_field not in mapping.fields:
        problems.append(
            f"'{action.canonical_field}' is not a canonical field; the layer cannot "
            "invent one, that would be a contract change"
        )

    if action.new_source_field not in incident.upstream_fields_present:
        problems.append(
            f"'{action.new_source_field}' was not present in the upstream payload "
            f"(saw: {', '.join(incident.upstream_fields_present) or 'nothing'})"
        )

    taken = {
        src: canon
        for canon, src in mapping.fields.items()
        if canon != action.canonical_field
    }
    if action.new_source_field in taken:
        problems.append(
            f"'{action.new_source_field}' is already the source for "
            f"'{taken[action.new_source_field]}'; stealing it would break that field"
        )

    return problems


def _evidence_objections(
    candidate: Mapping,
    canonical_field: str,
    policy: Policy,
    settings: Settings,
) -> list[str]:
    """Run the pipeline with the candidate mapping and try to disprove the repair."""
    problems: list[str] = []

    try:
        result = dry_run(candidate, settings)
    except Exception as exc:  # noqa: BLE001 - any failure here is a reason to stop
        return [f"verification run failed: {type(exc).__name__}: {exc}"]

    if policy.require_contract_pass and result.violations:
        problems.append(
            "contract still violated after the change: "
            + "; ".join(f"{v.kind} on {v.field}" for v in result.violations[:3])
        )

    if policy.require_baseline_match:
        reference = baseline_mod.load_baseline(settings.baseline_path)
        if canonical_field not in reference:
            problems.append(
                f"no known-good baseline for '{canonical_field}', so the repair cannot be verified"
            )
        else:
            problems += baseline_mod.compare(
                canonical_field,
                result.profiles[canonical_field],
                reference[canonical_field],
                null_rate_delta=policy.null_rate_delta,
                mean_relative_delta=policy.mean_relative_delta,
            )

    return problems


def _escalate(
    incident: Incident,
    diagnosis: Diagnosis | None,
    reasons: list[str],
    settings: Settings,
) -> Resolution:
    report = escalation.write_report(incident, diagnosis, reasons, settings.incidents_dir)
    return Resolution(
        outcome="escalated",
        incident_id=incident.incident_id,
        reasons=reasons,
        diagnosis=diagnosis,
        report_path=report,
    )
