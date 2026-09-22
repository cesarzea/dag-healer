"""The diagnosis interface.

Two implementations ship: Claude Code over its CLI, and a deterministic mock
used by the tests. The seam exists because the interesting part of this project
is what happens to a diagnosis *after* it is produced, and that part has to be
testable without a model in the loop.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any

from ..incident import Incident


@dataclass
class ProposedAction:
    type: str
    canonical_field: str | None = None
    new_source_field: str | None = None
    rationale: str = ""

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "ProposedAction":
        return ProposedAction(
            type=str(raw.get("type", "escalate")),
            canonical_field=raw.get("canonical_field"),
            new_source_field=raw.get("new_source_field"),
            rationale=str(raw.get("rationale", "")),
        )


@dataclass
class ContentCheck:
    """The model's assessment of values, not an independent semantic guarantee."""

    canonical_field: str = ""
    new_source_field: str = ""
    verdict: str = "insufficient_evidence"
    reference_summary: str = ""
    candidate_summary: str = ""
    reason: str = ""

    @staticmethod
    def from_dict(raw: Any) -> "ContentCheck | None":
        if not isinstance(raw, dict):
            return None
        return ContentCheck(**{
            name: value if isinstance(value := raw.get(name), str) else ""
            for name in ("canonical_field", "new_source_field", "verdict",
                         "reference_summary", "candidate_summary", "reason")
        })


@dataclass
class Diagnosis:
    cause_class: str
    ownership: str  # internal | customer | provider | unknown
    confidence: float
    summary: str
    action: ProposedAction
    raw: dict[str, Any] = field(default_factory=dict)
    content_check: ContentCheck | None = None

    @staticmethod
    def from_dict(raw: dict[str, Any]) -> "Diagnosis":
        return Diagnosis(
            cause_class=str(raw.get("cause_class", "unknown")),
            ownership=str(raw.get("ownership", "unknown")),
            confidence=float(raw.get("confidence", 0.0)),
            summary=str(raw.get("summary", "")),
            action=ProposedAction.from_dict(raw.get("proposed_action") or {}),
            raw=raw,
            content_check=ContentCheck.from_dict(raw.get("content_check")),
        )


class LLMBackend(abc.ABC):
    """Produces a diagnosis from an incident. Never acts on it."""

    name: str = "abstract"

    @abc.abstractmethod
    def diagnose(self, incident: Incident) -> Diagnosis:  # pragma: no cover - interface
        ...
