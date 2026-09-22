"""A scripted backend, so the safety machinery can be tested exhaustively.

Every test in this repo that matters is about what the healer refuses to do.
Those tests need a diagnosis that is wrong in a specific, chosen way, which a
real model will not reliably produce on demand.
"""

from __future__ import annotations

from typing import Any

from ..incident import Incident
from .base import Diagnosis, LLMBackend


class MockBackend(LLMBackend):
    name = "mock"

    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self._response = response

    def diagnose(self, incident: Incident) -> Diagnosis:
        if self._response is not None:
            return Diagnosis.from_dict(self._response)

        # Default behaviour: the naive-but-correct answer for a renamed field.
        missing = [
            v["field"] for v in incident.violations if v["kind"] == "missing_source_field" and v["field"]
        ]
        if not missing:
            return Diagnosis.from_dict(
                {"cause_class": "unknown", "ownership": "unknown", "confidence": 0.0,
                 "summary": "no missing field in evidence", "proposed_action": {"type": "escalate"}}
            )

        canonical = missing[0]
        known = set(incident.mapping_fields.values())
        candidates = [f for f in incident.upstream_fields_present if f not in known]
        return Diagnosis.from_dict(
            {
                "cause_class": "schema_drift_renamed_field",
                "ownership": "customer",
                "confidence": 0.8,
                "summary": f"'{canonical}' lost its source field upstream",
                "proposed_action": {
                    "type": "remap_field",
                    "canonical_field": canonical,
                    "new_source_field": candidates[0] if candidates else None,
                    "rationale": "first unmapped upstream field",
                },
            }
        )
