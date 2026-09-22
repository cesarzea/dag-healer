"""A scripted backend for reproducible tests of the repair checks.

Negative tests need diagnoses that are wrong in specific ways, which a real
model will not reliably produce on demand. This backend also supplies the
expected renamed-field proposal for the default orders fixture. It performs
no semantic analysis and is not a replacement for live-model evaluation.
"""

from __future__ import annotations

import logging
from typing import Any

from ..baseline import sample_values
from ..incident import Incident
from .base import Diagnosis, LLMBackend

log = logging.getLogger(__name__)


class MockBackend(LLMBackend):
    name = "mock"

    def __init__(self, response: dict[str, Any] | None = None) -> None:
        self._response = response

    def diagnose(self, incident: Incident) -> Diagnosis:
        if self._response is not None:
            log.debug("using the diagnosis it was handed, with no model involved")
            return Diagnosis.from_dict(self._response)

        # Choose the expected field in the default fixture, without semantic review.
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
        source = candidates[0] if candidates else None
        reference = incident.baseline.get(canonical, {}).get("examples", [])
        current = sample_values([row.get(source) for row in incident.upstream_samples])
        return Diagnosis.from_dict(
            {
                "cause_class": "schema_drift_renamed_field",
                "ownership": "customer",
                "confidence": 0.8,
                "summary": f"'{canonical}' lost its source field upstream",
                "content_check": {
                    "canonical_field": canonical,
                    "new_source_field": source,
                    "verdict": "equivalent",
                    "reference_summary": f"Historical examples supplied to the scripted fixture: {reference}.",
                    "candidate_summary": f"Current examples supplied to the scripted fixture: {current}.",
                    "reason": "Scripted equivalence claim for exercising the healer's gates; the mock performs no semantic analysis.",
                },
                "proposed_action": {
                    "type": "remap_field",
                    "canonical_field": canonical,
                    "new_source_field": source,
                    "rationale": "first unmapped upstream field",
                },
            }
        )
