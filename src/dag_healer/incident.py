"""Structured incidents.

A failure is only automatable if it arrives with its evidence attached. A
stack trace in a log file is not evidence; it is a pointer to evidence that
somebody has to go and collect at two in the morning. So the pipeline builds
the incident at the moment it fails, while the context is still in memory.
"""

from __future__ import annotations

import logging

import datetime as _dt
import json
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .contracts import Contract
from .errors import ContractViolation
from .mapping import Mapping

_SAMPLE_ROWS = 3
# Replace values of these top-level sample fields before storing or sending
# them for diagnosis. This is a name-based filter, not recursive PII detection.
log = logging.getLogger(__name__)

_REDACT = {"email", "phone", "address", "customer_email", "billing_address", "token"}


def is_sensitive_field(name: str) -> bool:
    return name.lower() in _REDACT


def _redact(record: dict[str, Any]) -> dict[str, Any]:
    return {
        k: ("<redacted>" if is_sensitive_field(k) else v)
        for k, v in record.items()
    }


@dataclass
class Incident:
    incident_id: str
    created_at: str
    entity: str
    dag_id: str
    task_id: str
    error_type: str
    error_message: str
    contract_fields: list[dict[str, Any]]
    mapping_fields: dict[str, str]
    mapping_version: int
    violations: list[dict[str, Any]] = field(default_factory=list)
    upstream_fields_present: list[str] = field(default_factory=list)
    upstream_samples: list[dict[str, Any]] = field(default_factory=list)
    baseline: dict[str, Any] = field(default_factory=dict)
    recent_mapping_history: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "created_at": self.created_at,
            "entity": self.entity,
            "dag_id": self.dag_id,
            "task_id": self.task_id,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "contract_fields": self.contract_fields,
            "mapping_fields": self.mapping_fields,
            "mapping_version": self.mapping_version,
            "violations": self.violations,
            "upstream_fields_present": self.upstream_fields_present,
            "upstream_samples": self.upstream_samples,
            "baseline": self.baseline,
            "recent_mapping_history": self.recent_mapping_history,
        }

    def save(self, directory: str | Path) -> Path:
        d = Path(directory)
        d.mkdir(parents=True, exist_ok=True)
        p = d / f"{self.incident_id}.json"
        p.write_text(json.dumps(self.as_dict(), indent=2, default=str) + "\n", encoding="utf-8")
        log.debug("WRITE %s: incident=%s, violations=%d, samples=%d", p, self.incident_id, len(self.violations), len(self.upstream_samples))
        return p

    @staticmethod
    def load(path: str | Path) -> "Incident":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return Incident(**raw)


def build(
    *,
    error: Exception,
    contract: Contract,
    mapping: Mapping,
    raw_records: list[dict[str, Any]],
    baseline: dict[str, Any],
    dag_id: str,
    task_id: str,
) -> Incident:
    present: list[str] = sorted(set().union(*(r.keys() for r in raw_records)) if raw_records else set())
    violations = error.as_dict()["violations"] if isinstance(error, ContractViolation) else []

    return Incident(
        incident_id=f"inc_{_dt.datetime.now(_dt.timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}",
        created_at=_dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        entity=contract.entity,
        dag_id=dag_id,
        task_id=task_id,
        error_type=type(error).__name__,
        error_message=str(error),
        contract_fields=[
            {"name": f.name, "type": f.type, "required": f.required,
             "description": f.description, "sample_for_diagnosis": f.sample_for_diagnosis}
            for f in contract.fields
        ],
        mapping_fields=dict(mapping.fields),
        mapping_version=mapping.version,
        violations=violations,
        upstream_fields_present=present,
        upstream_samples=[_redact(r) for r in raw_records[:_SAMPLE_ROWS]],
        baseline=baseline,
        recent_mapping_history=mapping.history[-3:],
    )
