"""Loading and enforcing data contracts.

A contract describes the canonical shape downstream consumers depend on. It is
checked on every run, and it is *never* modified by the reliability layer: a
system that is allowed to relax its own contract in order to make a failure go
away has no contract at all.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from .errors import ContractViolation, Violation

_MAX_SAMPLES = 3


@dataclass
class FieldSpec:
    name: str
    type: str
    required: bool = True
    unique: bool = False
    pattern: str | None = None
    min: float | None = None
    max: float | None = None
    allowed: list[str] | None = None
    description: str | None = None
    sample_for_diagnosis: bool = False


@dataclass
class Contract:
    entity: str
    version: int
    primary_key: str
    fields: list[FieldSpec]
    min_rows: int = 0

    @property
    def field_names(self) -> list[str]:
        return [f.name for f in self.fields]

    def field(self, name: str) -> FieldSpec | None:
        for f in self.fields:
            if f.name == name:
                return f
        return None


def load_contract(path: str | Path) -> Contract:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    fields = [FieldSpec(**spec) for spec in raw["fields"]]
    expectations = raw.get("row_expectations") or {}
    return Contract(
        entity=raw["entity"],
        version=int(raw["version"]),
        primary_key=raw["primary_key"],
        fields=fields,
        min_rows=int(expectations.get("min_rows", 0)),
    )


def _type_ok(value: Any, declared: str) -> bool:
    if declared == "string":
        return isinstance(value, str)
    if declared == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if declared == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if declared == "timestamp":
        if isinstance(value, datetime):
            return True
        if not isinstance(value, str):
            return False
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        return True
    raise ValueError(f"unsupported contract type: {declared}")


def validate(records: list[dict[str, Any]], contract: Contract) -> list[Violation]:
    """Return every way `records` breaks `contract`. Empty list means clean."""
    violations: list[Violation] = []

    if len(records) < contract.min_rows:
        violations.append(
            Violation(
                kind="too_few_rows",
                field=None,
                detail=f"expected at least {contract.min_rows} row(s), got {len(records)}",
            )
        )

    present = set().union(*(r.keys() for r in records)) if records else set()

    for spec in contract.fields:
        if spec.required and spec.name not in present and records:
            violations.append(
                Violation(
                    kind="missing_field",
                    field=spec.name,
                    detail=(
                        f"required canonical field '{spec.name}' is absent from every "
                        f"record; fields actually present: {sorted(present)}"
                    ),
                    sample=sorted(present),
                )
            )
            continue

        nulls, wrong_type, out_of_range, bad_pattern, not_allowed = [], [], [], [], []
        seen: set[Any] = set()
        dupes: list[Any] = []

        for record in records:
            value = record.get(spec.name)

            if value is None:
                if spec.required:
                    nulls.append(record.get(contract.primary_key))
                continue

            if not _type_ok(value, spec.type):
                wrong_type.append(value)
                continue

            if spec.pattern and not re.match(spec.pattern, str(value)):
                bad_pattern.append(value)
            if spec.allowed is not None and value not in spec.allowed:
                not_allowed.append(value)
            if spec.min is not None and isinstance(value, (int, float)) and value < spec.min:
                out_of_range.append(value)
            if spec.max is not None and isinstance(value, (int, float)) and value > spec.max:
                out_of_range.append(value)
            if spec.unique:
                if value in seen:
                    dupes.append(value)
                seen.add(value)

        for kind, offenders, detail in (
            ("null_in_required_field", nulls, "null in a required field"),
            ("wrong_type", wrong_type, f"value is not of declared type '{spec.type}'"),
            ("out_of_range", out_of_range, "value outside the declared min/max"),
            ("pattern_mismatch", bad_pattern, f"value does not match {spec.pattern!r}"),
            ("value_not_allowed", not_allowed, f"value not in {spec.allowed!r}"),
            ("duplicate_key", dupes, "duplicate value in a unique field"),
        ):
            if offenders:
                violations.append(
                    Violation(
                        kind=kind,
                        field=spec.name,
                        detail=f"{len(offenders)} record(s): {detail}",
                        sample=offenders[:_MAX_SAMPLES],
                    )
                )

    return violations


def enforce(records: list[dict[str, Any]], contract: Contract) -> None:
    """Raise ContractViolation if `records` do not satisfy `contract`."""
    violations = validate(records, contract)
    if violations:
        raise ContractViolation(entity=contract.entity, violations=violations)
