# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""Profiles of the last known-good run, used to catch plausible-but-wrong repairs.

Passing the contract is necessary but not sufficient. If the upstream renames
`total_price` to `order_total` and also happens to expose `shipping_price`, a
repair that points `total_amount` at `shipping_price` satisfies every type and
range rule in the contract and quietly destroys the numbers downstream. Nothing
breaks; the data just becomes wrong, which is worse than a failed DAG.

So a repair must also look like the column it replaces.
"""

from __future__ import annotations

import json
import logging
import statistics
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


@dataclass
class FieldProfile:
    count: int
    null_rate: float
    kind: str  # "numeric" | "categorical" | "other"
    mean: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    distinct: int | None = None
    examples: list[Any] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def profile_field(values: list[Any]) -> FieldProfile:
    count = len(values)
    non_null = [v for v in values if v is not None]
    null_rate = 0.0 if count == 0 else 1 - (len(non_null) / count)
    numeric = [v for v in non_null if isinstance(v, (int, float)) and not isinstance(v, bool)]

    if numeric and len(numeric) == len(non_null):
        return FieldProfile(
            count=count,
            null_rate=round(null_rate, 4),
            kind="numeric",
            mean=round(statistics.fmean(numeric), 6),
            minimum=min(numeric),
            maximum=max(numeric),
            distinct=len(set(numeric)),
        )
    if non_null and all(isinstance(v, str) for v in non_null):
        return FieldProfile(
            count=count,
            null_rate=round(null_rate, 4),
            kind="categorical",
            distinct=len(set(non_null)),
        )
    return FieldProfile(count=count, null_rate=round(null_rate, 4), kind="other")


def sample_values(values: list[Any]) -> list[Any]:
    """Keep at most three distinct scalar examples, with bounded text length."""
    samples: list[Any] = []
    for value in values:
        if not isinstance(value, (str, int, float, bool)):
            continue
        if isinstance(value, str):
            if not value.strip() or value == "<redacted>":
                continue
            if len(value) > 240:
                value = value[:226] + "...[truncated]"
        if value not in samples:
            samples.append(value)
        if len(samples) == 3:
            break
    return samples


def profile(
    records: list[dict[str, Any]],
    fields: list[str],
    *,
    sample_fields: set[str] | None = None,
) -> dict[str, FieldProfile]:
    profiles = {f: profile_field([r.get(f) for r in records]) for f in fields}
    for name in sample_fields or ():
        if name in profiles:
            profiles[name].examples = sample_values([r.get(name) for r in records])
    return profiles


def save_baseline(path: str | Path, profiles: dict[str, FieldProfile]) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps({k: v.as_dict() for k, v in profiles.items()}, indent=2) + "\n",
        encoding="utf-8",
    )
    log.debug("WRITE %s: saved profiles for %s", p, ", ".join(profiles))
    return p


def load_baseline(path: str | Path) -> dict[str, FieldProfile]:
    p = Path(path)
    if not p.exists():
        return {}
    raw = json.loads(p.read_text(encoding="utf-8"))
    return {k: FieldProfile(**v) for k, v in raw.items()}


def describe(field_name: str, candidate: FieldProfile, reference: FieldProfile) -> str:
    """How the candidate column compares with the reference, whatever the verdict.

    `compare` only speaks when something is wrong, which makes a repair that
    passes verification look like nothing was checked. The measurements are the
    evidence, so they are worth stating either way.
    """
    if candidate.kind != reference.kind:
        return f"'{field_name}' kind {reference.kind} -> {candidate.kind}"

    parts = []
    if candidate.kind == "numeric" and None not in (candidate.mean, reference.mean):
        assert candidate.mean is not None and reference.mean is not None
        drift = (
            abs(candidate.mean - reference.mean) / abs(reference.mean)
            if reference.mean
            else 0.0
        )
        parts.append(
            f"average {reference.mean:,.2f} -> {candidate.mean:,.2f} ({drift:.0%} away)"
        )
    parts.append(f"null rate {reference.null_rate:.2%} -> {candidate.null_rate:.2%}")
    return f"'{field_name}' {', '.join(parts)}"


def compare(
    field_name: str,
    candidate: FieldProfile,
    reference: FieldProfile,
    *,
    null_rate_delta: float = 0.05,
    mean_relative_delta: float = 0.25,
) -> list[str]:
    """Return human-readable reasons the candidate does not resemble the reference."""
    problems: list[str] = []

    if candidate.kind != reference.kind:
        problems.append(
            f"'{field_name}' changed shape: was {reference.kind}, candidate is {candidate.kind}"
        )
        return problems

    if abs(candidate.null_rate - reference.null_rate) > null_rate_delta:
        problems.append(
            f"'{field_name}' null rate moved from {reference.null_rate:.2%} to "
            f"{candidate.null_rate:.2%} (tolerance {null_rate_delta:.2%})"
        )

    if candidate.kind == "numeric" and reference.mean not in (None, 0):
        assert candidate.mean is not None and reference.mean is not None
        relative = abs(candidate.mean - reference.mean) / abs(reference.mean)
        if relative > mean_relative_delta:
            problems.append(
                f"'{field_name}' average moved from {reference.mean:,.2f} to "
                f"{candidate.mean:,.2f} ({relative:.0%} away, tolerance "
                f"{mean_relative_delta:.0%})"
            )

    return problems
