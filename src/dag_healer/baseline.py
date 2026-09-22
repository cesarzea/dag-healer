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
import statistics
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any


@dataclass
class FieldProfile:
    count: int
    null_rate: float
    kind: str  # "numeric" | "categorical" | "other"
    mean: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    distinct: int | None = None

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


def profile(records: list[dict[str, Any]], fields: list[str]) -> dict[str, FieldProfile]:
    return {f: profile_field([r.get(f) for r in records]) for f in fields}


def save_baseline(path: str | Path, profiles: dict[str, FieldProfile]) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(
        json.dumps({k: v.as_dict() for k, v in profiles.items()}, indent=2) + "\n",
        encoding="utf-8",
    )
    return p


def load_baseline(path: str | Path) -> dict[str, FieldProfile]:
    p = Path(path)
    if not p.exists():
        return {}
    raw = json.loads(p.read_text(encoding="utf-8"))
    return {k: FieldProfile(**v) for k, v in raw.items()}


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
                f"'{field_name}' mean moved from {reference.mean:g} to {candidate.mean:g} "
                f"({relative:.0%} away, tolerance {mean_relative_delta:.0%})"
            )

    return problems
