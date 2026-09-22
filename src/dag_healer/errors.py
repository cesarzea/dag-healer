"""Exception types that carry enough structure to diagnose a failure."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Violation:
    """A single way in which a batch of records failed its contract."""

    kind: str
    field: str | None
    detail: str
    sample: Any = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "field": self.field,
            "detail": self.detail,
            "sample": self.sample,
        }


class PipelineError(Exception):
    """Base class for pipeline failures we know how to describe."""


@dataclass
class ContractViolation(PipelineError):
    """Raised when extracted records do not satisfy the data contract.

    Carries the structured violations rather than only a message, because the
    diagnosis step needs evidence, not prose.
    """

    entity: str
    violations: list[Violation] = field(default_factory=list)

    def __post_init__(self) -> None:
        super().__init__(str(self))

    def __str__(self) -> str:
        lines = [f"{len(self.violations)} contract violation(s) on '{self.entity}':"]
        for v in self.violations:
            where = f" [{v.field}]" if v.field else ""
            lines.append(f"  - {v.kind}{where}: {v.detail}")
        return "\n".join(lines)

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity,
            "violations": [v.as_dict() for v in self.violations],
        }


class SourceUnavailable(PipelineError):
    """The upstream API could not be reached or returned a server error."""


class RateLimited(PipelineError):
    """The upstream API asked us to slow down."""
