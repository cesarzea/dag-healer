"""The incident queue.

Incidents are files in a directory, and the reliability layer drains them. That
is deliberately dull, and it is what keeps the layer decoupled from the
pipelines it serves: a pipeline's only obligation is to file its evidence and
fail honestly. It does not know the layer exists, and the layer does not need a
hook inside anybody's DAG.

It also means one layer can serve many pipelines without any of them changing.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from .healer import Resolution


log = logging.getLogger(__name__)

RESOLUTION_SUFFIX = ".resolution.json"


def resolution_path(incidents_dir: str | Path, incident_id: str) -> Path:
    return Path(incidents_dir) / f"{incident_id}{RESOLUTION_SUFFIX}"


def incidents(incidents_dir: str | Path) -> list[Path]:
    """Every incident on file, oldest first.

    The explicit filter matters: resolutions live beside the incidents they
    resolve and `inc_*.json` happily matches `inc_123.resolution.json` too,
    which silently doubled the queue until a test caught it.
    """
    d = Path(incidents_dir)
    if not d.exists():
        return []
    return [p for p in sorted(d.glob("inc_*.json")) if not p.name.endswith(RESOLUTION_SUFFIX)]


def pending(incidents_dir: str | Path) -> list[Path]:
    """Incidents with no resolution recorded yet, oldest first."""
    d = Path(incidents_dir)
    unresolved = [p for p in incidents(d) if not resolution_path(d, p.stem).exists()]
    log.debug("queue: %d incident(s) on file, %d unresolved", len(incidents(d)), len(unresolved))
    return unresolved


def record(incidents_dir: str | Path, resolution: "Resolution") -> Path:
    payload: dict[str, Any] = {
        "incident_id": resolution.incident_id,
        "outcome": resolution.outcome,
        "reasons": resolution.reasons,
        "mapping_version": resolution.mapping_version,
        "review_required": resolution.review_required,
        "changed_anything": resolution.changed,
        "report": str(resolution.report_path) if resolution.report_path else None,
        "diagnosis": asdict(resolution.diagnosis) if resolution.diagnosis else None,
        # Kept so the record says what was verified, not just what was concluded.
        "checks": [asdict(c) for c in resolution.checks],
    }
    log.debug("recording the outcome and every gate result beside the incident")
    target = resolution_path(incidents_dir, resolution.incident_id)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    log.debug("WRITE %s: outcome=%s, checks=%d", target, resolution.outcome, len(resolution.checks))
    return target
