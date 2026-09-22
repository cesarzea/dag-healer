"""What the layer produces when it decides not to act.

An escalation is the normal outcome, not the failure mode. It is worth as much
as a repair if it arrives with the evidence already gathered and the ownership
already argued, because that is most of the work a human would have done.
"""

from __future__ import annotations

import datetime as _dt
from pathlib import Path

from .backends.base import Diagnosis
from .incident import Incident


def write_report(
    incident: Incident,
    diagnosis: Diagnosis | None,
    reasons: list[str],
    directory: str | Path,
) -> Path:
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)
    target = d / f"{incident.incident_id}.escalation.md"

    lines = [
        f"# Escalation: {incident.incident_id}",
        "",
        f"- **Entity**: `{incident.entity}`",
        f"- **DAG / task**: `{incident.dag_id}` / `{incident.task_id}`",
        f"- **Detected**: {incident.created_at}",
        f"- **Written**: {_dt.datetime.now(_dt.timezone.utc).isoformat(timespec='seconds')}",
        f"- **Mapping version**: {incident.mapping_version}",
        "",
        "## Why this was not repaired automatically",
        "",
    ]
    lines += [f"- {r}" for r in reasons] or ["- no reason recorded"]

    if diagnosis is not None:
        lines += [
            "",
            "## Diagnosis",
            "",
            f"- **Class**: `{diagnosis.cause_class}`",
            f"- **Ownership**: `{diagnosis.ownership}`",
            f"- **Confidence**: {diagnosis.confidence:.2f}",
            f"- **Proposed action**: `{diagnosis.action.type}`"
            + (
                f" ({diagnosis.action.canonical_field} -> {diagnosis.action.new_source_field})"
                if diagnosis.action.type == "remap_field"
                else ""
            ),
            "",
            f"> {diagnosis.summary}",
        ]

    lines += ["", "## Contract violations", ""]
    for v in incident.violations:
        where = f"`{v['field']}`" if v.get("field") else "-"
        lines.append(f"- **{v['kind']}** {where}: {v['detail']}")

    lines += [
        "",
        "## Upstream fields seen in this batch",
        "",
        "```",
        ", ".join(incident.upstream_fields_present) or "(none)",
        "```",
        "",
        "## Current mapping",
        "",
        "```yaml",
        *[f"{k}: {v}" for k, v in incident.mapping_fields.items()],
        "```",
        "",
    ]

    target.write_text("\n".join(lines), encoding="utf-8")
    return target
