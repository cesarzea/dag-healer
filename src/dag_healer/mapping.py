"""The mapping layer: the one place upstream change is absorbed.

Canonical field names come from the contract and never move. What the upstream
API calls them is configuration, which is why a renamed source field is a
config change rather than a code change, and therefore something that can be
repaired and verified mechanically.
"""

from __future__ import annotations

import datetime as _dt
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)

_DEFAULT_HEADER = (
    "# Upstream-to-canonical field mapping.\n"
    "# Managed jointly by engineers and the reliability layer; every\n"
    "# automated change is recorded under `history` as pending review.\n"
)


def _leading_comment(text: str) -> str | None:
    """The comment block at the top of a mapping file, if it has one.

    Carried across automated edits, because the header is where the layer's
    limits are written down for whoever opens the file next. A layer whose
    whole argument is restraint should not overwrite the paragraph describing
    what it is not allowed to do.
    """
    kept: list[str] = []
    for line in text.splitlines():
        if not line.startswith("#") and line.strip():
            break
        kept.append(line)
    while kept and not kept[-1].strip():
        kept.pop()
    return "\n".join(kept) + "\n" if kept else None


@dataclass
class Mapping:
    entity: str
    version: int
    source: str
    endpoint: str
    records_path: str
    fields: dict[str, str]
    history: list[dict[str, Any]] = field(default_factory=list)
    path: Path | None = None
    header: str | None = None

    def source_for(self, canonical: str) -> str | None:
        return self.fields.get(canonical)

    def apply(self, raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Project upstream records onto the canonical field names.

        A source field that is absent upstream yields None rather than being
        dropped, so the contract check sees a null it can report instead of a
        silently missing column.
        """
        out: list[dict[str, Any]] = []
        for raw in raw_records:
            out.append({canon: raw.get(src) for canon, src in self.fields.items()})
        return out

    def missing_sources(self, raw_records: list[dict[str, Any]]) -> dict[str, str]:
        """Canonical fields whose source field is absent from every upstream record.

        Worth separating from "the value happened to be null", because a field
        that vanished from the payload is the signature of a schema change,
        while a null is ordinary data. Conflating them turns a diagnosable
        failure into a vague one.
        """
        if not raw_records:
            return {}
        present: set[str] = set().union(*(r.keys() for r in raw_records))
        return {
            canon: src for canon, src in self.fields.items() if src not in present
        }

    def with_remap(self, canonical: str, new_source: str, reason: str) -> "Mapping":
        """Return a copy with one canonical field pointed at a new source field.

        Deliberately the only mutation available. It cannot add or remove a
        canonical field, because those are contract changes and the layer is
        not allowed to make contract changes.
        """
        if canonical not in self.fields:
            raise KeyError(
                f"'{canonical}' is not a canonical field of '{self.entity}'; "
                "the reliability layer may only repoint existing fields"
            )
        old_source = self.fields[canonical]
        new_fields = dict(self.fields)
        new_fields[canonical] = new_source
        entry = {
            "at": _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
            "field": canonical,
            "from": old_source,
            "to": new_source,
            "reason": reason,
            "applied_by": "dag-healer",
            "review": "pending",
        }
        return Mapping(
            entity=self.entity,
            version=self.version + 1,
            source=self.source,
            endpoint=self.endpoint,
            records_path=self.records_path,
            fields=new_fields,
            history=[*self.history, entry],
            path=self.path,
            header=self.header,
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "entity": self.entity,
            "version": self.version,
            "source": self.source,
            "endpoint": self.endpoint,
            "records_path": self.records_path,
            "fields": self.fields,
            "history": self.history,
        }

    def save(self, path: str | Path | None = None) -> Path:
        target = Path(path or self.path or "")
        if not str(target):
            raise ValueError("no path to save mapping to")
        log.debug(
            "writing %s at v%d, keeping the header a human wrote",
            target.name,
            self.version,
        )
        header = self.header or _DEFAULT_HEADER
        target.write_text(
            header + yaml.safe_dump(self.as_dict(), sort_keys=False, allow_unicode=True),
            encoding="utf-8",
        )
        return target


def load_mapping(path: str | Path) -> Mapping:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    raw = yaml.safe_load(text)
    return Mapping(
        entity=raw["entity"],
        version=int(raw["version"]),
        source=raw["source"],
        endpoint=raw["endpoint"],
        records_path=raw["records_path"],
        fields=dict(raw["fields"]),
        history=list(raw.get("history") or []),
        path=p,
        header=_leading_comment(text),
    )
