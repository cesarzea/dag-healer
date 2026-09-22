"""The pipeline itself: extract, map, validate, load.

Short on purpose. The point of this repo is not the pipeline, it is what
happens when the pipeline fails.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import baseline as baseline_mod
from . import incident as incident_mod
from .config import Settings
from .contracts import load_contract, validate
from .errors import ContractViolation, Violation
from .extract import fetch_raw
from .mapping import Mapping, load_mapping


@dataclass
class RunResult:
    ok: bool
    rows: int
    mapping_version: int
    violations: list[dict[str, Any]] = field(default_factory=list)
    incident_path: Path | None = None
    profiles: dict[str, baseline_mod.FieldProfile] = field(default_factory=dict)


@dataclass
class DryRunResult:
    """An unvalidated candidate run, used to test a proposed repair."""

    records: list[dict[str, Any]]
    violations: list[Any]
    profiles: dict[str, baseline_mod.FieldProfile]


def _missing_source_violations(mapping: Mapping, raw: list[dict[str, Any]]) -> list[Violation]:
    present = sorted(set().union(*(r.keys() for r in raw))) if raw else []
    return [
        Violation(
            kind="missing_source_field",
            field=canonical,
            detail=(
                f"upstream no longer returns '{source}', which is the source for "
                f"canonical field '{canonical}'"
            ),
            sample=present,
        )
        for canonical, source in mapping.missing_sources(raw).items()
    ]


def dry_run(mapping: Mapping, settings: Settings) -> DryRunResult:
    """Fetch and map with a candidate mapping, without loading anything anywhere."""
    contract = load_contract(settings.contract_path)
    raw = fetch_raw(mapping, settings.base_url)
    mapped = mapping.apply(raw)
    return DryRunResult(
        records=mapped,
        violations=_missing_source_violations(mapping, raw) + validate(mapped, contract),
        profiles=baseline_mod.profile(mapped, contract.field_names),
    )


def load_to_warehouse(
    records: list[dict[str, Any]],
    contract_fields: list[str],
    settings: Settings,
) -> int:
    """Load the canonical records. Full refresh, because the demo is not the point."""
    path = settings.warehouse_path
    path.parent.mkdir(parents=True, exist_ok=True)

    quoted = [f'"{c}"' for c in contract_fields]
    column_defs = ", ".join(f"{q} TEXT" for q in quoted)
    columns = ", ".join(quoted)
    placeholders = ", ".join("?" for _ in contract_fields)
    table = settings.entity

    rows = [
        tuple(None if r.get(c) is None else str(r.get(c)) for c in contract_fields)
        for r in records
    ]

    with sqlite3.connect(path) as conn:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({column_defs})")
        conn.execute(f"DELETE FROM {table}")
        conn.executemany(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})", rows
        )

    return len(rows)


def run_ingest(
    settings: Settings,
    *,
    dag_id: str = "orders_ingest",
    task_id: str = "validate_and_load",
) -> RunResult:
    """Run the pipeline. On a contract failure, write an incident and raise."""
    contract = load_contract(settings.contract_path)
    mapping = load_mapping(settings.mapping_path)

    raw = fetch_raw(mapping, settings.base_url)
    mapped = mapping.apply(raw)
    violations = _missing_source_violations(mapping, raw) + validate(mapped, contract)

    if violations:
        error = ContractViolation(entity=contract.entity, violations=violations)
        inc = incident_mod.build(
            error=error,
            contract=contract,
            mapping=mapping,
            raw_records=raw,
            baseline={k: v.as_dict() for k, v in baseline_mod.load_baseline(settings.baseline_path).items()},
            dag_id=dag_id,
            task_id=task_id,
        )
        path = inc.save(settings.incidents_dir)
        raise ContractViolationWithIncident(error, path) from error

    rows = load_to_warehouse(mapped, contract.field_names, settings)
    profiles = baseline_mod.profile(mapped, contract.field_names)
    baseline_mod.save_baseline(settings.baseline_path, profiles)

    return RunResult(ok=True, rows=rows, mapping_version=mapping.version, profiles=profiles)


class ContractViolationWithIncident(ContractViolation):
    """A contract violation that knows where its evidence was filed."""

    def __init__(self, original: ContractViolation, incident_path: Path) -> None:
        # Set before delegating: the parent dataclass renders str(self) in its
        # __post_init__, and that rendering reads incident_path.
        self.incident_path = incident_path
        super().__init__(entity=original.entity, violations=original.violations)

    def __str__(self) -> str:
        return f"{super().__str__()}\n  incident: {self.incident_path}"
