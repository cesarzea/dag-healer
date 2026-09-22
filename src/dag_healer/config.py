"""Where everything lives. One object, so tests can point it at a tmp dir."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class Policy:
    levels: dict[str, str] = field(default_factory=dict)
    allowed_actions: list[str] = field(default_factory=list)
    require_contract_pass: bool = True
    require_baseline_match: bool = True
    null_rate_delta: float = 0.05
    mean_relative_delta: float = 0.25
    min_confidence: float = 0.6

    def level_for(self, cause_class: str) -> str:
        return self.levels.get(cause_class, "escalate")


def load_policy(path: str | Path) -> Policy:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    verification = raw.get("verification") or {}
    tolerance = verification.get("baseline_tolerance") or {}
    return Policy(
        levels=dict(raw.get("levels") or {}),
        allowed_actions=list(raw.get("allowed_actions") or []),
        require_contract_pass=bool(verification.get("require_contract_pass", True)),
        require_baseline_match=bool(verification.get("require_baseline_match", True)),
        null_rate_delta=float(tolerance.get("null_rate_delta", 0.05)),
        mean_relative_delta=float(tolerance.get("mean_relative_delta", 0.25)),
        min_confidence=float(verification.get("min_confidence", 0.6)),
    )


@dataclass
class Settings:
    root: Path = ROOT
    entity: str = "orders"
    base_url: str = field(default_factory=lambda: os.environ.get("SHOP_API_URL", "http://127.0.0.1:8099"))

    @property
    def contract_path(self) -> Path:
        return self.root / "contracts" / f"{self.entity}.contract.yml"

    @property
    def mapping_path(self) -> Path:
        return self.root / "mappings" / f"{self.entity}.mapping.yml"

    @property
    def baseline_path(self) -> Path:
        return self.root / "baselines" / f"{self.entity}.baseline.json"

    @property
    def policy_path(self) -> Path:
        return self.root / "policy.yml"

    @property
    def incidents_dir(self) -> Path:
        return self.root / "incidents"

    @property
    def warehouse_path(self) -> Path:
        return self.root / "data" / "warehouse.sqlite"

    def as_dict(self) -> dict[str, Any]:
        return {"root": str(self.root), "entity": self.entity, "base_url": self.base_url}
