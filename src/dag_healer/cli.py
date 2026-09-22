"""Command line entry point, so the whole loop can be driven without Airflow."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .backends.claude_code import ClaudeCodeBackend
from .backends.mock import MockBackend
from .config import ROOT, Settings, load_policy
from .healer import heal
from .mapping import load_mapping
from .pipeline import ContractViolationWithIncident, run_ingest


def _settings(args: argparse.Namespace) -> Settings:
    return Settings(root=Path(args.root), entity=args.entity, base_url=args.base_url)


def _backend(name: str):
    if name == "claude-code":
        return ClaudeCodeBackend()
    if name == "mock":
        return MockBackend()
    raise SystemExit(f"unknown backend: {name}")


def cmd_ingest(args: argparse.Namespace) -> int:
    settings = _settings(args)
    try:
        result = run_ingest(settings)
    except ContractViolationWithIncident as exc:
        print(str(exc), file=sys.stderr)
        print(f"\nincident filed: {exc.incident_path}", file=sys.stderr)
        print("run:  python -m dag_healer.cli heal --latest", file=sys.stderr)
        return 1
    print(f"loaded {result.rows} rows using mapping v{result.mapping_version}")
    return 0


def cmd_heal(args: argparse.Namespace) -> int:
    settings = _settings(args)

    if args.latest:
        candidates = sorted(settings.incidents_dir.glob("inc_*.json"))
        if not candidates:
            print("no incidents on file", file=sys.stderr)
            return 1
        incident_path = candidates[-1]
    elif args.incident:
        incident_path = Path(args.incident)
    else:
        print("pass --incident PATH or --latest", file=sys.stderr)
        return 2

    resolution = heal(incident_path, settings, _backend(args.backend), load_policy(settings.policy_path))
    print(f"[{resolution.incident_id}] {resolution.summary()}")

    if resolution.outcome == "escalated":
        for reason in resolution.reasons:
            print(f"  - {reason}")
        if resolution.report_path:
            print(f"  report: {resolution.report_path}")
        return 1

    for reason in resolution.reasons:
        print(f"  - {reason}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    settings = _settings(args)
    mapping = load_mapping(settings.mapping_path)
    incidents = sorted(settings.incidents_dir.glob("inc_*.json"))
    pending = [h for h in mapping.history if h.get("review") == "pending"]

    print(json.dumps(
        {
            "entity": settings.entity,
            "mapping_version": mapping.version,
            "fields": mapping.fields,
            "incidents_on_file": len(incidents),
            "changes_pending_review": pending,
        },
        indent=2,
    ))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dag-healer", description=__doc__)
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--entity", default="orders")
    parser.add_argument("--base-url", default=Settings().base_url)

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("ingest", help="run the pipeline once").set_defaults(func=cmd_ingest)

    heal_parser = sub.add_parser("heal", help="diagnose and, if verifiable, repair an incident")
    heal_parser.add_argument("--incident")
    heal_parser.add_argument("--latest", action="store_true")
    heal_parser.add_argument("--backend", default="claude-code", choices=["claude-code", "mock"])
    heal_parser.set_defaults(func=cmd_heal)

    sub.add_parser("status", help="show mapping version and anything pending review").set_defaults(
        func=cmd_status
    )

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
