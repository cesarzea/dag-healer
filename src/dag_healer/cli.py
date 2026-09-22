# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""Command line entry point, so the whole loop can be driven without Airflow."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import textwrap
from pathlib import Path

from .backends.claude_code import ClaudeCodeBackend
from .backends.mock import MockBackend
from .config import ROOT, Settings, load_policy
from . import queue as queue_mod
from .healer import heal
from .mapping import load_mapping
from .pipeline import ContractViolationWithIncident, run_ingest


def _settings(args: argparse.Namespace) -> Settings:
    return Settings(root=Path(args.root), entity=args.entity, base_url=args.base_url)


class _Narration(logging.Formatter):
    """Labels the running commentary so it is not mistaken for a result.

    The printed lines say what happened. These say what is happening, which is
    what a reader needs in the gaps: the diagnosis call in particular can sit
    silent for a long time against a real model, and silence in the middle of
    an automated repair is not a reassuring thing to watch.
    """

    def format(self, record: logging.LogRecord) -> str:
        if record.levelno <= logging.DEBUG:
            return f"      DEBUG  {record.getMessage()}"
        return f"  {record.getMessage()}"


_DEBUG = False


def _rule(label: str) -> None:
    """Close a block of DEBUG commentary before the results start.

    Without it the two run together and a reader cannot tell the running
    account of what is happening from the report of what happened.
    """
    if not _DEBUG:
        return
    print()
    print(f"  {'─' * 4} {label} " + "─" * max(0, 66 - len(label)))


def _configure_output(*, debug: bool) -> None:
    global _DEBUG
    _DEBUG = debug
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(_Narration())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    # Only this project's modules become chatty. Turning the root logger down to
    # DEBUG instead would bury the narration under every library in the venv.
    logging.getLogger("dag_healer").setLevel(logging.DEBUG if debug else logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _backend(name: str, diagnosis: str | None = None):
    if diagnosis is not None:
        # Run the gates against a diagnosis you wrote yourself, with no model in
        # the loop. The point of the exercise is usually to watch a plausible
        # and wrong repair get refused, which no real backend will produce on cue.
        try:
            canned = json.loads(diagnosis)
        except json.JSONDecodeError:
            canned = json.loads(Path(diagnosis).read_text(encoding="utf-8"))
        return MockBackend(canned)
    if name == "claude-code":
        return ClaudeCodeBackend()
    if name == "mock":
        return MockBackend()
    raise SystemExit(f"unknown backend: {name}")


# Violation kinds are identifiers meant for code. Nobody reading a failure
# should have to translate `missing_source_field` in their head.
_IN_ENGLISH = {
    "missing_source_field": (
        "'{field}' has nowhere to come from: the upstream field it reads was not sent"
    ),
    "null_in_required_field": "'{field}' is required by the contract, and arrived empty",
    "out_of_range": "'{field}' arrived outside the range the contract allows",
    "bad_type": "'{field}' arrived as the wrong type",
    "pattern_mismatch": "'{field}' does not match the shape the contract requires",
    "too_few_rows": "fewer rows arrived than the contract allows",
}


def _in_english(violation) -> str:
    kind = getattr(violation, "kind", None) or violation.get("kind", "")
    field = getattr(violation, "field", None) or violation.get("field", "") or ""
    detail = getattr(violation, "detail", None) or violation.get("detail", "")
    sentence = _IN_ENGLISH.get(kind)
    if not sentence:
        return f"{kind}: {detail}"
    # The detail often opens with a count ("120 record(s): ..."), which is the
    # only part of it the sentence above does not already say.
    count = detail.split(" record")[0] if "record(s)" in detail else ""
    return sentence.format(field=field) + (f" in all {count} records" if count else "")


def _short(path, root) -> str:
    """A path as someone would type it, not as the filesystem spells it."""
    try:
        return str(Path(path).resolve().relative_to(Path(root).resolve()))
    except ValueError:
        return str(path)


def _wrapped(label: str, text: str, width: int = 62) -> str:
    """One labelled field, wrapped so every line ends in the same place.

    textwrap.fill does not know about the label sitting in front of the first
    line, so it lets that one run twelve words longer than the rest.
    """
    indent = " " * 22
    lines = textwrap.wrap(str(text), width=width) or [""]
    return "\n".join([f"        {label:<13} {lines[0]}"] + [indent + ln for ln in lines[1:]])


def _profile_line(name: str, p) -> str:
    if p.kind == "numeric":
        shape = f"average {p.mean:<10,.2f} from {p.minimum:<9,.2f} to {p.maximum:<,.2f}"
        kind = "number"
    else:
        n = p.distinct
        shape = "" if n is None else ("1 value throughout" if n == 1 else f"{n} different values")
        kind = "text"
    empty = "never empty" if p.null_rate == 0 else f"{p.null_rate:.1%} empty"
    return f"      {name:<14} {kind:<8} {shape:<44} {empty}"


def cmd_ingest(args: argparse.Namespace) -> int:
    settings = _settings(args)
    mapping = load_mapping(settings.mapping_path)

    # The DEBUG commentary narrates the work as it happens; these lines report
    # it afterwards, so the two do not describe the same step in two places and
    # in the wrong order.
    try:
        result = run_ingest(settings)
    except ContractViolationWithIncident as exc:
        # Flushed so the narration stays in order when stdout is redirected and
        # stderr is not, which is exactly what the demo does.
        _rule("what the pipeline did")
        sys.stdout.flush()
        print(
            f"  extract    from {settings.base_url}{mapping.endpoint} with mapping "
            f"v{mapping.version}",
            file=sys.stderr,
        )
        print("  validate   FAILED against the contract:", file=sys.stderr)
        for v in exc.violations:
            print(f"              - {_in_english(v)}", file=sys.stderr)
        print(
            "  load       nothing. A run that fails its contract loads none of it,",
            file=sys.stderr,
        )
        print(
            "            so the warehouse keeps the numbers it already had.",
            file=sys.stderr,
        )
        print(
            f"  incident   evidence saved to {_short(exc.incident_path, args.root)}",
            file=sys.stderr,
        )
        print(
            "            (gathered now, because none of it survives the run)",
            file=sys.stderr,
        )
        return 1

    _rule("what the pipeline did")
    print(f"  extract    {result.rows} records from {settings.base_url}{mapping.endpoint}")
    print(
        f"  map        onto {len(mapping.fields)} canonical fields of '{settings.entity}' "
        f"(mapping v{mapping.version})"
    )
    print("  validate   contract satisfied")
    print(f"  load       {result.rows} rows into {_short(settings.warehouse_path, args.root)}")
    print(f"  baseline   what a good run looks like, saved to {_short(settings.baseline_path, args.root)}")
    for name, p in result.profiles.items():
        print(_profile_line(name, p))
    return 0


def cmd_heal(args: argparse.Namespace) -> int:
    settings = _settings(args)

    if args.pending:
        targets = queue_mod.pending(settings.incidents_dir)
        if not targets:
            print("queue is empty")
            return 0
    elif args.latest:
        candidates = queue_mod.incidents(settings.incidents_dir)
        if not candidates:
            print("no incidents on file", file=sys.stderr)
            return 1
        targets = [candidates[-1]]
    elif args.incident:
        targets = [Path(args.incident)]
    else:
        print("pass --incident PATH, --latest or --pending", file=sys.stderr)
        return 2

    backend = _backend(args.backend, args.diagnosis)
    policy = load_policy(settings.policy_path)
    escalated = 0

    for incident_path in targets:
        print(f"  incident   {_short(incident_path, args.root)}")
        resolution = heal(incident_path, settings, backend, policy)

        _rule("what the reliability layer decided")
        d = resolution.diagnosis
        if d is not None:
            proposes = d.action.type
            if d.action.type == "remap_field":
                proposes += f":  {d.action.canonical_field}  <-  {d.action.new_source_field}"
            print(f"  diagnosis  from '{backend.name}'. Nothing below is trusted yet:")
            print()
            # Laid out as fields and wrapped, rather than dumped as JSON. A real
            # model answers in paragraphs, and a paragraph on one 600-character
            # line is not something a person can read. The exact reply is kept
            # verbatim in the resolution file beside the incident.
            print(_wrapped("class", f"{d.cause_class}   (confidence {d.confidence:.2f})"))
            print(_wrapped("blames", d.ownership))
            print(_wrapped("proposes", proposes))
            print(_wrapped("because", d.summary))
            if d.action.rationale:
                print(_wrapped("in detail", d.action.rationale))
            if d.content_check:
                content = d.content_check
                print(_wrapped("content", f"{content.verdict} (diagnosis-source assessment, not independent proof)"))
                print(_wrapped("previous", content.reference_summary))
                print(_wrapped("current", content.candidate_summary))
                print(_wrapped("evidence", content.reason))
            print()

        # Self-explanatory on purpose: this block also shows up in an Airflow
        # task log and in a bare CLI run, where there is no narration above it.
        print(
            "  checks     five gates in the healer, shared by every diagnosis source."
        )
        print(
            "             Python evaluates them without additional model calls. The"
        )
        print(
            "             content gate checks the supplied assessment, not its truth."
        )
        print("             Any STOP refuses automatic repair; PASS is not proof of meaning.")
        print()
        for check in resolution.checks:
            print(f"      {check}")

        print(f"  outcome    {resolution.summary()}")
        if resolution.outcome == "escalated":
            escalated += 1
            if resolution.report_path:
                print(f"             a human gets {_short(resolution.report_path, args.root)}")

    return 1 if escalated else 0


def cmd_status(args: argparse.Namespace) -> int:
    settings = _settings(args)
    mapping = load_mapping(settings.mapping_path)
    incidents = queue_mod.incidents(settings.incidents_dir)
    pending_incidents = queue_mod.pending(settings.incidents_dir)
    awaiting_review = [h for h in mapping.history if h.get("review") == "pending"]

    if args.json:
        print(json.dumps(
            {
                "entity": settings.entity,
                "mapping_version": mapping.version,
                "fields": mapping.fields,
                "incidents_on_file": len(incidents),
                "incidents_in_queue": len(pending_incidents),
                "changes_pending_review": awaiting_review,
            },
            indent=2,
        ))
        return 0

    _rule("current state")
    print(f"  entity     {settings.entity}, mapping v{mapping.version}")
    print(f"  queue      {len(incidents)} incident(s) on file, {len(pending_incidents)} still unresolved")

    if not awaiting_review:
        print("  review     nothing waiting for a human")
        return 0

    # This list is the whole point of `auto_then_review`: the layer acted, and
    # it is telling someone exactly what it did so they can disagree.
    print(f"  review     {len(awaiting_review)} automated change(s) waiting to be confirmed:")
    for h in awaiting_review:
        print(f"      {h['field']}: {h['from']} -> {h['to']}")
        print(f"        applied {h['at']} by {h['applied_by']}")
        print(f"        because {h['reason']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dag-healer", description=__doc__)
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--entity", default="orders")
    parser.add_argument("--base-url", default=Settings().base_url)
    parser.add_argument(
        "--no-debug",
        action="store_true",
        help="drop the DEBUG commentary and print results only",
    )

    # Accepted on either side of the subcommand, because `ingest --no-debug` is
    # where a hand reaches for it. SUPPRESS keeps the subparser's absence from
    # overwriting a value given before the subcommand.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--no-debug",
        action="store_true",
        default=argparse.SUPPRESS,
        help="drop the DEBUG commentary and print results only",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser(
        "ingest", help="run the pipeline once", parents=[common]
    ).set_defaults(func=cmd_ingest)

    heal_parser = sub.add_parser(
        "heal",
        help="diagnose and, if verifiable, repair an incident",
        parents=[common],
    )
    heal_parser.add_argument("--incident")
    heal_parser.add_argument("--latest", action="store_true")
    heal_parser.add_argument(
        "--pending", action="store_true", help="drain every unresolved incident"
    )
    heal_parser.add_argument("--backend", default="claude-code", choices=["claude-code", "mock"], help=argparse.SUPPRESS)
    heal_parser.add_argument(
        "--diagnosis",
        metavar="JSON_OR_PATH",
        help="run the gates against this diagnosis instead of asking a backend",
    )
    heal_parser.set_defaults(func=cmd_heal)

    status_parser = sub.add_parser(
        "status",
        help="show mapping version and anything pending review",
        parents=[common],
    )
    status_parser.add_argument("--json", action="store_true", help="machine-readable output")
    status_parser.set_defaults(func=cmd_status)

    args = parser.parse_args(argv)
    _configure_output(debug=not args.no_debug)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
