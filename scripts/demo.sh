#!/usr/bin/env bash
# Interactive walkthrough. Use --no-pause for an unattended recording.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ ! -x .venv/bin/python ]; then
  printf '%s\n' 'The demo needs its Python environment. Run make install, then try again.' >&2
  exit 1
fi

if ! .venv/bin/python -c 'import httpx, yaml' >/dev/null 2>&1; then
  printf '%s\n' 'The Python environment is incomplete. Run make install, then try again.' >&2
  exit 1
fi

export PYTHONPATH="src${PYTHONPATH:+:$PYTHONPATH}"
# The Python entry point checks services and asks before starting missing ones.
# Unbuffered output keeps startup progress and phase explanations visible.
exec .venv/bin/python -u -m dag_healer.demo "$@"
