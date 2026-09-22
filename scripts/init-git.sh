#!/usr/bin/env bash
# First commit, ready to push.
set -euo pipefail
cd "$(dirname "$0")/.."

# Two files could not be written directly by the remote file bridge
# (macOS protects Makefiles and .github/workflows). They were delivered under
# neutral names; put them where they belong.
[ -f Makefile.txt ] && mv -f Makefile.txt Makefile
if [ -f ci-tests-workflow.yml ]; then
  mkdir -p .github/workflows
  mv -f ci-tests-workflow.yml .github/workflows/tests.yml
fi
chmod +x scripts/*.sh

git init -q
git add -A
git commit -q -m "dag-healer: verified self-healing for one class of pipeline failure

An experiment in what has to exist before an AI agent is allowed to repair a
data pipeline: structured incidents, an allowlist of actions, a mapping layer
that is the only machine-editable surface, and a verification step that
compares a proposed repair against the last known-good run.

The test that matters is the one where a plausible repair passes the data
contract and is rejected anyway."
echo "committed. add a remote and push:"
echo "  git remote add origin git@github.com:cesarzea/dag-healer.git"
echo "  git branch -M main && git push -u origin main"
