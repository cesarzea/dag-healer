# DAG-Healer (https://github.com/cesarzea/dag-healer)
# Copyright (c) 2026 César Pedro Zea Gómez (https://www.cesarzea.com)
# SPDX-License-Identifier: MIT

"""dag-healer: a proof of concept for AI-assisted pipeline diagnosis and repair.

An LLM proposes an action from failure evidence. The healer applies policy
and deterministic checks before editing a field mapping. The guided example
demonstrates both rejected proposals and an incorrect mapping these limited
checks accept; passing them does not establish business correctness.
"""

__version__ = "0.1.0"
