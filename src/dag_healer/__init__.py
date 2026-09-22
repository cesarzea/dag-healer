"""dag-healer: a small, auditable self-healing layer for data pipelines.

The premise is not that an LLM can fix a broken pipeline. It is that a narrow
class of pipeline failures is both *recurring* and *mechanically verifiable*,
and that those two properties together are what make automation safe. Anything
outside that intersection is escalated to a human on purpose.
"""

__version__ = "0.1.0"
