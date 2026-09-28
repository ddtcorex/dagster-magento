"""Pure functions that turn validated catalog rows into a plan of REST
Operations against a resolved Magento id space.

A writer never sends a request itself - planning and executing stay
separate so a plan can be inspected, retried or diffed before anything
touches Magento. Every writer returns a PlanResult.
"""

from dataclasses import dataclass, field

from dagster_magento.operation import Operation, RowError


@dataclass
class PlanResult:
    """The output of every writer: the operations it planned, plus a
    RowError for every row it could not plan at all (for example a
    reference the resolver cannot resolve)."""

    operations: list[Operation] = field(default_factory=list)
    failed: list[RowError] = field(default_factory=list)
