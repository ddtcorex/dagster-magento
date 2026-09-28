"""Plans create/update operations for product categories and their store-specific values.

Category operations are organized as:
1. One ensure_categories call with all paths to resolve/create them.
2. One PUT per row's attributes (if non-empty) with global scope.
3. One PUT per store code in store_values (if any) with that store_code.
"""

from typing import Any

from dagster_magento.models import CategoryRow
from dagster_magento.operation import Operation, RowError
from dagster_magento.resolvers import ResolveError
from dagster_magento.writers import PlanResult


def plan_categories(rows: list[CategoryRow], resolver) -> PlanResult:
    """Plan category upserts and store-specific updates.

    First attempts to ensure all paths at once; if that fails, retries per row
    so only unresolving paths become RowErrors. For each row that resolves:
    - Emit one PUT per non-empty attributes dict (global scope).
    - Emit one PUT per store code in store_values.
    """
    # Try to resolve all paths at once.
    paths = [row.path for row in rows]
    try:
        path_ids = resolver.ensure_categories(paths)
    except ResolveError:
        # Retry per row to isolate failures.
        path_ids = {}
        failed = []
        for row in rows:
            try:
                result = resolver.ensure_categories([row.path])
                path_ids[row.path] = result[row.path]
            except ResolveError as error:
                failed.append(RowError(row_ref=row.path, message=str(error)))
        # Continue with non-failing rows.
        rows = [r for r in rows if r.path not in {f.row_ref for f in failed}]
    else:
        failed = []

    operations: list[Operation] = []

    for row in rows:
        category_id = path_ids[row.path]

        # Emit a global attribute update if attributes is non-empty.
        if row.attributes:
            operations.append(
                Operation(
                    method="PUT",
                    endpoint=f"categories/{category_id}",
                    payload={"category": {"id": category_id, **row.attributes}},
                    row_refs=(row.path,),
                )
            )

        # Emit one PUT per store code in store_values.
        for store_code, localized in row.store_values.items():
            operations.append(
                Operation(
                    method="PUT",
                    endpoint=f"categories/{category_id}",
                    payload={"category": {"id": category_id, **localized}},
                    row_refs=(row.path,),
                    store_code=store_code,
                )
            )

    return PlanResult(operations=operations, failed=failed)
