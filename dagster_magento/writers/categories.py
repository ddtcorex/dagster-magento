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

# Keys the writer itself always sets in the category payload. A row whose
# attributes carry any of these would otherwise have its value silently
# overridden by the last-write-wins **row.attributes spread - reject the row
# instead of guessing which value the caller meant.
RESERVED_CATEGORY_KEYS = frozenset({"id", "parent_id", "path", "name"})

# Like RESERVED_CATEGORY_KEYS, but for per-store localized values. "name" is
# legitimate per store (it is localized), but "id", "parent_id", and "path"
# would shadow the writer-owned category identity and tree position.
RESERVED_CATEGORY_STORE_KEYS = frozenset({"id", "parent_id", "path"})


def plan_categories(rows: list[CategoryRow], resolver) -> PlanResult:
    """Plan category upserts and store-specific updates.

    First checks all rows for reserved keys and rejects any that shadow
    writer-owned fields. Then attempts to ensure all paths at once; if that
    fails, retries per row so only unresolving paths become RowErrors.
    For each row that resolves:
    - Emit one PUT per non-empty attributes dict (global scope).
    - Emit one PUT per store code in store_values.
    """
    # Check for reserved keys before calling ensure_categories.
    failed: list[RowError] = []
    validated_rows = []

    for row in rows:
        # Check attributes for reserved keys.
        shadowed_attrs = sorted(RESERVED_CATEGORY_KEYS & row.attributes.keys())
        if shadowed_attrs:
            failed.append(
                RowError(
                    row_ref=row.path,
                    message=f"attributes shadow writer-owned keys: {', '.join(shadowed_attrs)}",
                )
            )
            continue

        # Check each store_values dict for reserved keys.
        store_errors = {}
        for store_code, localized in row.store_values.items():
            shadowed_store = sorted(RESERVED_CATEGORY_STORE_KEYS & localized.keys())
            if shadowed_store:
                store_errors[store_code] = shadowed_store

        if store_errors:
            # Report all store violations in one message.
            msg_parts = []
            for store_code in sorted(store_errors.keys()):
                keys = store_errors[store_code]
                msg_parts.append(f"store_values[{store_code}]: {', '.join(keys)}")
            failed.append(
                RowError(
                    row_ref=row.path,
                    message="shadow writer-owned keys: " + "; ".join(msg_parts),
                )
            )
            continue

        validated_rows.append(row)

    # Try to resolve all remaining paths at once.
    paths = [row.path for row in validated_rows]
    path_ids: dict[str, int] = {}

    if paths:
        try:
            path_ids = resolver.ensure_categories(paths)
        except ResolveError:
            # Retry per row to isolate failures.
            resolved = []
            for row in validated_rows:
                try:
                    result = resolver.ensure_categories([row.path])
                    path_ids[row.path] = result[row.path]
                    resolved.append(row)
                except ResolveError as error:
                    failed.append(RowError(row_ref=row.path, message=str(error)))
            validated_rows = resolved

    operations: list[Operation] = []

    for row in validated_rows:
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
