"""Plans inventory operations for sources, stocks, links, and source items."""

from dagster_magento.models import (
    SourceRow,
    StockRow,
    StockSourceLinkRow,
    SourceItemRow,
)
from dagster_magento.operation import Operation, RowError
from dagster_magento.writers import PlanResult
from dagster_magento.resolvers import ResolveError


def plan_sources(rows: list[SourceRow]) -> PlanResult:
    """Plan inventory source operations.

    Args:
        rows: List of SourceRow objects to process.

    Returns:
        PlanResult with operations.
    """
    operations: list[Operation] = []

    for row in rows:
        # Each source becomes a separate operation
        payload = {
            "source": {
                "source_code": row.source_code,
                "name": row.name,
                "enabled": row.enabled,
                "country_id": row.country_id,
                "postcode": row.postcode,
            }
        }

        operations.append(
            Operation(
                method="POST",
                endpoint="inventory/sources",
                payload=payload,
                row_refs=(row.source_code,),
            )
        )

    return PlanResult(operations=operations)


def plan_stocks(rows: list[StockRow], resolver) -> PlanResult:
    """Plan inventory stock operations.

    Args:
        rows: List of StockRow objects to process.
        resolver: Resolver to validate website codes.

    Returns:
        PlanResult with operations, failed rows.
    """
    operations: list[Operation] = []
    failed: list[RowError] = []

    for row in rows:
        # Validate all website codes
        valid = True
        for website_code in row.websites:
            try:
                resolver.website_id(website_code)
            except ResolveError:
                failed.append(
                    RowError(
                        row_ref=row.name,
                        message=f"unknown website code: {website_code}",
                    )
                )
                valid = False
                break

        if not valid:
            continue

        # Build sales_channels from website codes
        sales_channels = [
            {"type": "website", "code": code} for code in row.websites
        ]

        payload = {
            "stock": {
                "name": row.name,
                "extension_attributes": {
                    "sales_channels": sales_channels,
                },
            }
        }

        operations.append(
            Operation(
                method="POST",
                endpoint="inventory/stocks",
                payload=payload,
                row_refs=(row.name,),
            )
        )

    return PlanResult(operations=operations, failed=failed)


def plan_stock_source_links(
    rows: list[StockSourceLinkRow], stock_ids: dict[str, int]
) -> PlanResult:
    """Plan inventory stock-source link operations.

    Args:
        rows: List of StockSourceLinkRow objects to process.
        stock_ids: Mapping of stock name to stock ID.

    Returns:
        PlanResult with operations (one for all valid items) and failed rows.
    """
    failed: list[RowError] = []
    items: list[dict] = []

    for row in rows:
        if row.stock not in stock_ids:
            failed.append(
                RowError(
                    row_ref=f"{row.stock}/{row.source_code}",
                    message=f"unknown stock name: {row.stock}",
                )
            )
            continue

        items.append(
            {
                "stock_id": stock_ids[row.stock],
                "source_code": row.source_code,
                "priority": row.priority,
            }
        )

    operations: list[Operation] = []
    if items:
        # Collect row_refs from successfully processed items
        row_refs = tuple(
            f"{row.stock}/{row.source_code}"
            for row in rows
            if row.stock in stock_ids
        )

        operations.append(
            Operation(
                method="POST",
                endpoint="inventory/stock-source-links",
                payload={"links": items},
                row_refs=row_refs,
                list_key="links",
            )
        )

    return PlanResult(operations=operations, failed=failed)


def plan_source_items(rows: list[SourceItemRow]) -> PlanResult:
    """Plan inventory source item operations.

    Handles duplicate (source_code, sku) pairs by keeping the last and
    skipping earlier occurrences.

    Args:
        rows: List of SourceItemRow objects to process.

    Returns:
        PlanResult with operations, skipped row refs.
    """
    # Track which (source_code, sku) pairs we've seen and their indices
    seen_pairs: dict[tuple[str, str], int] = {}
    items: list[dict] = []
    row_refs: list[str] = []
    skipped: list[str] = []

    for idx, row in enumerate(rows):
        pair_key = (row.source_code, row.sku)
        row_ref = f"{row.source_code}/{row.sku}"

        if pair_key in seen_pairs:
            # We've seen this pair before; skip the earlier one
            prev_idx = seen_pairs[pair_key]
            prev_item_idx = None

            # Find the index of the previous item in our items list
            # We need to find which item in our items list corresponds to prev_idx
            for item_idx, item in enumerate(items):
                if (
                    item["source_code"] == row.source_code
                    and item["sku"] == row.sku
                ):
                    prev_item_idx = item_idx
                    break

            if prev_item_idx is not None:
                # Move the earlier row_ref to skipped
                skipped.append(row_refs[prev_item_idx])
                # Replace the item with the new one
                items[prev_item_idx] = {
                    "sku": row.sku,
                    "source_code": row.source_code,
                    "quantity": row.quantity,
                    "status": row.status,
                }
                # Replace the row_ref
                row_refs[prev_item_idx] = row_ref

            seen_pairs[pair_key] = idx
        else:
            # First time seeing this pair
            seen_pairs[pair_key] = idx
            items.append(
                {
                    "sku": row.sku,
                    "source_code": row.source_code,
                    "quantity": row.quantity,
                    "status": row.status,
                }
            )
            row_refs.append(row_ref)

    operations: list[Operation] = []
    if items:
        operations.append(
            Operation(
                method="POST",
                endpoint="inventory/source-items",
                payload={"sourceItems": items},
                row_refs=tuple(row_refs),
                list_key="sourceItems",
            )
        )

    return PlanResult(operations=operations, skipped=skipped)
