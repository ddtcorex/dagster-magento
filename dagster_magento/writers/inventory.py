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
        PlanResult with operations (one per valid link) and failed rows.
    """
    failed: list[RowError] = []
    operations: list[Operation] = []

    for row in rows:
        if row.stock not in stock_ids:
            failed.append(
                RowError(
                    row_ref=f"{row.stock}/{row.source_code}",
                    message=f"unknown stock name: {row.stock}",
                )
            )
            continue

        # One operation per link item (executor wraps via list_key).
        payload = {
            "stock_id": stock_ids[row.stock],
            "source_code": row.source_code,
            "priority": row.priority,
        }

        operations.append(
            Operation(
                method="POST",
                endpoint="inventory/stock-source-links",
                payload=payload,
                row_refs=(f"{row.stock}/{row.source_code}",),
                list_key="links",
            )
        )

    return PlanResult(operations=operations, failed=failed)


def plan_source_items(rows: list[SourceItemRow]) -> PlanResult:
    """Plan inventory source item operations.

    Handles duplicate (source_code, sku) pairs by keeping the last and
    skipping earlier occurrences. Duplicate tracking is done in one pass
    with a dict mapping pair keys to their final row (insertion order
    preserved by tracking position of last occurrence).

    Args:
        rows: List of SourceItemRow objects to process.

    Returns:
        PlanResult with operations (one per unique pair) and skipped row refs.
    """
    # Identify duplicates: map (source_code, sku) to the LAST occurrence's row.
    # On duplicate, mark the earlier row ref as skipped.
    final_items: dict[tuple[str, str], SourceItemRow] = {}
    skipped: list[str] = []

    for row in rows:
        pair_key = (row.source_code, row.sku)
        row_ref = f"{row.source_code}/{row.sku}"

        if pair_key in final_items:
            # We have a duplicate; the earlier one is skipped.
            skipped.append(row_ref)
            # Update to the new (later) row.
            final_items[pair_key] = row
        else:
            # First occurrence of this pair.
            final_items[pair_key] = row

    operations: list[Operation] = []

    # One operation per unique (source_code, sku) pair.
    for pair_key, row in final_items.items():
        payload = {
            "sku": row.sku,
            "source_code": row.source_code,
            "quantity": row.quantity,
            "status": row.status,
        }

        operations.append(
            Operation(
                method="POST",
                endpoint="inventory/source-items",
                payload=payload,
                row_refs=(f"{row.source_code}/{row.sku}",),
                list_key="sourceItems",
            )
        )

    return PlanResult(operations=operations, skipped=skipped)
