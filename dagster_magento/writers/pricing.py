"""Plans pricing operations for products: base prices, special prices, and tier prices."""

from typing import Literal, Mapping

from dagster_magento.models import PriceRow
from dagster_magento.operation import Operation, RowError
from dagster_magento.writers import PlanResult


def plan_prices(
    rows: list[PriceRow],
    tier_mode: Literal["add", "replace"] = "replace",
    current_tiers: dict[str, list[dict]] | None = None,
    website_ids: Mapping[str, int] | None = None,
) -> PlanResult:
    """Plan pricing operations for base prices, special prices, and tier prices.

    Args:
        rows: List of PriceRow objects to process.
        tier_mode: "add" to POST new tiers, "replace" to PUT (overwrite) or DELETE.
        current_tiers: Current tiers by SKU for delete operations in replace mode.
        website_ids: Mapping of website codes to IDs for tier price resolution.

    Returns:
        PlanResult with operations, failed rows, and skipped rows.
    """
    operations: list[Operation] = []
    failed: list[RowError] = []
    current_tiers = current_tiers or {}
    website_ids = website_ids or {}

    for row in rows:
        row_operations: list[Operation] = []
        row_failed = False

        # Emit base price if present.
        if row.price is not None:
            row_operations.append(
                Operation(
                    method="POST",
                    endpoint="products/base-prices",
                    payload={"sku": row.sku, "price": row.price, "store_id": row.store_id},
                    row_refs=(row.sku,),
                    list_key="prices",
                )
            )

        # Emit special price if present.
        if row.special_price is not None:
            payload = {
                "sku": row.sku,
                "price": row.special_price,
                "store_id": row.store_id,
            }
            # Omit date keys if None.
            if row.special_from is not None:
                payload["price_from"] = row.special_from
            if row.special_to is not None:
                payload["price_to"] = row.special_to

            row_operations.append(
                Operation(
                    method="POST",
                    endpoint="products/special-price",
                    payload=payload,
                    row_refs=(row.sku,),
                    list_key="prices",
                )
            )

        # Handle tier prices.
        if row.tiers is not None:
            # Resolve all tier prices first to check for website errors.
            tier_website_ids = []
            for tier in row.tiers:
                website_id = _resolve_website_id(
                    tier.website, website_ids
                )
                if website_id is None:
                    # Unknown website code: fail the entire row.
                    failed.append(
                        RowError(
                            row_ref=row.sku,
                            message=f"unknown website code: {tier.website}",
                        )
                    )
                    row_failed = True
                    break
                tier_website_ids.append(website_id)

            if row_failed:
                # Skip all operations for this row.
                continue

            # Process tier prices.
            if len(row.tiers) == 0:
                # Empty tiers: delete current tiers in replace mode.
                if tier_mode == "replace":
                    current = current_tiers.get(row.sku, [])
                    for tier_dict in current:
                        # Create a fresh copy of the tier dict.
                        row_operations.append(
                            Operation(
                                method="POST",
                                endpoint="products/tier-prices-delete",
                                payload=dict(tier_dict),
                                row_refs=(row.sku,),
                                list_key="prices",
                            )
                        )
            else:
                # Non-empty tiers: add or replace.
                method = "POST" if tier_mode == "add" else "PUT"
                for tier, website_id in zip(row.tiers, tier_website_ids):
                    row_operations.append(
                        Operation(
                            method=method,
                            endpoint="products/tier-prices",
                            payload={
                                "sku": row.sku,
                                "price": tier.price,
                                "price_type": tier.price_type,
                                "website_id": website_id,
                                "customer_group": tier.customer_group,
                                "quantity": tier.qty,
                            },
                            row_refs=(row.sku,),
                            list_key="prices",
                        )
                    )

        # Add all operations for this row only if the row didn't fail.
        if not row_failed:
            operations.extend(row_operations)

    return PlanResult(operations=operations, failed=failed)


def _resolve_website_id(website_code: str, website_ids: Mapping[str, int]) -> int | None:
    """Resolve a website code to a website ID.

    Rules:
    - "all" -> 0
    - all digits -> int(code)
    - a key in website_ids -> website_ids[code]
    - otherwise -> None
    """
    if website_code == "all":
        return 0

    if website_code.isdigit():
        return int(website_code)

    if website_code in website_ids:
        return website_ids[website_code]

    return None
