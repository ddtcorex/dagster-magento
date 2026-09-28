"""Test plan_prices writer for pricing operations."""

from dagster_magento.models import PriceRow, TierPrice
from dagster_magento.writers.pricing import plan_prices


def test_base_price_item_shape_and_list_key():
    """Base price becomes POST operation with correct shape and list_key."""
    rows = [
        PriceRow(sku="SKU-001", price=99.99, store_id=0),
    ]

    result = plan_prices(rows)

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert op.method == "POST"
    assert op.endpoint == "products/base-prices"
    assert op.list_key == "prices"
    assert op.payload == {"sku": "SKU-001", "price": 99.99, "store_id": 0}
    assert op.row_refs == ("SKU-001",)
    assert op.bulk is None


def test_special_price_carries_dates():
    """Special price includes price_from and price_to when present."""
    rows = [
        PriceRow(
            sku="SKU-002",
            special_price=49.99,
            special_from="2026-01-01",
            special_to="2026-12-31",
            store_id=0,
        ),
    ]

    result = plan_prices(rows)

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert op.method == "POST"
    assert op.endpoint == "products/special-price"
    assert op.list_key == "prices"
    assert op.payload == {
        "sku": "SKU-002",
        "price": 49.99,
        "store_id": 0,
        "price_from": "2026-01-01",
        "price_to": "2026-12-31",
    }
    assert op.row_refs == ("SKU-002",)


def test_special_price_omits_none_dates():
    """Special price omits price_from/price_to keys when None."""
    rows = [
        PriceRow(sku="SKU-003", special_price=29.99, store_id=0),
    ]

    result = plan_prices(rows)

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert op.endpoint == "products/special-price"
    # Payload should NOT have price_from or price_to keys
    assert op.payload == {
        "sku": "SKU-003",
        "price": 29.99,
        "store_id": 0,
    }
    assert "price_from" not in op.payload
    assert "price_to" not in op.payload


def test_tiers_replace_uses_put():
    """Tier prices in replace mode use PUT method."""
    rows = [
        PriceRow(
            sku="SKU-004",
            tiers=[
                TierPrice(qty=10, price=90.0),
                TierPrice(qty=50, price=85.0, customer_group="CUSTOM", website="all"),
            ],
        ),
    ]

    result = plan_prices(rows, tier_mode="replace")

    assert result.failed == []
    assert len(result.operations) == 2

    op1 = result.operations[0]
    assert op1.method == "PUT"
    assert op1.endpoint == "products/tier-prices"
    assert op1.list_key == "prices"
    assert op1.payload == {
        "sku": "SKU-004",
        "price": 90.0,
        "price_type": "fixed",
        "website_id": 0,
        "customer_group": "ALL GROUPS",
        "quantity": 10,
    }

    op2 = result.operations[1]
    assert op2.method == "PUT"
    assert op2.payload == {
        "sku": "SKU-004",
        "price": 85.0,
        "price_type": "fixed",
        "website_id": 0,
        "customer_group": "CUSTOM",
        "quantity": 50,
    }


def test_tiers_add_uses_post():
    """Tier prices in add mode use POST method."""
    rows = [
        PriceRow(
            sku="SKU-005",
            tiers=[
                TierPrice(qty=5, price=95.0),
            ],
        ),
    ]

    result = plan_prices(rows, tier_mode="add")

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert op.method == "POST"
    assert op.endpoint == "products/tier-prices"


def test_empty_tiers_deletes_current_in_replace_mode():
    """Empty tiers list in replace mode creates delete operations for current tiers."""
    current_tiers = {
        "SKU-006": [
            {
                "sku": "SKU-006",
                "price": 80.0,
                "price_type": "fixed",
                "website_id": 0,
                "customer_group": "ALL GROUPS",
                "quantity": 10,
            },
            {
                "sku": "SKU-006",
                "price": 75.0,
                "price_type": "fixed",
                "website_id": 0,
                "customer_group": "ALL GROUPS",
                "quantity": 20,
            },
        ],
    }
    rows = [
        PriceRow(sku="SKU-006", tiers=[]),
    ]

    result = plan_prices(rows, tier_mode="replace", current_tiers=current_tiers)

    assert result.failed == []
    assert len(result.operations) == 2

    # Both should be POST to tier-prices-delete
    op1 = result.operations[0]
    assert op1.method == "POST"
    assert op1.endpoint == "products/tier-prices-delete"
    assert op1.list_key == "prices"
    # Payload should be a fresh copy of the current tier dict
    assert op1.payload == {
        "sku": "SKU-006",
        "price": 80.0,
        "price_type": "fixed",
        "website_id": 0,
        "customer_group": "ALL GROUPS",
        "quantity": 10,
    }

    op2 = result.operations[1]
    assert op2.payload == {
        "sku": "SKU-006",
        "price": 75.0,
        "price_type": "fixed",
        "website_id": 0,
        "customer_group": "ALL GROUPS",
        "quantity": 20,
    }


def test_empty_tiers_deletes_nothing_if_no_current_tiers():
    """Empty tiers list with no current tiers emits nothing in replace mode."""
    rows = [
        PriceRow(sku="SKU-007", tiers=[]),
    ]

    result = plan_prices(rows, tier_mode="replace", current_tiers={})

    assert result.failed == []
    assert len(result.operations) == 0


def test_empty_tiers_add_mode_emits_nothing():
    """Empty tiers list in add mode emits nothing."""
    rows = [
        PriceRow(sku="SKU-008", tiers=[]),
    ]

    result = plan_prices(rows, tier_mode="add")

    assert result.failed == []
    assert len(result.operations) == 0


def test_none_fields_emit_nothing():
    """None fields (price, special_price, tiers) emit nothing."""
    rows = [
        PriceRow(sku="SKU-009"),  # All None
    ]

    result = plan_prices(rows)

    assert result.failed == []
    assert len(result.operations) == 0


def test_multiple_rows_with_mixed_fields():
    """Multiple rows with different field combinations produce correct operations."""
    rows = [
        PriceRow(sku="SKU-010", price=100.0, store_id=0),
        PriceRow(sku="SKU-011", special_price=50.0, store_id=0),
        PriceRow(sku="SKU-012", tiers=[TierPrice(qty=10, price=90.0)]),
    ]

    result = plan_prices(rows)

    assert result.failed == []
    assert len(result.operations) == 3

    assert result.operations[0].endpoint == "products/base-prices"
    assert result.operations[1].endpoint == "products/special-price"
    assert result.operations[2].endpoint == "products/tier-prices"


def test_website_resolution_all_to_zero():
    """Website code 'all' resolves to website_id 0."""
    rows = [
        PriceRow(
            sku="SKU-013",
            tiers=[
                TierPrice(qty=10, price=90.0, website="all"),
            ],
        ),
    ]

    result = plan_prices(rows)

    assert result.failed == []
    op = result.operations[0]
    assert op.payload["website_id"] == 0


def test_website_resolution_numeric_string():
    """Numeric website code resolves to int."""
    rows = [
        PriceRow(
            sku="SKU-014",
            tiers=[
                TierPrice(qty=10, price=90.0, website="2"),
            ],
        ),
    ]

    result = plan_prices(rows)

    assert result.failed == []
    op = result.operations[0]
    assert op.payload["website_id"] == 2


def test_website_resolution_via_mapping():
    """Website code resolved via website_ids mapping."""
    website_ids = {"primary": 1, "secondary": 2}
    rows = [
        PriceRow(
            sku="SKU-015",
            tiers=[
                TierPrice(qty=10, price=90.0, website="primary"),
            ],
        ),
    ]

    result = plan_prices(rows, website_ids=website_ids)

    assert result.failed == []
    op = result.operations[0]
    assert op.payload["website_id"] == 1


def test_website_resolution_unknown_code_fails_row():
    """Unknown website code marks row as failed, emits no operations."""
    website_ids = {"known": 1}
    rows = [
        PriceRow(
            sku="SKU-016",
            tiers=[
                TierPrice(qty=10, price=90.0, website="unknown"),
            ],
        ),
    ]

    result = plan_prices(rows, website_ids=website_ids)

    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "SKU-016"
    assert "unknown" in result.failed[0].message.lower()
    assert "website" in result.failed[0].message.lower()
    assert len(result.operations) == 0


def test_all_or_nothing_per_row_on_website_error():
    """If any tier in a row has an unknown website, row emits no operations."""
    website_ids = {"known": 1}
    rows = [
        PriceRow(
            sku="SKU-017",
            price=100.0,  # This would normally be an operation
            tiers=[
                TierPrice(qty=10, price=90.0, website="unknown"),
            ],
        ),
    ]

    result = plan_prices(rows, website_ids=website_ids)

    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "SKU-017"
    # Should have no operations - the whole row is rejected
    assert len(result.operations) == 0


def test_row_refs_always_sku():
    """All operations have row_refs=(sku,)."""
    rows = [
        PriceRow(sku="SKU-018", price=100.0, store_id=0),
        PriceRow(sku="SKU-019", special_price=50.0, store_id=0),
        PriceRow(sku="SKU-020", tiers=[TierPrice(qty=10, price=90.0)]),
    ]

    result = plan_prices(rows)

    assert all(op.row_refs == (op.payload["sku"],) for op in result.operations)


def test_operations_have_no_bulk():
    """No operations carry a bulk spec."""
    rows = [
        PriceRow(sku="SKU-021", price=100.0, store_id=0),
        PriceRow(sku="SKU-022", special_price=50.0, store_id=0),
        PriceRow(sku="SKU-023", tiers=[TierPrice(qty=10, price=90.0)]),
    ]

    result = plan_prices(rows)

    assert all(op.bulk is None for op in result.operations)


def test_replace_tiers_share_the_sku_chunk_key():
    """Replace-mode tier operations have chunk_key=sku to stay together."""
    rows = [
        PriceRow(
            sku="SKU-024",
            tiers=[
                TierPrice(qty=10, price=90.0),
                TierPrice(qty=20, price=85.0),
            ],
        ),
    ]

    result = plan_prices(rows, tier_mode="replace")

    assert result.failed == []
    assert len(result.operations) == 2
    # Both tier operations should have chunk_key="SKU-024"
    assert result.operations[0].chunk_key == "SKU-024"
    assert result.operations[1].chunk_key == "SKU-024"


def test_add_tiers_have_no_chunk_key():
    """Add-mode tier operations have chunk_key=None (no chunking constraint)."""
    rows = [
        PriceRow(
            sku="SKU-025",
            tiers=[
                TierPrice(qty=10, price=90.0),
                TierPrice(qty=20, price=85.0),
            ],
        ),
    ]

    result = plan_prices(rows, tier_mode="add")

    assert result.failed == []
    assert len(result.operations) == 2
    # Both tier operations should have chunk_key=None
    assert result.operations[0].chunk_key is None
    assert result.operations[1].chunk_key is None


def test_base_and_special_prices_have_no_chunk_key():
    """Base and special price operations always have chunk_key=None."""
    rows = [
        PriceRow(
            sku="SKU-026",
            price=100.0,
            special_price=50.0,
            store_id=0,
        ),
    ]

    result = plan_prices(rows)

    assert len(result.operations) == 2
    # Base price
    assert result.operations[0].chunk_key is None
    # Special price
    assert result.operations[1].chunk_key is None


def test_zero_price_is_sent():
    """Price and special_price values of 0 are sent (they are valid)."""
    rows = [
        PriceRow(sku="SKU-027", price=0, store_id=0),
        PriceRow(sku="SKU-028", special_price=0, store_id=0),
    ]

    result = plan_prices(rows)

    assert result.failed == []
    assert len(result.operations) == 2
    # First op: base price with price=0
    assert result.operations[0].endpoint == "products/base-prices"
    assert result.operations[0].payload["price"] == 0
    # Second op: special price with price=0
    assert result.operations[1].endpoint == "products/special-price"
    assert result.operations[1].payload["price"] == 0
