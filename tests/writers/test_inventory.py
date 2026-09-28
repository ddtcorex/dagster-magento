"""Test inventory writers for sources, stocks, links, and source items."""

from dagster_magento.models import (
    SourceRow,
    StockRow,
    StockSourceLinkRow,
    SourceItemRow,
)
from dagster_magento.writers.inventory import (
    plan_sources,
    plan_stocks,
    plan_stock_source_links,
    plan_source_items,
)
from conftest import FakeResolver


def test_sources_post_with_correct_shape():
    """Sources POST with source_code, name, enabled, country_id, postcode."""
    rows = [
        SourceRow(
            source_code="warehouse-1",
            name="Warehouse 1",
            enabled=True,
            country_id="US",
            postcode="12345",
        ),
    ]

    result = plan_sources(rows)

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert op.method == "POST"
    assert op.endpoint == "inventory/sources"
    assert op.list_key is None
    assert op.bulk is None
    assert op.payload == {
        "source": {
            "source_code": "warehouse-1",
            "name": "Warehouse 1",
            "enabled": True,
            "country_id": "US",
            "postcode": "12345",
        }
    }
    assert op.row_refs == ("warehouse-1",)


def test_sources_multiple_rows():
    """Multiple source rows produce one operation per row."""
    rows = [
        SourceRow(
            source_code="warehouse-1",
            name="Warehouse 1",
            enabled=True,
            country_id="US",
            postcode="12345",
        ),
        SourceRow(
            source_code="warehouse-2",
            name="Warehouse 2",
            enabled=False,
            country_id="CA",
            postcode="A1A 1A1",
        ),
    ]

    result = plan_sources(rows)

    assert result.failed == []
    assert len(result.operations) == 2
    assert result.operations[0].row_refs == ("warehouse-1",)
    assert result.operations[1].row_refs == ("warehouse-2",)


def test_stock_sales_channels_from_website_codes():
    """Stocks POST with extension_attributes.sales_channels from website codes."""
    resolver = FakeResolver(websites={"base": 1, "secondary": 2})
    rows = [
        StockRow(name="Stock 1", websites=["base", "secondary"]),
    ]

    result = plan_stocks(rows, resolver)

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert op.method == "POST"
    assert op.endpoint == "inventory/stocks"
    assert op.list_key is None
    assert op.bulk is None
    assert op.payload == {
        "stock": {
            "name": "Stock 1",
            "extension_attributes": {
                "sales_channels": [
                    {"type": "website", "code": "base"},
                    {"type": "website", "code": "secondary"},
                ]
            },
        }
    }
    assert op.row_refs == ("Stock 1",)


def test_stock_single_website():
    """Stock with a single website code."""
    resolver = FakeResolver(websites={"primary": 1})
    rows = [
        StockRow(name="Main Stock", websites=["primary"]),
    ]

    result = plan_stocks(rows, resolver)

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert op.payload == {
        "stock": {
            "name": "Main Stock",
            "extension_attributes": {
                "sales_channels": [
                    {"type": "website", "code": "primary"},
                ]
            },
        }
    }


def test_stock_unknown_website_code_fails_row():
    """Unknown website code in stock row fails that row, no operation."""
    resolver = FakeResolver(websites={"known": 1})
    rows = [
        StockRow(name="Stock 1", websites=["unknown"]),
    ]

    result = plan_stocks(rows, resolver)

    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "Stock 1"
    assert "unknown" in result.failed[0].message.lower()
    assert len(result.operations) == 0


def test_stock_mixed_known_unknown_website():
    """If any website code is unknown, entire row fails."""
    resolver = FakeResolver(websites={"known": 1})
    rows = [
        StockRow(name="Stock 1", websites=["known", "unknown"]),
    ]

    result = plan_stocks(rows, resolver)

    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "Stock 1"
    assert len(result.operations) == 0


def test_stock_empty_websites_list():
    """Stock with empty websites list still produces operation."""
    resolver = FakeResolver(websites={})
    rows = [
        StockRow(name="Stock 1", websites=[]),
    ]

    result = plan_stocks(rows, resolver)

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert op.payload == {
        "stock": {
            "name": "Stock 1",
            "extension_attributes": {
                "sales_channels": []
            },
        }
    }


def test_links_resolve_stock_name_to_id():
    """Stock-source links use stock_ids dict to look up stock name -> id."""
    stock_ids = {"Stock 1": 10, "Stock 2": 20}
    rows = [
        StockSourceLinkRow(stock="Stock 1", source_code="warehouse-1", priority=1),
        StockSourceLinkRow(stock="Stock 2", source_code="warehouse-2", priority=2),
    ]

    result = plan_stock_source_links(rows, stock_ids)

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert op.method == "POST"
    assert op.endpoint == "inventory/stock-source-links"
    assert op.list_key == "links"
    assert op.bulk is None
    assert op.payload == {
        "links": [
            {
                "stock_id": 10,
                "source_code": "warehouse-1",
                "priority": 1,
            },
            {
                "stock_id": 20,
                "source_code": "warehouse-2",
                "priority": 2,
            },
        ]
    }
    assert op.row_refs == ("Stock 1/warehouse-1", "Stock 2/warehouse-2")


def test_links_unknown_stock_name_fails_row():
    """Unknown stock name in link row creates RowError, no operation."""
    stock_ids = {"known": 1}
    rows = [
        StockSourceLinkRow(stock="unknown", source_code="warehouse-1", priority=1),
    ]

    result = plan_stock_source_links(rows, stock_ids)

    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "unknown/warehouse-1"
    assert "unknown" in result.failed[0].message.lower()
    assert len(result.operations) == 0


def test_links_mixed_known_unknown_stocks():
    """If a row has unknown stock, it fails; other rows still process."""
    stock_ids = {"Stock 1": 10, "Stock 2": 20}
    rows = [
        StockSourceLinkRow(stock="Stock 1", source_code="warehouse-1", priority=1),
        StockSourceLinkRow(stock="unknown", source_code="warehouse-2", priority=2),
        StockSourceLinkRow(stock="Stock 2", source_code="warehouse-3", priority=3),
    ]

    result = plan_stock_source_links(rows, stock_ids)

    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "unknown/warehouse-2"
    # Only the known stocks are in the operation
    assert len(result.operations) == 1
    assert len(result.operations[0].payload["links"]) == 2


def test_source_items_use_source_items_list_key():
    """Source items POST with list_key='sourceItems'."""
    rows = [
        SourceItemRow(sku="SKU-001", source_code="warehouse-1", quantity=100, status=1),
    ]

    result = plan_source_items(rows)

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert op.method == "POST"
    assert op.endpoint == "inventory/source-items"
    assert op.list_key == "sourceItems"
    assert op.bulk is None
    assert op.payload == {
        "sourceItems": [
            {
                "sku": "SKU-001",
                "source_code": "warehouse-1",
                "quantity": 100,
                "status": 1,
            }
        ]
    }
    assert op.row_refs == ("warehouse-1/SKU-001",)


def test_source_items_multiple_rows():
    """Multiple source items in one operation."""
    rows = [
        SourceItemRow(sku="SKU-001", source_code="warehouse-1", quantity=100, status=1),
        SourceItemRow(sku="SKU-002", source_code="warehouse-1", quantity=50, status=1),
        SourceItemRow(sku="SKU-003", source_code="warehouse-2", quantity=75, status=0),
    ]

    result = plan_source_items(rows)

    assert result.failed == []
    assert len(result.operations) == 1

    op = result.operations[0]
    assert len(op.payload["sourceItems"]) == 3
    assert op.row_refs == (
        "warehouse-1/SKU-001",
        "warehouse-1/SKU-002",
        "warehouse-2/SKU-003",
    )


def test_source_items_duplicate_pair_keeps_last_skips_earlier():
    """Duplicate (source_code, sku) pair: keep LAST, skip earlier."""
    rows = [
        SourceItemRow(sku="SKU-001", source_code="warehouse-1", quantity=100, status=1),
        SourceItemRow(sku="SKU-001", source_code="warehouse-1", quantity=200, status=0),
        SourceItemRow(sku="SKU-002", source_code="warehouse-1", quantity=50, status=1),
    ]

    result = plan_source_items(rows)

    assert result.failed == []
    assert len(result.operations) == 1
    # Should have 2 items (the duplicate's last row + SKU-002)
    assert len(result.operations[0].payload["sourceItems"]) == 2
    # The last SKU-001/warehouse-1 should be in the operation
    assert result.operations[0].payload["sourceItems"][0]["quantity"] == 200
    assert result.operations[0].payload["sourceItems"][0]["status"] == 0
    # Earlier duplicate should be in skipped
    assert len(result.skipped) == 1
    assert "warehouse-1/SKU-001" in result.skipped


def test_source_items_three_duplicates_keeps_last_skips_two():
    """Three duplicate (source_code, sku) pairs: keep last, skip first two."""
    rows = [
        SourceItemRow(sku="SKU-001", source_code="warehouse-1", quantity=100, status=1),
        SourceItemRow(sku="SKU-001", source_code="warehouse-1", quantity=200, status=1),
        SourceItemRow(sku="SKU-001", source_code="warehouse-1", quantity=300, status=0),
    ]

    result = plan_source_items(rows)

    assert result.failed == []
    assert len(result.operations) == 1
    # Should have 1 item (the last one)
    assert len(result.operations[0].payload["sourceItems"]) == 1
    assert result.operations[0].payload["sourceItems"][0]["quantity"] == 300
    # Two earlier duplicates should be skipped
    assert len(result.skipped) == 2
    assert all("warehouse-1/SKU-001" in ref for ref in result.skipped)


def test_source_items_zero_quantity():
    """Source item with quantity 0 is sent."""
    rows = [
        SourceItemRow(sku="SKU-001", source_code="warehouse-1", quantity=0, status=1),
    ]

    result = plan_source_items(rows)

    assert result.failed == []
    assert len(result.operations) == 1
    assert result.operations[0].payload["sourceItems"][0]["quantity"] == 0


def test_source_items_status_values():
    """Source items support status 0 and 1."""
    rows = [
        SourceItemRow(sku="SKU-001", source_code="warehouse-1", quantity=100, status=0),
        SourceItemRow(sku="SKU-002", source_code="warehouse-1", quantity=50, status=1),
    ]

    result = plan_source_items(rows)

    assert result.failed == []
    assert result.operations[0].payload["sourceItems"][0]["status"] == 0
    assert result.operations[0].payload["sourceItems"][1]["status"] == 1


def test_links_empty_stock_ids():
    """Empty stock_ids dict with rows fails all rows."""
    stock_ids = {}
    rows = [
        StockSourceLinkRow(stock="Stock 1", source_code="warehouse-1", priority=1),
    ]

    result = plan_stock_source_links(rows, stock_ids)

    assert len(result.failed) == 1
    assert len(result.operations) == 0
