import pytest
from dagster_magento.models import (
    ProductRow,
    PriceRow,
    validate_rows,
)
from dagster_magento.operation import RowError
from pydantic import BaseModel


def test_product_row_defaults():
    """ProductRow applies defaults correctly."""
    row = ProductRow(sku="test-sku")

    assert row.sku == "test-sku"
    assert row.type == "simple"
    assert row.attribute_set == "Default"
    assert row.name is None
    assert row.price is None
    assert row.status is None
    assert row.visibility is None
    assert row.weight is None
    assert row.websites == ["base"]
    assert row.categories == []
    assert row.attributes == {}
    assert row.store_values == {}
    assert row.variations == []
    assert row.configurable_attributes == []
    assert row.bundle_options == []
    assert row.grouped_links == []
    assert row.downloadable_links == []
    assert row.downloadable_samples == []
    assert row.images == []


def test_configurable_requires_configurable_attributes_when_variations_present():
    """ProductRow validates that configurable products with variations have configurable_attributes."""
    from dagster_magento.models import Variation

    # Valid: configurable with attributes specified
    row = ProductRow(
        sku="configurable-sku",
        type="configurable",
        variations=[Variation(sku="child-1", attributes={"color": "red"})],
        configurable_attributes=["color"],
    )
    assert row.type == "configurable"

    # Invalid: configurable with variations but no configurable_attributes
    with pytest.raises(ValueError, match="configurable_attributes"):
        ProductRow(
            sku="configurable-sku",
            type="configurable",
            variations=[Variation(sku="child-1", attributes={"color": "red"})],
            configurable_attributes=[],
        )

    # Invalid: variation missing a required configurable attribute
    with pytest.raises(ValueError, match="configurable"):
        ProductRow(
            sku="configurable-sku",
            type="configurable",
            variations=[Variation(sku="child-1", attributes={"size": "large"})],
            configurable_attributes=["color"],
        )


def test_price_row_tiers_none_vs_empty_are_distinct():
    """PriceRow distinguishes between tiers=None (do not touch) and tiers=[] (remove all)."""
    # tiers=None (default, do not touch)
    row1 = PriceRow(sku="test-sku")
    assert row1.tiers is None

    # tiers=[] (remove all)
    row2 = PriceRow(sku="test-sku", tiers=[])
    assert row2.tiers == []
    assert row1.tiers != row2.tiers


def test_validate_rows_collects_errors_with_row_ref():
    """validate_rows returns both valid rows and errors with proper row_ref."""
    # Valid row model for testing
    class SimpleRow(BaseModel):
        code: str
        value: int

    raw = [
        {"code": "A", "value": 10},
        {"code": "B", "value": "not-an-int"},  # Invalid
        {"value": 30},  # Missing required field
    ]

    valid, errors = validate_rows(SimpleRow, raw, id_field="code")

    # Should have one valid row
    assert len(valid) == 1
    assert valid[0].code == "A"
    assert valid[0].value == 10

    # Should have two errors
    assert len(errors) == 2

    # First error should use the id_field value
    err1 = [e for e in errors if e.row_ref == "B"][0]
    assert "value" in err1.message.lower() or "int" in err1.message.lower()

    # Second error should use row index as fallback
    err2 = [e for e in errors if e.row_ref == "row 2"][0]
    assert isinstance(err2, RowError)
