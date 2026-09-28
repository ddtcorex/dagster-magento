from dagster_magento.formats import columns
from dagster_magento.models import BundleOption, BundleSelection, Variation


def test_parse_configurable_variations_sample_shape():
    """Sample shape from the native configurable_variations column."""
    value = "sku=A,size=S,color=Gray,default=1|sku=B,size=S,color=Green"

    result = columns.parse_configurable_variations(value)

    assert result == [
        Variation(sku="A", attributes={"size": "S", "color": "Gray"}),
        Variation(sku="B", attributes={"size": "S", "color": "Green"}),
    ]


def test_parse_configurable_variations_empty_value_is_empty_list():
    assert columns.parse_configurable_variations("") == []


def test_parse_bundle_values_groups_by_option_name():
    """Sample shape from the native bundle_values column: two selections
    under the same option name collapse into one BundleOption."""
    value = (
        "name=Opt1,type=select,required=1,sku=S1,price=15.0000,default=0,"
        "default_qty=1.0000,price_type=fixed|"
        "name=Opt1,type=select,required=1,sku=S2,price=20.0000,default=1,"
        "default_qty=1.0000,price_type=fixed"
    )

    result = columns.parse_bundle_values(value)

    assert result == [
        BundleOption(
            title="Opt1",
            type="select",
            required=True,
            selections=[
                BundleSelection(sku="S1", qty=1.0, price=15.0, price_type="fixed", is_default=False),
                BundleSelection(sku="S2", qty=1.0, price=20.0, price_type="fixed", is_default=True),
            ],
        )
    ]


def test_parse_bundle_values_distinct_names_stay_first_seen_order():
    value = "name=B,type=select,required=0,sku=S1|name=A,type=select,required=0,sku=S2"

    result = columns.parse_bundle_values(value)

    assert [option.title for option in result] == ["B", "A"]


def test_parse_additional_attributes_with_quoted_comma():
    """A value double-quoted to contain a comma keeps the comma intact
    and loses the wrapping quotes."""
    value = 'color=Gray,desc="a, b"'

    result = columns.parse_additional_attributes(value)

    assert result == {"color": "Gray", "desc": "a, b"}


def test_parse_additional_attributes_empty_value_is_empty_dict():
    assert columns.parse_additional_attributes("") == {}


def test_parse_categories_splits_comma_separated_paths():
    value = (
        "Default Category/Women/Tops/Hoodies & Sweatshirts,"
        "Default Category/Collections/Performance Fabrics"
    )

    result = columns.parse_categories(value)

    assert result == [
        "Default Category/Women/Tops/Hoodies & Sweatshirts",
        "Default Category/Collections/Performance Fabrics",
    ]


def test_parse_associated_skus_drops_qty_and_dedupes_keeping_first():
    value = "SKU1=2.0000,SKU2,SKU1=9.0000"

    assert columns.parse_associated_skus(value) == ["SKU1", "SKU2"]


def test_parse_associated_sku_pairs_keeps_qty_and_defaults_bare_sku_to_zero():
    value = "SKU1=2.0000,SKU2"

    assert columns.parse_associated_sku_pairs(value) == [("SKU1", 2.0), ("SKU2", 0.0)]
