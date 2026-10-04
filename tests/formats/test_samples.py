"""Parse real Firebear/native Magento sample import files end to end.

Marked `samples`: these tests download real files over HTTP (see
`tests.samples.fetch`) and are excluded from the default test run
(pyproject.toml's `addopts`). Run explicitly with:
`.venv/bin/pytest -m samples -q`.
"""

import pytest

from dagster_magento.formats import catalog, readers
from tests.samples import fetch, fetch_as_xml

pytestmark = pytest.mark.samples


def test_product_all_types_csv_parses_all_five_types():
    rows = list(readers.read_rows(fetch("product_all_types.csv")))

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    assert {product.type for product in products} == {
        "bundle",
        "configurable",
        "downloadable",
        "grouped",
        "simple",
    }


def test_product_all_types_xlsx_matches_csv_skus():
    csv_rows = list(readers.read_rows(fetch("product_all_types.csv")))
    xlsx_rows = list(readers.read_rows(fetch("products_all_types.xlsx")))

    csv_products, csv_errors = catalog.products_from_rows(csv_rows)
    xlsx_products, xlsx_errors = catalog.products_from_rows(xlsx_rows)

    assert csv_errors == []
    assert xlsx_errors == []
    assert {product.sku for product in xlsx_products} == {product.sku for product in csv_products}


def test_categories_attributes_advanced_pricing_msi_parse_without_failures():
    category_rows = list(readers.read_rows(fetch("categories.csv")))
    attribute_rows = list(readers.read_rows(fetch("attributes.csv")))
    price_rows = list(readers.read_rows(fetch("advanced_pricing.csv")))
    source_item_rows = list(readers.read_rows(fetch("msi_source_qty.csv")))

    categories, category_errors = catalog.categories_from_rows(category_rows)
    attributes, attribute_errors = catalog.attributes_from_rows(attribute_rows)
    attribute_sets = catalog.attribute_set_assignments_from_rows(attribute_rows)
    prices, price_errors = catalog.prices_from_rows(price_rows)
    source_items, source_item_errors = catalog.source_items_from_rows(source_item_rows)

    assert category_errors == []
    assert attribute_errors == []
    assert price_errors == []
    assert source_item_errors == []
    assert len(categories) > 0
    assert len(attributes) > 0
    assert len(attribute_sets) > 0
    assert len(prices) > 0
    assert len(source_items) > 0


def test_native_catalog_product_csv_parses():
    rows = list(readers.read_rows(fetch("catalog_product.csv")))

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    assert len(products) > 0


def test_product_all_types_xml_parses_the_same_five_types():
    """The xml container over the real sample column layout, end to end."""
    rows = list(readers.read_rows(fetch_as_xml("product_all_types.csv")))

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    assert {product.type for product in products} == {
        "bundle",
        "configurable",
        "downloadable",
        "grouped",
        "simple",
    }
