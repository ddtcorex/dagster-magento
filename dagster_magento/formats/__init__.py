"""File adapters for the native Magento catalog import column encoding.

`columns` holds the pure per-column string parsers, `readers` reads csv/json
rows off disk, `catalog` maps those raw rows onto the canonical models in
`dagster_magento.models`. Re-exported here so a caller can do
`from dagster_magento.formats import read_rows, products_from_rows` without
reaching into the submodules directly.
"""

from dagster_magento.formats.catalog import (
    attribute_set_assignments_from_rows,
    attributes_from_rows,
    categories_from_rows,
    prices_from_rows,
    products_from_rows,
    source_items_from_rows,
)
from dagster_magento.formats.columns import (
    ColumnParseError,
    parse_additional_attributes,
    parse_associated_sku_pairs,
    parse_associated_skus,
    parse_bundle_values,
    parse_categories,
    parse_configurable_variations,
    parse_pipe_groups,
)
from dagster_magento.formats.readers import read_rows

__all__ = [
    "attribute_set_assignments_from_rows",
    "attributes_from_rows",
    "categories_from_rows",
    "prices_from_rows",
    "products_from_rows",
    "source_items_from_rows",
    "ColumnParseError",
    "parse_additional_attributes",
    "parse_associated_sku_pairs",
    "parse_associated_skus",
    "parse_bundle_values",
    "parse_categories",
    "parse_configurable_variations",
    "parse_pipe_groups",
    "read_rows",
]
