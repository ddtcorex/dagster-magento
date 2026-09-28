"""Sample-driven catalog import against the live sandbox, sync then bulk.

Imports the Firebear sample subset (attributes and set assignments,
categories, sources, products, prices, source items, the first 3 images)
in dependency order, reads every SKU back through REST, reruns to prove
the diff skips what is already there, then resets the sandbox and repeats
the import in bulk mode. See conftest.py for how to run it.
"""

import time
import urllib.parse

import pytest
from live_support import SANDBOX_PROJECT, make_resource, prepare_sandbox, reload_env, sandbox

from dagster_magento import (
    import_attribute_sets,
    import_attributes,
    import_categories,
    import_media,
    import_prices,
    import_products,
    import_source_items,
    import_sources,
)
from dagster_magento.formats import (
    attribute_set_assignments_from_rows,
    attributes_from_rows,
    categories_from_rows,
    prices_from_rows,
    products_from_rows,
    read_rows,
    source_items_from_rows,
)
from dagster_magento.models import AttributeRow, AttributeSetRow, CategoryRow, ProductRow, SourceRow
from dagster_magento.resolvers import Resolver
from tests.samples import fetch

pytestmark = pytest.mark.live

MEDIA_IMAGE_LIMIT = 3

# The sample catalog was exported from a store with the Luma sample data,
# so its products reference attributes a fresh install does not have, and
# `color` exists but is in no attribute set. These rows recreate that
# precondition; they are setup, not part of what the test proves.
LUMA_ATTRIBUTES = [
    AttributeRow(code="size", frontend_input="select", label="Size", scope="global"),
    AttributeRow(code="pattern", frontend_input="select", label="Pattern", scope="global"),
    *(
        AttributeRow(code=code, frontend_input="boolean", label=code, scope="global")
        for code in ("eco_collection", "erin_recommends", "new", "performance_fabric", "sale")
    ),
]
LUMA_SET_ASSIGNMENTS = [
    AttributeSetRow(
        name="Default",
        groups={
            "Product Details": [
                "color", "size", "pattern", "eco_collection", "erin_recommends",
                "new", "performance_fabric", "sale",
            ]
        },
    )
]

# Magento attribute codes allow only letters, digits and underscores
# (native REST answers "Invalid value ... for the attribute_code field"),
# and the sample's attributes.csv uses hyphens. The test renames them so
# the attribute and set-assignment path is still proven end to end.
def _attribute_code(code: str) -> str:
    return code.replace("-", "_")


# Row refs whose failure is expected on a fresh sandbox, each with why.
# Every other failure fails the test.
EXPECTED_FAILURES: dict[str, str] = {}

# Rows the diff can never prove unchanged, so a rerun rewrites them (they
# must succeed, not be skipped). Products with type parts and store values
# are always changed by design (Task 16 ruling on product diff); the
# importers listed in NOT_DIFFED take no snapshot at all.
NOT_DIFFED = {
    "attributes": "plan_attributes always PUTs an existing attribute; no snapshot",
    "attribute_sets": "set assignments are re-planned every run (idempotent POSTs)",
    "categories": "import_categories ignores diff; the writer is idempotent by path",
    "sources": "import_sources ignores diff; POST inventory/sources overwrites",
}


def load_catalog() -> dict[str, list]:
    """Map the sample files onto row models, in dependency order."""
    quiet = lambda message: None  # noqa: E731 (dropped-column warnings are expected)
    attribute_rows = list(read_rows(fetch("attributes.csv")))
    attributes, errors = attributes_from_rows(attribute_rows, warn=quiet)
    assert errors == []
    sets = attribute_set_assignments_from_rows(attribute_rows)
    categories, errors = categories_from_rows(list(read_rows(fetch("categories.csv"))), warn=quiet)
    assert errors == []
    products, errors = products_from_rows(list(read_rows(fetch("product_all_types.csv"))), warn=quiet)
    assert errors == []
    prices, errors = prices_from_rows(list(read_rows(fetch("advanced_pricing.csv"))), warn=quiet)
    assert errors == []
    source_items, errors = source_items_from_rows(list(read_rows(fetch("msi_source_qty.csv"))), warn=quiet)
    assert errors == []

    attributes = LUMA_ATTRIBUTES + [
        attribute.model_copy(update={"code": _attribute_code(attribute.code)}) for attribute in attributes
    ]
    sets = LUMA_SET_ASSIGNMENTS + [
        AttributeSetRow(
            name=row.name,
            based_on=row.based_on,
            groups={group: [_attribute_code(code) for code in codes] for group, codes in row.groups.items()},
        )
        for row in sets
    ]
    # Products need every category they name; categories.csv covers only
    # three, so the product paths are ensured too (the writer is by path).
    known = {category.path for category in categories}
    for path in dict.fromkeys(path for product in products for path in product.categories):
        if path not in known:
            categories.append(CategoryRow(path=path))
            known.add(path)
    # The sample repo has no sources file; msi_source_qty.csv names them.
    sources = [
        SourceRow(source_code=code, name=code, country_id="US", postcode="00000")
        for code in dict.fromkeys(item.source_code for item in source_items)
        if code != "default"
    ]

    return {
        "attributes": attributes,
        "attribute_sets": sets,
        "categories": categories,
        "sources": sources,
        "products": products,
        "prices": prices,
        "source_items": source_items,
        "media": _first_images(products, MEDIA_IMAGE_LIMIT),
    }


def _first_images(products: list[ProductRow], limit: int) -> list[ProductRow]:
    # Bounded to keep network time small: every sample image is remote.
    rows, taken = [], 0
    for product in products:
        images = product.images[: limit - taken]
        if images:
            rows.append(ProductRow(sku=product.sku, type=product.type, images=images))
            taken += len(images)
        if taken == limit:
            break
    return rows


IMPORTERS = {
    "attributes": import_attributes,
    "attribute_sets": import_attribute_sets,
    "categories": import_categories,
    "sources": import_sources,
    "products": import_products,
    "prices": import_prices,
    "source_items": import_source_items,
    "media": import_media,
}


def run_catalog(resource, catalog: dict[str, list], mode: str) -> dict[str, tuple]:
    """Run every importer in dependency order; returns name -> (result, seconds)."""
    results = {}
    for name, importer in IMPORTERS.items():
        started = time.monotonic()
        result = importer(resource, catalog[name], mode=mode)
        results[name] = (result, time.monotonic() - started)
        print(f"[{mode}] {name}: {result.to_metadata()} in {results[name][1]:.1f}s")
    return results


def unexpected_failures(results: dict[str, tuple]) -> list[dict]:
    return [
        error
        for result, _ in results.values()
        for error in result.errors
        if error["status"] == "failed" and not set(error["row_ids"]) <= EXPECTED_FAILURES.keys()
    ]


def assert_catalog_imported(resource, catalog: dict[str, list], results: dict[str, tuple]) -> None:
    assert unexpected_failures(results) == []
    for name, (result, _) in results.items():
        assert result.pending == 0, name
        assert result.succeeded + result.skipped_unchanged + result.failed == len(catalog[name]), name
    assert_products_read_back(resource, catalog["products"])
    assert_source_items_read_back(resource, catalog["source_items"])


def assert_products_read_back(resource, products: list[ProductRow]) -> None:
    resolver = Resolver(resource)
    for row in products:
        if row.sku in EXPECTED_FAILURES:
            continue
        product = resource.get(f"products/{urllib.parse.quote(row.sku, safe='')}")
        assert product["type_id"] == row.type, row.sku
        assert product["name"] == row.name, row.sku
        if row.price is not None and owns_price(row):
            assert float(product["price"]) == row.price, row.sku
        links = product["extension_attributes"].get("category_links") or []
        assert sorted(int(link["category_id"]) for link in links) == sorted(
            resolver.category_id(path) for path in row.categories
        ), row.sku


def owns_price(row: ProductRow) -> bool:
    # Magento derives the price of a configurable and of a dynamic-price
    # bundle (price_type 0) from their children and reads it back as 0,
    # whatever the row sent; the sample rows still carry one.
    if row.type == "configurable":
        return False
    return not (row.type == "bundle" and row.attributes.get("price_type") == 0)


def assert_source_items_read_back(resource, source_items) -> None:
    current = {
        (item["source_code"], item["sku"]): (float(item["quantity"]), int(item["status"]))
        for item in resource.get_paginated("inventory/source-items")
    }
    for row in source_items:
        assert current[(row.source_code, row.sku)] == (row.quantity, row.status), (row.source_code, row.sku)


@pytest.fixture(scope="module")
def catalog():
    return load_catalog()


def test_full_sample_catalog_imports_in_sync_mode(catalog):
    prepare_sandbox()
    resource = make_resource()

    started = time.monotonic()
    results = run_catalog(resource, catalog, "sync")
    print(f"[sync] total {time.monotonic() - started:.1f}s")

    assert_catalog_imported(resource, catalog, results)


def test_second_run_is_all_skipped(catalog):
    resource = make_resource()

    results = run_catalog(resource, catalog, "sync")

    assert unexpected_failures(results) == []
    for name, (result, _) in results.items():
        total = len(catalog[name])
        if name in NOT_DIFFED:
            assert result.failed == 0 and result.succeeded + result.skipped_unchanged == total, name
        elif name == "products":
            rewritten = {row.sku for row in catalog[name] if not product_is_diffable(row)}
            assert result.succeeded == len(rewritten), name
            assert result.skipped_unchanged == total - len(rewritten), name
        else:
            assert result.skipped_unchanged == total, name


def product_is_diffable(row: ProductRow) -> bool:
    parts = (
        "store_values", "variations", "configurable_attributes", "bundle_options",
        "grouped_links", "downloadable_links", "downloadable_samples",
    )
    return not any(getattr(row, part) for part in parts)


@pytest.mark.skipif(not SANDBOX_PROJECT.is_dir(), reason="bulk run resets the local sandbox")
def test_same_catalog_imports_in_bulk_mode(catalog):
    """Same catalog, bulk submission path, after a sandbox reset.

    Environment blocker, measured 2026-09-28 on Magento 2.4.9 with the four
    `async.operations.all` consumers `scripts/sandbox.sh consumers` starts:
    the consumer drops a variable subset of the published operations. The
    RabbitMQ counters for the 18-operation product bulk read publish 20,
    deliver 20, ack 14; the six lost operations are never started at all
    (`magento_operation.status = 4`, `started_at` NULL, no `error_code`), and
    Magento logs nothing for them. The library therefore reports them
    `pending` after its timeout, exactly as designed, and the rows they
    belong to do not exist for the later price and media stages.

    It is neither concurrency nor store scope. Nine 5-operation probe bursts
    lost 4, 4, 4, 3, 0, 4, 0, 0 and 4 operations respectively with four
    consumers running, and with a single consumer one burst lost 1 while the
    next lost none; bursts through `/rest/all/` and through a store code lose
    operations alike. Until that loss is understood, this test can fail on
    `pending` rows while `test_full_sample_catalog_imports_in_sync_mode`,
    `test_second_run_is_all_skipped` and the three tests in
    test_price_storefront.py are unaffected.
    """
    started = time.monotonic()
    sandbox("reset", timeout=3600)
    reload_env()
    print(f"[bulk] sandbox reset in {time.monotonic() - started:.1f}s")
    prepare_sandbox()
    resource = make_resource()

    started = time.monotonic()
    results = run_catalog(resource, catalog, "bulk")
    print(f"[bulk] total {time.monotonic() - started:.1f}s")

    assert_catalog_imported(resource, catalog, results)
