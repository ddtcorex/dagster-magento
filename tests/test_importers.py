import pytest
import requests_mock

from dagster_magento import importers
from dagster_magento.executor import MagentoImportError
from dagster_magento.importers import (
    import_attribute_sets,
    import_prices,
    import_products,
    import_source_items,
    import_sources,
    import_stock_source_links,
    to_materialize_result,
)
from dagster_magento.resource import MagentoAuthError, MagentoResource
from dagster_magento.upload import UploadResult

BASE = "https://shop.test/rest/all/V1"


def make_resource(**overrides):
    defaults = dict(
        base_url="https://shop.test",
        username="admin",
        password="secret-password",
        store_view="all",
    )
    defaults.update(overrides)
    resource = MagentoResource(**defaults)
    resource._sleep = lambda seconds: None  # tests must never actually sleep
    return resource


def mock_catalog(m, products=None):
    """Token, attribute set, website and attribute metadata every product
    import needs, plus the snapshot GET returning `products`."""
    m.post(f"{BASE}/integration/admin/token", json="token")
    m.get(f"{BASE}/products", json={"items": products or []})
    m.get(
        f"{BASE}/eav/attribute-sets/list",
        json={"items": [{"attribute_set_name": "Default", "attribute_set_id": 4}]},
    )
    m.get(f"{BASE}/store/websites", json=[{"code": "base", "id": 1}, {"code": "fr", "id": 2}])
    m.get(
        f"{BASE}/categories",
        json={
            "id": 1,
            "name": "Root Catalog",
            "children_data": [
                {
                    "id": 2,
                    "name": "Default Category",
                    "children_data": [{"id": 5, "name": "Men"}, {"id": 6, "name": "Women"}],
                }
            ],
        },
    )
    m.get(
        f"{BASE}/products/attributes",
        json={
            "items": [
                {
                    "attribute_id": 93,
                    "attribute_code": "color",
                    "frontend_input": "select",
                    "backend_type": "int",
                    "options": [
                        {"label": " ", "value": ""},
                        {"label": "Red", "value": "12"},
                        {"label": "Blue", "value": "13"},
                    ],
                }
            ]
        },
    )


def writes(m):
    return [r for r in m.request_history if r.method in ("POST", "PUT", "DELETE") and "token" not in r.url]


def test_import_products_second_run_reports_all_skipped():
    rows = [
        {"sku": "A", "name": "Shirt A", "price": 10, "status": 1, "attributes": {"color": "Red"}},
        {"sku": "B", "name": "Shirt B", "price": 12.5, "visibility": 4},
    ]
    existing = [
        {
            "sku": "A",
            "type_id": "simple",
            "attribute_set_id": 4,
            "name": "Shirt A",
            "price": "10.000000",
            "status": 1,
            "extension_attributes": {"website_ids": [1]},
            "custom_attributes": [{"attribute_code": "color", "value": "12"}],
        },
        {
            "sku": "B",
            "type_id": "simple",
            "attribute_set_id": 4,
            "name": "Shirt B",
            "price": 12.5,
            "visibility": "4",
            "extension_attributes": {"website_ids": ["1"]},
            "custom_attributes": [],
        },
    ]
    with requests_mock.Mocker() as m:
        mock_catalog(m, products=existing)
        result = import_products(make_resource(), rows)

    assert result == UploadResult(succeeded=0, failed=0, skipped_unchanged=2)
    assert writes(m) == []


def test_import_prices_uses_base_prices_not_products_endpoint():
    with requests_mock.Mocker() as m:
        m.post(f"{BASE}/integration/admin/token", json="token")
        for info in ("base-prices-information", "special-price-information", "tier-prices-information"):
            m.post(f"{BASE}/products/{info}", json=[])
        m.post(f"{BASE}/products/base-prices", json=[])
        result = import_prices(make_resource(), [{"sku": "A", "price": 9.99}], mode="bulk")

    assert result == UploadResult(succeeded=1, failed=0)
    sent = writes(m)[-1]
    assert sent.url == f"{BASE}/products/base-prices"
    assert sent.json() == {"prices": [{"sku": "A", "price": 9.99, "store_id": 0}]}
    assert not any(r.url.startswith(f"{BASE}/products/A") for r in m.request_history)
    assert not any("async/bulk" in r.url for r in m.request_history)


def test_import_products_bulk_mode_submits_async_bulk():
    with requests_mock.Mocker() as m:
        mock_catalog(m)
        m.post("https://shop.test/rest/all/async/bulk/V1/products", json={"bulk_uuid": "u1"})
        m.get(f"{BASE}/bulk/u1/detailed-status", json={"operations_list": [{"id": 0, "status": 1}]})
        result = import_products(make_resource(), [{"sku": "NEW", "name": "New"}], mode="bulk")

    assert result == UploadResult(succeeded=1, failed=0)
    bulk_request = writes(m)[0]
    assert bulk_request.url == "https://shop.test/rest/all/async/bulk/V1/products"
    assert bulk_request.json()[0]["product"]["sku"] == "NEW"


def test_validation_failures_are_counted_as_failed():
    rows = [
        {"source_code": "eu", "name": "EU", "country_id": "FR", "postcode": "75001"},
        {"source_code": "us", "name": "US"},
    ]
    with requests_mock.Mocker() as m:
        m.post(f"{BASE}/integration/admin/token", json="token")
        m.post(f"{BASE}/inventory/sources", json={})
        result = import_sources(make_resource(), rows)

    assert (result.succeeded, result.failed) == (1, 1)
    assert result.errors[0]["row_ids"] == ["us"]
    assert result.errors[0]["status"] == "failed"
    assert "country_id" in result.errors[0]["message"]
    assert len(writes(m)) == 1


class RecordingLogger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)

    warning = info


def test_to_materialize_result_raises_above_ratio_after_metadata(monkeypatch):
    logger = RecordingLogger()
    monkeypatch.setattr(importers, "get_dagster_logger", lambda: logger)
    result = UploadResult(succeeded=1, failed=1, skipped_unchanged=3)

    materialized = to_materialize_result(result)
    assert materialized.metadata["failed"] == 1
    assert materialized.metadata["skipped_unchanged"] == 3

    logger.messages.clear()
    with pytest.raises(MagentoImportError):
        to_materialize_result(result, fail_on_error_ratio=0.1)
    assert any("'failed': 1" in message for message in logger.messages)


def test_importer_logs_counts_before_its_own_ratio_raise(monkeypatch):
    logger = RecordingLogger()
    monkeypatch.setattr(importers, "get_dagster_logger", lambda: logger)
    rows = [{"source_code": "us", "name": "US"}]
    with requests_mock.Mocker() as m:
        m.post(f"{BASE}/integration/admin/token", json="token")
        with pytest.raises(MagentoImportError):
            import_sources(make_resource(), rows, fail_on_error_ratio=0.5)
    assert any("'failed': 1" in message for message in logger.messages)


def configurable_row(sku, children):
    return {
        "sku": sku,
        "type": "configurable",
        "name": sku,
        "configurable_attributes": ["color"],
        "variations": [
            {"sku": child, "attributes": {"color": color}} for child, color in children
        ],
    }


def test_row_with_several_operations_counts_once():
    rows = [configurable_row("P1", [("P1-R", "Red"), ("P1-B", "Blue")]), {"sku": "S1", "name": "S1"}]
    with requests_mock.Mocker() as m:
        mock_catalog(m)
        m.post(f"{BASE}/products", json={})
        m.post(f"{BASE}/configurable-products/P1/options", json=1)
        m.post(
            f"{BASE}/configurable-products/P1/child",
            [{"json": True}, {"status_code": 400, "json": {"message": "child missing"}}],
        )
        result = import_products(make_resource(), rows)

    assert (result.succeeded, result.failed) == (1, 1)
    assert [error["row_ids"] for error in result.errors] == [["P1"]]
    assert "child missing" in result.errors[0]["message"]


def test_products_link_children_after_parents():
    rows = [
        configurable_row("P1", [("C1", "Red")]),
        {"sku": "C1", "name": "C1", "attributes": {"color": "Red"}},
    ]
    with requests_mock.Mocker() as m:
        mock_catalog(m)
        m.post(f"{BASE}/products", json={})
        m.post(f"{BASE}/configurable-products/P1/options", json=1)
        m.post(f"{BASE}/configurable-products/P1/child", json=True)
        result = import_products(make_resource(), rows)

    assert result == UploadResult(succeeded=2, failed=0)
    urls = [r.url for r in writes(m)]
    last_product = max(i for i, url in enumerate(urls) if url == f"{BASE}/products")
    first_link = min(i for i, url in enumerate(urls) if "configurable-products" in url)
    assert last_product < first_link


def test_existing_configurable_links_only_children_not_yet_attached():
    # Verified live on 2.4.9: POST configurable-products/{sku}/child for a
    # child that is already linked answers 400 "The product is already
    # attached.", so a rerun failed every configurable row.
    parent = {**existing_product("P1", 5), "type_id": "configurable"}
    rows = [configurable_row("P1", [("C1", "Red"), ("C2", "Blue")])]
    with requests_mock.Mocker() as m:
        mock_catalog(m, products=[parent])
        m.put(f"{BASE}/products/P1", json={})
        m.post(f"{BASE}/configurable-products/P1/options", json=1)
        m.get(f"{BASE}/configurable-products/P1/children", json=[{"sku": "C1"}])
        m.post(f"{BASE}/configurable-products/P1/child", json=True)
        result = import_products(make_resource(), rows)

    assert result == UploadResult(succeeded=1, failed=0)
    child_posts = [r.json() for r in writes(m) if r.url.endswith("/child")]
    assert child_posts == [{"childSku": "C2"}]


def test_plan_skipped_rows_count_as_skipped_unchanged():
    existing = [{"sku": "A", "type_id": "simple", "attribute_set_id": 4, "custom_attributes": []}]
    with requests_mock.Mocker() as m:
        mock_catalog(m, products=existing)
        result = import_products(
            make_resource(), [{"sku": "A", "name": "Renamed"}], diff=False, behavior="create_only"
        )

    assert result == UploadResult(succeeded=0, failed=0, skipped_unchanged=1)
    assert writes(m) == []


def test_duplicate_price_skus_merge_last_wins():
    rows = [
        {"sku": "A", "price": 1, "special_price": 0.5},
        {"sku": "A", "price": 2},
    ]
    with requests_mock.Mocker() as m:
        m.post(f"{BASE}/integration/admin/token", json="token")
        for info in ("base-prices-information", "special-price-information", "tier-prices-information"):
            m.post(f"{BASE}/products/{info}", json=[])
        m.post(f"{BASE}/products/base-prices", json=[])
        m.post(f"{BASE}/products/special-price", json=[])
        result = import_prices(make_resource(), rows)

    assert result == UploadResult(succeeded=1, failed=0, skipped_unchanged=1)
    sent = {r.url: r.json() for r in writes(m) if "information" not in r.url}
    assert sent[f"{BASE}/products/base-prices"]["prices"][0]["price"] == 2
    assert sent[f"{BASE}/products/special-price"]["prices"][0]["price"] == 0.5


def test_attribute_sets_converge_in_passes():
    default = {"attribute_set_name": "Default", "attribute_set_id": 4}
    apparel = {"attribute_set_name": "Apparel", "attribute_set_id": 10}
    with requests_mock.Mocker() as m:
        m.post(f"{BASE}/integration/admin/token", json="token")
        m.get(
            f"{BASE}/eav/attribute-sets/list",
            [{"json": {"items": [default]}}, {"json": {"items": [default, apparel]}}],
        )
        m.get(
            f"{BASE}/products/attribute-sets/groups/list",
            [
                {"json": {"items": []}},
                {"json": {"items": [{"attribute_group_name": "Details", "attribute_group_id": 20}]}},
            ],
        )
        m.post(f"{BASE}/products/attribute-sets", json={"attribute_set_id": 10})
        m.post(f"{BASE}/products/attribute-sets/groups", json={"attribute_group_id": 20})
        m.post(f"{BASE}/products/attribute-sets/attributes", json=1)
        result = import_attribute_sets(
            make_resource(), [{"name": "Apparel", "groups": {"Details": ["color"]}}]
        )

    assert result == UploadResult(succeeded=1, failed=0)
    assert [r.url for r in writes(m)] == [
        f"{BASE}/products/attribute-sets",
        f"{BASE}/products/attribute-sets/groups",
        f"{BASE}/products/attribute-sets/attributes",
    ]


def test_auth_error_propagates_from_importer():
    rows = [{"source_code": "eu", "name": "EU", "country_id": "FR", "postcode": "75001"}]
    with requests_mock.Mocker() as m:
        m.post(f"{BASE}/integration/admin/token", status_code=401, json={"message": "bad"})
        with pytest.raises(MagentoAuthError):
            import_sources(make_resource(), rows)


def test_source_items_skip_unchanged_pairs_and_keep_last_duplicate():
    rows = [
        {"sku": "A", "source_code": "eu", "quantity": 1, "status": 1},
        {"sku": "A", "source_code": "eu", "quantity": 5, "status": 1},
        {"sku": "B", "source_code": "eu", "quantity": 3, "status": 1},
    ]
    current = [
        {"sku": "A", "source_code": "eu", "quantity": 5.0, "status": 1},
        {"sku": "B", "source_code": "eu", "quantity": 2.0, "status": 1},
    ]
    with requests_mock.Mocker() as m:
        m.post(f"{BASE}/integration/admin/token", json="token")
        m.get(f"{BASE}/inventory/source-items", json={"items": current})
        m.post(f"{BASE}/inventory/source-items", json=[])
        result = import_source_items(make_resource(), rows)

    assert result == UploadResult(succeeded=1, failed=0, skipped_unchanged=2)
    assert writes(m)[0].json() == {
        "sourceItems": [{"sku": "B", "source_code": "eu", "quantity": 3.0, "status": 1}]
    }


def test_stock_source_links_read_stock_ids_when_not_given():
    with requests_mock.Mocker() as m:
        m.post(f"{BASE}/integration/admin/token", json="token")
        m.get(f"{BASE}/inventory/stocks", json={"items": [{"name": "EU", "stock_id": 2}]})
        m.post(f"{BASE}/inventory/stock-source-links", json=[])
        result = import_stock_source_links(
            make_resource(), [{"stock": "EU", "source_code": "eu", "priority": 1}]
        )

    assert result == UploadResult(succeeded=1, failed=0)
    assert writes(m)[0].json() == {"links": [{"stock_id": 2, "source_code": "eu", "priority": 1}]}


def existing_product(sku, category_id):
    return {
        "sku": sku,
        "type_id": "simple",
        "attribute_set_id": 4,
        "name": sku,
        "extension_attributes": {
            "website_ids": [1],
            "category_links": [{"position": 0, "category_id": str(category_id)}],
        },
        "custom_attributes": [],
    }


def test_product_website_or_category_change_is_not_skipped():
    rows = [
        {"sku": "A", "name": "A", "categories": ["Women"]},
        {"sku": "B", "name": "B", "categories": ["Men"], "websites": ["base", "fr"]},
        {"sku": "C", "name": "C", "categories": ["Men"]},
    ]
    existing = [existing_product("A", 5), existing_product("B", 5), existing_product("C", 5)]
    with requests_mock.Mocker() as m:
        mock_catalog(m, products=existing)
        m.put(f"{BASE}/products/A", json={})
        m.put(f"{BASE}/products/B", json={})
        result = import_products(make_resource(), rows)

    assert result == UploadResult(succeeded=2, failed=0, skipped_unchanged=1)
    assert [r.url for r in writes(m)] == [f"{BASE}/products/A", f"{BASE}/products/B"]


def test_product_with_store_values_is_never_skipped():
    rows = [
        {"sku": "A", "name": "A", "categories": ["Men"], "store_values": {"fr": {"name": "A fr"}}}
    ]
    with requests_mock.Mocker() as m:
        mock_catalog(m, products=[existing_product("A", 5)])
        m.put(f"{BASE}/products/A", json={})
        m.put("https://shop.test/rest/fr/V1/products/A", json={})
        result = import_products(make_resource(), rows)

    assert result == UploadResult(succeeded=1, failed=0)
    assert [r.url for r in writes(m)] == [
        f"{BASE}/products/A",
        "https://shop.test/rest/fr/V1/products/A",
    ]


def test_price_rows_for_different_stores_are_both_written():
    rows = [{"sku": "A", "price": 1, "store_id": 0}, {"sku": "A", "price": 2, "store_id": 1}]
    with requests_mock.Mocker() as m:
        m.post(f"{BASE}/integration/admin/token", json="token")
        for info in ("base-prices-information", "special-price-information", "tier-prices-information"):
            m.post(f"{BASE}/products/{info}", json=[])
        m.post(f"{BASE}/products/base-prices", json=[])
        result = import_prices(make_resource(), rows)

    assert result == UploadResult(succeeded=2, failed=0)
    assert writes(m)[-1].json() == {
        "prices": [{"sku": "A", "price": 1, "store_id": 0}, {"sku": "A", "price": 2, "store_id": 1}]
    }


def test_failed_parent_sends_no_configurable_requests():
    with requests_mock.Mocker() as m:
        mock_catalog(m)
        m.post(f"{BASE}/products", status_code=400, json={"message": "invalid product"})
        m.post(f"{BASE}/configurable-products/P1/options", json=1)
        m.post(f"{BASE}/configurable-products/P1/child", json=True)
        result = import_products(make_resource(), [configurable_row("P1", [("C1", "Red")])])

    assert (result.succeeded, result.failed) == (0, 1)
    assert not any("configurable-products/" in r.url for r in m.request_history)


def disable_snapshot(status):
    snap = existing_product("A", 5)
    snap["status"] = status
    return snap


def test_disable_writes_enabled_product_even_when_fields_match():
    with requests_mock.Mocker() as m:
        mock_catalog(m, products=[disable_snapshot(1)])
        m.put(f"{BASE}/products/A", json={})
        result = import_products(
            make_resource(), [{"sku": "A", "name": "A", "categories": ["Men"]}], behavior="disable"
        )

    assert result == UploadResult(succeeded=1, failed=0)
    assert writes(m)[0].json()["product"] == {"sku": "A", "status": 2}


def test_disable_skips_already_disabled_product():
    with requests_mock.Mocker() as m:
        mock_catalog(m, products=[disable_snapshot("2")])
        result = import_products(make_resource(), [{"sku": "A", "status": 1}], behavior="disable")

    assert result == UploadResult(succeeded=0, failed=0, skipped_unchanged=1)
    assert writes(m) == []


TYPE_ERROR_400 = {
    "message": (
        'Error occurred during "custom_attributes" processing. '
        'Attribute "default_sort_by" has invalid value. '
        'The "string" value\'s type is invalid. '
        'The "string[]" type was expected. Verify and try again.'
    )
}


def _mock_category_tree(m):
    m.post(f"{BASE}/integration/admin/token", json="token")
    m.get(
        f"{BASE}/categories",
        json={
            "id": 1,
            "name": "Root Catalog",
            "children_data": [
                {"id": 2, "name": "Default Category", "children_data": [{"id": 5, "name": "Men"}]}
            ],
        },
    )


def _put_bodies(m, sku_or_id):
    return [
        request.json()
        for request in m.request_history
        if request.method == "PUT" and request.url.endswith(f"/categories/{sku_or_id}")
    ]


def test_categories_retry_without_default_sort_by_on_246_type_error():
    """Magento 2.4.6 types default_sort_by as string[] (2.4.9: string), so
    the plain string is rejected there with 400 - and 2.4.6 stores nothing
    for the array shape either, so there is no shape to negotiate: rows
    failing with exactly that error are re-planned without the key and run
    again, instead of failing the whole import over one unwritable
    attribute."""
    from dagster_magento.importers import import_categories
    from dagster_magento.models import CategoryRow

    row = CategoryRow(
        path="Default Category/Men",
        attributes={"default_sort_by": "position", "description": "Men"},
    )
    with requests_mock.Mocker() as m:
        _mock_category_tree(m)
        m.put(
            f"{BASE}/categories/5",
            [{"status_code": 400, "json": TYPE_ERROR_400}, {"status_code": 200, "json": {"id": 5}}],
        )

        result = import_categories(make_resource(), [row])

    assert result.failed == 0
    assert result.succeeded == 1
    bodies = _put_bodies(m, 5)
    assert len(bodies) == 2
    assert bodies[0]["category"]["custom_attributes"] == [
        {"attribute_code": "default_sort_by", "value": "position"},
        {"attribute_code": "description", "value": "Men"},
    ]
    assert bodies[1]["category"]["custom_attributes"] == [
        {"attribute_code": "description", "value": "Men"}
    ]


def test_categories_do_not_retry_other_400s():
    """A 400 that is not the default_sort_by type error fails the row
    loudly: the retry must not mask real problems."""
    from dagster_magento.importers import import_categories
    from dagster_magento.models import CategoryRow

    row = CategoryRow(
        path="Default Category/Men",
        attributes={"default_sort_by": "position", "description": "Men"},
    )
    with requests_mock.Mocker() as m:
        _mock_category_tree(m)
        m.put(f"{BASE}/categories/5", status_code=400, json={"message": "Something else broke"})

        result = import_categories(make_resource(), [row])

    assert result.failed == 1
    assert len(_put_bodies(m, 5)) == 1


def test_categories_retry_reports_failure_when_it_still_fails():
    """The retry runs once: if the row still fails without the key, it
    stays failed instead of looping."""
    from dagster_magento.importers import import_categories
    from dagster_magento.models import CategoryRow

    row = CategoryRow(
        path="Default Category/Men",
        attributes={"default_sort_by": "position", "description": "Men"},
    )
    with requests_mock.Mocker() as m:
        _mock_category_tree(m)
        m.put(
            f"{BASE}/categories/5",
            [
                {"status_code": 400, "json": TYPE_ERROR_400},
                {"status_code": 400, "json": {"message": "Something else broke"}},
            ],
        )

        result = import_categories(make_resource(), [row])

    assert result.failed == 1
    assert len(_put_bodies(m, 5)) == 2


def test_partial_update_row_keeps_explicitness_through_validation():
    # The importer re-validates rows it is given; a ProductRow instance must
    # keep which fields its caller actually set, or every default would be
    # sent again as if it were explicit.
    from dagster_magento.models import ProductRow

    existing = [
        {
            "sku": "C1",
            "type_id": "configurable",
            "attribute_set_id": 9,
            "name": "Old",
            "extension_attributes": {"website_ids": [1, 2]},
            "custom_attributes": [],
        }
    ]
    with requests_mock.Mocker() as m:
        mock_catalog(m, products=existing)
        m.put(f"{BASE}/products/C1", json={})
        result = import_products(make_resource(), [ProductRow(sku="C1", name="New")], use_bridge="never")

    assert result == UploadResult(succeeded=1, failed=0)
    [put] = writes(m)
    assert put.json() == {"product": {"sku": "C1", "name": "New", "custom_attributes": []}}


def test_sync_grouped_parent_listed_first_is_saved_after_its_child():
    rows = [
        {"sku": "G1", "type": "grouped", "grouped_links": [{"sku": "C1"}]},
        {"sku": "C1", "name": "Child"},
    ]
    with requests_mock.Mocker() as m:
        mock_catalog(m)
        m.post(f"{BASE}/products", json={})
        result = import_products(make_resource(), rows, mode="sync", use_bridge="never")

    assert result.failed == 0
    assert [r.json()["product"]["sku"] for r in writes(m)] == ["C1", "G1"]
