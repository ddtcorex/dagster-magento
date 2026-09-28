from urllib.parse import parse_qs, urlparse

import requests_mock

import pytest

from dagster_magento.diff import (
    normalize,
    price_matches_snapshot,
    product_matches_snapshot,
    snapshot_media,
    snapshot_prices,
    snapshot_products,
    snapshot_source_items,
    split_changed,
)
from dagster_magento.models import PriceRow, ProductRow
from dagster_magento.resolvers import AttributeMeta, ResolveError
from dagster_magento.resource import MagentoResource


def make_resource(**overrides):
    defaults = dict(
        base_url="https://shop.test",
        username="admin",
        password="secret-password",
        store_view="all",
    )
    defaults.update(overrides)
    return MagentoResource(**defaults)


def mock_token(m):
    m.post(
        "https://shop.test/rest/all/V1/integration/admin/token",
        json="fake-token-123",
    )


def request_params(request) -> dict:
    # Uses the raw (case-preserving) URL rather than requests_mock's own
    # `request.query`, which lower-cases every key - `searchCriteria[pageSize]`
    # would otherwise be indistinguishable from a hand-rolled `pagesize`.
    return {k: v[0] for k, v in parse_qs(urlparse(request.url).query).items()}


def test_normalize_decimal_and_datetime():
    assert normalize(10, "decimal") == "10.0000"
    assert normalize("19.99", "decimal") == "19.9900"
    assert normalize(1.00005, "decimal") == "1.0001"
    assert normalize(None, "decimal") is None

    assert normalize("2024-01-15", "datetime") == "2024-01-15 00:00:00"
    assert normalize("2024-01-15 10:30:00", "datetime") == "2024-01-15 10:30:00"
    assert normalize("2024-01-15T12:30:00+02:00", "datetime") == "2024-01-15 10:30:00"
    assert normalize("2024-01-15T10:30:00Z", "datetime") == "2024-01-15 10:30:00"
    assert normalize(None, "datetime") is None

    assert normalize("red, blue,green", "multiselect") == ("blue", "green", "red")
    assert normalize(["green", "red", "blue"], "multiselect") == ("blue", "green", "red")

    assert normalize("42", "int") == 42
    assert normalize(None, "int") is None

    assert normalize("  padded  ", "text") == "padded"
    assert normalize(None, "text") is None


def test_snapshot_products_chunks_skus_by_50():
    resource = make_resource()
    skus = [f"sku-{i}" for i in range(55)]

    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/products",
            [
                {
                    "json": {
                        "items": [
                            {
                                "sku": "sku-0",
                                "name": "First",
                                "price": 10,
                                "custom_attributes": [
                                    {"attribute_code": "color", "value": "49"}
                                ],
                            }
                        ]
                    }
                },
                {"json": {"items": [{"sku": "sku-50", "name": "Last", "price": 20}]}},
            ],
        )
        result = snapshot_products(resource, skus, fields=["name", "price"])

    product_requests = [r for r in m.request_history if r.path.endswith("/v1/products")]
    assert len(product_requests) == 2

    first_params = request_params(product_requests[0])
    assert first_params["searchCriteria[filterGroups][0][filters][0][field]"] == "sku"
    assert first_params["searchCriteria[filterGroups][0][filters][0][value]"] == ",".join(
        skus[:50]
    )
    assert first_params["searchCriteria[filterGroups][0][filters][0][condition_type]"] == "in"
    assert first_params["searchCriteria[pageSize]"] == "50"
    assert first_params["fields"] == "items[sku,name,price,custom_attributes]"

    second_params = request_params(product_requests[1])
    assert second_params["searchCriteria[filterGroups][0][filters][0][value]"] == ",".join(
        skus[50:]
    )
    assert second_params["searchCriteria[pageSize]"] == "5"

    assert result["sku-0"] == {"name": "First", "price": 10, "color": "49"}
    assert result["sku-50"] == {"name": "Last", "price": 20}
    # sku-1..sku-49 and sku-51..sku-54 were never returned by Magento.
    assert "sku-1" not in result
    assert len(result) == 2


def test_snapshot_products_reads_null_items_as_no_match():
    # Verified live on 2.4.9: with a `fields=items[...]` projection and no
    # matching SKU, Magento answers {"items": null}, not an empty list.
    resource = make_resource()

    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get("https://shop.test/rest/all/V1/products", json={"items": None})
        result = snapshot_products(resource, ["new-sku"], fields=["name"])

    assert result == {}


def test_snapshot_products_queries_a_comma_sku_alone_with_eq():
    resource = make_resource()

    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/products",
            json={"items": [{"sku": "a,b", "name": "Comma sku"}]},
        )
        snapshot_products(resource, ["a,b"], fields=["name"])

    params = request_params(m.request_history[-1])
    assert params["searchCriteria[filterGroups][0][filters][0][condition_type]"] == "eq"
    assert params["searchCriteria[filterGroups][0][filters][0][value]"] == "a,b"


def test_snapshot_prices_merges_three_information_endpoints():
    resource = make_resource()

    with requests_mock.Mocker() as m:
        mock_token(m)
        m.post(
            "https://shop.test/rest/all/V1/products/base-prices-information",
            json=[{"sku": "A", "price": 10.0, "store_id": 0}],
        )
        m.post(
            "https://shop.test/rest/all/V1/products/special-price-information",
            json=[
                {
                    "sku": "A",
                    "price": 8.0,
                    "store_id": 0,
                    "price_from": "2024-01-01",
                    "price_to": "2024-01-31",
                }
            ],
        )
        m.post(
            "https://shop.test/rest/all/V1/products/tier-prices-information",
            json=[
                {
                    "sku": "A",
                    "price": 9.0,
                    "price_type": "fixed",
                    "website_id": 0,
                    "customer_group": "ALL GROUPS",
                    "quantity": 5,
                }
            ],
        )
        result = snapshot_prices(resource, ["A"])

    assert result == {
        "A": {
            "base": {0: 10.0},
            "special": [
                {
                    "sku": "A",
                    "price": 8.0,
                    "store_id": 0,
                    "price_from": "2024-01-01",
                    "price_to": "2024-01-31",
                }
            ],
            "tiers": [
                {
                    "sku": "A",
                    "price": 9.0,
                    "price_type": "fixed",
                    "website_id": 0,
                    "customer_group": "ALL GROUPS",
                    "quantity": 5,
                }
            ],
        }
    }

    base_request = next(
        r for r in m.request_history if r.path.endswith("base-prices-information")
    )
    assert base_request.json() == {"skus": ["A"]}


def test_snapshot_prices_leaves_unknown_skus_absent():
    resource = make_resource()

    with requests_mock.Mocker() as m:
        mock_token(m)
        m.post(
            "https://shop.test/rest/all/V1/products/base-prices-information", json=[]
        )
        m.post(
            "https://shop.test/rest/all/V1/products/special-price-information", json=[]
        )
        m.post(
            "https://shop.test/rest/all/V1/products/tier-prices-information", json=[]
        )
        result = snapshot_prices(resource, ["missing"])

    assert result == {}


def test_snapshot_source_items_keys_by_source_code_and_sku():
    resource = make_resource()

    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/inventory/source-items",
            json={
                "items": [
                    {"sku": "A", "source_code": "default", "quantity": 5.0, "status": 1},
                    {"sku": "A", "source_code": "warehouse2", "quantity": 0.0, "status": 0},
                ]
            },
        )
        result = snapshot_source_items(resource, ["A"])

    assert result == {
        ("default", "A"): (5.0, 1),
        ("warehouse2", "A"): (0.0, 0),
    }

    # get_paginated wraps the sku `in` filter this function builds with its
    # own searchCriteria[page_size]/[current_page] convention - assert both
    # halves so a regression in either side is caught here.
    request = [r for r in m.request_history if r.path.endswith("/v1/inventory/source-items")][0]
    params = request_params(request)
    assert params["searchCriteria[filterGroups][0][filters][0][field]"] == "sku"
    assert params["searchCriteria[filterGroups][0][filters][0][value]"] == "A"
    assert params["searchCriteria[filterGroups][0][filters][0][condition_type]"] == "in"
    assert params["searchCriteria[page_size]"] == "200"
    assert params["searchCriteria[current_page]"] == "1"


def test_snapshot_source_items_follows_pages():
    resource = make_resource()
    first_page = [
        {"sku": f"sku-{i}", "source_code": "default", "quantity": float(i), "status": 1}
        for i in range(200)
    ]
    second_page = [
        {"sku": "sku-200", "source_code": "default", "quantity": 3.0, "status": 1},
        {"sku": "sku-201", "source_code": "default", "quantity": 0.0, "status": 0},
    ]

    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/inventory/source-items",
            [
                {"json": {"items": first_page}},
                {"json": {"items": second_page}},
            ],
        )
        result = snapshot_source_items(resource, ["irrelevant-for-this-test"])

    assert len(result) == 202
    assert result[("default", "sku-0")] == (0.0, 1)
    assert result[("default", "sku-201")] == (0.0, 0)

    requests = [r for r in m.request_history if r.path.endswith("/v1/inventory/source-items")]
    assert len(requests) == 2
    assert request_params(requests[0])["searchCriteria[current_page]"] == "1"
    assert request_params(requests[1])["searchCriteria[current_page]"] == "2"


def test_snapshot_media_returns_the_bare_list_response():
    resource = make_resource()

    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/products/sku-a/media",
            json=[{"id": 1, "media_type": "image", "position": 1, "file": "/a/b.jpg"}],
        )
        result = snapshot_media(resource, "sku-a")

    assert result == [{"id": 1, "media_type": "image", "position": 1, "file": "/a/b.jpg"}]


def test_split_changed_skips_identical_and_keeps_new_skus():
    rows = [{"sku": "A", "name": "Same"}, {"sku": "B", "name": "New"}]
    snapshot = {"A": {"name": "Same"}}

    changed, skipped_count = split_changed(
        rows,
        snapshot,
        key=lambda row: row["sku"],
        project=lambda row: {"name": row["name"]},
        project_existing=lambda existing: {"name": existing["name"]},
    )

    assert changed == [{"sku": "B", "name": "New"}]
    assert skipped_count == 1


def test_split_changed_keeps_rows_whose_projected_values_differ():
    rows = [{"sku": "A", "name": "Changed"}]
    snapshot = {"A": {"name": "Old"}}

    changed, skipped_count = split_changed(
        rows,
        snapshot,
        key=lambda row: row["sku"],
        project=lambda row: {"name": row["name"]},
        project_existing=lambda existing: {"name": existing["name"]},
    )

    assert changed == rows
    assert skipped_count == 0


def test_split_changed_treats_multiselect_order_as_equal():
    rows = [{"sku": "A", "colors": "red,blue,green"}]
    snapshot = {"A": {"colors": ["green", "red", "blue"]}}

    changed, skipped_count = split_changed(
        rows,
        snapshot,
        key=lambda row: row["sku"],
        project=lambda row: {"colors": normalize(row["colors"], "multiselect")},
        project_existing=lambda existing: {
            "colors": normalize(existing["colors"], "multiselect")
        },
    )

    assert changed == []
    assert skipped_count == 1


class StubResolver:
    """Only the lookups product_matches_snapshot needs, with fixed ids."""

    def __init__(self):
        self.meta = {
            "color": AttributeMeta(93, "color", "select", "int", "global", {"red": "12", "blue": "13"}),
            "sizes": AttributeMeta(94, "sizes", "multiselect", "varchar", "global", {"s": "1", "m": "2"}),
            "eco": AttributeMeta(95, "eco", "boolean", "int", "global", {}),
        }

    def attribute(self, code):
        if code not in self.meta:
            raise ResolveError(f"unknown attribute: {code}")
        return self.meta[code]

    def option_id(self, code, label, create=True):
        options = self.attribute(code).options
        if label.lower() not in options:
            raise ResolveError(f"unknown option {label}")
        return options[label.lower()]

    def attribute_set_id(self, name):
        return {"Default": 4}[name]

    def website_id(self, code):
        return {"base": 1, "fr": 2}[code]

    def category_id(self, path):
        return {"Men": 5, "Women": 6}[path]


def product_snapshot(**overrides):
    snap = {
        "type_id": "simple",
        "attribute_set_id": 4,
        "name": "Shirt",
        "price": "10.000000",
        "status": 1,
        "extension_attributes": {
            "website_ids": [1],
            "category_links": [{"position": 0, "category_id": "5"}],
        },
        "color": "12",
        "sizes": "2,1",
    }
    snap.update(overrides)
    return snap


def product_row(**overrides):
    row = dict(
        sku="A",
        name="Shirt",
        price=10,
        status=1,
        categories=["Men"],
        attributes={"color": "Red", "sizes": ["S", "M"]},
    )
    row.update(overrides)
    return ProductRow(**row)


def test_product_matches_snapshot_when_everything_it_sends_is_equal():
    assert product_matches_snapshot(product_row(), product_snapshot(), StubResolver())


def test_product_does_not_match_when_a_field_or_attribute_differs():
    resolver = StubResolver()
    assert not product_matches_snapshot(product_row(price=11), product_snapshot(), resolver)
    assert not product_matches_snapshot(product_row(), product_snapshot(color="13"), resolver)


def test_product_does_not_match_when_websites_or_categories_differ():
    resolver = StubResolver()
    assert not product_matches_snapshot(product_row(websites=["base", "fr"]), product_snapshot(), resolver)
    assert not product_matches_snapshot(product_row(categories=["Women"]), product_snapshot(), resolver)
    assert not product_matches_snapshot(product_row(), product_snapshot(extension_attributes=None), resolver)


@pytest.mark.parametrize(
    "part",
    [
        {"store_values": {"fr": {"name": "Chemise"}}},
        {
            "type": "configurable",
            "configurable_attributes": ["color"],
            "variations": [{"sku": "A-R", "attributes": {"color": "Red"}}],
        },
        {"grouped_links": [{"sku": "B"}]},
        {"downloadable_samples": [{"title": "t", "url": "https://files.test/s.pdf"}]},
    ],
)
def test_product_with_unsnapshotted_parts_never_matches(part):
    assert not product_matches_snapshot(product_row(**part), product_snapshot(), StubResolver())


def test_product_with_an_unknown_option_label_does_not_match():
    row = product_row(attributes={"color": "Green", "sizes": ["S", "M"]})
    assert not product_matches_snapshot(row, product_snapshot(), StubResolver())


def test_product_select_ids_and_boolean_labels_compare_like_the_writer_sends_them():
    # Same value rules as the product writer: a non-string select value is
    # an option id, a boolean label maps to 1/0 (Magento reads back "1").
    row = product_row(attributes={"color": 12, "eco": "Yes"})
    assert product_matches_snapshot(row, product_snapshot(eco="1"), StubResolver())
    assert not product_matches_snapshot(row, product_snapshot(eco="0"), StubResolver())


def price_snapshot():
    return {
        "base": {0: 10.0, 1: 9.0},
        "special": [
            {"sku": "A", "price": 8.0, "store_id": 1, "price_from": "2026-01-01 00:00:00", "price_to": None}
        ],
        "tiers": [
            {
                "sku": "A",
                "price": 7.0,
                "price_type": "fixed",
                "website_id": 0,
                "customer_group": "ALL GROUPS",
                "quantity": 5,
            }
        ],
    }


def test_price_matches_snapshot_per_store_special_and_tiers():
    row = PriceRow(
        sku="A",
        price=9,
        store_id=1,
        special_price=8,
        special_from="2026-01-01",
        tiers=[{"qty": 5, "price": 7}],
    )
    assert price_matches_snapshot(row, price_snapshot())


def test_price_does_not_match_another_store_or_changed_tiers():
    assert not price_matches_snapshot(PriceRow(sku="A", price=9, store_id=0), price_snapshot())
    assert not price_matches_snapshot(PriceRow(sku="A", price=9, store_id=2), price_snapshot())
    assert not price_matches_snapshot(PriceRow(sku="A", tiers=[{"qty": 10, "price": 7}]), price_snapshot())
    assert not price_matches_snapshot(PriceRow(sku="A", special_price=8, store_id=0), price_snapshot())


def test_price_tier_website_resolution():
    snap = price_snapshot()
    snap["tiers"][0]["website_id"] = 2
    row = PriceRow(sku="A", tiers=[{"qty": 5, "price": 7, "website": "fr"}])
    assert price_matches_snapshot(row, snap, website_ids={"fr": 2})
    assert not price_matches_snapshot(row, snap)
