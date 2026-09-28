from urllib.parse import parse_qs, urlparse

import requests_mock

from dagster_magento.diff import (
    normalize,
    snapshot_media,
    snapshot_prices,
    snapshot_products,
    snapshot_source_items,
    split_changed,
)
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
