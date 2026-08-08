import logging

import pytest
import requests_mock
from dagster_magento.resource import MagentoAuthError, MagentoResource
from dagster_magento.upload import UploadResult


def make_resource(**overrides):
    defaults = dict(
        base_url="https://shop.test",
        username="admin",
        password="secret-password",
        store_view="all",
    )
    defaults.update(overrides)
    return MagentoResource(**defaults)


def test_repr_never_exposes_the_password():
    resource = make_resource(password="super-secret-password")
    assert "super-secret-password" not in repr(resource)


def test_get_fetches_token_then_calls_endpoint():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get(
            "https://shop.test/rest/all/V1/store/storeConfigs",
            json=[{"id": 1}],
        )
        result = resource.get("store/storeConfigs")

    assert result == [{"id": 1}]
    token_request = m.request_history[0]
    data_request = m.request_history[1]
    assert data_request.headers["Authorization"] == "Bearer fake-token-123"
    assert token_request.json() == {"username": "admin", "password": "secret-password"}


def test_get_reuses_cached_token_across_calls():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get("https://shop.test/rest/all/V1/store/storeConfigs", json=[])
        m.get("https://shop.test/rest/all/V1/products", json={"items": []})

        resource.get("store/storeConfigs")
        resource.get("products")

    token_calls = [r for r in m.request_history if r.path.endswith("/integration/admin/token")]
    assert len(token_calls) == 1


def test_get_refreshes_token_on_401_then_succeeds():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            [
                {"json": "stale-token", "status_code": 200},
                {"json": "fresh-token", "status_code": 200},
            ],
        )
        m.get(
            "https://shop.test/rest/all/V1/store/storeConfigs",
            [
                {"status_code": 401, "json": {"message": "expired"}},
                {"json": [{"id": 1}], "status_code": 200},
            ],
        )
        result = resource.get("store/storeConfigs")

    assert result == [{"id": 1}]
    # requests_mock lowercases `.path`; use `.path_url` to preserve the
    # original-case endpoint segment ("storeConfigs") we're matching on.
    data_requests = [r for r in m.request_history if r.path_url.endswith("/storeConfigs")]
    assert data_requests[0].headers["Authorization"] == "Bearer stale-token"
    assert data_requests[1].headers["Authorization"] == "Bearer fresh-token"


def test_get_retries_once_on_503_then_succeeds():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get(
            "https://shop.test/rest/all/V1/store/storeConfigs",
            [
                {"status_code": 503, "json": {"message": "unavailable"}},
                {"json": [{"id": 1}], "status_code": 200},
            ],
        )
        result = resource.get("store/storeConfigs")

    assert result == [{"id": 1}]
    # requests_mock lowercases `.path`; use `.path_url` to preserve the
    # original-case endpoint segment ("storeConfigs") we're matching on.
    data_requests = [r for r in m.request_history if r.path_url.endswith("/storeConfigs")]
    assert len(data_requests) == 2


def test_get_raises_after_401_retry_still_fails():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get(
            "https://shop.test/rest/all/V1/store/storeConfigs",
            status_code=401,
            json={"message": "still unauthorized"},
        )
        try:
            resource.get("store/storeConfigs")
            assert False, "expected HTTPError"
        except Exception as error:
            assert "401" in str(error)


def test_basic_debug_logging_includes_endpoint_store_view_and_params_without_verbose(caplog):
    resource = make_resource(verbose_logging=False)
    with caplog.at_level(logging.DEBUG):
        with requests_mock.Mocker() as m:
            m.post(
                "https://shop.test/rest/all/V1/integration/admin/token",
                json="fake-token-123",
            )
            m.get(
                "https://shop.test/rest/all/V1/store/storeConfigs",
                json=[{"id": 1}],
            )
            resource.get("store/storeConfigs", params={"foo": "bar"})

    assert "storeConfigs" in caplog.text
    assert "all" in caplog.text  # store_view
    assert "foo" in caplog.text  # params logged unconditionally - small, always safe
    # Response body must NOT be logged without verbose_logging - it can be huge.
    assert '"id": 1' not in caplog.text and "'id': 1" not in caplog.text


def test_verbose_logging_additionally_logs_the_response_body(caplog):
    resource = make_resource(verbose_logging=True)
    with caplog.at_level(logging.DEBUG):
        with requests_mock.Mocker() as m:
            m.post(
                "https://shop.test/rest/all/V1/integration/admin/token",
                json="fake-token-123",
            )
            m.get(
                "https://shop.test/rest/all/V1/store/storeConfigs",
                json=[{"id": 1}],
            )
            resource.get("store/storeConfigs")

    assert '"id": 1' in caplog.text or "'id': 1" in caplog.text


def test_token_value_never_appears_in_logs_even_with_verbose_logging_and_401_retry(caplog):
    resource = make_resource(verbose_logging=True)
    with caplog.at_level(logging.DEBUG):
        with requests_mock.Mocker() as m:
            m.post(
                "https://shop.test/rest/all/V1/integration/admin/token",
                [
                    {"json": "super-secret-stale-token", "status_code": 200},
                    {"json": "super-secret-fresh-token", "status_code": 200},
                ],
            )
            m.get(
                "https://shop.test/rest/all/V1/store/storeConfigs",
                [
                    {"status_code": 401, "json": {"message": "expired"}},
                    {"json": [{"id": 1}], "status_code": 200},
                ],
            )
            resource.get("store/storeConfigs")

    assert "super-secret-stale-token" not in caplog.text
    assert "super-secret-fresh-token" not in caplog.text
    assert "secret-password" not in caplog.text


def test_fetch_token_failure_raises_magento_auth_error_not_http_error():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            status_code=401,
            json={"message": "Invalid credentials"},
        )
        with pytest.raises(MagentoAuthError):
            resource.get("store/storeConfigs")


def test_upload_rows_aborts_immediately_on_auth_failure_instead_of_retrying_per_row():
    resource = make_resource()
    rows = [{"sku": f"SKU{i}"} for i in range(5)]

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            status_code=401,
            json={"message": "Invalid credentials"},
        )
        with pytest.raises(MagentoAuthError):
            resource.upload_rows("products", rows)

    token_requests = [
        r for r in m.request_history if r.path_url.endswith("/integration/admin/token")
    ]
    assert len(token_requests) == 1  # NOT 5 - must not retry auth per row


def test_get_paginated_stops_after_a_single_partial_page():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get(
            "https://shop.test/rest/all/V1/products",
            json={"items": [{"sku": "A"}, {"sku": "B"}]},
        )
        result = resource.get_paginated("products", page_size=1000)

    assert result == [{"sku": "A"}, {"sku": "B"}]
    data_requests = [r for r in m.request_history if r.path.endswith("/products")]
    assert len(data_requests) == 1
    assert "searchCriteria%5Bpage_size%5D=1000" in data_requests[0].url
    assert "searchCriteria%5Bcurrent_page%5D=1" in data_requests[0].url


def test_get_paginated_continues_to_a_second_page():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        page_one = {"items": [{"sku": f"SKU{i}"} for i in range(3)]}
        page_two = {"items": [{"sku": "SKU-LAST"}]}
        m.get(
            "https://shop.test/rest/all/V1/products",
            [{"json": page_one}, {"json": page_two}],
        )
        result = resource.get_paginated("products", page_size=3)

    assert result == page_one["items"] + page_two["items"]
    data_requests = [r for r in m.request_history if r.path.endswith("/products")]
    assert len(data_requests) == 2
    assert "searchCriteria%5Bcurrent_page%5D=1" in data_requests[0].url
    assert "searchCriteria%5Bcurrent_page%5D=2" in data_requests[1].url


def test_get_paginated_respects_custom_response_key():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get(
            "https://shop.test/rest/all/V1/orders",
            json={"results": [{"id": 1}]},
        )
        result = resource.get_paginated("orders", page_size=50, response_key="results")

    assert result == [{"id": 1}]


def test_post_sends_json_body_with_bearer_token():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.post(
            "https://shop.test/rest/all/V1/products",
            status_code=200,
            json={"sku": "NEW-SKU"},
        )
        response = resource.post("products", {"product": {"sku": "NEW-SKU"}})

    assert response.status_code == 200
    data_requests = [
        r for r in m.request_history if r.path.endswith("/products") and r.method == "POST"
    ]
    assert data_requests[0].json() == {"product": {"sku": "NEW-SKU"}}
    assert data_requests[0].headers["Authorization"] == "Bearer fake-token-123"


def test_post_raises_on_http_error():
    resource = make_resource()
    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.post(
            "https://shop.test/rest/all/V1/products",
            status_code=400,
            json={"message": "Validation failed"},
        )
        try:
            resource.post("products", {"product": {"sku": "BAD"}})
            assert False, "expected HTTPError"
        except Exception as error:
            assert "400" in str(error)


def test_upload_rows_without_wrap_key_posts_one_row_per_request():
    resource = make_resource()
    rows = [{"sku": "A"}, {"sku": "B"}, {"sku": "C"}]

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.post(
            "https://shop.test/rest/all/V1/configurable-products/parent-sku/child",
            status_code=200,
        )
        result = resource.upload_rows("configurable-products/parent-sku/child", rows)

    data_requests = [
        r
        for r in m.request_history
        if r.path.endswith("/configurable-products/parent-sku/child")
    ]
    assert len(data_requests) == 3
    assert [r.json() for r in data_requests] == rows
    assert result == UploadResult(succeeded=3, failed=0, errors=[])


def test_upload_rows_with_wrap_key_groups_into_chunks():
    resource = make_resource()
    rows = [{"sku": f"SKU{i}"} for i in range(5)]

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.post(
            "https://shop.test/rest/all/V1/inventory/source-items",
            status_code=200,
        )
        result = resource.upload_rows(
            "inventory/source-items", rows, chunk_size=2, wrap_key="sourceItems"
        )

    data_requests = [
        r for r in m.request_history if r.path.endswith("/inventory/source-items")
    ]
    assert len(data_requests) == 3  # chunks of 2, 2, 1
    assert data_requests[0].json() == {"sourceItems": rows[0:2]}
    assert data_requests[1].json() == {"sourceItems": rows[2:4]}
    assert data_requests[2].json() == {"sourceItems": rows[4:5]}
    assert result == UploadResult(succeeded=5, failed=0, errors=[])


def test_upload_rows_continues_past_a_failed_chunk_and_reports_row_ids():
    resource = make_resource()
    rows = [{"sku": "A"}, {"sku": "B"}, {"sku": "C"}]

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.post(
            "https://shop.test/rest/all/V1/inventory/source-items",
            [
                {"status_code": 200},
                {"status_code": 400, "json": {"message": "Validation Failed"}},
                {"status_code": 200},
            ],
        )
        result = resource.upload_rows(
            "inventory/source-items", rows, chunk_size=1, wrap_key="sourceItems"
        )

    assert result.succeeded == 2
    assert result.failed == 1
    assert result.errors[0]["row_ids"] == ["B"]
    assert result.errors[0]["status_code"] == 400


def test_upload_rows_uses_custom_row_id_field():
    resource = make_resource()
    rows = [{"id": "CUST-1"}]

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.post(
            "https://shop.test/rest/all/V1/customers",
            status_code=400,
            json={"message": "Bad customer"},
        )
        result = resource.upload_rows("customers", rows, row_id_field="id")

    assert result.errors[0]["row_ids"] == ["CUST-1"]
