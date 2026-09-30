import requests
import requests_mock
import pytest

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


def test_retries_503_three_times_with_backoff_then_raises(monkeypatch):
    # Patching random.uniform to 0 strips the jitter so the recorded sleeps
    # are exactly the base backoff values: 0.5 * 2**0, 0.5 * 2**1, 0.5 * 2**2.
    monkeypatch.setattr("dagster_magento.resource.random.uniform", lambda a, b: 0)
    resource = make_resource()
    sleeps = []
    resource._sleep = sleeps.append

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get(
            "https://shop.test/rest/all/V1/store/storeConfigs",
            [
                {"status_code": 503, "json": {"message": "unavailable"}},
                {"status_code": 503, "json": {"message": "unavailable"}},
                {"status_code": 503, "json": {"message": "unavailable"}},
                {"status_code": 503, "json": {"message": "unavailable"}},
            ],
        )
        with pytest.raises(requests.exceptions.HTTPError):
            resource.get("store/storeConfigs")

    data_requests = [r for r in m.request_history if r.path_url.endswith("/storeConfigs")]
    assert len(data_requests) == 4
    assert sleeps == [0.5, 1, 2]


def test_honours_retry_after_header_on_429(monkeypatch):
    monkeypatch.setattr("dagster_magento.resource.random.uniform", lambda a, b: 0)
    resource = make_resource()
    sleeps = []
    resource._sleep = sleeps.append

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get(
            "https://shop.test/rest/all/V1/store/storeConfigs",
            [
                {
                    "status_code": 429,
                    "json": {"message": "too many requests"},
                    "headers": {"Retry-After": "3"},
                },
                {"status_code": 200, "json": [{"id": 1}]},
            ],
        )
        result = resource.get("store/storeConfigs")

    assert result == [{"id": 1}]
    assert sleeps == [3.0]


def test_does_not_retry_400():
    resource = make_resource()
    sleeps = []
    resource._sleep = sleeps.append

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get(
            "https://shop.test/rest/all/V1/store/storeConfigs",
            status_code=400,
            json={"message": "bad request"},
        )
        with pytest.raises(requests.exceptions.HTTPError):
            resource.get("store/storeConfigs")

    data_requests = [r for r in m.request_history if r.path_url.endswith("/storeConfigs")]
    assert len(data_requests) == 1
    assert sleeps == []


def test_401_refresh_counter_is_per_request():
    resource = make_resource()
    # Prime the token so both calls start with a cached token rather than
    # consuming the initial fetch - isolates the assertion to the 401 retry
    # path, which must refresh once per _request call, not once ever.
    resource._token = "primed-token"

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            [
                {"json": "fresh-token-1", "status_code": 200},
                {"json": "fresh-token-2", "status_code": 200},
            ],
        )
        m.get(
            "https://shop.test/rest/all/V1/store/storeConfigs",
            [
                {"status_code": 401, "json": {"message": "expired"}},
                {"json": [{"id": 1}], "status_code": 200},
            ],
        )
        m.get(
            "https://shop.test/rest/all/V1/products",
            [
                {"status_code": 401, "json": {"message": "expired"}},
                {"json": {"items": []}, "status_code": 200},
            ],
        )
        first = resource.get("store/storeConfigs")
        second = resource.get("products")

    assert first == [{"id": 1}]
    assert second == {"items": []}
    token_requests = [
        r for r in m.request_history if r.path_url.endswith("/integration/admin/token")
    ]
    assert len(token_requests) == 2


def test_store_code_overrides_store_view_in_url():
    resource = make_resource()

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get(
            "https://shop.test/rest/fr/V1/products",
            json={"items": []},
        )
        result = resource.get("products", store_code="fr")

    assert result == {"items": []}
    data_requests = [r for r in m.request_history if r.path_url.endswith("/products")]
    assert len(data_requests) == 1
    assert data_requests[0].path_url == "/rest/fr/V1/products"


def test_put_and_delete_send_bearer_and_method():
    resource = make_resource()

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.put(
            "https://shop.test/rest/all/V1/products/SKU-1",
            status_code=200,
            json={"sku": "SKU-1"},
        )
        m.delete(
            "https://shop.test/rest/all/V1/products/SKU-1",
            status_code=200,
            json=True,
        )
        put_response = resource.put("products/SKU-1", {"product": {"sku": "SKU-1"}})
        delete_response = resource.delete("products/SKU-1")

    assert put_response.status_code == 200
    assert delete_response.status_code == 200

    put_requests = [r for r in m.request_history if r.method == "PUT"]
    delete_requests = [r for r in m.request_history if r.method == "DELETE"]
    assert put_requests[0].json() == {"product": {"sku": "SKU-1"}}
    assert put_requests[0].headers["Authorization"] == "Bearer fake-token-123"
    assert delete_requests[0].headers["Authorization"] == "Bearer fake-token-123"


TOKEN = "https://shop.test/rest/all/V1/integration/admin/token"
CATEGORIES = "https://shop.test/rest/all/V1/categories"


def _quiet_resource():
    resource = make_resource()
    resource._sleep = lambda seconds: None
    return resource


def test_post_is_not_retried_on_a_gateway_error():
    """A 502/503/504 after a POST may come from a gateway that timed out while
    Magento committed the write: retrying would create a second category,
    option or bulk. POST therefore only retries 429."""
    resource = _quiet_resource()
    with requests_mock.Mocker() as m:
        m.post(TOKEN, json="fake-token-123")
        created = m.post(CATEGORIES, [{"status_code": 502}, {"status_code": 200, "json": {"id": 7}}])

        with pytest.raises(requests.exceptions.HTTPError):
            resource.post("categories", {"category": {"name": "Men"}})

    assert created.call_count == 1


def test_get_is_still_retried_on_a_gateway_error():
    resource = _quiet_resource()
    with requests_mock.Mocker() as m:
        m.post(TOKEN, json="fake-token-123")
        read = m.get(CATEGORIES, [{"status_code": 502}, {"status_code": 200, "json": {"id": 1}}])

        assert resource.get("categories") == {"id": 1}

    assert read.call_count == 2


def test_post_is_retried_on_429():
    resource = _quiet_resource()
    with requests_mock.Mocker() as m:
        m.post(TOKEN, json="fake-token-123")
        created = m.post(CATEGORIES, [{"status_code": 429}, {"status_code": 200, "json": {"id": 7}}])

        assert resource.post("categories", {"category": {"name": "Men"}}).json() == {"id": 7}

    assert created.call_count == 2
