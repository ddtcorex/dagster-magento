import logging
import requests_mock
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
