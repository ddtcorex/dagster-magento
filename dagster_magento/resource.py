import time

import requests
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field


class MagentoResource(ConfigurableResource):
    base_url: str
    username: str
    password: str = Field(repr=False, json_schema_extra={"dagster__is_secret": True})
    store_view: str
    verbose_logging: bool = False

    _token: str | None = None

    def _url(self, endpoint: str) -> str:
        return f"{self.base_url}/rest/{self.store_view}/V1/{endpoint}"

    def _fetch_token(self) -> str:
        logger = get_dagster_logger()
        logger.info(f"Fetching Magento admin token (store_view={self.store_view})")
        response = requests.post(
            self._url("integration/admin/token"),
            json={"username": self.username, "password": self.password},
            timeout=30,
        )
        response.raise_for_status()
        self._token = response.json()
        return self._token

    def _send(self, method: str, url: str, token: str, **kwargs) -> requests.Response:
        # `kwargs` here is only ever `params=` or `json=` from `_request` below -
        # the Authorization header is added right here, never passed through
        # logging, so logging `kwargs` anywhere is safe by construction.
        headers = {"Authorization": f"Bearer {token}"}
        return requests.request(method, url, headers=headers, timeout=30, **kwargs)

    def _request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        logger = get_dagster_logger()

        if self._token is None:
            self._fetch_token()

        url = self._url(endpoint)
        params = kwargs.get("params")
        # params/store_view are small and always safe to log; the request/
        # response BODY is gated behind verbose_logging below since a single
        # upload_rows() call can carry hundreds of thousands of rows.
        logger.debug(f"{method} {endpoint} store_view={self.store_view} params={params}")

        if self.verbose_logging and "json" in kwargs:
            logger.debug(f"{method} {endpoint} request body: {kwargs['json']}")

        started = time.monotonic()
        response = self._send(method, url, self._token, **kwargs)
        elapsed = time.monotonic() - started
        logger.debug(f"{method} {endpoint} -> {response.status_code} in {elapsed:.2f}s")

        if response.status_code == 401:
            logger.warning(f"Magento token expired (401 on {endpoint}), refreshing and retrying")
            self._fetch_token()
            started = time.monotonic()
            response = self._send(method, url, self._token, **kwargs)
            elapsed = time.monotonic() - started
            logger.debug(f"{method} {endpoint} retry -> {response.status_code} in {elapsed:.2f}s")

        if response.status_code == 503:
            logger.warning(f"Magento returned 503 on {endpoint}, retrying once after backoff")
            time.sleep(1)
            started = time.monotonic()
            response = self._send(method, url, self._token, **kwargs)
            elapsed = time.monotonic() - started
            logger.debug(f"{method} {endpoint} retry -> {response.status_code} in {elapsed:.2f}s")

        if self.verbose_logging:
            logger.debug(f"{method} {endpoint} response body: {response.text[:2000]}")

        response.raise_for_status()
        return response

    def get(self, endpoint: str, params: dict | None = None):
        response = self._request("GET", endpoint, params=params)
        return response.json()
