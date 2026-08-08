import time

import requests
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field

from dagster_magento.upload import UploadResult, chunk_rows, run_upload


class MagentoAuthError(Exception):
    """Raised when fetching a Magento admin token fails - never caught by
    upload_rows' per-row/chunk retry logic, since a bad credential or a
    locked account should abort immediately, not be retried once per row."""


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
        try:
            response = requests.post(
                self._url("integration/admin/token"),
                json={"username": self.username, "password": self.password},
                timeout=30,
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise MagentoAuthError(
                f"Failed to fetch Magento admin token (store_view={self.store_view}): {error}"
            ) from error
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

    def get_paginated(
        self,
        endpoint: str,
        params: dict | None = None,
        page_size: int = 1000,
        response_key: str = "items",
    ) -> list:
        logger = get_dagster_logger()
        base_params = dict(params or {})
        page = 1
        items = []

        while True:
            page_params = {
                **base_params,
                "searchCriteria[page_size]": page_size,
                "searchCriteria[current_page]": page,
            }
            response = self.get(endpoint, params=page_params)
            if not isinstance(response, dict):
                logger.warning(
                    f"get_paginated({endpoint}): expected a dict response with "
                    f"key '{response_key}', got {type(response).__name__} - "
                    f"this endpoint may not support search-criteria pagination"
                )
                break
            if response_key not in response:
                logger.warning(
                    f"get_paginated({endpoint}): response_key '{response_key}' not "
                    f"found in response (keys: {list(response.keys())}) - stopping pagination"
                )
                break
            page_items = response.get(response_key, [])
            logger.info(f"Fetched page {page} of {endpoint} ({len(page_items)} items)")
            items.extend(page_items)

            if len(page_items) < page_size:
                break
            page += 1

        logger.info(f"Pagination complete for {endpoint}: {len(items)} items across {page} pages")
        return items

    def post(self, endpoint: str, payload: dict) -> requests.Response:
        return self._request("POST", endpoint, json=payload)

    def upload_rows(
        self,
        endpoint: str,
        rows: list,
        chunk_size: int = 200,
        wrap_key: str | None = None,
        row_id_field: str = "sku",
    ) -> UploadResult:
        logger = get_dagster_logger()

        if wrap_key is None:
            chunks = [[row] for row in rows]

            def send(chunk):
                self.post(endpoint, chunk[0])
        else:
            chunks = chunk_rows(rows, chunk_size)

            def send(chunk):
                self.post(endpoint, {wrap_key: chunk})

        return run_upload(chunks, send, row_id_field, logger)
