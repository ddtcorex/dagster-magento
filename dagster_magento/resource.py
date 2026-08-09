import time

import requests
from dagster import ConfigurableResource, get_dagster_logger
from pydantic import Field

from dagster_magento.bulk import AsyncBulkResult, run_async_upload
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

    def _url(self, endpoint: str, api_prefix: str = "V1") -> str:
        return f"{self.base_url}/rest/{self.store_view}/{api_prefix}/{endpoint}"

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

    def _request(
        self, method: str, endpoint: str, api_prefix: str = "V1", **kwargs
    ) -> requests.Response:
        logger = get_dagster_logger()

        if self._token is None:
            self._fetch_token()

        url = self._url(endpoint, api_prefix)
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

    def post(self, endpoint: str, payload: dict | list) -> requests.Response:
        return self._request("POST", endpoint, json=payload)

    def put(self, endpoint: str, payload: dict) -> requests.Response:
        return self._request("PUT", endpoint, json=payload)

    def delete(self, endpoint: str) -> requests.Response:
        return self._request("DELETE", endpoint)

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

    def upload_rows_async(
        self,
        endpoint: str,
        rows: list,
        chunk_size: int = 200,
        row_id_field: str = "sku",
    ) -> AsyncBulkResult:
        """Submit rows to Magento's core async/bulk API for high-volume writes.

        Each chunk is POSTed as a JSON array to `async/bulk/V1/{endpoint}` -
        Magento queues one operation per array element and returns immediately
        (202) with a bulk_uuid, before the operations actually run. Use
        get_bulk_status() to find out whether they succeeded. There's no
        wrap_key here (unlike upload_rows): async/bulk always takes an array of
        individual operation payloads, one per row.
        """
        logger = get_dagster_logger()
        chunks = chunk_rows(rows, chunk_size)

        def send(chunk):
            response = self._request("POST", endpoint, api_prefix="async/bulk/V1", json=chunk)
            return response.json()

        return run_async_upload(chunks, send, row_id_field, logger)

    def get_bulk_status(self, bulk_uuid: str) -> dict:
        return self.get(f"bulk/{bulk_uuid}/detailed-status")

    def resolve_attribute_options(self, attribute_code: str, labels: list) -> dict:
        """Map select/multiselect attribute option labels to their Magento option_id,
        creating any missing options along the way.

        Every EAV select/multiselect attribute stores an integer option_id
        internally, but data sources (supplier feeds, CSVs, ...) give you the
        human-readable label instead - this resolves labels to ids via
        `GET products/attributes/{code}` (existing options) and
        `POST products/attributes/{code}/options` (for labels with no match),
        matching case-insensitively and trimmed. The returned dict is keyed by
        the exact label strings passed in, so callers can look values up
        without re-normalizing them.

        Concurrent calls for the same attribute_code (e.g. two parallel runs)
        can race and create duplicate options with the same label - Magento's
        add-option endpoint doesn't dedupe. Same limitation as this had in every
        integration that's had to solve it; not addressed here.
        """
        attribute = self.get(f"products/attributes/{attribute_code}")
        existing_by_key = {
            option["label"].strip().casefold(): int(option["value"])
            for option in attribute.get("options", [])
            if option.get("value") not in (None, "")
        }

        result = {}
        created_by_key = {}
        for label in labels:
            key = label.strip().casefold()
            if not key:
                continue
            if key in existing_by_key:
                result[label] = existing_by_key[key]
            elif key in created_by_key:
                result[label] = created_by_key[key]
            else:
                response = self.post(
                    f"products/attributes/{attribute_code}/options",
                    {"option": {"label": label.strip()}},
                )
                option_id = int(response.json())
                created_by_key[key] = option_id
                result[label] = option_id

        return result
