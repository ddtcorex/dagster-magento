import random
import time
from typing import ClassVar

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

    # Statuses worth retrying: rate-limited (429) and transient upstream/
    # gateway failures (502/503/504). Anything else (400, 401 handled
    # separately, 404, ...) is a data/auth problem a retry cannot fix.
    RETRY_STATUSES: ClassVar[tuple[int, ...]] = (429, 502, 503, 504)
    _RETRY_BASE_DELAY_SECONDS: ClassVar[float] = 0.5
    _MAX_RETRIES: ClassVar[int] = 3

    _token: str | None = None

    def _url(
        self, endpoint: str, api_prefix: str = "V1", store_code: str | None = None
    ) -> str:
        scope = store_code if store_code is not None else self.store_view
        return f"{self.base_url}/rest/{scope}/{api_prefix}/{endpoint}"

    def _sleep(self, seconds: float) -> None:
        # Wrapped so tests can monkeypatch this instead of actually sleeping.
        time.sleep(seconds)

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

    def _send_timed(
        self, method: str, url: str, endpoint: str, logger, label: str = "", **kwargs
    ) -> requests.Response:
        # Shared by the initial send and every retry site (401 refresh,
        # RETRY_STATUSES backoff loop) so the timing/logging shape - and the
        # exact log line text the logging tests assert on - lives in one
        # place instead of being copy-pasted per call site. `label` is ""
        # for the first send and " retry" for every retry, matching the
        # original per-site log lines byte for byte.
        started = time.monotonic()
        response = self._send(method, url, self._token, **kwargs)
        elapsed = time.monotonic() - started
        logger.debug(f"{method} {endpoint}{label} -> {response.status_code} in {elapsed:.2f}s")
        return response

    def _request(
        self,
        method: str,
        endpoint: str,
        api_prefix: str = "V1",
        store_code: str | None = None,
        **kwargs,
    ) -> requests.Response:
        logger = get_dagster_logger()

        if self._token is None:
            self._fetch_token()

        url = self._url(endpoint, api_prefix, store_code)
        params = kwargs.get("params")
        # params/store_view are small and always safe to log; the request/
        # response BODY is gated behind verbose_logging below since a single
        # upload_rows() call can carry hundreds of thousands of rows.
        logger.debug(f"{method} {endpoint} store_view={self.store_view} params={params}")

        if self.verbose_logging and "json" in kwargs:
            logger.debug(f"{method} {endpoint} request body: {kwargs['json']}")

        response = self._send_timed(method, url, endpoint, logger, **kwargs)

        # 401 handling is scoped to this single _request call: one refresh-
        # and-retry per call, never a counter shared across calls, so a
        # second call that also hits a 401 gets its own fresh refresh.
        if response.status_code == 401:
            logger.warning(f"Magento token expired (401 on {endpoint}), refreshing and retrying")
            self._fetch_token()
            response = self._send_timed(method, url, endpoint, logger, " retry", **kwargs)

        attempt = 0
        while response.status_code in self.RETRY_STATUSES and attempt < self._MAX_RETRIES:
            retry_after = self._parse_retry_after(response)
            base_delay = self._RETRY_BASE_DELAY_SECONDS * (2**attempt)
            delay = max(retry_after, base_delay) + random.uniform(0, 0.1)
            logger.warning(
                f"Magento returned {response.status_code} on {endpoint}, "
                f"retrying (attempt {attempt + 1}/{self._MAX_RETRIES}) after {delay:.2f}s"
            )
            self._sleep(delay)
            response = self._send_timed(method, url, endpoint, logger, " retry", **kwargs)
            attempt += 1

        if self.verbose_logging:
            logger.debug(f"{method} {endpoint} response body: {response.text[:2000]}")

        response.raise_for_status()
        return response

    @staticmethod
    def _parse_retry_after(response: requests.Response) -> float:
        # Magento sends Retry-After as an integer number of seconds (not the
        # HTTP-date form) on 429 responses; missing/unparseable means "no
        # server-provided floor", so the exponential backoff alone applies.
        header_value = response.headers.get("Retry-After")
        if header_value is None:
            return 0.0
        try:
            return float(header_value)
        except ValueError:
            return 0.0

    def get(self, endpoint: str, params: dict | None = None, store_code: str | None = None):
        response = self._request("GET", endpoint, params=params, store_code=store_code)
        return response.json()

    def get_paginated(
        self,
        endpoint: str,
        params: dict | None = None,
        page_size: int = 1000,
        response_key: str = "items",
        store_code: str | None = None,
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
            response = self.get(endpoint, params=page_params, store_code=store_code)
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

    def post(
        self, endpoint: str, payload: dict | list, store_code: str | None = None
    ) -> requests.Response:
        return self._request("POST", endpoint, json=payload, store_code=store_code)

    def put(
        self, endpoint: str, payload: dict, store_code: str | None = None
    ) -> requests.Response:
        return self._request("PUT", endpoint, json=payload, store_code=store_code)

    def delete(self, endpoint: str, store_code: str | None = None) -> requests.Response:
        return self._request("DELETE", endpoint, store_code=store_code)

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
