"""Helpers for Magento's core async/bulk API.

Two halves, deliberately kept out of `resource.py`:

- submitting rows (`AsyncBulkResult`/`run_async_upload`, used by
  `MagentoResource.upload_rows_async`), where `accepted`/`rejected` only mean
  "Magento queued the operation", not "it finished";
- turning a queued bulk back into per-operation outcomes
  (`map_detailed_status`/`wait_bulk`, used by the executor), which needs only
  one `bulk_detailed_status` call per poll, so it stays unit-testable against
  a plain stub the way `upload.py` is kept independent of `resource.py`.
"""

import time
from dataclasses import dataclass, field
from typing import Callable

import requests

from dagster_magento.upload import http_error_details

# Magento\Framework\Bulk\OperationInterface status constants.
STATUS_COMPLETE = 1
STATUS_RETRIABLY_FAILED = 2
STATUS_NOT_RETRIABLY_FAILED = 3
STATUS_OPEN = 4
STATUS_REJECTED = 5


@dataclass
class AsyncBulkResult:
    """Result of submitting rows to Magento's core async/bulk API.

    Unlike UploadResult (upload.py), `accepted`/`rejected` mean "Magento queued
    the operation" - not "the operation finished". Poll MagentoResource.get_bulk_status()
    with each bulk_uuid to find out whether queued operations actually succeeded.
    """

    bulk_uuids: list
    accepted: int
    rejected: int
    errors: list = field(default_factory=list)

    def to_metadata(self) -> dict:
        return {
            "bulk_uuids": self.bulk_uuids,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "error_count": len(self.errors),
        }


def run_async_upload(
    chunks: list,
    send: Callable[[list], dict],
    row_id_field: str,
    logger,
) -> AsyncBulkResult:
    bulk_uuids = []
    accepted = 0
    rejected = 0
    errors = []

    for index, chunk in enumerate(chunks):
        row_ids = [row.get(row_id_field) for row in chunk]
        try:
            response = send(chunk)
        except requests.exceptions.HTTPError as error:
            rejected += len(chunk)
            status_code, message = http_error_details(error)
            errors.append(
                {
                    "chunk_index": index,
                    "row_ids": row_ids,
                    "status_code": status_code,
                    "message": message,
                }
            )
            logger.warning(
                f"Async bulk chunk {index}/{len(chunks)} failed to submit ({len(chunk)} rows, "
                f"row_ids={row_ids[:10]}): {message}"
            )
            continue

        bulk_uuids.append(response.get("bulk_uuid"))
        for item in response.get("request_items", []):
            item_id = item.get("id", 0)
            row_id = row_ids[item_id] if item_id < len(row_ids) else None
            if item.get("status") == "rejected":
                rejected += 1
                errors.append(
                    {
                        "chunk_index": index,
                        "row_ids": [row_id],
                        "status_code": None,
                        "message": str(item.get("errors")),
                    }
                )
            else:
                accepted += 1

    logger.info(
        f"Async bulk submission complete: {accepted} accepted, {rejected} rejected "
        f"across {len(chunks)} chunk(s)"
    )
    return AsyncBulkResult(
        bulk_uuids=bulk_uuids, accepted=accepted, rejected=rejected, errors=errors
    )


def map_detailed_status(response: dict, count: int) -> list[tuple[int | None, str | None]]:
    """Index a detailed-status response by operation id, not by list order.

    Magento does not guarantee operations_list is sorted by id, so a
    caller matching the request's index-th item must look up by id."""
    by_id: dict[int, tuple[int | None, str | None]] = {}
    for operation in response.get("operations_list", []):
        operation_id = operation.get("id")
        if operation_id is None:
            continue
        by_id[operation_id] = (operation.get("status"), operation.get("result_message"))
    return [by_id.get(i, (None, None)) for i in range(count)]


def wait_bulk(
    resource,
    bulk_uuid: str,
    count: int,
    timeout_s: float = 600,
    poll_interval_s: float = 2.0,
    clock=time.monotonic,
    sleep=time.sleep,
    skip_ids: frozenset[int] = frozenset(),
) -> list[tuple[int | None, str | None]]:
    """Poll detailed-status until no operation is OPEN or missing, or the
    timeout expires. Never raises on timeout - callers decide what an
    open/missing status at the deadline means for their own operation.

    `skip_ids` are operation ids never waited for: the items Magento rejected
    at submission have no operation, so they would only end at the timeout."""
    deadline = clock() + timeout_s
    while True:
        response = resource.bulk_detailed_status(bulk_uuid)
        statuses = map_detailed_status(response, count)
        if not any(
            status is None or status == STATUS_OPEN
            for index, (status, _) in enumerate(statuses)
            if index not in skip_ids
        ):
            return statuses
        if clock() >= deadline:
            return statuses
        sleep(poll_interval_s)
