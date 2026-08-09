from dataclasses import dataclass, field
from typing import Callable

import requests


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
            status_code = error.response.status_code if error.response is not None else None
            response_body = error.response.text[:1000] if error.response is not None else ""
            message = f"{error} - response body: {response_body}" if response_body else str(error)
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
