import json
from dataclasses import dataclass, field
from typing import Callable

import requests



def http_error_details(error: requests.exceptions.HTTPError) -> tuple[int | None, str]:
    """The status code and a row-safe message for a failed request.

    The message embeds the first 1000 characters of the response body, minus
    Magento's `trace` key: in developer mode it carries a full stack trace
    with server paths that has no place in a row error."""
    response = error.response
    if response is None:
        return None, str(error)
    body = response.text
    try:
        parsed = response.json()
    except ValueError:
        parsed = None
    if isinstance(parsed, dict) and "trace" in parsed:
        body = json.dumps({key: value for key, value in parsed.items() if key != "trace"})
    body = body[:1000]
    return response.status_code, f"{error} - response body: {body}" if body else str(error)

@dataclass
class UploadResult:
    succeeded: int
    failed: int
    errors: list = field(default_factory=list)
    pending: int = 0
    skipped_unchanged: int = 0

    def to_metadata(self) -> dict:
        return {
            "succeeded": self.succeeded,
            "failed": self.failed,
            "pending": self.pending,
            "skipped_unchanged": self.skipped_unchanged,
            "error_count": len(self.errors),
        }

    def merge(self, other: "UploadResult") -> "UploadResult":
        """Merge another UploadResult into this one, summing counts and concatenating errors."""
        return UploadResult(
            succeeded=self.succeeded + other.succeeded,
            failed=self.failed + other.failed,
            pending=self.pending + other.pending,
            skipped_unchanged=self.skipped_unchanged + other.skipped_unchanged,
            errors=self.errors + other.errors,
        )


def chunk_rows(rows: list, chunk_size: int) -> list:
    return [rows[i : i + chunk_size] for i in range(0, len(rows), chunk_size)]


def run_upload(
    chunks: list,
    send: Callable[[list], None],
    row_id_field: str,
    logger,
) -> UploadResult:
    succeeded = 0
    failed = 0
    errors = []

    for index, chunk in enumerate(chunks):
        try:
            send(chunk)
            succeeded += len(chunk)
        except requests.exceptions.HTTPError as error:
            failed += len(chunk)
            row_ids = [row.get(row_id_field) for row in chunk]
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
                f"Chunk {index}/{len(chunks)} failed ({len(chunk)} rows, "
                f"row_ids={row_ids[:10]}): {message}"
            )

    logger.info(f"Upload complete: {succeeded} succeeded, {failed} failed across {len(chunks)} chunks")
    return UploadResult(succeeded=succeeded, failed=failed, errors=errors)
