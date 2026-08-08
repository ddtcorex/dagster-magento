from dataclasses import dataclass, field
from typing import Callable

import requests


@dataclass
class UploadResult:
    succeeded: int
    failed: int
    errors: list = field(default_factory=list)

    def to_metadata(self) -> dict:
        return {
            "succeeded": self.succeeded,
            "failed": self.failed,
            "error_count": len(self.errors),
        }


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
            status_code = error.response.status_code if error.response is not None else None
            errors.append(
                {
                    "chunk_index": index,
                    "row_ids": row_ids,
                    "status_code": status_code,
                    "message": str(error),
                }
            )
            logger.warning(
                f"Chunk {index}/{len(chunks)} failed ({len(chunk)} rows, "
                f"row_ids={row_ids[:10]}): {error}"
            )

    logger.info(f"Upload complete: {succeeded} succeeded, {failed} failed across {len(chunks)} chunks")
    return UploadResult(succeeded=succeeded, failed=failed, errors=errors)
