"""Runs planned Operations against a MagentoResource, either as ordinary
synchronous REST calls or through the async Bulk API, and turns the
per-row outcome into an UploadResult.

Kept independent of the writers that produce Operations - this module
only knows how to send an already-built Operation and interpret the
response, not how a row became one.
"""

from typing import Literal

import requests
from dagster import get_dagster_logger

from dagster_magento.bulk import STATUS_COMPLETE, STATUS_OPEN, STATUS_RETRIABLY_FAILED, wait_bulk
from dagster_magento.operation import Operation
from dagster_magento.upload import UploadResult, chunk_rows

# Production defaults from the design spec: list endpoints (prices, source
# items, ...) chunk larger than bulk submissions do.
_DEFAULT_LIST_CHUNK_SIZE = 1000
_DEFAULT_BULK_CHUNK_SIZE = 200


class MagentoImportError(Exception):
    """Raised by check_error_ratio when the failed-row ratio exceeds the
    caller-supplied threshold."""


def _chunk_list_operations(ops: list[Operation], size: int) -> list[list[Operation]]:
    """Chunk list-endpoint operations, keeping consecutive ops with the same
    non-None chunk_key together in the same request.

    A unit (a run of ops sharing one chunk_key, or a single op with chunk_key
    None) is never split across chunks. If adding a unit would exceed size,
    start a new chunk. A unit larger than size goes in a chunk of its own.
    Order is preserved.
    """
    chunks = []
    current_chunk = []
    current_size = 0

    i = 0
    while i < len(ops):
        op = ops[i]

        # Collect consecutive ops with the same non-None chunk_key.
        unit = [op]
        if op.chunk_key is not None:
            j = i + 1
            while j < len(ops) and ops[j].chunk_key == op.chunk_key:
                unit.append(ops[j])
                j += 1

        unit_size = len(unit)

        # If adding this unit would exceed size and current_chunk is not empty,
        # start a new chunk.
        if current_size > 0 and current_size + unit_size > size:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0

        # Add the unit to the current chunk.
        current_chunk.extend(unit)
        current_size += unit_size

        # Move past the unit.
        i += len(unit)

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def check_error_ratio(result: UploadResult, fail_on_error_ratio: float | None) -> None:
    """Raise MagentoImportError when failed / (succeeded + failed + pending)
    is above fail_on_error_ratio. None disables the check entirely."""
    if fail_on_error_ratio is None:
        return
    total = result.succeeded + result.failed + result.pending
    if total == 0:
        return
    ratio = result.failed / total
    if ratio > fail_on_error_ratio:
        raise MagentoImportError(
            f"Error ratio {ratio:.2%} ({result.failed} failed of {total}) "
            f"exceeds fail_on_error_ratio={fail_on_error_ratio:.2%}"
        )


def execute(
    resource,
    operations: list[Operation],
    mode: Literal["sync", "bulk"] = "sync",
    chunk_size: int | None = None,
    bulk_timeout_s: float = 600,
    poll_interval_s: float = 2.0,
) -> UploadResult:
    if mode == "bulk":
        return _execute_bulk(resource, operations, chunk_size, bulk_timeout_s, poll_interval_s)
    return _execute_sync(resource, operations, chunk_size)


def _dispatch(resource, method: str, endpoint: str, payload, store_code):
    if method == "DELETE":
        return resource.delete(endpoint, store_code=store_code)
    if method == "PUT":
        return resource.put(endpoint, payload, store_code=store_code)
    return resource.post(endpoint, payload, store_code=store_code)


def _http_error_details(error: requests.exceptions.HTTPError) -> tuple[int | None, str]:
    status_code = error.response.status_code if error.response is not None else None
    response_body = error.response.text[:1000] if error.response is not None else ""
    message = f"{error} - response body: {response_body}" if response_body else str(error)
    return status_code, message


def _execute_sync(resource, operations: list[Operation], chunk_size: int | None) -> UploadResult:
    logger = get_dagster_logger()
    result = UploadResult(succeeded=0, failed=0)

    list_ops = [op for op in operations if op.list_key is not None]
    single_ops = [op for op in operations if op.list_key is None]

    groups: dict[tuple, list[Operation]] = {}
    for op in list_ops:
        key = (op.method, op.endpoint, op.store_code, op.list_key)
        groups.setdefault(key, []).append(op)

    effective_chunk_size = chunk_size if chunk_size is not None else _DEFAULT_LIST_CHUNK_SIZE
    for (method, endpoint, store_code, list_key), ops in groups.items():
        for chunk in _chunk_list_operations(ops, effective_chunk_size):
            result = result.merge(
                _send_list_chunk(resource, method, endpoint, store_code, list_key, chunk, logger)
            )

    for op in single_ops:
        result = result.merge(_send_single(resource, op, logger))

    return result


def _send_single(resource, op: Operation, logger) -> UploadResult:
    # v0.1.0 behaviour: one request per row, catch-log-continue on
    # HTTPError. MagentoAuthError is not requests.exceptions.HTTPError, so
    # it is never caught here and aborts the whole run, as it must.
    try:
        _dispatch(resource, op.method, op.endpoint, op.payload, op.store_code)
    except requests.exceptions.HTTPError as error:
        status_code, message = _http_error_details(error)
        logger.warning(f"{op.endpoint}: row(s) {op.row_refs} failed: {message}")
        return UploadResult(
            succeeded=0,
            failed=len(op.row_refs),
            errors=[
                {
                    "row_ids": list(op.row_refs),
                    "status": "failed",
                    "status_code": status_code,
                    "message": message,
                }
            ],
        )
    return UploadResult(succeeded=len(op.row_refs), failed=0)


def _send_list_chunk(resource, method, endpoint, store_code, list_key, chunk, logger) -> UploadResult:
    # A fresh dict per chunk, and a copy of each row's own payload dict -
    # the operations' own payloads are never mutated, per the
    # Operation/BulkSpec immutability contract.
    payload = {list_key: [dict(op.payload) for op in chunk]}
    try:
        response = _dispatch(resource, method, endpoint, payload, store_code)
    except requests.exceptions.HTTPError as error:
        status_code, message = _http_error_details(error)
        row_ids = [ref for op in chunk for ref in op.row_refs]
        logger.warning(f"{endpoint}: chunk of {len(chunk)} operation(s) failed: {message}")
        return UploadResult(
            succeeded=0,
            failed=len(row_ids),
            errors=[
                {"row_ids": row_ids, "status": "failed", "status_code": status_code, "message": message}
            ],
        )

    # A 2xx list-endpoint response body is itself a list of failed items,
    # e.g. [{"message": "... SKU: %sku.", "parameters": {"sku": "B-404"}}].
    # Everything not named there succeeded.
    failed_items = response.json() or []
    succeeded = 0
    failed = 0
    errors = []
    for op in chunk:
        failed_item = _find_failed_item(op.row_refs, failed_items)
        if failed_item is None:
            succeeded += len(op.row_refs)
            continue
        failed += len(op.row_refs)
        message = _fill_message(failed_item.get("message", ""), failed_item.get("parameters"))
        errors.append(
            {"row_ids": list(op.row_refs), "status": "failed", "status_code": None, "message": message}
        )
        logger.warning(f"{endpoint}: row(s) {op.row_refs} failed: {message}")

    return UploadResult(succeeded=succeeded, failed=failed, errors=errors)


def _find_failed_item(row_refs: tuple, failed_items: list) -> dict | None:
    for item in failed_items:
        values = _parameter_values(item.get("parameters"))
        if any(ref in values for ref in row_refs):
            return item
    return None


def _parameter_values(parameters) -> list:
    if isinstance(parameters, dict):
        return [str(value) for value in parameters.values()]
    if isinstance(parameters, list):
        return [str(value) for value in parameters]
    return []


def _fill_message(message: str, parameters) -> str:
    # %name placeholders come from a dict, %1/%2/... from a list.
    if isinstance(parameters, dict):
        for key, value in parameters.items():
            message = message.replace(f"%{key}", str(value))
    elif isinstance(parameters, list):
        for index, value in enumerate(parameters, start=1):
            message = message.replace(f"%{index}", str(value))
    return message


def _execute_bulk(
    resource,
    operations: list[Operation],
    chunk_size: int | None,
    timeout_s: float,
    poll_interval_s: float,
) -> UploadResult:
    logger = get_dagster_logger()
    result = UploadResult(succeeded=0, failed=0)

    non_bulk_ops = [op for op in operations if op.bulk is None]
    bulk_ops = [op for op in operations if op.bulk is not None]

    if non_bulk_ops:
        logger.info(f"{len(non_bulk_ops)} operation(s) without a bulk spec running through sync mode")
        result = result.merge(_execute_sync(resource, non_bulk_ops, chunk_size))

    groups: dict[tuple, list[Operation]] = {}
    for op in bulk_ops:
        key = (op.method, op.bulk.endpoint, op.store_code)
        groups.setdefault(key, []).append(op)

    effective_chunk_size = chunk_size if chunk_size is not None else _DEFAULT_BULK_CHUNK_SIZE
    for (method, bulk_endpoint, store_code), ops in groups.items():
        for chunk in chunk_rows(ops, effective_chunk_size):
            result = result.merge(
                _process_bulk_chunk(
                    resource, method, bulk_endpoint, store_code, chunk, timeout_s, poll_interval_s, logger
                )
            )

    return result


def _process_bulk_chunk(
    resource,
    method: str,
    bulk_endpoint: str,
    store_code,
    ops: list[Operation],
    timeout_s: float,
    poll_interval_s: float,
    logger,
    allow_retry: bool = True,
) -> UploadResult:
    # Copy each payload before it leaves this module - BulkSpec payloads
    # are immutable once built (operation.py); submit_bulk sends this list
    # as a bare JSON array, not {"items": [...]}.
    items = [dict(op.bulk.payload) for op in ops]
    bulk_uuid = resource.submit_bulk(method, bulk_endpoint, items, store_code=store_code)
    statuses = wait_bulk(
        resource, bulk_uuid, count=len(ops), timeout_s=timeout_s, poll_interval_s=poll_interval_s
    )

    succeeded = 0
    failed = 0
    pending = 0
    errors = []
    retry_ops = []

    # index i of statuses matches operation id i, i.e. the i-th submitted
    # item, per submit_bulk/wait_bulk's contract - so ops and statuses are
    # matched positionally here.
    for op, (status, message) in zip(ops, statuses):
        if status == STATUS_COMPLETE:
            succeeded += len(op.row_refs)
        elif status == STATUS_RETRIABLY_FAILED and allow_retry:
            retry_ops.append(op)
        elif status == STATUS_OPEN or status is None:
            pending += len(op.row_refs)
            errors.append(
                {
                    "row_ids": list(op.row_refs),
                    "status": "pending",
                    "status_code": None,
                    "message": message or "operation still open at the bulk wait timeout",
                }
            )
        else:
            failed += len(op.row_refs)
            errors.append(
                {
                    "row_ids": list(op.row_refs),
                    "status": "failed",
                    "status_code": None,
                    "message": message or f"bulk operation status {status}",
                }
            )

    result = UploadResult(succeeded=succeeded, failed=failed, pending=pending, errors=errors)

    if pending:
        logger.warning(
            f"{pending} row(s) on {bulk_endpoint} still pending after {timeout_s}s - check that "
            "the 'async.operations.all' message queue consumer is running"
        )

    if retry_ops:
        # One resubmission only - whatever the retry's own outcome is,
        # it is not retried again (a second STATUS_RETRIABLY_FAILED falls
        # through to the failed branch above since allow_retry is False).
        logger.info(f"Resubmitting {len(retry_ops)} retriable operation(s) on {bulk_endpoint} in a new bulk")
        result = result.merge(
            _process_bulk_chunk(
                resource,
                method,
                bulk_endpoint,
                store_code,
                retry_ops,
                timeout_s,
                poll_interval_s,
                logger,
                allow_retry=False,
            )
        )

    return result
