"""Runs planned Operations against a MagentoResource, either as ordinary
synchronous REST calls or through the async Bulk API, and turns the
per-row outcome into an UploadResult.

Kept independent of the writers that produce Operations - this module
only knows how to send an already-built Operation and interpret the
response, not how a row became one.
"""

import re
from typing import Literal

import requests
from dagster import get_dagster_logger

from dagster_magento.bulk import STATUS_COMPLETE, STATUS_OPEN, STATUS_RETRIABLY_FAILED, wait_bulk
from dagster_magento.operation import Operation
from dagster_magento.upload import UploadResult, chunk_rows, http_error_details

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


def _phase(op: Operation) -> int:
    return op.bulk.phase if op.bulk is not None else 0


def _execute_sync(resource, operations: list[Operation], chunk_size: int | None) -> UploadResult:
    # BulkSpec.phase orders sync mode too: a grouped or bundle parent listed
    # before its children must still be saved after them, or Magento rejects
    # it with "The Product with the ... SKU doesn't exist". Phases ascend and
    # each phase keeps the caller's order.
    result = UploadResult(succeeded=0, failed=0)
    for phase in sorted({_phase(op) for op in operations}):
        phase_ops = [op for op in operations if _phase(op) == phase]
        result = result.merge(_execute_sync_phase(resource, phase_ops, chunk_size))
    return result


def _execute_sync_phase(resource, operations: list[Operation], chunk_size: int | None) -> UploadResult:
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
        status_code, message = http_error_details(error)
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
        status_code, message = http_error_details(error)
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
    # e.g. [{"message": "... SKU: %SKU.", "parameters": {"SKU": "B-404"}}].
    # An item is attributed to the operation whose SKU it names (and whose
    # store or source, when it names one); everything not named succeeded.
    body = response.json()
    failed_items = [item for item in body if isinstance(item, dict)] if isinstance(body, list) else []
    attributed: set[int] = set()
    succeeded_ops = []
    failed = 0
    errors = []
    for op in chunk:
        index = _find_failed_item(op, failed_items)
        if index is None:
            succeeded_ops.append(op)
            continue
        attributed.add(index)
        item = failed_items[index]
        failed += len(op.row_refs)
        message = _fill_message(item.get("message", ""), item.get("parameters"))
        errors.append(
            {"row_ids": list(op.row_refs), "status": "failed", "status_code": None, "message": message}
        )
        logger.warning(f"{endpoint}: row(s) {op.row_refs} failed: {message}")

    # Pessimistic by design: an item that names no row (Magento answers a
    # rejected price with only {"fieldName": "Price", "fieldValue": -5})
    # cannot be pinned on one row, so every other row of this request fails
    # with its message. Reporting them succeeded would hide a real rejection.
    unattributed = [item for index, item in enumerate(failed_items) if index not in attributed]
    if unattributed and succeeded_ops:
        message = "; ".join(
            _fill_message(item.get("message", ""), item.get("parameters")) for item in unattributed
        )
        message = f"rejected item(s) in this request name no row, failing every row: {message}"
        row_ids = [ref for op in succeeded_ops for ref in op.row_refs]
        failed += len(row_ids)
        errors.append({"row_ids": row_ids, "status": "failed", "status_code": None, "message": message})
        logger.warning(f"{endpoint}: row(s) {row_ids} failed: {message}")
        succeeded_ops = []

    succeeded = sum(len(op.row_refs) for op in succeeded_ops)
    return UploadResult(succeeded=succeeded, failed=failed, errors=errors)


# Parameter keys (casefolded) that scope a failed item further than its SKU,
# mapped to the payload field they must equal: a failure for SKU X on store 1
# must not fail X's store 0 operation sent in the same request.
_SCOPE_PARAMETER_FIELDS = {
    "storeid": "store_id",
    "store_id": "store_id",
    "sourcecode": "source_code",
    "source_code": "source_code",
}


def _find_failed_item(op: Operation, failed_items: list[dict]) -> int | None:
    for index, item in enumerate(failed_items):
        if _item_names_operation(item, op):
            return index
    return None


_PLACEHOLDER = re.compile(r"%([A-Za-z_]\w*)")


def _named_parameters(message: str, parameters) -> dict:
    """The failed item's parameters as a name -> value dict.

    Magento's price storage answers with a positional list whose order is the
    order the message first names its placeholders ("Row ID: SKU = %SKU, Store
    ID: %storeId." with ["X", "1"]; a placeholder that repeats takes one
    value). The generic "Invalid attribute %fieldName = %fieldValue." form names
    the field in its first value, so ["SKU", "X"] is also {"SKU": "X"}."""
    if isinstance(parameters, dict):
        named = dict(parameters)
    elif isinstance(parameters, list):
        names = list(dict.fromkeys(_PLACEHOLDER.findall(message)))
        if not names:
            # Numbered placeholders ("Not found: %1"): nothing says which value
            # is the SKU, so every string value is a candidate, as before.
            return {"sku": [value for value in parameters if isinstance(value, str)]}
        named = dict(zip(names, parameters))
    else:
        return {}
    field, value = named.get("fieldName"), named.get("fieldValue")
    if isinstance(field, str) and value is not None:
        named.setdefault(field, value)
    return named


def _item_names_operation(item: dict, op: Operation) -> bool:
    """Whether one failed item names this operation.

    Only a parameter named SKU (any case) is compared to the operation's SKU,
    never price or quantity values: a rejected price of 5 must not fail the
    row whose SKU is "5". A store or source the item names must also match the
    operation's own, so a failure for SKU X in store 1 leaves X in store 0 alone.
    """
    parameters = _named_parameters(item.get("message", ""), item.get("parameters"))
    payload = op.payload if isinstance(op.payload, dict) else {}
    sku = payload.get("sku")
    candidates = {str(sku)} if sku is not None else {str(ref) for ref in op.row_refs}

    named = []
    for key, value in parameters.items():
        if str(key).casefold() != "sku":
            continue
        values = value if isinstance(value, list) else [value]
        named.extend(str(item) for item in values)
    if not any(value in candidates for value in named):
        return False
    for key, value in parameters.items():
        field = _SCOPE_PARAMETER_FIELDS.get(str(key).casefold())
        if field is not None and field in payload and str(payload[field]) != str(value):
            return False
    return True


def _fill_message(message: str, parameters) -> str:
    # %name placeholders come from a dict or, in order, from a list; %1/%2/...
    # always from a list.
    if isinstance(parameters, list) and re.search(r"%\d", message):
        for index, value in enumerate(parameters, start=1):
            message = message.replace(f"%{index}", str(value))
        return message
    named = _named_parameters(message, parameters)
    return _PLACEHOLDER.sub(lambda match: str(named.get(match.group(1), match.group(0))), message)


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
        key = (op.bulk.phase, op.method, op.bulk.endpoint, op.store_code)
        groups.setdefault(key, []).append(op)

    effective_chunk_size = chunk_size if chunk_size is not None else _DEFAULT_BULK_CHUNK_SIZE
    # Phases ascend, insertion order within a phase: every group of one phase
    # is submitted and waited on before the next phase starts, so a grouped
    # or bundle parent save never runs while its children's saves are still
    # in flight. sorted is stable, so same-phase groups keep the writer's
    # order (mains before follow-ups).
    for (phase, method, bulk_endpoint, store_code), ops in sorted(groups.items(), key=lambda item: item[0][0]):
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
    rejected: dict[int, str] = {}
    try:
        bulk_uuid = resource.submit_bulk(method, bulk_endpoint, items, store_code=store_code)
    except requests.exceptions.HTTPError as error:
        # Spec section 7: a 4xx on one chunk fails that chunk's rows and the
        # run continues. MagentoAuthError is not an HTTPError and still
        # aborts. Magento answers a partial rejection with 400 too, after it
        # scheduled the accepted items: when the body carries the bulk uuid,
        # those are still polled below and only the rejected ones fail here.
        status_code, submit_message = http_error_details(error)
        bulk_uuid, rejected = _partial_submission(error)
        if bulk_uuid is None:
            row_ids = [ref for op in ops for ref in op.row_refs]
            logger.warning(f"{bulk_endpoint}: bulk chunk of {len(ops)} operation(s) rejected: {submit_message}")
            return UploadResult(
                succeeded=0,
                failed=len(row_ids),
                errors=[
                    {"row_ids": row_ids, "status": "failed", "status_code": status_code, "message": submit_message}
                ],
            )
        logger.warning(
            f"{bulk_endpoint}: bulk {bulk_uuid} partially rejected ({len(rejected)} item(s)); "
            "polling the accepted operations"
        )
        rejected = {index: message or submit_message for index, message in rejected.items()}
    statuses = wait_bulk(
        resource,
        bulk_uuid,
        count=len(ops),
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
        skip_ids=frozenset(rejected),
    )

    succeeded = 0
    failed = 0
    pending = 0
    errors = []
    retry_ops = []

    # index i of statuses matches operation id i, i.e. the i-th submitted
    # item, per submit_bulk/wait_bulk's contract - so ops and statuses are
    # matched positionally here.
    for index, (op, (status, message)) in enumerate(zip(ops, statuses)):
        if index in rejected:
            failed += len(op.row_refs)
            errors.append(
                {"row_ids": list(op.row_refs), "status": "failed", "status_code": None, "message": rejected[index]}
            )
        elif status == STATUS_COMPLETE:
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


def _partial_submission(error: requests.exceptions.HTTPError) -> tuple[str | None, dict[int, str]]:
    """The bulk uuid and the rejected item ids (with their errors) a failed
    submission still carries, or (None, {}) when nothing was scheduled.

    Looks at the top level of the error body and under `parameters`, the
    two places a webapi error response can hold them."""
    try:
        body = error.response.json() if error.response is not None else None
    except ValueError:
        return None, {}
    if not isinstance(body, dict):
        return None, {}
    for candidate in (body, body.get("parameters")):
        if not isinstance(candidate, dict) or not candidate.get("bulk_uuid"):
            continue
        rejected = {}
        for item in candidate.get("request_items") or []:
            if isinstance(item, dict) and item.get("status") == "rejected" and item.get("id") is not None:
                errors = item.get("errors")
                rejected[int(item["id"])] = str(errors) if errors not in (None, "", True) else ""
        return str(candidate["bulk_uuid"]), rejected
    return None, {}
