import logging

import pytest
import requests
import requests_mock

from dagster_magento import bulk, executor
from dagster_magento.executor import MagentoImportError, check_error_ratio, execute
from dagster_magento.operation import BulkSpec, Operation
from dagster_magento.resource import MagentoResource
from dagster_magento.upload import UploadResult


def make_resource(**overrides):
    defaults = dict(
        base_url="https://shop.test",
        username="admin",
        password="secret-password",
        store_view="all",
    )
    defaults.update(overrides)
    return MagentoResource(**defaults)


def make_http_error(status_code, message):
    response = requests.Response()
    response.status_code = status_code
    error = requests.exceptions.HTTPError(message)
    error.response = response
    return error


class JsonResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


class StubResource:
    """Stands in for MagentoResource - executor tests exercise the
    grouping, chunking and status-mapping logic on top of the resource,
    not MagentoResource's own HTTP behavior (that is covered in
    test_resource.py and test_bulk.py). `responses` is a queue consumed in
    call order by post/put/delete; `bulk_uuids` is a queue consumed by
    submit_bulk."""

    def __init__(self, responses=None, bulk_uuids=None):
        self.post_calls = []
        self.put_calls = []
        self.delete_calls = []
        self.submit_bulk_calls = []
        self._responses = list(responses or [])
        self._bulk_uuids = list(bulk_uuids or [])

    def _next_response(self):
        outcome = self._responses.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return JsonResponse(outcome)

    def post(self, endpoint, payload=None, store_code=None):
        self.post_calls.append((endpoint, payload, store_code))
        return self._next_response()

    def put(self, endpoint, payload=None, store_code=None):
        self.put_calls.append((endpoint, payload, store_code))
        return self._next_response()

    def delete(self, endpoint, store_code=None):
        self.delete_calls.append((endpoint, store_code))
        return self._next_response()

    def submit_bulk(self, method, bulk_endpoint, items, store_code=None):
        self.submit_bulk_calls.append((method, bulk_endpoint, list(items), store_code))
        outcome = self._bulk_uuids.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_sync_single_operations_catch_and_continue():
    ops = [
        Operation(method="POST", endpoint="products", payload={"sku": "A"}, row_refs=("A",)),
        Operation(method="POST", endpoint="products", payload={"sku": "B"}, row_refs=("B",)),
        Operation(method="POST", endpoint="products", payload={"sku": "C"}, row_refs=("C",)),
    ]
    resource = StubResource(responses=[[], make_http_error(400, "Bad Request"), []])

    result = execute(resource, ops, mode="sync")

    assert len(resource.post_calls) == 3
    assert result.succeeded == 2
    assert result.failed == 1
    assert result.errors[0]["row_ids"] == ["B"]
    assert result.errors[0]["status"] == "failed"
    assert result.errors[0]["status_code"] == 400


def test_sync_auth_error_aborts():
    from dagster_magento.resource import MagentoAuthError

    ops = [Operation(method="POST", endpoint="products", payload={"sku": "A"}, row_refs=("A",))]
    resource = StubResource(responses=[MagentoAuthError("bad credentials")])

    with pytest.raises(MagentoAuthError):
        execute(resource, ops, mode="sync")


def test_list_endpoint_chunks_by_1000_and_wraps_list_key():
    ops = [
        Operation(
            method="POST",
            endpoint="products/base-prices",
            payload={"sku": f"SKU{i}", "price": 10},
            row_refs=(f"SKU{i}",),
            list_key="prices",
        )
        for i in range(2500)
    ]
    resource = StubResource(responses=[[], [], []])

    result = execute(resource, ops, mode="sync")

    sizes = [len(payload["prices"]) for _, payload, _ in resource.post_calls]
    assert sizes == [1000, 1000, 500]
    assert result.succeeded == 2500
    assert result.failed == 0
    # never mutate the operations' own payloads while wrapping chunks
    assert ops[0].payload == {"sku": "SKU0", "price": 10}


def test_list_endpoint_maps_failed_items_to_rows_by_sku():
    ops = [
        Operation(
            method="POST",
            endpoint="products/base-prices",
            payload={"sku": "A-1", "price": 10},
            row_refs=("A-1",),
            list_key="prices",
        ),
        Operation(
            method="POST",
            endpoint="products/base-prices",
            payload={"sku": "B-404", "price": 20},
            row_refs=("B-404",),
            list_key="prices",
        ),
        Operation(
            method="POST",
            endpoint="products/base-prices",
            payload={"sku": "C-2", "price": 5},
            row_refs=("C-2",),
            list_key="prices",
        ),
    ]
    failed_items = [
        {"message": "Requested product doesn't exist. SKU: %sku.", "parameters": {"sku": "B-404"}},
        {"message": "Not found: %1", "parameters": ["C-2"]},
    ]
    resource = StubResource(responses=[failed_items])

    result = execute(resource, ops, mode="sync")

    assert result.succeeded == 1
    assert result.failed == 2
    messages = {error["row_ids"][0]: error["message"] for error in result.errors}
    assert messages["B-404"] == "Requested product doesn't exist. SKU: B-404."
    assert messages["C-2"] == "Not found: C-2"


def test_list_endpoint_http_error_fails_whole_chunk():
    # A real MagentoResource this time (not StubResource) - the retry/
    # backoff machinery in _request lives there, and this test pins the
    # brief's rule that an HTTPError fails every row in the chunk it hit,
    # not just the one that finally raised.
    resource = make_resource()
    resource._sleep = lambda seconds: None  # tests must never actually sleep

    ops = [
        Operation(
            method="POST",
            endpoint="products/base-prices",
            payload={"sku": f"SKU{i}", "price": 10},
            row_refs=(f"SKU{i}",),
            list_key="prices",
        )
        for i in range(4)
    ]

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.post(
            "https://shop.test/rest/all/V1/products/base-prices",
            [
                # First chunk (SKU0-2): a retryable 429 (the only status a
                # POST retries), then a non-retryable 500 that ends the
                # retry loop and raises.
                {"status_code": 429},
                {"status_code": 500, "json": {"message": "Internal error"}},
                # Second chunk (SKU3): succeeds with no failed items.
                {"status_code": 200, "json": []},
            ],
        )
        result = execute(resource, ops, mode="sync", chunk_size=3)

    assert result.succeeded == 1
    assert result.failed == 3
    assert len(result.errors) == 1
    error = result.errors[0]
    assert sorted(error["row_ids"]) == ["SKU0", "SKU1", "SKU2"]
    assert error["status"] == "failed"
    assert error["status_code"] == 500


def test_bulk_mode_groups_by_endpoint_and_chunks_by_200(monkeypatch):
    ops_a = [
        Operation(
            method="POST",
            endpoint="async/bulk/V1/products",
            payload=None,
            row_refs=(f"A{i}",),
            bulk=BulkSpec(endpoint="products", payload={"product": {"sku": f"A{i}"}}),
        )
        for i in range(250)
    ]
    ops_b = [
        Operation(
            method="PUT",
            endpoint="async/bulk/V1/products/bySku",
            payload=None,
            row_refs=(f"B{i}",),
            bulk=BulkSpec(endpoint="products/bySku", payload={"product": {"sku": f"B{i}"}}),
        )
        for i in range(10)
    ]
    resource = StubResource(bulk_uuids=["u1", "u2", "u3"])
    monkeypatch.setattr(
        executor,
        "wait_bulk",
        lambda resource, bulk_uuid, count, **kwargs: [(bulk.STATUS_COMPLETE, None)] * count,
    )

    result = execute(resource, ops_a + ops_b, mode="bulk")

    assert result.succeeded == 260
    assert result.failed == 0
    sizes = sorted(len(items) for _, _, items, _ in resource.submit_bulk_calls)
    assert sizes == [10, 50, 200]
    endpoints = {endpoint for _, endpoint, _, _ in resource.submit_bulk_calls}
    assert endpoints == {"products", "products/bySku"}


def test_bulk_mode_resubmits_retriable_once_then_fails(monkeypatch):
    op = Operation(
        method="POST",
        endpoint="async/bulk/V1/products",
        payload=None,
        row_refs=("A1",),
        bulk=BulkSpec(endpoint="products", payload={"product": {"sku": "A1"}}),
    )
    resource = StubResource(bulk_uuids=["u1", "u2"])
    canned = iter(
        [
            [(bulk.STATUS_RETRIABLY_FAILED, "lock timeout")],
            [(bulk.STATUS_RETRIABLY_FAILED, "lock timeout")],
        ]
    )
    monkeypatch.setattr(
        executor,
        "wait_bulk",
        lambda resource, bulk_uuid, count, **kwargs: next(canned),
    )

    result = execute(resource, [op], mode="bulk")

    assert len(resource.submit_bulk_calls) == 2
    assert result.succeeded == 0
    assert result.failed == 1
    assert result.errors[0]["status"] == "failed"
    assert result.errors[0]["message"] == "lock timeout"


def test_bulk_mode_reports_pending_after_timeout_and_logs_consumer_hint(monkeypatch, caplog):
    op = Operation(
        method="POST",
        endpoint="async/bulk/V1/products",
        payload=None,
        row_refs=("A1",),
        bulk=BulkSpec(endpoint="products", payload={"product": {"sku": "A1"}}),
    )
    resource = StubResource(bulk_uuids=["u1"])
    monkeypatch.setattr(
        executor,
        "wait_bulk",
        lambda resource, bulk_uuid, count, **kwargs: [(bulk.STATUS_OPEN, None)],
    )

    with caplog.at_level(logging.WARNING):
        result = execute(resource, [op], mode="bulk", bulk_timeout_s=1)

    assert result.pending == 1
    assert result.succeeded == 0
    assert result.failed == 0
    assert "async.operations.all" in caplog.text


def test_bulk_mode_runs_non_bulk_operations_through_sync(monkeypatch, caplog):
    non_bulk_op = Operation(method="POST", endpoint="products", payload={"sku": "X"}, row_refs=("X",))
    bulk_op = Operation(
        method="POST",
        endpoint="async/bulk/V1/products",
        payload=None,
        row_refs=("Y",),
        bulk=BulkSpec(endpoint="products", payload={"product": {"sku": "Y"}}),
    )
    resource = StubResource(responses=[[]], bulk_uuids=["u1"])
    monkeypatch.setattr(
        executor,
        "wait_bulk",
        lambda resource, bulk_uuid, count, **kwargs: [(bulk.STATUS_COMPLETE, None)] * count,
    )

    with caplog.at_level(logging.INFO):
        result = execute(resource, [non_bulk_op, bulk_op], mode="bulk")

    assert result.succeeded == 2
    assert len(resource.post_calls) == 1
    assert resource.post_calls[0][0] == "products"
    assert len(resource.submit_bulk_calls) == 1
    assert "sync mode" in caplog.text.lower()


def test_list_chunking_never_splits_a_chunk_key():
    """Consecutive operations with the same non-None chunk_key are never split."""
    # 5 ops: A(no key), B1+B2+B3 (shared key "B"), C(no key)
    # With chunk_size=3, they should chunk as [A], [B1,B2,B3], [C]
    ops = [
        Operation(
            method="PUT",
            endpoint="products/tier-prices",
            payload={"sku": "A", "qty": 10},
            row_refs=("A",),
            list_key="prices",
            chunk_key=None,
        ),
        Operation(
            method="PUT",
            endpoint="products/tier-prices",
            payload={"sku": "B", "qty": 10},
            row_refs=("B",),
            list_key="prices",
            chunk_key="B",
        ),
        Operation(
            method="PUT",
            endpoint="products/tier-prices",
            payload={"sku": "B", "qty": 20},
            row_refs=("B",),
            list_key="prices",
            chunk_key="B",
        ),
        Operation(
            method="PUT",
            endpoint="products/tier-prices",
            payload={"sku": "B", "qty": 50},
            row_refs=("B",),
            list_key="prices",
            chunk_key="B",
        ),
        Operation(
            method="PUT",
            endpoint="products/tier-prices",
            payload={"sku": "C", "qty": 10},
            row_refs=("C",),
            list_key="prices",
            chunk_key=None,
        ),
    ]
    resource = StubResource(responses=[[], [], []])

    result = execute(resource, ops, mode="sync", chunk_size=3)

    # Should produce 3 chunks
    assert len(resource.put_calls) == 3
    # Chunk 1: [A]
    assert len(resource.put_calls[0][1]["prices"]) == 1
    assert resource.put_calls[0][1]["prices"][0]["sku"] == "A"
    # Chunk 2: [B1, B2, B3] - all B's together despite chunk_size=3
    assert len(resource.put_calls[1][1]["prices"]) == 3
    assert all(item["sku"] == "B" for item in resource.put_calls[1][1]["prices"])
    # Chunk 3: [C]
    assert len(resource.put_calls[2][1]["prices"]) == 1
    assert resource.put_calls[2][1]["prices"][0]["sku"] == "C"
    assert result.succeeded == 5
    assert result.failed == 0


def test_oversized_chunk_key_unit_is_sent_alone():
    """A unit with a chunk_key larger than chunk_size goes in a chunk of its own."""
    # 4 operations all sharing chunk_key "LARGE", chunk_size=3
    # Should produce 1 chunk with all 4
    ops = [
        Operation(
            method="PUT",
            endpoint="products/tier-prices",
            payload={"sku": f"X", "qty": i},
            row_refs=(f"X",),
            list_key="prices",
            chunk_key="LARGE",
        )
        for i in range(4)
    ]
    resource = StubResource(responses=[[]])

    result = execute(resource, ops, mode="sync", chunk_size=3)

    assert len(resource.put_calls) == 1
    assert len(resource.put_calls[0][1]["prices"]) == 4
    assert result.succeeded == 4
    assert result.failed == 0


def test_check_error_ratio_raises_above_threshold_and_ignores_none():
    result = UploadResult(succeeded=1, failed=9, pending=0)

    check_error_ratio(result, None)  # never raises, no matter how bad the ratio is

    with pytest.raises(MagentoImportError):
        check_error_ratio(result, fail_on_error_ratio=0.5)

    check_error_ratio(result, fail_on_error_ratio=0.95)  # 0.9 <= 0.95, no raise


def test_bulk_mode_submits_later_phases_only_after_earlier_ones_complete(monkeypatch):
    """A grouped/bundle parent save must not run while its children's saves
    are still in flight: with concurrent consumers the parent fails with
    'The Product with ... doesn't exist' (seen live on 2.4.6). Phase-1
    groups are therefore submitted only after every phase-0 group has been
    waited on, in one submission order the stub records."""
    submitted = []

    # Note the list order: the parent comes first, so insertion order alone
    # would submit it first. Phase order has to win over list order.
    ops = [
        Operation(
            method="POST", endpoint="products", payload=None, row_refs=("PARENT",),
            bulk=BulkSpec(endpoint="products", payload={"product": {"sku": "PARENT"}}, phase=1),
        ),
        Operation(
            method="POST", endpoint="products", payload=None, row_refs=("CHILD",),
            bulk=BulkSpec(endpoint="products", payload={"product": {"sku": "CHILD"}}, phase=0),
        ),
    ]
    resource = StubResource(bulk_uuids=["u-phase-0", "u-phase-1"])
    monkeypatch.setattr(
        executor,
        "wait_bulk",
        lambda resource, bulk_uuid, count, **kwargs: (
            submitted.append(bulk_uuid) or [(bulk.STATUS_COMPLETE, None)] * count
        ),
    )

    result = execute(resource, ops, mode="bulk")

    assert result.succeeded == 2
    first_items = resource.submit_bulk_calls[0][2]
    assert [item["product"]["sku"] for item in first_items] == ["CHILD"]
    assert submitted == ["u-phase-0", "u-phase-1"]


def test_sync_mode_runs_phases_in_ascending_order_stable_within_a_phase():
    """Sync mode honours BulkSpec.phase just as bulk mode does: a grouped or
    bundle parent listed before its children is saved after them, or Magento
    rejects it with 'The Product with the C1 SKU doesn't exist'."""

    def op(sku, phase):
        return Operation(
            method="POST", endpoint="products", payload={"product": {"sku": sku}}, row_refs=(sku,),
            bulk=BulkSpec(endpoint="products", payload={"product": {"sku": sku}}, phase=phase),
        )

    plain = Operation(method="POST", endpoint="links", payload={"sku": "L"}, row_refs=("L",))
    ops = [op("PARENT", 1), op("C1", 0), plain, op("C2", 0)]
    resource = StubResource(responses=[[], [], [], []])

    result = execute(resource, ops, mode="sync")

    assert result.succeeded == 4
    assert [call[1].get("product", call[1]).get("sku") for call in resource.post_calls] == [
        "C1", "L", "C2", "PARENT"
    ]


def _price_op(sku, price, store_id=0):
    return Operation(
        method="POST",
        endpoint="products/base-prices",
        payload={"sku": sku, "price": price, "store_id": store_id},
        row_refs=(sku,),
        list_key="prices",
    )


def test_unattributable_failed_item_fails_every_row_of_the_request():
    """Magento rejects a negative price with an item naming only the field and
    value, not the SKU. The executor cannot tell which row it was, so every
    row of that request fails with the message: never a false success."""
    ops = [_price_op("A", -5), _price_op("B", 3)]
    failed_items = [
        {"message": "Invalid attribute %fieldName = %fieldValue.",
         "parameters": {"fieldName": "Price", "fieldValue": -5}}
    ]
    resource = StubResource(responses=[failed_items])

    result = execute(resource, ops, mode="sync")

    assert result.succeeded == 0
    assert result.failed == 2
    failed_rows = sorted(ref for error in result.errors for ref in error["row_ids"])
    assert failed_rows == ["A", "B"]
    assert all("Invalid attribute Price = -5." in error["message"] for error in result.errors)


def test_failed_item_is_not_matched_on_a_numeric_price_value():
    # SKU "5" and a rejected price of 5: the value is a price, not a SKU, so
    # the item is unattributable and fails the whole request, not just "5".
    ops = [_price_op("5", 1), _price_op("B", 5)]
    failed_items = [
        {"message": "Invalid attribute %fieldName = %fieldValue.",
         "parameters": {"fieldName": "Price", "fieldValue": 5}}
    ]
    resource = StubResource(responses=[failed_items])

    result = execute(resource, ops, mode="sync")

    assert result.failed == 2
    assert result.succeeded == 0


def test_failed_item_naming_a_store_fails_only_that_store_operation():
    ops = [_price_op("X", 10, store_id=0), _price_op("X", 12, store_id=1)]
    failed_items = [
        {"message": "Requested store is not found. Row ID: SKU = %SKU, Store ID: %storeId.",
         "parameters": {"SKU": "X", "storeId": "1"}}
    ]
    resource = StubResource(responses=[failed_items])

    result = execute(resource, ops, mode="sync")

    assert result.succeeded == 1
    assert result.failed == 1
    [error] = result.errors
    assert error["row_ids"] == ["X"]
    assert "Store ID: 1" in error["message"]


def make_http_error_with_body(status_code, body):
    import json

    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(body).encode()
    error = requests.exceptions.HTTPError(f"{status_code} Client Error")
    error.response = response
    return error


def _bulk_op(sku, phase=0):
    return Operation(
        method="POST", endpoint="products", payload=None, row_refs=(sku,),
        bulk=BulkSpec(endpoint="products", payload={"product": {"sku": sku}}, phase=phase),
    )


def test_bulk_submission_4xx_fails_only_that_chunk_and_the_run_continues(monkeypatch):
    """Spec section 7: a 4xx fails that chunk's rows, the run continues. The
    chunk that finished before it and the chunks and phases after it keep
    their own outcome."""
    polled = []
    monkeypatch.setattr(
        executor,
        "wait_bulk",
        lambda resource, bulk_uuid, count, **kwargs: (
            polled.append(bulk_uuid) or [(bulk.STATUS_COMPLETE, None)] * count
        ),
    )
    rejected = make_http_error_with_body(400, {"message": "Error processing 1 element of input data"})
    resource = StubResource(bulk_uuids=["u-first", rejected, "u-phase-1"])
    ops = [_bulk_op("A"), _bulk_op("B"), _bulk_op("P", phase=1)]

    result = execute(resource, ops, mode="bulk", chunk_size=1)

    assert result.succeeded == 2
    assert result.failed == 1
    [error] = result.errors
    assert error["row_ids"] == ["B"]
    assert error["status_code"] == 400
    assert "Error processing 1 element" in error["message"]
    assert polled == ["u-first", "u-phase-1"]


def test_bulk_partial_rejection_still_polls_the_accepted_operations(monkeypatch):
    """Magento schedules the accepted operations and still answers 400 with
    the bulk uuid when some items are rejected: those accepted operations are
    polled and merged, the rejected ones fail with their own error."""
    waits = []

    def fake_wait(resource, bulk_uuid, count, **kwargs):
        waits.append((bulk_uuid, kwargs.get("skip_ids")))
        return [(bulk.STATUS_COMPLETE, None), (None, None), (bulk.STATUS_COMPLETE, None)]

    monkeypatch.setattr(executor, "wait_bulk", fake_wait)
    partial = make_http_error_with_body(
        400,
        {
            "bulk_uuid": "u-partial",
            "request_items": [
                {"id": 0, "status": "accepted"},
                {"id": 1, "status": "rejected", "errors": "sku is required"},
                {"id": 2, "status": "accepted"},
            ],
            "errors": True,
        },
    )
    resource = StubResource(bulk_uuids=[partial])

    result = execute(resource, [_bulk_op("A"), _bulk_op("B"), _bulk_op("C")], mode="bulk")

    assert waits == [("u-partial", frozenset({1}))]
    assert result.succeeded == 2
    assert result.failed == 1
    [error] = result.errors
    assert error["row_ids"] == ["B"]
    assert "sku is required" in error["message"]


def test_bulk_submission_auth_error_still_aborts():
    from dagster_magento.resource import MagentoAuthError

    resource = StubResource(bulk_uuids=[MagentoAuthError("bad credentials")])

    with pytest.raises(MagentoAuthError):
        execute(resource, [_bulk_op("A")], mode="bulk")


TIER_MESSAGE = (
    "Invalid attribute SKU = %SKU. Row ID: SKU = %SKU, Website ID: %websiteId, "
    "Customer Group: %customerGroup, Quantity: %qty."
)


def test_positional_parameters_fill_each_named_placeholder_once_even_when_it_repeats():
    """Magento answers tier price failures with a list and a message that names
    %SKU twice; the values map to the distinct names in first-appearance order,
    never one value per occurrence."""
    filled = executor._fill_message(TIER_MESSAGE, ["ABC", "1", "ALL GROUPS", "2"])

    assert filled == (
        "Invalid attribute SKU = ABC. Row ID: SKU = ABC, Website ID: 1, "
        "Customer Group: ALL GROUPS, Quantity: 2."
    )


def test_a_list_failure_for_store_one_does_not_fail_the_same_sku_in_store_zero():
    ops = [_price_op("X", 10, store_id=0), _price_op("X", 12, store_id=1)]
    failed_items = [
        {"message": "Requested store is not found. Row ID: SKU = %SKU, Store ID: %storeId.",
         "parameters": ["X", "1"]}
    ]

    result = execute(StubResource(responses=[failed_items]), ops, mode="sync")

    assert (result.succeeded, result.failed) == (1, 1)
    assert "Store ID: 1" in result.errors[0]["message"]


def test_a_list_failure_does_not_match_a_row_whose_sku_equals_a_numeric_parameter():
    ops = [_price_op("ABC", 1, store_id=7), _price_op("7", 2, store_id=0)]
    failed_items = [
        {"message": "Requested store is not found. Row ID: SKU = %SKU, Store ID: %storeId.",
         "parameters": ["ABC", "7"]}
    ]

    result = execute(StubResource(responses=[failed_items]), ops, mode="sync")

    assert result.errors[0]["row_ids"] == ["ABC"]
    assert (result.succeeded, result.failed) == (1, 1)
