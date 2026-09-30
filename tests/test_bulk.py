"""Both halves of the async bulk API: submitting rows through
`upload_rows_async` (where a result only means "Magento queued it") and
resolving a bulk back into per-operation outcomes for the executor.
"""

import logging

import pytest
import requests
import requests_mock

from dagster_magento import bulk
from dagster_magento.bulk import AsyncBulkResult, run_async_upload
from dagster_magento.resource import MagentoResource


def make_http_error(status_code, message):
    response = requests.Response()
    response.status_code = status_code
    error = requests.exceptions.HTTPError(message)
    error.response = response
    return error


def make_resource(**overrides):
    defaults = dict(
        base_url="https://shop.test",
        username="admin",
        password="secret-password",
        store_view="all",
    )
    defaults.update(overrides)
    return MagentoResource(**defaults)


class FakeBulkResource:
    """Stands in for MagentoResource in wait_bulk tests - a canned sequence
    of detailed-status responses, one per call, clamped to the last entry
    once the sequence runs out."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    def bulk_detailed_status(self, bulk_uuid):
        response = self._responses[min(self.calls, len(self._responses) - 1)]
        self.calls += 1
        return response


# -- submission side (run_async_upload / upload_rows_async) --------------------


def test_run_async_upload_counts_accepted_items_and_collects_bulk_uuid():
    chunks = [[{"sku": "A"}, {"sku": "B"}]]

    def send(chunk):
        return {
            "bulk_uuid": "uuid-1",
            "request_items": [
                {"id": 0, "status": "accepted"},
                {"id": 1, "status": "accepted"},
            ],
            "errors": False,
        }

    result = run_async_upload(chunks, send, row_id_field="sku", logger=logging.getLogger("test"))

    assert result == AsyncBulkResult(bulk_uuids=["uuid-1"], accepted=2, rejected=0, errors=[])


def test_run_async_upload_reports_rejected_items_with_their_row_id():
    chunks = [[{"sku": "A"}, {"sku": "BAD-SKU"}]]

    def send(chunk):
        return {
            "bulk_uuid": "uuid-1",
            "request_items": [
                {"id": 0, "status": "accepted"},
                {"id": 1, "status": "rejected", "errors": {"message": "SKU already exists"}},
            ],
            "errors": True,
        }

    result = run_async_upload(chunks, send, row_id_field="sku", logger=logging.getLogger("test"))

    assert result.accepted == 1
    assert result.rejected == 1
    assert result.errors[0]["row_ids"] == ["BAD-SKU"]
    assert "SKU already exists" in result.errors[0]["message"]


def test_run_async_upload_continues_past_a_chunk_that_fails_to_submit():
    chunks = [[{"sku": "A"}], [{"sku": "B"}], [{"sku": "C"}]]
    attempted = []

    def send(chunk):
        attempted.append(chunk)
        if chunk == [{"sku": "B"}]:
            raise make_http_error(401, "Unauthorized")
        return {"bulk_uuid": "uuid", "request_items": [{"id": 0, "status": "accepted"}]}

    result = run_async_upload(chunks, send, row_id_field="sku", logger=logging.getLogger("test"))

    assert attempted == chunks  # all three chunks were attempted, including after the failure
    assert result.accepted == 2
    assert result.rejected == 1
    assert result.errors == [
        {"chunk_index": 1, "row_ids": ["B"], "status_code": 401, "message": "Unauthorized"}
    ]


def test_run_async_upload_only_catches_http_error_not_other_exceptions():
    chunks = [[{"sku": "A"}]]

    def send(chunk):
        raise TypeError("a bug in my own code, not a Magento rejection")

    with pytest.raises(TypeError):
        run_async_upload(chunks, send, row_id_field="sku", logger=logging.getLogger("test"))


def test_run_async_upload_includes_response_body_in_submit_error_message():
    chunks = [[{"sku": "A"}]]

    def send(chunk):
        error = make_http_error(400, "400 Client Error: None for url: x")
        error.response._content = b'{"message": "Invalid request body"}'
        raise error

    result = run_async_upload(chunks, send, row_id_field="sku", logger=logging.getLogger("test"))

    assert "Invalid request body" in result.errors[0]["message"]


def test_async_bulk_result_to_metadata():
    result = AsyncBulkResult(
        bulk_uuids=["uuid-1"],
        accepted=10,
        rejected=2,
        errors=[{"chunk_index": 0, "row_ids": ["X"], "status_code": None, "message": "m"}],
    )
    assert result.to_metadata() == {
        "bulk_uuids": ["uuid-1"],
        "accepted": 10,
        "rejected": 2,
        "error_count": 1,
    }


# -- polling side (submit_bulk / detailed status / wait_bulk) ------------------


def test_submit_bulk_posts_items_to_async_bulk_url_and_returns_uuid():
    resource = make_resource()
    items = [{"product": {"sku": "A"}}, {"product": {"sku": "B"}}]

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.post(
            "https://shop.test/rest/all/async/bulk/V1/products",
            json={
                "bulk_uuid": "u1",
                "request_items": [
                    {"id": 0, "data_hash": "h0", "status": "accepted"},
                    {"id": 1, "data_hash": "h1", "status": "accepted"},
                ],
                "errors": False,
            },
        )
        bulk_uuid = resource.submit_bulk("POST", "products", items)

    assert bulk_uuid == "u1"
    data_requests = [
        r for r in m.request_history if r.path_url.endswith("/async/bulk/V1/products")
    ]
    assert len(data_requests) == 1
    # The async bulk body is a bare JSON array, not {"items": [...]} -
    # verified live: Magento rejects the wrapped form with 400 "Request
    # body must be an array".
    assert data_requests[0].json() == items
    assert data_requests[0].headers["Authorization"] == "Bearer fake-token-123"


def test_submit_bulk_raises_on_http_error():
    resource = make_resource()

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.post(
            "https://shop.test/rest/all/async/bulk/V1/products",
            status_code=400,
            json={"message": "Bad request"},
        )
        try:
            resource.submit_bulk("POST", "products", [{"product": {"sku": "A"}}])
            assert False, "expected HTTPError"
        except Exception as error:
            assert "400" in str(error)


def test_bulk_detailed_status_gets_v1_bulk_endpoint():
    resource = make_resource()

    with requests_mock.Mocker() as m:
        m.post(
            "https://shop.test/rest/all/V1/integration/admin/token",
            json="fake-token-123",
        )
        m.get(
            "https://shop.test/rest/all/V1/bulk/u1/detailed-status",
            json={"operations_list": [{"id": 0, "status": 1, "result_message": None}]},
        )
        result = resource.bulk_detailed_status("u1")

    assert result == {"operations_list": [{"id": 0, "status": 1, "result_message": None}]}


def test_map_detailed_status_matches_by_operation_id_not_list_order():
    # operations_list given out of order (ids 2, 0, 1) - the result must be
    # indexed by id, not by position in the list.
    response = {
        "operations_list": [
            {"id": 2, "status": bulk.STATUS_NOT_RETRIABLY_FAILED, "result_message": "bad set"},
            {"id": 0, "status": bulk.STATUS_COMPLETE, "result_message": None},
            {"id": 1, "status": bulk.STATUS_COMPLETE, "result_message": None},
        ]
    }

    result = bulk.map_detailed_status(response, count=3)

    assert result == [
        (bulk.STATUS_COMPLETE, None),
        (bulk.STATUS_COMPLETE, None),
        (bulk.STATUS_NOT_RETRIABLY_FAILED, "bad set"),
    ]


def test_map_detailed_status_missing_ids_are_none():
    response = {
        "operations_list": [
            {"id": 0, "status": bulk.STATUS_COMPLETE, "result_message": None},
            {"id": 2, "status": bulk.STATUS_COMPLETE, "result_message": None},
        ]
    }

    result = bulk.map_detailed_status(response, count=3)

    assert result == [
        (bulk.STATUS_COMPLETE, None),
        (None, None),
        (bulk.STATUS_COMPLETE, None),
    ]


def test_wait_bulk_polls_until_no_open_operations():
    resource = FakeBulkResource(
        [
            {"operations_list": [{"id": 0, "status": bulk.STATUS_OPEN, "result_message": None}]},
            {
                "operations_list": [
                    {"id": 0, "status": bulk.STATUS_COMPLETE, "result_message": "ok"}
                ]
            },
        ]
    )
    sleeps = []

    result = bulk.wait_bulk(
        resource,
        "u1",
        count=1,
        timeout_s=60,
        poll_interval_s=2.0,
        clock=lambda: 0.0,
        sleep=sleeps.append,
    )

    assert result == [(bulk.STATUS_COMPLETE, "ok")]
    assert resource.calls == 2
    assert sleeps == [2.0]


def test_wait_bulk_returns_open_statuses_at_timeout():
    resource = FakeBulkResource(
        [{"operations_list": [{"id": 0, "status": bulk.STATUS_OPEN, "result_message": None}]}]
    )
    # deadline = 0 + 10 = 10; first check sees 5.0 (< 10, so it sleeps and
    # polls again); second check sees 11.0 (>= 10, so it returns without
    # sleeping again). Exactly 3 clock() calls are consumed: none left over.
    clock_values = iter([0.0, 5.0, 11.0])
    sleeps = []

    result = bulk.wait_bulk(
        resource,
        "u1",
        count=1,
        timeout_s=10,
        poll_interval_s=2.0,
        clock=lambda: next(clock_values),
        sleep=sleeps.append,
    )

    assert result == [(bulk.STATUS_OPEN, None)]
    assert resource.calls == 2
    assert sleeps == [2.0]


def test_wait_bulk_does_not_wait_for_skipped_operation_ids():
    # A rejected item of a partially accepted bulk never gets an operation,
    # so waiting for it would only ever end at the timeout.
    from dagster_magento.bulk import STATUS_COMPLETE, wait_bulk

    class Resource:
        calls = 0

        def bulk_detailed_status(self, bulk_uuid):
            Resource.calls += 1
            return {"operations_list": [{"id": 0, "status": STATUS_COMPLETE}]}

    statuses = wait_bulk(
        Resource(), "u", count=2, timeout_s=60, poll_interval_s=0,
        skip_ids=frozenset({1}), sleep=lambda seconds: None,
    )

    assert Resource.calls == 1
    assert statuses == [(STATUS_COMPLETE, None), (None, None)]
