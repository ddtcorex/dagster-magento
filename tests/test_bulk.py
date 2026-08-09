import logging

import pytest
import requests

from dagster_magento.bulk import AsyncBulkResult, run_async_upload


def make_http_error(status_code, message):
    response = requests.Response()
    response.status_code = status_code
    error = requests.exceptions.HTTPError(message)
    error.response = response
    return error


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
