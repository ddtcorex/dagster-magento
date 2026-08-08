import logging

import pytest
import requests

from dagster_magento.upload import UploadResult, chunk_rows, run_upload


def test_chunk_rows_splits_into_groups_of_the_given_size():
    rows = [{"sku": f"SKU{i}"} for i in range(5)]
    chunks = chunk_rows(rows, chunk_size=2)
    assert chunks == [
        [{"sku": "SKU0"}, {"sku": "SKU1"}],
        [{"sku": "SKU2"}, {"sku": "SKU3"}],
        [{"sku": "SKU4"}],
    ]


def test_chunk_rows_with_chunk_size_one_matches_row_count():
    rows = [{"sku": "A"}, {"sku": "B"}]
    chunks = chunk_rows(rows, chunk_size=1)
    assert chunks == [[{"sku": "A"}], [{"sku": "B"}]]


def make_http_error(status_code, message):
    response = requests.Response()
    response.status_code = status_code
    error = requests.exceptions.HTTPError(message)
    error.response = response
    return error


def test_run_upload_counts_all_chunks_as_succeeded_when_send_never_raises():
    chunks = [[{"sku": "A"}], [{"sku": "B"}, {"sku": "C"}]]
    result = run_upload(chunks, send=lambda chunk: None, row_id_field="sku", logger=logging.getLogger("test"))

    assert result == UploadResult(succeeded=3, failed=0, errors=[])


def test_run_upload_continues_past_a_failing_chunk():
    chunks = [[{"sku": "A"}], [{"sku": "B"}], [{"sku": "C"}]]
    attempted = []

    def send(chunk):
        attempted.append(chunk)
        if chunk == [{"sku": "B"}]:
            raise make_http_error(400, "Bad Request")

    result = run_upload(chunks, send=send, row_id_field="sku", logger=logging.getLogger("test"))

    assert attempted == chunks  # all three were attempted, including after the failure
    assert result.succeeded == 2
    assert result.failed == 1
    assert result.errors == [
        {"chunk_index": 1, "row_ids": ["B"], "status_code": 400, "message": "Bad Request"}
    ]


def test_run_upload_logs_a_warning_per_failed_chunk(caplog):
    chunks = [[{"sku": "BAD-SKU"}]]

    def send(chunk):
        raise make_http_error(400, "Bad Request")

    with caplog.at_level(logging.WARNING):
        run_upload(chunks, send=send, row_id_field="sku", logger=logging.getLogger("test"))

    assert "BAD-SKU" in caplog.text
    assert "400" in caplog.text or "Bad Request" in caplog.text


def test_run_upload_only_catches_http_error_not_other_exceptions():
    chunks = [[{"sku": "A"}]]

    def send(chunk):
        raise TypeError("a bug in my own code, not a Magento rejection")

    with pytest.raises(TypeError):
        run_upload(chunks, send=send, row_id_field="sku", logger=logging.getLogger("test"))


def test_upload_result_to_metadata():
    result = UploadResult(succeeded=10, failed=2, errors=[{"chunk_index": 0, "row_ids": ["X"], "status_code": 400, "message": "m"}])
    assert result.to_metadata() == {"succeeded": 10, "failed": 2, "error_count": 1}
