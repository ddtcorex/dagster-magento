import logging

import pytest
import requests

from dagster_magento.upload import DeleteMissingOutcome, UploadResult, chunk_rows, run_upload


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


def test_run_upload_includes_response_body_in_error_message():
    chunks = [[{"sku": "BAD-SKU"}]]

    def send(chunk):
        error = make_http_error(400, "400 Client Error: None for url: x")
        error.response._content = b'{"message": "The product does not exist"}'
        raise error

    result = run_upload(chunks, send=send, row_id_field="sku", logger=logging.getLogger("test"))

    assert "does not exist" in result.errors[0]["message"]


def test_upload_result_to_metadata():
    result = UploadResult(succeeded=10, failed=2, errors=[{"chunk_index": 0, "row_ids": ["X"], "status_code": 400, "message": "m"}])
    assert result.to_metadata() == {
        "succeeded": 10,
        "failed": 2,
        "pending": 0,
        "skipped_unchanged": 0,
        "error_count": 1,
        "delete_would": 0,
        "delete_deleted": 0,
    }


# -- delete_missing outcome -----------------------------------------------------


def test_upload_result_carries_the_delete_outcome():
    outcome = DeleteMissingOutcome(mode="preview", would_delete=("A", "B"))

    result = UploadResult(succeeded=1, failed=0, delete_missing=outcome)

    assert result.delete_missing is outcome
    assert result.to_metadata()["delete_would"] == 2
    assert result.to_metadata()["delete_deleted"] == 0


def test_upload_result_without_a_delete_outcome_reports_zero_counts():
    assert UploadResult(succeeded=1, failed=0).to_metadata()["delete_would"] == 0


def test_merge_carries_a_delete_outcome():
    outcome = DeleteMissingOutcome(mode="execute", would_delete=("A",), deleted=("A",))

    merged = UploadResult(succeeded=1, failed=0).merge(
        UploadResult(succeeded=0, failed=0, delete_missing=outcome)
    )

    assert merged.delete_missing is outcome


def test_merge_refuses_two_delete_outcomes():
    """A run deletes once; two outcomes mean a caller wired it wrong."""
    with pytest.raises(ValueError, match="two delete_missing outcomes"):
        UploadResult(succeeded=0, failed=0, delete_missing=DeleteMissingOutcome(mode="preview")).merge(
            UploadResult(succeeded=0, failed=0, delete_missing=DeleteMissingOutcome(mode="preview"))
        )
