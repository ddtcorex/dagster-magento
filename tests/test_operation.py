import pytest

from dagster_magento.operation import Operation, BulkSpec, RowError
from dagster_magento.upload import UploadResult


def test_upload_result_v010_construction_still_works():
    result = UploadResult(1, 0)
    assert result.succeeded == 1
    assert result.failed == 0
    assert result.pending == 0
    assert result.skipped_unchanged == 0


def test_to_metadata_includes_pending_and_skipped():
    result = UploadResult(
        succeeded=10,
        failed=2,
        pending=3,
        skipped_unchanged=5,
        errors=[],
    )
    metadata = result.to_metadata()
    assert metadata["succeeded"] == 10
    assert metadata["failed"] == 2
    assert metadata["pending"] == 3
    assert metadata["skipped_unchanged"] == 5
    assert metadata["error_count"] == 0


def test_merge_sums_counts_and_concatenates_errors():
    result1 = UploadResult(
        succeeded=5,
        failed=1,
        pending=2,
        skipped_unchanged=3,
        errors=[{"row_ids": ["A"], "status": "failed", "status_code": 400, "message": "error1"}],
    )
    result2 = UploadResult(
        succeeded=3,
        failed=2,
        pending=1,
        skipped_unchanged=1,
        errors=[{"row_ids": ["B"], "status": "pending", "status_code": 202, "message": "error2"}],
    )
    merged = result1.merge(result2)

    assert merged.succeeded == 8
    assert merged.failed == 3
    assert merged.pending == 3
    assert merged.skipped_unchanged == 4
    assert len(merged.errors) == 2
    assert merged.errors[0]["row_ids"] == ["A"]
    assert merged.errors[1]["row_ids"] == ["B"]


def test_operation_is_hashable_and_frozen():
    op = Operation(
        method="POST",
        endpoint="products",
        payload={"sku": "A"},
        row_refs=("row1", "row2"),
    )

    # Test frozen
    with pytest.raises(AttributeError):
        op.method = "PUT"

    # Test hashable
    ops_set = {op}
    assert op in ops_set

    # Two identical operations are the same
    op2 = Operation(
        method="POST",
        endpoint="products",
        payload={"sku": "A"},
        row_refs=("row1", "row2"),
    )
    assert op == op2
    assert hash(op) == hash(op2)
