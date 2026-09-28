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


def test_bulk_spec_is_hashable():
    spec = BulkSpec(endpoint="products/bySku", payload={"sku": "A"})

    # Test hashable
    specs_set = {spec}
    assert spec in specs_set

    # Two identical specs hash equal and dedupe in a set
    spec2 = BulkSpec(endpoint="products/bySku", payload={"sku": "A"})
    assert spec == spec2
    assert hash(spec) == hash(spec2)
    assert {spec, spec2} == {spec}  # dedupes


def test_operation_with_bulk_is_hashable():
    bulk = BulkSpec(endpoint="products/bySku", payload={"sku": "A"})
    op = Operation(
        method="POST",
        endpoint="async/bulk/V1/products/bySku",
        payload=None,
        row_refs=("row1", "row2"),
        bulk=bulk,
    )

    # Test hashable
    ops_set = {op}
    assert op in ops_set

    # Two operations with identical bulk specs hash equal
    bulk2 = BulkSpec(endpoint="products/bySku", payload={"sku": "A"})
    op2 = Operation(
        method="POST",
        endpoint="async/bulk/V1/products/bySku",
        payload=None,
        row_refs=("row1", "row2"),
        bulk=bulk2,
    )
    assert op == op2
    assert hash(op) == hash(op2)


def test_row_error_is_frozen_and_comparable():
    err = RowError(row_ref="row1", message="error message")

    # Test frozen
    with pytest.raises(AttributeError):
        err.row_ref = "row2"

    # Test comparable
    err2 = RowError(row_ref="row1", message="error message")
    assert err == err2

    err3 = RowError(row_ref="row1", message="different message")
    assert err != err3
