"""Executor and model behaviour pinned to bodies captured from a real
Magento 2.4.9 (tests/fixtures/magento, written by tests/live/test_probes.py).
Hand written payloads only ever proved the shapes we assumed."""

import json
import sys
from pathlib import Path

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent / "live"))

from fixture_capture import load_fixture  # noqa: E402

from dagster_magento import bulk, executor
from dagster_magento.executor import execute
from dagster_magento.models import PriceRow, validate_rows
from dagster_magento.operation import BulkSpec, Operation
from test_executor import StubResource  # noqa: E402


def _price_op(sku, price=1.0, store_id=0):
    return Operation(
        method="POST",
        endpoint="products/base-prices",
        payload={"sku": sku, "price": price, "store_id": store_id},
        row_refs=(sku,),
        list_key="prices",
    )


def _price_fixture(name):
    _, body = load_fixture("price_failed_items")
    return body[name]["body"]


def test_negative_price_item_fails_the_whole_request_with_a_readable_message():
    items = _price_fixture("negative_base_price")

    result = execute(StubResource(responses=[items]), [_price_op("A", -5), _price_op("B", 3)], mode="sync")

    assert (result.succeeded, result.failed) == (0, 2)
    assert all("Invalid attribute Price = -5." in error["message"] for error in result.errors)
    assert all("%fieldName" not in error["message"] for error in result.errors)


def test_unknown_sku_item_fails_only_the_row_that_names_it():
    items = _price_fixture("unknown_sku_base_price")
    unknown = items[0]["parameters"][1]

    result = execute(StubResource(responses=[items]), [_price_op(unknown), _price_op("OTHER")], mode="sync")

    assert (result.succeeded, result.failed) == (1, 1)
    [error] = result.errors
    assert error["row_ids"] == [unknown]
    assert f"Invalid attribute SKU = {unknown}." in error["message"]


def test_magento_accepts_an_inverted_special_price_range_so_the_library_must_not():
    assert _price_fixture("inverted_special_price_dates") == []

    row = {"sku": "S", "special_price": 5, "special_from": "2027-02-01 00:00:00", "special_to": "2027-01-01 00:00:00"}
    _, errors = validate_rows(PriceRow, [row], "sku")

    assert len(errors) == 1
    assert "special_to" in errors[0].message


def _http_error(status, body):
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body).encode()
    error = requests.exceptions.HTTPError(f"{status} Client Error")
    error.response = response
    return error


def test_rejected_bulk_submission_fails_the_chunk_and_keeps_the_stack_trace_out_of_the_message():
    status, body = load_fixture("bulk_rejected_submission")
    developer_body = dict(body, trace="#0 /var/www/html/vendor/magento/framework/Webapi/x.php(215): boom()")
    ops = [
        Operation(
            method="POST", endpoint="products", payload=None, row_refs=(sku,),
            bulk=BulkSpec(endpoint="products", payload={"product": {"sku": sku}}),
        )
        for sku in ("A", "B", "C")
    ]

    result = execute(StubResource(bulk_uuids=[_http_error(status, developer_body)]), ops, mode="bulk")

    assert (result.succeeded, result.failed) == (0, 3)
    [error] = result.errors
    assert error["status_code"] == 400
    assert "is required" in error["message"]
    assert "vendor" not in error["message"] and "trace" not in error["message"]


def test_consumer_failure_fixture_maps_each_operation_by_id():
    _, status = load_fixture("bulk_consumer_failure")

    mapped = bulk.map_detailed_status(status, 3)

    assert [state for state, _ in mapped] == [bulk.STATUS_COMPLETE, bulk.STATUS_NOT_RETRIABLY_FAILED, bulk.STATUS_COMPLETE]
    assert "sku" in mapped[1][1]


def test_consumer_failure_marks_only_the_failed_row(monkeypatch):
    _, status = load_fixture("bulk_consumer_failure")
    monkeypatch.setattr(
        executor, "wait_bulk", lambda resource, uuid, count, **kwargs: bulk.map_detailed_status(status, count)
    )
    ops = [
        Operation(
            method="POST", endpoint="products", payload=None, row_refs=(sku,),
            bulk=BulkSpec(endpoint="products", payload={"product": {"sku": sku}}),
        )
        for sku in ("A", "B", "C")
    ]

    result = execute(StubResource(bulk_uuids=["u"]), ops, mode="bulk")

    assert (result.succeeded, result.failed) == (2, 1)
    assert result.errors[0]["row_ids"] == ["B"]
