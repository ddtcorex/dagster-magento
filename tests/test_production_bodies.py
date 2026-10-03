"""What a caller still learns from a rejected write when Magento runs in
production mode, pinned to bodies captured from a real 2.4.9 store
(tests/fixtures/magento/production_error_bodies.json)."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "live"))

from fixture_capture import load_fixture  # noqa: E402
from test_executor import StubResource, make_http_error_with_body  # noqa: E402

from dagster_magento.executor import execute
from dagster_magento.operation import Operation


def _reject(name, sku):
    _, recorded = load_fixture("production_error_bodies")
    capture = recorded[name]
    op = Operation(method="POST", endpoint="products", payload={"sku": sku}, row_refs=(sku,))
    resource = StubResource(responses=[make_http_error_with_body(capture["status"], capture["body"])])
    return execute(resource, [op], mode="sync")


def test_a_library_rejection_keeps_its_reason_and_parameter_in_production_mode():
    result = _reject("library_rejected_write", "A")

    assert (result.succeeded, result.failed) == (0, 1)
    [error] = result.errors
    assert error["status_code"] == 400
    assert "Invalid product data" in error["message"]
    assert "Invalid attribute set entity type" in error["message"]


def test_a_bridge_rejection_keeps_its_message_in_production_mode():
    result = _reject("bridge_rejected_write", "B")

    [error] = result.errors
    assert "The separator must not be empty." in error["message"]
