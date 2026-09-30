"""Probes that record real Magento response shapes as fixtures.

Hermetic tests read the recorded bodies instead of hand written payloads, so
an assumption about Magento's wire format is either pinned to a capture or
visibly wrong. Each probe also asserts the keys the library reads, so another
Magento line that answers differently fails loudly in the compatibility
matrix. Bodies are written only with DAGSTER_CAPTURE_FIXTURES=1 (see
fixture_capture.py); a normal run only asserts.
"""

import os
import time

import pytest
import requests
from fixture_capture import save_fixture
from live_support import SANDBOX_PROJECT, make_resource, prepare_sandbox, sandbox

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(not SANDBOX_PROJECT.is_dir(), reason="drives the local sandbox"),
]

POLL_TIMEOUT_S = 180
POLL_INTERVAL_S = 3
STATUS_COMPLETE = 1
STATUS_NOT_RETRIABLY_FAILED = 3


def _product(sku: str) -> dict:
    return {"product": {"sku": sku, "name": sku, "price": 1.5, "attribute_set_id": 4, "type_id": "simple", "status": 1}}


def _sku(label: str) -> str:
    return f"probe-{label}-{int(time.time())}"


def _exists(resource, sku: str) -> bool:
    try:
        resource.get(f"products/{sku}")
    except requests.exceptions.HTTPError as error:
        assert error.response.status_code == 404
        return False
    return True


def _wait_until_closed(resource, bulk_uuid: str) -> dict:
    deadline = time.monotonic() + POLL_TIMEOUT_S
    while True:
        status = resource.bulk_detailed_status(bulk_uuid)
        operations = status.get("operations_list", [])
        if operations and all(operation["status"] != 4 for operation in operations):
            return status
        assert time.monotonic() < deadline, f"bulk {bulk_uuid} still open after {POLL_TIMEOUT_S}s"
        time.sleep(POLL_INTERVAL_S)


def test_probe_bulk_rejects_the_whole_submission_when_an_item_is_malformed():
    prepare_sandbox()
    resource = make_resource()
    base_url = os.environ["MAGENTO_BASE_URL"]
    good_a, good_c = _sku("a"), _sku("c")

    with pytest.raises(requests.exceptions.HTTPError) as caught:
        resource.submit_bulk("POST", "products", [_product(good_a), {"foo": 1}, _product(good_c)])

    response = caught.value.response
    body = response.json()
    save_fixture("bulk_rejected_submission", response.status_code, body, base_url=base_url)

    assert response.status_code == 400
    # No bulk was scheduled: there is no uuid and no per item list, so the
    # executor can only fail the whole chunk.
    assert "bulk_uuid" not in body and "request_items" not in body
    assert "message" in body and "parameters" in body
    time.sleep(POLL_INTERVAL_S * 2)
    assert not _exists(resource, good_a) and not _exists(resource, good_c)


def test_probe_bulk_reports_a_consumer_failure_per_operation():
    prepare_sandbox()
    resource = make_resource()
    base_url = os.environ["MAGENTO_BASE_URL"]
    good_a, good_c = _sku("d"), _sku("f")

    bulk_uuid = resource.submit_bulk("POST", "products", [_product(good_a), {"product": "x"}, _product(good_c)])
    status = _wait_until_closed(resource, bulk_uuid)
    for operation in status["operations_list"]:
        # The saved product's serialized result is large and not read.
        operation["result_serialized_data"] = None
    save_fixture("bulk_consumer_failure", 200, status, base_url=base_url)

    outcome = {operation["id"]: operation["status"] for operation in status["operations_list"]}
    assert outcome == {0: STATUS_COMPLETE, 1: STATUS_NOT_RETRIABLY_FAILED, 2: STATUS_COMPLETE}
    failed = status["operations_list"][outcome_index(status, 1)]
    assert failed["result_message"] and "error_code" in failed
    assert status["operation_count"] == 3


def outcome_index(status: dict, operation_id: int) -> int:
    return [operation["id"] for operation in status["operations_list"]].index(operation_id)


def test_probe_price_storage_failed_items():
    prepare_sandbox()
    resource = make_resource()
    base_url = os.environ["MAGENTO_BASE_URL"]
    sku = _sku("price")
    resource.post("products", _product(sku)).raise_for_status()

    negative = resource.post("products/base-prices", {"prices": [{"price": -5, "store_id": 0, "sku": sku}]})
    unknown_sku = resource.post(
        "products/base-prices", {"prices": [{"price": 5, "store_id": 0, "sku": f"{sku}-does-not-exist"}]}
    )
    inverted_dates = resource.post(
        "products/special-price",
        {
            "prices": [
                {
                    "price": 5,
                    "store_id": 0,
                    "sku": sku,
                    "price_from": "2027-02-01 00:00:00",
                    "price_to": "2027-01-01 00:00:00",
                }
            ]
        },
    )
    recorded = {
        "negative_base_price": {"status": negative.status_code, "body": negative.json()},
        "unknown_sku_base_price": {"status": unknown_sku.status_code, "body": unknown_sku.json()},
        "inverted_special_price_dates": {"status": inverted_dates.status_code, "body": inverted_dates.json()},
    }
    save_fixture("price_failed_items", 200, recorded, base_url=base_url)

    for name in ("negative_base_price", "unknown_sku_base_price"):
        items = recorded[name]["body"]
        assert isinstance(items, list) and items, f"{name}: expected a list of failed items, got {items!r}"
        for item in items:
            assert "message" in item and "parameters" in item, f"{name}: unexpected item {item!r}"
    # Magento does not validate the order of the dates: an inverted range is
    # stored and reported as a success, so only the library can reject it.
    assert recorded["inverted_special_price_dates"]["body"] == []


def test_probe_production_mode_error_bodies():
    """Production mode masks error detail; record what a rejected library write
    and a rejected bridge write still tell a caller. The sandbox is returned to
    developer mode even when a request or an assertion fails."""
    prepare_sandbox()
    resource = make_resource()
    base_url = os.environ["MAGENTO_BASE_URL"]
    recorded = {}
    try:
        assert "deploy mode: production" in sandbox("deploy-mode", "production", timeout=3600)
        for name, send in {
            "library_rejected_write": lambda: resource.post(
                "products",
                {"product": {"sku": _sku("prod"), "name": "x", "price": 1, "attribute_set_id": 999999, "type_id": "simple"}},
            ),
            "bridge_rejected_write": lambda: resource.post(
                "dagster-bridge/categories/upsert",
                {"paths": ["Default Category/Probe"], "root": "Default Category", "separator": ""},
            ),
        }.items():
            with pytest.raises(requests.exceptions.HTTPError) as caught:
                send()
            body = caught.value.response.json()
            recorded[name] = {"status": caught.value.response.status_code, "body": body}
            assert "message" in body, f"{name}: no message in {body!r}"
    finally:
        restored = sandbox("deploy-mode", "developer", timeout=3600)
    assert "deploy mode: developer" in restored
    save_fixture("production_error_bodies", 400, recorded, base_url=base_url)
