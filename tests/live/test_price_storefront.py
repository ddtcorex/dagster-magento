"""Price and store-scope round trips against the live sandbox.

Covers spec section 10: prices written through price storage reach a
full-page-cached storefront after cron, a bulk store-view update keeps
its store scope once the consumer runs it, and special price dates read
back unchanged under the sandbox timezone. Each test builds its own
product, so the file does not depend on the catalog E2E. See conftest.py
for how to run it.
"""

import re
import time

import pytest
import requests
from live_support import SANDBOX_PROJECT, make_resource, prepare_sandbox, sandbox

from dagster_magento import import_prices, import_products, import_source_items

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(not SANDBOX_PROJECT.is_dir(), reason="drives cron and bin/magento in the local sandbox"),
]

# cron:run only executes jobs whose scheduled minute has come, so the
# mview price reindex can land one cron minute after the write. Bounded:
# more than this means the path is broken, not slow.
CRON_ATTEMPTS = 4
CRON_WAIT_S = 30


def ensure_product(resource, sku: str, price: float, name: str) -> None:
    """A visible, in-stock simple product; stock is what keeps the product
    page from answering 404 once the inventory index has run."""
    result = import_products(
        resource,
        [{"sku": sku, "name": name, "price": price, "visibility": 4, "status": 1, "attributes": {"url_key": sku}}],
    )
    assert result.failed == 0, result.errors
    result = import_source_items(resource, [{"sku": sku, "source_code": "default", "quantity": 100, "status": 1}])
    assert result.failed == 0, result.errors


def storefront_get(url: str) -> requests.Response:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response


def shows_price(html: str, price: float) -> bool:
    # The product view renders the final price as data-price-amount="41.17".
    return re.search(rf'data-price-amount="{re.escape(f"{price:g}")}"', html) is not None


def cron_until_price(url: str, price: float) -> tuple[requests.Response, int]:
    """Run cron until the product page shows `price`, at most CRON_ATTEMPTS
    times; returns the last page and how many cron runs it took."""
    for attempt in range(1, CRON_ATTEMPTS + 1):
        sandbox("cron-run")
        page = storefront_get(url)
        if shows_price(page.text, price):
            return page, attempt
        if attempt < CRON_ATTEMPTS:
            time.sleep(CRON_WAIT_S)
    return page, CRON_ATTEMPTS


def test_price_change_reaches_storefront_after_cron_with_fpc():
    prepare_sandbox()
    resource = make_resource()
    sku, old_price, new_price = "dagster-live-fpc", 41.17, 57.23
    url = f"{resource.base_url}/{sku}.html"
    # Start from the old price, also on a rerun where the page is cached
    # with the new one.
    ensure_product(resource, sku, old_price, "Dagster live FPC")
    assert import_prices(resource, [{"sku": sku, "price": old_price}]).failed == 0
    page, _ = cron_until_price(url, old_price)
    assert shows_price(page.text, old_price)

    # Warm the page, then prove the next request is an FPC hit (Varnish in
    # the sandbox; the debug header is sent in developer mode).
    storefront_get(url)
    cached = storefront_get(url)
    print(f"[fpc] before change: X-Magento-Cache-Debug={cached.headers.get('X-Magento-Cache-Debug')} Age={cached.headers.get('Age')}")
    assert cached.headers.get("X-Magento-Cache-Debug") == "HIT"
    assert shows_price(cached.text, old_price)

    started = time.monotonic()
    result = import_prices(resource, [{"sku": sku, "price": new_price}])
    assert (result.succeeded, result.failed) == (1, 0), result.errors
    stale = storefront_get(url)
    print(f"[fpc] after write, before cron: new price shown={shows_price(stale.text, new_price)} cache={stale.headers.get('X-Magento-Cache-Debug')}")

    page, attempts = cron_until_price(url, new_price)
    print(f"[fpc] new price visible after {attempts} cron run(s), {time.monotonic() - started:.0f}s after the write, cache={page.headers.get('X-Magento-Cache-Debug')}")
    assert shows_price(page.text, new_price)
    assert not shows_price(page.text, old_price)


def test_bulk_store_view_update_keeps_store_scope():
    prepare_sandbox()  # creates store view "fr"
    resource = make_resource()
    sku, global_name = "dagster-live-scope", "Dagster live scope"
    ensure_product(resource, sku, 10.0, global_name)
    localized = f"Portee magasin {int(time.time())}"

    result = import_products(resource, [{"sku": sku, "store_values": {"fr": {"name": localized}}}], mode="bulk")

    assert (result.succeeded, result.failed, result.pending) == (1, 0, 0), result.errors
    assert resource.get(f"products/{sku}", store_code="fr")["name"] == localized
    assert resource.get(f"products/{sku}", store_code="all")["name"] == global_name
    assert resource.get(f"products/{sku}", store_code="default")["name"] == global_name


def test_special_price_dates_round_trip():
    prepare_sandbox()
    resource = make_resource()
    sku = "dagster-live-special-dates"
    ensure_product(resource, sku, 30.0, "Dagster live special dates")
    timezone = resource.get("store/storeConfigs")[0]["timezone"]
    price, start, end = 19.5, "2027-01-15 08:30:00", "2027-02-15 23:59:59"

    result = import_prices(
        resource, [{"sku": sku, "store_id": 0, "special_price": price, "special_from": start, "special_to": end}]
    )

    assert result.failed == 0, result.errors
    read = resource.post("products/special-price-information", {"skus": [sku]}).json()
    print(f"[special] timezone={timezone} read back={read}")
    # The dates are stored and read as given, with no shift to or from the
    # store timezone (America/Los_Angeles on a default install).
    assert [(item["store_id"], float(item["price"]), item["price_from"], item["price_to"]) for item in read] == [
        (0, price, start, end)
    ]
