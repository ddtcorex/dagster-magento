"""Hard delete against the live sandbox: preview, execute, and the order the
deletes are sent in.

Marked `live`; see conftest.py for how to run it.

A note on what this file does and does not prove. The plan assumed Magento
refuses to delete a configurable or a bundle that still has children, and
that the child-before-parent order is what makes a composite parent
deletable at all. Measured against this sandbox (Magento 2.4.9), that is not
true: `DELETE /V1/products/<sku>` deletes a bundle parent and a grouped
parent with their children still attached. The ordering is therefore
conventional hygiene and a reproducible request log, not a guard the store
would have rejected the reverse of. `dagster_magento/catalog.py` says so at
the definition, and the test below pins the order by what was actually sent
rather than by what the store refused.
"""

import urllib.parse
from unittest import mock

import pytest
from live_support import make_resource

from dagster_magento import import_categories, import_products, MagentoResource
from dagster_magento.models import CategoryRow, GroupedLink, ProductRow
from dagster_magento.resolvers import Resolver

pytestmark = pytest.mark.live

PARENT_SKU = "DELETE-ME-PARENT"
CHILD_SKU = "DELETE-ME-CHILD"
LOOSE_SKU = "DELETE-ME-LOOSE"

# The store is shared with the other live tests, and `delete_missing`
# compares the WHOLE catalog against the rows in the file. An unscoped run
# therefore names every product the e2e suite imported as a candidate, and an
# executing one would delete all of them. Scoping to a category of this
# file's own is what makes it testable beside the other live tests instead of
# only on a store that happens to hold nothing else.
CATEGORY_PATH = "dagster delete-missing scratch"
SKUS = (CHILD_SKU, LOOSE_SKU, PARENT_SKU)


def _row(sku, **overrides):
    # A price on every row: Magento refuses to create a simple product
    # without one ("The Price attribute value is empty"), and a seed that
    # fails tells nothing about the delete path this file exists to prove.
    overrides.setdefault("price", 10.0)
    return ProductRow(sku=sku, name=sku, **overrides)


def _exists(resource, sku) -> bool:
    """Whether the store still answers for this SKU.

    `resource.get` raises `HTTPError` on a 404, so the check goes through
    `resource._send`, which is the same authenticated call the rest of the
    client makes, and inspects the status code instead.

    `_fetch_token()` runs first: `_send` reads `resource._token`, which is
    only populated after a login, so a fresh resource answers 401 for every
    SKU and every existence check reads as "still there".
    """
    resource._fetch_token()
    url = resource._url(f"products/{urllib.parse.quote(sku, safe='')}")
    response = resource._send("GET", url, resource._token)
    return response.status_code != 404


def _delete(resource, sku) -> None:
    """Remove a SKU, tolerating one that is already gone.

    `resource.delete` raises on 404, so the pre-seed and the teardown both
    have to ignore a product that is not there. The `_exists` check is what
    distinguishes "already gone" from a real refusal worth failing on.
    """
    if _exists(resource, sku):
        resource.delete(f"products/{urllib.parse.quote(sku, safe='')}")


@pytest.fixture
def seeded():
    """A freshly seeded sandbox, torn down whatever the outcome.

    Function-scoped on purpose. Each test deletes what the previous one
    deleted, so a module-scoped seed would leave the second test with an
    empty store to prove a delete against, and it would pass for the wrong
    reason: an empty candidate set makes every delete assertion vacuous.

    Every SKU is removed on the way out, children first, because that is
    the order the library itself uses. A live test that leaves the sandbox
    dirty makes the next run fail for the wrong reason.
    """
    resource = make_resource()
    for sku in SKUS:
        _delete(resource, sku)

    cats = import_categories(resource, [CategoryRow(path=CATEGORY_PATH)], use_bridge="never")
    assert cats.failed == 0, cats.errors
    # `Resolver.category_id` rather than a hand-rolled listing: it owns the
    # tree walk and the path normalisation, and `GET categories` answers a
    # bare structure that has no `items` key to index into.
    category_id = Resolver(resource).category_id(CATEGORY_PATH)

    created = import_products(
        resource,
        [
            _row(CHILD_SKU, categories=[CATEGORY_PATH]),
            _row(LOOSE_SKU, categories=[CATEGORY_PATH]),
            _row(
                PARENT_SKU,
                type="grouped",
                categories=[CATEGORY_PATH],
                grouped_links=[GroupedLink(sku=CHILD_SKU, qty=1)],
            ),
        ],
        use_bridge="never",
    )
    assert created.failed == 0, created.errors
    assert _exists(resource, PARENT_SKU)
    assert _exists(resource, CHILD_SKU)

    yield resource, {category_id}

    for sku in SKUS:
        _delete(resource, sku)


def test_delete_missing_previews_without_deleting(seeded):
    """Preview names the vanished SKUs and leaves the store alone."""
    resource, scope = seeded

    result = import_products(
        resource, [_row(PARENT_SKU, type="grouped", categories=[CATEGORY_PATH])],
        delete_missing="preview", delete_scope=scope, use_bridge="never",
    )

    assert result.delete_missing.would_delete == (CHILD_SKU, LOOSE_SKU)
    assert result.delete_missing.deleted == ()
    assert _exists(resource, LOOSE_SKU), "preview must not delete"
    assert _exists(resource, CHILD_SKU), "preview must not delete"
    assert _exists(resource, PARENT_SKU)


def test_delete_missing_execute_removes_vanished_products(seeded):
    """The composite parent is in the file, so it survives; its child is not."""
    resource, scope = seeded

    result = import_products(
        resource, [_row(PARENT_SKU, type="grouped", categories=[CATEGORY_PATH])],
        delete_missing="execute", delete_scope=scope, use_bridge="never",
    )

    assert result.delete_missing.would_delete == (CHILD_SKU, LOOSE_SKU)
    assert result.delete_missing.deleted == (CHILD_SKU, LOOSE_SKU)
    assert result.delete_missing.failed == ()
    assert not _exists(resource, LOOSE_SKU)
    assert not _exists(resource, CHILD_SKU)
    assert _exists(resource, PARENT_SKU), "a parent kept in the file must survive"


def test_delete_missing_sends_a_composite_parent_after_its_children(seeded):
    """The order is pinned by what was sent, not by what the store refused.

    Magento does not refuse the reverse, so an assertion built on a refusal
    would pass for the wrong reason. This one watches the HTTP calls: the
    grouped parent must not be asked to go before the child it links to.
    """
    resource, scope = seeded

    sent: list[str] = []
    original = MagentoResource.delete

    # `MagentoResource` is a frozen pydantic model, so it cannot carry a
    # recording attribute: `resource.delete = ...` raises. Patching the
    # method on the class is the only form that survives, and
    # `mock.patch.object` puts the original back when the block ends.
    def _recording_delete(self, endpoint, store_code=None):
        sent.append(endpoint)
        return original(self, endpoint, store_code=store_code)

    with mock.patch.object(MagentoResource, "delete", _recording_delete):
        result = import_products(
            resource, [], delete_missing="execute", delete_scope=scope,
            use_bridge="never",
        )

    deletes = [endpoint.rsplit("/", 1)[-1] for endpoint in sent]
    assert set(deletes) == set(SKUS), "every candidate is sent exactly once"
    assert deletes.index(CHILD_SKU) < deletes.index(PARENT_SKU), (
        f"child must go first, got {deletes}"
    )
    assert set(result.delete_missing.deleted) == set(SKUS)
    for sku in SKUS:
        assert not _exists(resource, sku), sku
