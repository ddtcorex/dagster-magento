"""`delete_missing`: what the catalog has and the source file does not.

The assertions read the requests_mock request history rather than a planned
operation list. A delete that is planned but never sent passes an operation
list test, and that is exactly the failure mode this feature is prone to: it
looks wired and does nothing.

The listing read and the snapshot read both hit `products`, so the mock
separates them by query string. The snapshot carries search criteria (it is
handed the SKUs to look up); the listing carries none, because it asks for
every product in the store.
"""

import pytest
import requests_mock

from dagster_magento.importers import import_products
from dagster_magento.upload import DeleteMissingOutcome, UploadResult

BASE = "https://shop.test/rest/all/V1"

LISTING_FIELDS = "items[sku,type_id,attribute_set_id,extension_attributes]"


def make_resource(**overrides):
    from dagster_magento.resource import MagentoResource

    defaults = dict(
        base_url="https://shop.test",
        username="admin",
        password="secret-password",
        store_view="all",
    )
    defaults.update(overrides)
    resource = MagentoResource(**defaults)
    resource._sleep = lambda seconds: None
    return resource


def _is_listing(request):
    """The listing is the products GET with no filter groups on it.

    `get_paginated` adds searchCriteria[page_size]/[current_page] to every
    call, so page parameters cannot tell the two reads apart; only the
    snapshot filters on a field group, because only the snapshot is handed
    the SKUs to look up.

    The key is matched lowercased: requests_mock normalises query keys, so
    a check for the camel-case spelling never fires and every request looks
    like a listing.
    """
    qs = request.qs
    keys = qs.keys() if hasattr(qs, "keys") else []
    return not any(str(key).lower().startswith("searchcriteria[filtergroups]") for key in keys)


def _wire(m, existing=(), listing=()):
    m.post(f"{BASE}/integration/admin/token", json="token")
    # attribute-sets/list is a search-criteria endpoint and answers with the
    # {"items": [...]} envelope; store/websites is read flat and does not.
    m.get(
        f"{BASE}/eav/attribute-sets/list",
        json={"items": [{"attribute_set_id": 4, "attribute_set_name": "Default"}]},
    )
    m.get(f"{BASE}/store/websites", json=[{"id": 1, "code": "base"}])
    m.get(f"{BASE}/products/attributes", json={"items": []})
    m.post(f"{BASE}/products", json={"id": 1, "sku": "A", "entity_id": 1})
    # The listing read, paged: the first page carries the items, the second
    # is empty, which is how get_paginated learns it has reached the end.
    # Registered BEFORE the snapshot so it cannot shadow it: requests_mock
    # takes the first matcher that matches, not the most specific one.
    #
    # One response, not one response per page: get_paginated stops as soon as
    # a page is shorter than page_size, so a small catalog is read in a
    # single call and a response list would hand back one product per call.
    m.get(
        f"{BASE}/products",
        json={"items": list(listing)},
        additional_matcher=_is_listing,
    )
    # The snapshot read: filter groups present, so it never matches the
    # listing matcher above.
    m.get(
        f"{BASE}/products",
        json={"items": list(existing)},
        additional_matcher=lambda r: not _is_listing(r),
    )


def _deletes(m):
    return [r.url for r in m.request_history if r.method == "DELETE"]


def _listing_requests(m):
    """The delete listing read, and nothing else.

    Scoped to `products` without filter groups: an import makes other
    unfiltered reads (the bridge capability probe, store/websites), and
    counting those would make this test pass for the wrong reason.
    """
    return [
        r
        for r in m.request_history
        if r.method == "GET" and _is_listing(r) and "/products" in r.url
    ]


def test_delete_missing_preview_sends_no_delete_and_names_the_candidates():
    rows = [{"sku": "A", "name": "Kept"}]

    with requests_mock.Mocker() as m:
        _wire(m, existing=[{"sku": "A", "type_id": "simple", "name": "Kept"}],
              listing=[{"sku": "A"}, {"sku": "GONE"}])
        result = import_products(make_resource(), rows, delete_missing="preview")

    assert _deletes(m) == []
    assert result.delete_missing == DeleteMissingOutcome(
        mode="preview", would_delete=("GONE",)
    )


def test_delete_missing_execute_actually_deletes():
    """The request history is the assertion, not the operation list."""
    rows = [{"sku": "A", "name": "Kept"}]

    with requests_mock.Mocker() as m:
        _wire(m, existing=[{"sku": "A", "type_id": "simple", "name": "Kept"}],
              listing=[{"sku": "A"}, {"sku": "GONE"}])
        m.delete(f"{BASE}/products/GONE", status_code=200)
        result = import_products(make_resource(), rows, delete_missing="execute")

    assert _deletes(m) == [f"{BASE}/products/GONE"]
    assert result.delete_missing.deleted == ("GONE",)
    assert result.to_metadata()["delete_deleted"] == 1


def test_delete_missing_none_lists_nothing_at_all():
    """The default must not cost a listing read on every run."""
    rows = [{"sku": "A", "name": "New"}]

    with requests_mock.Mocker() as m:
        _wire(m)
        result = import_products(make_resource(), rows)

    assert result.delete_missing is None
    assert _listing_requests(m) == [], "delete_missing=None must issue zero listing requests"


def test_delete_missing_never_counts_a_delete_as_an_import_row():
    """A delete has no row of its own, so it must not inflate the counts.

    The row is unchanged upstream, so the import skips it and the delete
    happens anyway. Both counts stay where the import alone would leave
    them: the delete is reported in its own outcome, not as a succeeded row.
    """
    rows = [{"sku": "A", "name": "Kept"}]

    with requests_mock.Mocker() as m:
        _wire(m, existing=[{"sku": "A", "type_id": "simple", "name": "Kept"}],
              listing=[{"sku": "A"}, {"sku": "GONE"}])
        m.delete(f"{BASE}/products/GONE", status_code=200)
        result = import_products(make_resource(), rows, delete_missing="execute")

    assert result == UploadResult(
        succeeded=0,
        failed=0,
        skipped_unchanged=1,
        delete_missing=DeleteMissingOutcome(
            mode="execute", would_delete=("GONE",), deleted=("GONE",)
        ),
    )


def test_delete_missing_does_not_delete_a_row_that_failed_validation():
    """A row the caller is trying to fix must not make its SKU look deleted."""
    rows = [{"sku": "A", "name": "Broken", "price": "not-a-number"}]

    with requests_mock.Mocker() as m:
        _wire(m, listing=[{"sku": "A"}])
        m.delete(f"{BASE}/products/A", status_code=200)
        result = import_products(make_resource(), rows, delete_missing="execute")

    assert _deletes(m) == []
    assert result.delete_missing.would_delete == ()
    assert result.failed == 1


def test_delete_missing_reports_a_failed_delete_in_the_error_count():
    rows = [{"sku": "A", "name": "Kept"}]

    with requests_mock.Mocker() as m:
        _wire(m, existing=[{"sku": "A", "type_id": "simple", "name": "Kept"}],
              listing=[{"sku": "A"}, {"sku": "LOCKED"}])
        m.delete(f"{BASE}/products/LOCKED", status_code=409,
                 json={"message": "The product could not be deleted"})
        result = import_products(make_resource(), rows, delete_missing="execute")

    assert result.delete_missing.failed == ("LOCKED",)
    assert result.delete_missing.deleted == ()
    assert result.failed == 1


def test_delete_missing_scope_narrows_the_candidates():
    rows = [{"sku": "A", "name": "Kept"}]
    listing = [
        {"sku": "A", "extension_attributes": {"category_links": [{"category_id": 7}]}},
        {"sku": "OTHER", "extension_attributes": {"category_links": [{"category_id": 9}]}},
    ]

    with requests_mock.Mocker() as m:
        _wire(m, existing=[{"sku": "A", "type_id": "simple", "name": "Kept"}], listing=listing)
        result = import_products(
            make_resource(), rows, delete_missing="preview", delete_scope={9}
        )

    assert result.delete_missing.would_delete == ("OTHER",)


def test_delete_missing_rejects_a_scope_it_cannot_match_before_writing():
    """A caller mistake must not be discovered after rows are saved."""
    rows = [{"sku": "A", "name": "New"}]

    with requests_mock.Mocker() as m:
        _wire(m)
        with pytest.raises(ValueError, match="delete_scope"):
            import_products(
                make_resource(), rows, delete_missing="preview", delete_scope="brand"
            )

    assert _deletes(m) == []
    assert [r for r in m.request_history if r.method == "POST" and "token" not in r.url] == []


def test_delete_missing_rejects_an_unknown_mode_before_writing():
    rows = [{"sku": "A", "name": "New"}]

    with requests_mock.Mocker() as m:
        _wire(m)
        with pytest.raises(ValueError, match="delete_missing must be"):
            import_products(make_resource(), rows, delete_missing="dry")

    assert [r for r in m.request_history if r.method == "POST" and "token" not in r.url] == []


def test_delete_missing_deletes_children_before_a_configurable_parent():
    rows = [{"sku": "KEPT", "name": "Kept"}]

    with requests_mock.Mocker() as m:
        _wire(
            m,
            existing=[{"sku": "KEPT", "type_id": "simple", "name": "Kept"}],
            listing=[
                {"sku": "KEPT", "type_id": "simple"},
                {"sku": "PARENT", "type_id": "configurable"},
                {"sku": "CHILD", "type_id": "simple"},
            ],
        )
        m.delete(f"{BASE}/products/CHILD", status_code=200)
        m.delete(f"{BASE}/products/PARENT", status_code=200)
        result = import_products(make_resource(), rows, delete_missing="execute")

    assert _deletes(m) == [
        f"{BASE}/products/CHILD",
        f"{BASE}/products/PARENT",
    ]
    assert result.delete_missing.deleted == ("CHILD", "PARENT")