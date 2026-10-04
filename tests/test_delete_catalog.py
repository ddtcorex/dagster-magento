"""The delete candidate listing and the arithmetic on top of it.

`snapshot_products` in `diff.py` answers only the SKUs it is handed, so it
cannot answer "which products exist": a difference computed from it is always
empty. These functions are the listing a delete run compares against, kept
apart from `diff.py` so both can be tested against a stub.
"""

import pytest

from dagster_magento.catalog import (
    candidates_for_delete,
    delete_candidates,
    delete_operations,
    guard_execute,
    missing_skus,
    order_for_delete,
)
from dagster_magento.upload import DeleteMissingOutcome


class _StubResource:
    """Answers paginated `GET products` with `{items: [...]}` pages."""

    def __init__(self, items):
        self.items = items
        self.calls = []

    def get_paginated(self, endpoint, params=None, page_size=1000, **kwargs):
        self.calls.append((endpoint, params, page_size))
        return list(self.items)


def _item(sku, type_id="simple", category_ids=(), attribute_set_id=None):
    return {
        "sku": sku,
        "type_id": type_id,
        "attribute_set_id": attribute_set_id,
        "extension_attributes": {
            "category_links": [{"category_id": value} for value in category_ids]
        },
    }


def test_delete_candidates_projects_sku_type_set_and_categories():
    resource = _StubResource(
        [_item("A", "configurable", [7, 9], 4), _item("B")]
    )

    catalog = delete_candidates(resource)

    assert set(catalog) == {"A", "B"}
    assert catalog["A"]["type_id"] == "configurable"
    assert catalog["A"]["category_ids"] == {7, 9}
    assert catalog["A"]["attribute_set_id"] == 4
    assert catalog["B"]["category_ids"] == set()


def test_delete_candidates_asks_only_for_the_fields_it_uses():
    resource = _StubResource([_item("A")])

    delete_candidates(resource)

    endpoint, params, page_size = resource.calls[0]
    assert endpoint == "products"
    assert params["fields"] == "items[sku,type_id,attribute_set_id,extension_attributes]"
    assert page_size == 1000


def test_delete_candidates_prefers_the_bridge_when_it_has_the_index():
    class _Bridge:
        PRODUCT_INDEX = "products.index"

        def __init__(self):
            self.used = False

        def has(self, capability):
            return True

        def index_by_sku(self):
            self.used = True
            # The real client returns {sku: item}; the sku is the index key.
            return {
                "BRIDGED": {
                    "sku": "BRIDGED",
                    "type_id": "simple",
                    "attribute_set_id": None,
                    "extension_attributes": {
                        "category_links": [{"category_id": 3}]
                    },
                }
            }

    bridge = _Bridge()
    resource = _StubResource([_item("REST-ONLY")])

    catalog = delete_candidates(resource, bridge=bridge)

    assert set(catalog) == {"BRIDGED"}
    assert resource.calls == [], "the bridge index answers the listing on its own"


def test_delete_candidates_falls_back_to_rest_when_the_bridge_fails():
    class _Bridge:
        PRODUCT_INDEX = "products.index"

        def has(self, capability):
            return True

        def index_by_sku(self):
            raise RuntimeError("module exploded")

    resource = _StubResource([_item("A")])

    catalog = delete_candidates(resource, bridge=_Bridge())

    assert set(catalog) == {"A"}


def test_delete_candidates_raises_when_the_bridge_is_required_and_fails():
    class _Bridge:
        PRODUCT_INDEX = "products.index"

        def has(self, capability):
            return True

        def index_by_sku(self):
            raise RuntimeError("module exploded")

    with pytest.raises(Exception, match="listing failed"):
        delete_candidates(_StubResource([_item("A")]), bridge=_Bridge(), require_bridge=True)


# -- delete_scope ---------------------------------------------------------------


def test_candidates_for_delete_narrows_by_category_id():
    catalog = {
        "A": {"type_id": "simple", "category_ids": {7}, "attribute_set_id": 1},
        "B": {"type_id": "simple", "category_ids": {9}, "attribute_set_id": 1},
    }

    assert candidates_for_delete(catalog, {7}) == {"A"}


def test_candidates_for_delete_narrows_by_attribute_set_id():
    catalog = {
        "A": {"type_id": "simple", "category_ids": set(), "attribute_set_id": 4},
        "B": {"type_id": "simple", "category_ids": set(), "attribute_set_id": 5},
    }

    assert candidates_for_delete(catalog, {4}) == {"A"}


def test_candidates_for_delete_without_a_scope_keeps_everything():
    catalog = {"A": {"type_id": "simple"}, "B": {"type_id": "simple"}}

    assert candidates_for_delete(catalog, None) == {"A", "B"}


def test_candidates_for_delete_rejects_an_unsupported_shape():
    with pytest.raises(ValueError, match="delete_scope"):
        candidates_for_delete({"A": {"type_id": "simple"}}, ["not", "a", "set"])


def test_candidates_for_delete_rejects_an_attribute_code_scope():
    """An attribute-code scope is not supported in this release.

    The listing carries no per-SKU attribute values, so a code filter would
    have to guess. A filter that can return the wrong candidate set is worse
    than no filter, so the shape is refused rather than half-implemented.
    """
    with pytest.raises(ValueError, match="delete_scope"):
        candidates_for_delete({"A": {"type_id": "simple"}}, "brand")


def test_candidates_for_delete_rejects_an_empty_scope_as_a_mistake():
    """An empty set would silently select nothing, which reads as a clean run."""
    with pytest.raises(ValueError, match="delete_scope"):
        candidates_for_delete({"A": {"type_id": "simple"}}, set())


# -- the difference -------------------------------------------------------------


def test_missing_skus_is_the_difference_against_the_rows():
    catalog = {"A": {}, "B": {}, "C": {}}

    assert missing_skus(catalog, ["A", "C"]) == ("B",)


def test_missing_skus_ignores_a_row_that_failed_validation():
    """A row that failed validation must not make its own SKU look deleted.

    Otherwise a run fixing one broken row deletes the very product the caller
    was trying to repair.
    """
    catalog = {"A": {}, "B": {}}

    assert missing_skus(catalog, [], invalid=("B",)) == ("A",)
    assert missing_skus(catalog, [], invalid=()) == ("A", "B")


def test_missing_skus_treats_a_catalog_sku_the_file_has_as_present():
    catalog = {"A": {}, "B": {}}

    assert missing_skus(catalog, ["A", "B"]) == ()


# -- ordering -------------------------------------------------------------------


def test_order_for_delete_puts_children_before_parents():
    catalog = {
        "PARENT": {"type_id": "configurable"},
        "CHILD": {"type_id": "simple"},
        "SET": {"type_id": "bundle"},
    }

    assert list(order_for_delete({"PARENT", "CHILD", "SET"}, catalog)) == [
        "CHILD",
        "PARENT",
        "SET",
    ]


def test_order_for_delete_is_stable_within_a_partition():
    catalog = {sku: {"type_id": "simple"} for sku in ("C", "A", "B")}

    assert list(order_for_delete({"B", "A", "C"}, catalog)) == ["A", "B", "C"]


def test_order_for_delete_treats_an_unknown_type_as_a_child():
    """A SKU the listing could not type must not be left for last.

    If Magento refuses it as a parent, it fails as one failed row, which the
    error ratio already reports. Leaving it for last would guarantee the
    failure instead of merely permitting it.
    """
    catalog = {"UNKNOWN": {}, "PARENT": {"type_id": "configurable"}}

    assert list(order_for_delete({"UNKNOWN", "PARENT"}, catalog)) == [
        "UNKNOWN",
        "PARENT",
    ]


def test_delete_candidates_uses_rest_for_a_category_scope():
    """The bridge index carries no category links, so a category scope must
    not be answered from it.

    Asking the bridge would return an empty `category_ids` for every SKU,
    which the filter reads as "nothing matches" instead of "the module cannot
    answer this" - a scope that silently deletes nothing.
    """

    class _Bridge:
        PRODUCT_INDEX = "products.index"

        def has(self, capability):
            return True

        def index_by_sku(self):
            return {
                "A": {"sku": "A", "type_id": "simple", "attribute_set_id": None},
            }

    resource = _StubResource([_item("A", category_ids=[7])])

    catalog = delete_candidates(resource, bridge=_Bridge(), scope={7})

    assert catalog["A"]["category_ids"] == {7}
    assert resource.calls, "a category scope has to read the REST listing"


def test_delete_candidates_uses_rest_for_any_scope_not_only_a_category_one():
    """An attribute-set scope has to read REST too, and the reason is not
    obvious.

    The bridge index carries `attribute_set_id`, so the scope looks
    answerable from it. It is not: the filter matches a scope value against
    category links OR attribute-set ids, and `{4}` cannot say which it meant.
    Taking the index here would drop every category link and silently widen
    or narrow the delete.
    """

    class _Bridge:
        PRODUCT_INDEX = "products.index"

        def has(self, capability):
            return True

        def index_by_sku(self):
            return {
                "A": {"sku": "A", "type_id": "simple", "attribute_set_id": 4},
            }

    resource = _StubResource([_item("A", category_ids=[7], attribute_set_id=9)])

    catalog = delete_candidates(resource, bridge=_Bridge(), scope={4})

    assert catalog["A"]["attribute_set_id"] == 9
    assert resource.calls, "a scope has to read the REST listing"


# -- planning and executing the deletes ----------------------------------------


def test_delete_operations_build_one_delete_per_sku_in_order():
    operations = delete_operations(
        ("CHILD", "PARENT"), {"CHILD": {"type_id": "simple"}, "PARENT": {"type_id": "configurable"}}
    )

    assert [(op.method, op.endpoint) for op in operations] == [
        ("DELETE", "products/CHILD"),
        ("DELETE", "products/PARENT"),
    ]
    assert operations[0].store_code == "default"
    # A delete carries no row of its own: the row that used to exist is the
    # thing that is gone, so attaching row_refs would inflate the counts
    # _fold_by_row reports for the import.
    assert operations[0].row_refs == ()


def test_delete_operations_carry_the_caller_store_code():
    operations = delete_operations(("A",), {}, store_code="de")

    assert operations[0].store_code == "de"


def test_delete_operations_of_nothing_plan_nothing():
    assert delete_operations((), {}) == []


def test_guard_execute_sends_nothing_in_preview():
    calls = []

    outcome = guard_execute(("A", "B"), "preview", _Recorder(calls))

    assert calls == [], "a preview must not touch Magento at all"
    assert outcome == DeleteMissingOutcome(mode="preview", would_delete=("A", "B"))


def test_guard_execute_deletes_in_execute():
    recorder = _Recorder([])

    outcome = guard_execute(("A",), "execute", recorder)

    assert recorder.deleted == ["A"]
    assert outcome == DeleteMissingOutcome(mode="execute", would_delete=("A",), deleted=("A",))


def test_guard_execute_reports_a_failed_delete_without_raising():
    class _Failing(_Recorder):
        def delete(self, sku, store_code=None):
            self.attempts.append(sku)
            self.deleted.append(sku)
            raise RuntimeError("product is locked")

    outcome = guard_execute(("A",), "execute", _Failing([]))

    assert outcome.failed == ("A",)
    assert outcome.deleted == ()


def test_guard_execute_rejects_an_unknown_mode():
    with pytest.raises(ValueError, match="mode must be 'preview' or 'execute'"):
        guard_execute(("A",), "dry", _Recorder([]))


class _Recorder:
    def __init__(self, calls):
        self.calls = calls
        self.attempts = []
        self.deleted = []

    def delete(self, sku, store_code=None):
        self.attempts.append(sku)
        self.deleted.append(sku)
        self.calls.append(sku)
