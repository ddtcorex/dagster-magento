"""The catalog listing a delete run compares against, and the arithmetic.

`snapshot_products` in `diff.py` answers only the SKUs it is handed: it
filters the search on the sku field, so the difference between it and the
rows of the file is structurally empty and a delete built on it could never
fire. This module owns the full listing and the pure functions on top of it,
kept apart from `diff.py` so both can be tested against a stub.

Nothing here sends anything through the executor. A listing read is a read;
the operations are built by `delete_operations` and run by `guard_execute`,
which is the only place in the delete path that touches Magento.
"""

from dagster import get_dagster_logger

from dagster_magento.executor import MagentoImportError
from dagster_magento.operation import Operation
from dagster_magento.upload import DeleteMissingOutcome

# The three fields the whole feature reads: the identity, the type the delete
# ordering needs, and the two a `delete_scope` filter can match on. A full
# product body is never fetched, because a 100k-product store would pay for
# every field of every product to learn its SKU.
_LISTING_FIELDS = "items[sku,type_id,attribute_set_id,extension_attributes]"
_CATALOG_PAGE_SIZE = 1000


def _category_ids(item: dict) -> set:
    links = (item.get("extension_attributes") or {}).get("category_links") or []
    return {
        int(link["category_id"])
        for link in links
        if link.get("category_id") is not None
    }


def _listing_entry(item: dict) -> dict:
    return {
        "type_id": item.get("type_id"),
        "category_ids": _category_ids(item),
        "attribute_set_id": item.get("attribute_set_id"),
    }

def delete_candidates(resource, bridge=None, require_bridge: bool = False, scope=None) -> dict:
    """Every product in the store, as `{sku: {type_id, category_ids,
    attribute_set_id}}`.

    The bridge product index answers the identity and the entity fields in
    one cached pass when the module is installed. It carries no
    `extension_attributes` (see `REST_ONLY_PRODUCT_FIELDS` in `diff.py`), so
    any non-None scope takes the REST listing instead: the scope filter
    matches a value against category links OR attribute-set ids, and only
    REST carries the first. Asking the bridge would return an empty
    `category_ids` for every SKU, which the filter reads as "nothing
    matches" rather than as "the module cannot answer this" - a scope that
    silently deletes nothing.

    A bridge failure falls back to REST with a warning unless
    `require_bridge` is set, matching `snapshot_products`' own contract:
    `auto` degrades, `require` raises rather than quietly running a
    different path.
    """
    if bridge is not None and bridge.has(bridge.PRODUCT_INDEX) and scope is None:
        try:
            return {
                sku: _listing_entry(item)
                for sku, item in bridge.index_by_sku().items()
            }
        except Exception as error:  # noqa: BLE001 - mirrors snapshot_products
            if require_bridge:
                raise MagentoImportError(
                    f"bridge product listing failed: {error}"
                ) from error
            get_dagster_logger().warning(
                f"bridge product listing failed ({error}); using the REST listing"
            )

    return {
        item["sku"]: _listing_entry(item)
        for item in resource.get_paginated(
            "products", params={"fields": _LISTING_FIELDS}, page_size=_CATALOG_PAGE_SIZE
        )
    }


def candidates_for_delete(catalog: dict, scope) -> set:
    """Narrow the listing to the part of the catalog the source file is
    authoritative for.

    `scope` is None (the whole catalog, the honest mirror semantics) or a
    set of category IDs or attribute-set IDs, matched against the fields the
    listing already carries. Anything else is refused: a typo that silently
    became "the whole catalog" is the failure this function exists to
    prevent, and so is a filter that quietly matches nothing and reads as a
    clean run.
    """
    if scope is None:
        return set(catalog)
    if isinstance(scope, str):
        raise ValueError(
            "delete_scope must be None, or a set of category or attribute-set "
            f"ids; an attribute code ({scope!r}) is not supported, because the "
            "listing carries no per-SKU attribute values to match it against"
        )
    if isinstance(scope, (set, frozenset)) and all(isinstance(value, int) for value in scope):
        if not scope:
            raise ValueError(
                "delete_scope must be None, or a non-empty set of category or "
                "attribute-set ids; an empty set would select nothing and read "
                "as a clean run"
            )
        wanted = set(scope)
        return {
            sku
            for sku, entry in catalog.items()
            if wanted & set(entry.get("category_ids") or ())
            or entry.get("attribute_set_id") in wanted
        }
    raise ValueError(
        "delete_scope must be None, or a set of category or attribute-set ids, "
        f"got {type(scope).__name__}"
    )


def missing_skus(catalog, row_skus, invalid=()) -> tuple:
    """`catalog - row_skus - invalid`, sorted.

    `invalid` matters: a row that failed validation is a row the caller is
    trying to fix, so its SKU must not be read as "vanished upstream". This
    is the single most important line in the delete path.
    """
    return tuple(sorted(set(catalog) - set(row_skus) - set(invalid)))


# Composite types whose children reference them by SKU. Removing a child
# first is the conventional order, and it is what keeps a run reproducible
# and readable in the request log.
#
# It is NOT a requirement the store enforces: measured against Magento
# 2.4.9, `DELETE /V1/products/<sku>` deletes a bundle or a grouped parent
# with its children still attached, and the same holds for a configurable.
# The ordering is therefore hygiene, not a guard that Magento would have
# rejected the reverse of. See tests/live/test_live_delete_missing.py.
_PARENT_TYPES = ("configurable", "bundle", "grouped")


def order_for_delete(skus, catalog: dict):
    """Children before parents, sorted by SKU inside each partition.

    A run is reproducible because both partitions are sorted. A SKU the
    listing could not type is treated as a child: it is the conservative
    placement, since a parent that is really a composite has to go last and
    nothing is lost by assuming the weaker case.
    """
    children = sorted(
        sku for sku in skus if catalog.get(sku, {}).get("type_id") not in _PARENT_TYPES
    )
    parents = sorted(
        sku for sku in skus if catalog.get(sku, {}).get("type_id") in _PARENT_TYPES
    )
    return children + parents


def delete_operations(skus, catalog: dict, store_code: str = "default") -> list:
    """One DELETE operation per SKU, children before parents.

    `row_refs` is empty on purpose. A delete has no row of its own, and
    `_fold_by_row` counts per row_ref: attaching the vanishing SKU would
    inflate the counts the importer reports for the rows the file did
    contain.

    The endpoint is built here rather than in the executor so the executor
    stays the one place that decides how an Operation becomes HTTP, and a
    SKU containing a slash or a space is rejected by Magento rather than
    silently splitting the path.
    """
    return [
        Operation(
            method="DELETE",
            endpoint=f"products/{sku}",
            payload=None,
            row_refs=(),
            store_code=store_code,
        )
        for sku in order_for_delete(skus, catalog)
    ]


def guard_execute(skus, mode: str, deleter, store_code: str = "default") -> DeleteMissingOutcome:
    """The mode guard: `preview` sends nothing, `execute` deletes.

    Deleting a product is not reversible from this library, so preview is a
    first-class outcome rather than a log line: it returns the exact set
    `execute` would have removed, which is what makes the dry-run-then-
    approve workflow possible at all.

    A delete that raises is counted, not raised. One locked product must not
    abort a run that has already removed a thousand others, and the caller's
    error ratio needs the count to see it.
    """
    if mode not in ("preview", "execute"):
        raise ValueError(f"mode must be 'preview' or 'execute', got {mode!r}")

    candidates = tuple(skus)
    if mode == "preview":
        return DeleteMissingOutcome(mode="preview", would_delete=candidates)

    logger = get_dagster_logger()
    deleted: list[str] = []
    failed: list[str] = []
    for sku in candidates:
        try:
            deleter.delete(sku, store_code=store_code)
        except Exception as error:  # noqa: BLE001 - one product must not abort the run
            failed.append(sku)
            logger.warning(f"delete {sku} failed: {error}")
            continue
        deleted.append(sku)

    return DeleteMissingOutcome(
        mode="execute", would_delete=candidates, deleted=tuple(deleted), failed=tuple(failed)
    )
