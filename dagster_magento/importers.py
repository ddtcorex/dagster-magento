"""One import function per catalog entity, composing validation, the diff
snapshot, the writer's plan and the executor into a single UploadResult.

Every importer counts one outcome per row, never one per operation: a
row that becomes several operations (a configurable product with its
options and child links, a price row with base, special and tier parts)
is failed if any of its operations failed, else pending if any is still
pending, else succeeded. MagentoAuthError is never caught here.
"""

import urllib.parse
from typing import Callable, Literal

import requests
from dagster import MaterializeResult, get_dagster_logger
from pydantic import BaseModel

from dagster_magento.diff import (
    PRODUCT_SNAPSHOT_FIELDS,
    normalize,
    price_matches_snapshot,
    product_matches_snapshot,
    snapshot_media,
    snapshot_prices,
    snapshot_products,
    snapshot_source_items,
    split_changed,
)
from dagster_magento.bridge import BridgeClient, BridgeMode
from dagster_magento.executor import MagentoImportError, check_error_ratio, execute
from dagster_magento.models import (
    AttributeRow,
    AttributeSetRow,
    CategoryRow,
    PriceRow,
    ProductRow,
    SourceItemRow,
    SourceRow,
    StockRow,
    StockSourceLinkRow,
    validate_rows,
)
from dagster_magento.operation import RowError
from dagster_magento.resolvers import ResolveError, Resolver
from dagster_magento.upload import UploadResult
from dagster_magento.writers import PlanResult
from dagster_magento.writers.attribute_sets import plan_attribute_sets
from dagster_magento.writers.attributes import plan_attributes
from dagster_magento.writers.categories import plan_categories
from dagster_magento.writers.inventory import (
    plan_source_items,
    plan_sources,
    plan_stock_source_links,
    plan_stocks,
)
from dagster_magento.writers.media import plan_media
from dagster_magento.writers.pricing import plan_prices
from dagster_magento.writers.products import plan_products

Mode = Literal["sync", "bulk"]

_MAX_ATTRIBUTE_SET_PASSES = 3


# -- shared helpers -------------------------------------------------------------



def _bridge(resource, use_bridge: BridgeMode, capabilities: tuple[str, ...]) -> BridgeClient | None:
    """The bridge client this importer should use, if any.

    `auto` uses whatever the store advertises and falls back per capability,
    `never` ignores an installed module, and `require` refuses to run when a
    capability this path needs is missing: that is a configuration error, not
    something to paper over with a slower path.
    """
    if use_bridge == "never":
        return None

    client = BridgeClient(resource)
    if use_bridge == "require":
        missing = sorted(capability for capability in capabilities if not client.has(capability))
        if missing:
            raise MagentoImportError(
                "the bridge is required for this import but the store does not offer: "
                + ", ".join(missing)
            )

    return client


def _resolver(resource, bridge: BridgeClient | None = None):
    """A resolver that may create categories through the bridge."""
    return Resolver(resource, bridge=bridge)


def _store_id_for(resolver, resource) -> int:
    """Numeric store a snapshot compares against, 0 for the whole catalog."""
    store_view = getattr(resource, "store_view", "all")
    if not store_view or store_view == "all":
        return 0
    return resolver.store_id(store_view)


def _validate(model: type[BaseModel], rows: list, id_field: str) -> tuple[list, list[RowError]]:
    # exclude_unset keeps which fields a caller's model instance actually
    # set: re-validating a full dump would mark every default as explicit,
    # and a partial product update would then reset type, set and websites.
    raw = [row.model_dump(exclude_unset=True) if isinstance(row, BaseModel) else row for row in rows]
    return validate_rows(model, raw, id_field)


def _row_error_dict(error: RowError) -> dict:
    return {"row_ids": [error.row_ref], "status": "failed", "status_code": None, "message": error.message}


def _fold_by_row(
    row_ids: list[str],
    result: UploadResult,
    plan_failed: list[RowError],
    validation_failed: list[RowError],
    skipped: int,
) -> UploadResult:
    """Fold per-operation outcomes into one outcome per row. The executor
    counts each operation's row_refs, so without this a row with three
    operations would be counted three times."""
    failed_refs = {error.row_ref for error in plan_failed}
    pending_refs = set()
    for error in result.errors:
        target = failed_refs if error["status"] == "failed" else pending_refs
        target.update(error["row_ids"])

    # One entry in row_ids per row (two price rows for one SKU in two
    # stores are two rows). A plan RowError for a row that has no entry
    # in row_ids is counted on its own.
    counted = set(row_ids)
    succeeded = pending = 0
    failed = sum(1 for error in plan_failed if error.row_ref not in counted)
    for ref in row_ids:
        if ref in failed_refs:
            failed += 1
        elif ref in pending_refs:
            pending += 1
        else:
            succeeded += 1

    return UploadResult(
        succeeded=succeeded,
        failed=failed + len(validation_failed),
        pending=pending,
        skipped_unchanged=skipped,
        errors=result.errors + [_row_error_dict(error) for error in [*plan_failed, *validation_failed]],
    )


def _complete(
    refs: list[str],
    plan: PlanResult,
    result: UploadResult,
    validation_failed: list[RowError],
    diff_skipped: int,
    fail_on_error_ratio: float | None,
    noop_succeeded: bool = False,
) -> UploadResult:
    """Build the folded result for `refs` (the rows handed to the writer)
    and apply the error ratio. A row the writer planned no operation for
    and neither failed nor skipped is already in the desired state, so it
    counts as skipped_unchanged, unless `noop_succeeded` says planning
    itself did the work (categories are created by the resolver)."""
    op_refs = {ref for op in plan.operations for ref in op.row_refs}
    settled = {error.row_ref for error in plan.failed} | set(plan.skipped)
    row_ids, noop = [], 0
    for ref in refs:
        if ref in op_refs or (noop_succeeded and ref not in settled):
            row_ids.append(ref)
        elif ref not in settled:
            noop += 1
    folded = _fold_by_row(
        row_ids, result, plan.failed, validation_failed, diff_skipped + len(plan.skipped) + noop
    )
    return _check_ratio(folded, fail_on_error_ratio)


def _check_ratio(folded: UploadResult, fail_on_error_ratio: float | None) -> UploadResult:
    # Logged before the check so a raise still leaves the counts in the run log.
    get_dagster_logger().info(f"Magento import result: {folded.to_metadata()}")
    check_error_ratio(folded, fail_on_error_ratio)
    return folded


def _run(
    resource,
    refs: list[str],
    plan: PlanResult,
    mode: Mode,
    validation_failed: list[RowError],
    fail_on_error_ratio: float | None,
    diff_skipped: int = 0,
    noop_succeeded: bool = False,
) -> UploadResult:
    result = execute(resource, plan.operations, mode=mode)
    return _complete(refs, plan, result, validation_failed, diff_skipped, fail_on_error_ratio, noop_succeeded)


def _last_wins(rows: list, key: Callable) -> tuple[list, int]:
    """Keep the last row per key, in first-seen order; earlier duplicates
    are dropped and counted, so a diff never compares a superseded row."""
    kept: dict = {}
    for row in rows:
        kept[key(row)] = row
    return list(kept.values()), len(rows) - len(kept)


# -- attributes, sets, categories ---------------------------------------------


def import_attributes(
    resource,
    rows,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
    use_bridge: BridgeMode = "auto",
):
    """Create or update attributes and their missing options. The writer
    already diffs options against the resolver's cache; `diff` is accepted
    for a uniform signature."""
    valid, invalid = _validate(AttributeRow, rows, "code")
    plan = plan_attributes(valid, _resolver(resource, _bridge(resource, use_bridge, ())), behavior=behavior)
    return _run(resource, [row.code for row in valid], plan, mode, invalid, fail_on_error_ratio)


def import_attribute_sets(
    resource,
    rows,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
    use_bridge: BridgeMode = "auto",
):
    """Pass-based: a set is created in one pass, its groups in the next and
    the attribute assignments once the groups exist, with the resolver's
    set cache refreshed between passes.

    Convergence: a pass converges when it plans nothing that has not
    already succeeded in an earlier pass. The writer re-plans attribute
    assignments on every pass (they are idempotent), so "plans no
    operations at all" would never hold. After 3 executed passes, every
    row that still plans a new operation fails with "attribute set not
    converged after 3 passes"."""
    valid, invalid = _validate(AttributeSetRow, rows, "name")
    bridge = _bridge(resource, use_bridge, ())
    resolver = _resolver(resource, bridge)
    active = list(valid)
    done: set = set()
    touched: list[str] = []
    plan_failed: list[RowError] = []
    result = UploadResult(succeeded=0, failed=0)

    for _ in range(_MAX_ATTRIBUTE_SET_PASSES):
        plan = plan_attribute_sets(active, resolver)
        plan_failed.extend(plan.failed)
        new_ops = [op for op in plan.operations if op not in done]
        if not new_ops:
            break
        pass_result = execute(resource, new_ops, mode=mode)
        result = result.merge(pass_result)
        failed_refs = {error.row_ref for error in plan.failed}
        failed_refs.update(ref for error in pass_result.errors for ref in error["row_ids"])
        touched.extend(ref for op in new_ops for ref in op.row_refs)
        done.update(op for op in new_ops if not set(op.row_refs) & failed_refs)
        active = [row for row in active if row.name not in failed_refs]
        resolver.refresh_attribute_sets()
    else:
        plan = plan_attribute_sets(active, resolver)
        plan_failed.extend(plan.failed)
        stuck = dict.fromkeys(ref for op in plan.operations if op not in done for ref in op.row_refs)
        plan_failed.extend(RowError(ref, "attribute set not converged after 3 passes") for ref in stuck)

    failed_names = {error.row_ref for error in plan_failed}
    unchanged = sum(1 for row in valid if row.name not in touched and row.name not in failed_names)
    folded = _fold_by_row(list(dict.fromkeys(touched)), result, plan_failed, invalid, unchanged)
    return _check_ratio(folded, fail_on_error_ratio)


# Magento 2.4.6 types the `default_sort_by` category attribute as string[]
# while 2.4.9 types it as string, so no single payload shape works on both:
# the plain string the sample files carry is rejected on 2.4.6 with 400, and
# 2.4.6 stores nothing for the array shape either (verified live: accepted
# with 200 but no row written and nothing to read back). There is nothing to
# negotiate, so rows failing with exactly that type error are re-planned
# without the key and executed again instead of failing the whole import over
# one attribute the store cannot persist through REST.
# Quote-agnostic: the executor embeds the raw JSON response body, so the
# quotes arrive backslash-escaped.
_DEFAULT_SORT_BY_TYPE_ERROR = ("string[]", "default_sort_by")


def _default_sort_by_rejected(error: dict) -> bool:
    """Whether one executor error is a store refusing the default_sort_by
    string. The match requires the attribute code, so an unrelated type
    error still fails its row loudly."""
    if error.get("status") != "failed" or error.get("status_code") != 400:
        return False
    message = error.get("message") or ""
    return all(part in message for part in _DEFAULT_SORT_BY_TYPE_ERROR)


def import_categories(
    resource,
    rows,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
    use_bridge: BridgeMode = "auto",
):
    """Ensure category paths and write their attributes. `diff` is ignored:
    the writer is idempotent by path, and a path it resolves or creates
    during planning counts as succeeded even with no attribute to write.

    Rows a store rejects over the `default_sort_by` string shape (Magento
    2.4.6, which types it as string[]) are retried once without that key,
    with a warning naming them: the value is not persistable through that
    store's REST API, and it must not fail the rest of the row."""
    valid, invalid = _validate(CategoryRow, rows, "path")
    plan = plan_categories(
        valid, _resolver(resource, _bridge(resource, use_bridge, (BridgeClient.CATEGORIES_UPSERT,)))
    )
    result = _run(
        resource, [row.path for row in valid], plan, mode, invalid, fail_on_error_ratio, noop_succeeded=True
    )
    return _retry_without_default_sort_by(resource, valid, result, mode, fail_on_error_ratio, use_bridge)


def _retry_without_default_sort_by(resource, valid, result, mode, fail_on_error_ratio, use_bridge="auto"):
    """Re-plan rows rejected over default_sort_by without the key.

    Returns the merged result, or `result` unchanged when no row needs the
    retry. The retry runs once: without the key the type error cannot recur,
    so a row that still fails stays failed instead of looping."""
    by_path = {row.path: row for row in valid}
    paths = sorted(
        {
            ref
            for error in result.errors
            if _default_sort_by_rejected(error)
            for ref in error.get("row_ids") or ()
            if ref in by_path and "default_sort_by" in by_path[ref].attributes
        }
    )
    if not paths:
        return result

    get_dagster_logger().warning(
        "default_sort_by is not writable through this store's REST API "
        "(Magento 2.4.6 types it as string[] and stores nothing for it); "
        f"retrying {len(paths)} row(s) without it: {', '.join(paths)}"
    )
    stripped = [
        row.model_copy(
            update={
                "attributes": {
                    key: value for key, value in row.attributes.items() if key != "default_sort_by"
                }
            }
        )
        for row in (by_path[path] for path in paths)
    ]
    plan = plan_categories(
        stripped,
        _resolver(resource, _bridge(resource, use_bridge, (BridgeClient.CATEGORIES_UPSERT,))),
    )
    retry = _run(
        resource,
        [row.path for row in stripped],
        plan,
        mode,
        [],
        fail_on_error_ratio,
        noop_succeeded=True,
    )
    return _merge_retry(result, retry, paths)


def _merge_retry(result: UploadResult, retry: UploadResult, paths: list[str]) -> UploadResult:
    """Fold a retry back into the first result: the retried rows' original
    failures are replaced by the retry's outcome for exactly those rows."""
    retried = set(paths)
    dropped = {
        ref
        for error in result.errors
        if error.get("status") == "failed"
        for ref in (error.get("row_ids") or ())
        if ref in retried
    }
    errors = [
        error
        for error in result.errors
        if not (
            error.get("status") == "failed"
            and set(error.get("row_ids") or ()) <= retried
        )
    ] + retry.errors
    return UploadResult(
        succeeded=result.succeeded + retry.succeeded,
        failed=result.failed - len(dropped) + retry.failed,
        pending=result.pending + retry.pending,
        skipped_unchanged=result.skipped_unchanged + retry.skipped_unchanged,
        errors=errors,
    )


# -- products -------------------------------------------------------------------


def _product_unchanged(row: ProductRow, snap: dict, resolver, behavior: str) -> bool:
    # disable only ever writes status 2, so the row's own fields are irrelevant.
    if behavior == "disable":
        return normalize(snap.get("status"), "int") == 2
    return product_matches_snapshot(row, snap, resolver)


def import_products(
    resource,
    rows,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
    use_bridge: BridgeMode = "auto",
):
    """Snapshot existing SKUs, skip rows `product_matches_snapshot` proves
    unchanged (diff=True), plan, then execute the main and store-value
    product operations before the type follow-ups, because a configurable
    child must exist before it is linked. Images are out of scope here:
    import_media owns them."""
    valid, invalid = _validate(ProductRow, rows, "sku")
    bridge = _bridge(
        resource, use_bridge, (BridgeClient.PRODUCT_INDEX, BridgeClient.ATTRIBUTE_VALUES)
    )
    resolver = _resolver(resource, bridge)
    skus = list(dict.fromkeys(row.sku for row in valid))
    row_codes = list(dict.fromkeys(code for row in valid for code in row.attributes))
    resolver.preload_attributes(row_codes)
    snapshot = snapshot_products(
        resource,
        skus,
        PRODUCT_SNAPSHOT_FIELDS,
        bridge=bridge,
        store_id=_store_id_for(resolver, resource),
        # Only codes the store knows: an unknown one fails its own row in the
        # writer, and must not make the module reject the whole snapshot.
        attribute_codes=[code for code in row_codes if _known_attribute(resolver, code)],
        require_bridge=use_bridge == "require",
    )

    changed, skipped = valid, 0
    if diff and snapshot:
        changed = [
            row
            for row in valid
            if row.sku not in snapshot or not _product_unchanged(row, snapshot[row.sku], resolver, behavior)
        ]
        skipped = len(valid) - len(changed)

    plan = plan_products(changed, resolver, existing=set(snapshot), behavior=behavior)
    parents = [
        op for op in plan.operations if op.endpoint == "products" or op.endpoint.startswith("products/")
    ]
    result = execute(resource, parents, mode=mode)
    # A follow-up of a parent that failed or is still pending would link
    # options or children onto a product that may not exist; that row is
    # already counted from the parent's outcome.
    unsettled = {ref for error in result.errors for ref in error["row_ids"]}
    follow_ups = [
        op for op in plan.operations if op not in parents and not set(op.row_refs) & unsettled
    ]
    follow_ups = _drop_attached_children(resource, follow_ups, existing=set(snapshot))
    result = result.merge(execute(resource, follow_ups, mode=mode))
    return _complete([row.sku for row in changed], plan, result, invalid, skipped, fail_on_error_ratio)


def _known_attribute(resolver, code: str) -> bool:
    try:
        resolver.attribute(code)
    except ResolveError:
        return False
    return True


def _drop_attached_children(resource, operations: list, existing: set[str]) -> list:
    """Drop configurable child links that already exist: Magento answers a
    re-link with 400 "The product is already attached." (verified live on
    2.4.9). Only parents that existed before this run can have children,
    so a new parent costs no extra GET."""
    attached: dict[str, set[str]] = {}
    kept = []
    for op in operations:
        parent = op.row_refs[0]
        if op.endpoint.endswith("/child") and parent in existing:
            if parent not in attached:
                children = resource.get(f"configurable-products/{urllib.parse.quote(parent, safe='')}/children")
                attached[parent] = {child["sku"] for child in children}
            if op.payload["childSku"] in attached[parent]:
                continue
        kept.append(op)
    return kept


# -- prices ---------------------------------------------------------------------


def _merge_prices(rows: list[PriceRow]) -> tuple[list[PriceRow], int]:
    """Merge base and special prices per (sku, store_id) and tiers per sku
    (tiers are not store scoped): a later non-None field overrides an
    earlier one with the same key. Rows for one SKU in different stores
    stay separate rows; the merged tiers ride on that SKU's first row.
    Returns the merged rows and how many rows were folded into another."""
    merged: dict[tuple[str, int], PriceRow] = {}
    tiers: dict[str, list] = {}
    for row in rows:
        if row.tiers is not None:
            tiers[row.sku] = row.tiers
        key = (row.sku, row.store_id)
        if key not in merged:
            merged[key] = row.model_copy(update={"tiers": None})
            continue
        for field in ("price", "special_price", "special_from", "special_to"):
            if getattr(row, field) is not None:
                setattr(merged[key], field, getattr(row, field))

    result = list(merged.values())
    for row in result:
        row.tiers = tiers.pop(row.sku, None)
    return result, len(rows) - len(merged)


def import_prices(
    resource,
    rows,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
    use_bridge: BridgeMode = "auto",
):
    """Base, special and tier prices through the price-storage list
    endpoints. Always sync: those endpoints take lists and have no bulk
    route, so `mode` is ignored."""
    valid, invalid = _validate(PriceRow, rows, "sku")
    merged, duplicates = _merge_prices(valid)
    snapshot = snapshot_prices(resource, list(dict.fromkeys(row.sku for row in merged)))
    website_ids = {}
    if any(row.tiers for row in merged):
        website_ids = {item["code"]: item["id"] for item in resource.get("store/websites")}

    changed, skipped = merged, 0
    if diff:
        changed = [
            row
            for row in merged
            if row.sku not in snapshot or not price_matches_snapshot(row, snapshot[row.sku], website_ids)
        ]
        skipped = len(merged) - len(changed)

    current_tiers = {sku: entry["tiers"] for sku, entry in snapshot.items()}
    plan = plan_prices(changed, current_tiers=current_tiers, website_ids=website_ids)
    refs = [row.sku for row in changed]
    return _run(resource, refs, plan, "sync", invalid, fail_on_error_ratio, duplicates + skipped)


# -- inventory ------------------------------------------------------------------


def import_sources(
    resource,
    rows,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
    use_bridge: BridgeMode = "auto",
):
    """Create or overwrite MSI sources. `diff` is ignored (no snapshot)."""
    valid, invalid = _validate(SourceRow, rows, "source_code")
    plan = plan_sources(valid)
    return _run(resource, [row.source_code for row in valid], plan, mode, invalid, fail_on_error_ratio)


def import_stocks(
    resource,
    rows,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
    use_bridge: BridgeMode = "auto",
):
    """Create MSI stocks bound to website sales channels. `diff` is ignored."""
    valid, invalid = _validate(StockRow, rows, "name")
    plan = plan_stocks(valid, _resolver(resource, _bridge(resource, use_bridge, ())))
    return _run(resource, [row.name for row in valid], plan, mode, invalid, fail_on_error_ratio)


def import_stock_source_links(
    resource,
    rows,
    stock_ids: dict[str, int] | None = None,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
    use_bridge: BridgeMode = "auto",
):
    """Link sources to stocks. `stock_ids` (name -> id) is read from
    GET inventory/stocks when not given. `diff` is ignored."""
    valid, invalid = _validate(StockSourceLinkRow, rows, "stock")
    if stock_ids is None:
        stocks = resource.get("inventory/stocks").get("items", [])
        stock_ids = {item["name"]: item["stock_id"] for item in stocks}
    plan = plan_stock_source_links(valid, stock_ids)
    refs = [f"{row.stock}/{row.source_code}" for row in valid]
    return _run(resource, refs, plan, mode, invalid, fail_on_error_ratio)


def import_source_items(
    resource,
    rows,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
    use_bridge: BridgeMode = "auto",
):
    """Write source item quantity and status, skipping pairs whose current
    (quantity, status) already match. The last row per pair wins."""
    valid, invalid = _validate(SourceItemRow, rows, "sku")
    unique, duplicates = _last_wins(valid, lambda row: (row.source_code, row.sku))

    changed, skipped = unique, 0
    if diff:
        snapshot = snapshot_source_items(resource, list(dict.fromkeys(row.sku for row in unique)))
        changed, skipped = split_changed(
            unique,
            snapshot,
            key=lambda row: (row.source_code, row.sku),
            project=lambda row: (normalize(row.quantity, "decimal"), int(row.status)),
            project_existing=lambda item: (normalize(item[0], "decimal"), int(item[1])),
        )

    plan = plan_source_items(changed)
    refs = [f"{row.source_code}/{row.sku}" for row in changed]
    return _run(resource, refs, plan, mode, invalid, fail_on_error_ratio, duplicates + skipped)


# -- media ----------------------------------------------------------------------


def import_media(
    resource,
    rows,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
    use_bridge: BridgeMode = "auto",
):
    """Upload product images. The gallery snapshot is always read (the
    writer needs it for image identity), so `diff` only documents that a
    matching image is never re-sent. A SKU whose gallery cannot be read
    fails its row."""
    valid, invalid = _validate(ProductRow, rows, "sku")
    current: dict[str, list[dict]] = {}
    unreadable: list[RowError] = []
    for sku in dict.fromkeys(row.sku for row in valid if row.images):
        try:
            current[sku] = snapshot_media(resource, urllib.parse.quote(sku, safe=""))
        except requests.exceptions.HTTPError as error:
            unreadable.append(RowError(sku, f"cannot read media gallery: {error}"))

    readable = [row for row in valid if row.sku not in {error.row_ref for error in unreadable}]
    plan = plan_media(readable, current)
    plan.failed.extend(unreadable)
    return _run(resource, [row.sku for row in valid], plan, mode, invalid, fail_on_error_ratio)


# -- Dagster --------------------------------------------------------------------


def to_materialize_result(
    result: UploadResult, fail_on_error_ratio: float | None = None
) -> MaterializeResult:
    """Attach the counts as asset metadata, then apply the error ratio. The
    metadata is logged first so a raise still leaves the counts visible."""
    materialized = MaterializeResult(metadata=result.to_metadata())
    get_dagster_logger().info(f"Magento import result: {result.to_metadata()}")
    check_error_ratio(result, fail_on_error_ratio)
    return materialized
