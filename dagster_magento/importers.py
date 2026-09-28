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
from dagster_magento.executor import check_error_ratio, execute
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
from dagster_magento.resolvers import Resolver
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


def _validate(model: type[BaseModel], rows: list, id_field: str) -> tuple[list, list[RowError]]:
    raw = [row.model_dump() if isinstance(row, BaseModel) else row for row in rows]
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
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
):
    """Create or update attributes and their missing options. The writer
    already diffs options against the resolver's cache; `diff` is accepted
    for a uniform signature."""
    valid, invalid = _validate(AttributeRow, rows, "code")
    plan = plan_attributes(valid, Resolver(resource), behavior=behavior)
    return _run(resource, [row.code for row in valid], plan, mode, invalid, fail_on_error_ratio)


def import_attribute_sets(
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
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
    resolver = Resolver(resource)
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


def import_categories(
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
):
    """Ensure category paths and write their attributes. `diff` is ignored:
    the writer is idempotent by path, and a path it resolves or creates
    during planning counts as succeeded even with no attribute to write."""
    valid, invalid = _validate(CategoryRow, rows, "path")
    plan = plan_categories(valid, Resolver(resource))
    return _run(
        resource, [row.path for row in valid], plan, mode, invalid, fail_on_error_ratio, noop_succeeded=True
    )


# -- products -------------------------------------------------------------------


def _product_unchanged(row: ProductRow, snap: dict, resolver, behavior: str) -> bool:
    # disable only ever writes status 2, so the row's own fields are irrelevant.
    if behavior == "disable":
        return normalize(snap.get("status"), "int") == 2
    return product_matches_snapshot(row, snap, resolver)


def import_products(
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
):
    """Snapshot existing SKUs, skip rows `product_matches_snapshot` proves
    unchanged (diff=True), plan, then execute the main and store-value
    product operations before the type follow-ups, because a configurable
    child must exist before it is linked. Images are out of scope here:
    import_media owns them."""
    valid, invalid = _validate(ProductRow, rows, "sku")
    resolver = Resolver(resource)
    skus = list(dict.fromkeys(row.sku for row in valid))
    snapshot = snapshot_products(resource, skus, PRODUCT_SNAPSHOT_FIELDS)

    changed, skipped = valid, 0
    if diff and snapshot:
        resolver.preload_attributes({code for row in valid for code in row.attributes})
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
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
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
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
):
    """Create or overwrite MSI sources. `diff` is ignored (no snapshot)."""
    valid, invalid = _validate(SourceRow, rows, "source_code")
    plan = plan_sources(valid)
    return _run(resource, [row.source_code for row in valid], plan, mode, invalid, fail_on_error_ratio)


def import_stocks(
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
):
    """Create MSI stocks bound to website sales channels. `diff` is ignored."""
    valid, invalid = _validate(StockRow, rows, "name")
    plan = plan_stocks(valid, Resolver(resource))
    return _run(resource, [row.name for row in valid], plan, mode, invalid, fail_on_error_ratio)


def import_stock_source_links(
    resource,
    rows,
    stock_ids: dict[str, int] | None = None,
    mode: Mode = "sync",
    diff=True,
    behavior="upsert",
    fail_on_error_ratio=None,
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
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
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
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
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
