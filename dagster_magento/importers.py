"""One import function per catalog entity, composing validation, the diff
snapshot, the writer's plan and the executor into a single UploadResult.

Every importer counts one outcome per row, never one per operation: a
row that becomes several operations (a configurable product with its
options and child links, a price row with base, special and tier parts)
is failed if any of its operations failed, else pending if any is still
pending, else succeeded. MagentoAuthError is never caught here.
"""

import copy
import urllib.parse
from typing import Callable, Literal

import requests
from dagster import MaterializeResult, get_dagster_logger
from pydantic import BaseModel

from dagster_magento.diff import (
    normalize,
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
from dagster_magento.resolvers import Resolver, ResolveError
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

# The product fields the diff snapshot reads back; custom attributes a row
# sets are compared on top of these.
PRODUCT_DIFF_FIELDS = ["type_id", "attribute_set_id", "status", "name", "price", "visibility", "weight"]
_PRODUCT_FIELD_KINDS = {
    "type_id": "text",
    "attribute_set_id": "int",
    "status": "int",
    "name": "text",
    "price": "decimal",
    "visibility": "int",
    "weight": "decimal",
}
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
    """Fold per-operation outcomes into one outcome per row ref. The
    executor counts each operation's row_refs, so without this a row with
    three operations would be counted three times."""
    failed_refs = {error.row_ref for error in plan_failed}
    pending_refs = set()
    for error in result.errors:
        target = failed_refs if error["status"] == "failed" else pending_refs
        target.update(error["row_ids"])

    succeeded = failed = pending = 0
    for ref in dict.fromkeys([*row_ids, *(error.row_ref for error in plan_failed)]):
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
    for ref in dict.fromkeys(refs):
        if ref in op_refs or (noop_succeeded and ref not in settled):
            row_ids.append(ref)
        elif ref not in settled:
            noop += 1
    folded = _fold_by_row(
        row_ids, result, plan.failed, validation_failed, diff_skipped + len(plan.skipped) + noop
    )
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
    the attribute assignments once the groups exist. Converged means a pass
    plans no operation that has not already succeeded (assignments are
    idempotent and always re-planned)."""
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
    check_error_ratio(folded, fail_on_error_ratio)
    return folded


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


def _row_attribute_value(meta, value):
    if meta.frontend_input == "select":
        return str(value)
    if meta.frontend_input == "multiselect":
        return normalize(value, "multiselect")
    return normalize(value, "decimal" if meta.backend_type == "decimal" else "text")


def _product_unchanged(row: ProductRow, existing: dict, resolver) -> bool:
    """Compare only the fields the writer would send for this row. A label
    the resolver cannot map yet (an unknown option or set) means changed,
    and the writer decides whether that creates it or fails the row."""
    try:
        desired = {
            "type_id": row.type,
            "attribute_set_id": row.attribute_set if row.attribute_set.isdigit()
            else resolver.attribute_set_id(row.attribute_set),
        }
        for field in ("name", "price", "status", "visibility", "weight"):
            if getattr(row, field) is not None:
                desired[field] = getattr(row, field)
        wanted = {field: normalize(value, _PRODUCT_FIELD_KINDS[field]) for field, value in desired.items()}
        current = {field: normalize(existing.get(field), _PRODUCT_FIELD_KINDS[field]) for field in desired}
        for code, value in row.attributes.items():
            meta = resolver.attribute(code)
            if meta.frontend_input == "select":
                value = resolver.option_id(code, value, create=False)
            elif meta.frontend_input == "multiselect":
                labels = value.split(",") if isinstance(value, str) else value
                value = [resolver.option_id(code, label.strip(), create=False) for label in labels]
            wanted[code] = _row_attribute_value(meta, value)
            current[code] = None if existing.get(code) is None else _row_attribute_value(meta, existing[code])
    except ResolveError:
        return False
    return wanted == current


def import_products(
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
):
    """Snapshot existing SKUs, skip unchanged rows (diff=True), plan, then
    execute the main and store-value product operations before the type
    follow-ups, because a configurable child must exist before it is linked."""
    valid, invalid = _validate(ProductRow, rows, "sku")
    resolver = Resolver(resource)
    snapshot = snapshot_products(resource, list(dict.fromkeys(row.sku for row in valid)), PRODUCT_DIFF_FIELDS)

    changed, skipped = valid, 0
    if diff and snapshot:
        resolver.preload_attributes({code for row in valid for code in row.attributes})
        changed = [
            row
            for row in valid
            if row.sku not in snapshot or not _product_unchanged(row, snapshot[row.sku], resolver)
        ]
        skipped = len(valid) - len(changed)

    plan = plan_products(changed, resolver, existing=set(snapshot), behavior=behavior)
    parents = [
        op for op in plan.operations if op.endpoint == "products" or op.endpoint.startswith("products/")
    ]
    follow_ups = [op for op in plan.operations if op not in parents]
    result = execute(resource, parents, mode=mode).merge(execute(resource, follow_ups, mode=mode))
    return _complete([row.sku for row in changed], plan, result, invalid, skipped, fail_on_error_ratio)


# -- prices ---------------------------------------------------------------------


def _merge_prices(rows: list[PriceRow]) -> tuple[list[PriceRow], int]:
    """Merge rows sharing a SKU: a later non-None field overrides an
    earlier one, so a file splitting base and special prices over two rows
    still sends both. Every row folded into an earlier one is counted."""
    merged: dict[str, PriceRow] = {}
    for row in rows:
        if row.sku not in merged:
            merged[row.sku] = copy.deepcopy(row)
            continue
        target = merged[row.sku]
        for field, value in row.model_dump(exclude_none=True).items():
            setattr(target, field, getattr(row, field))
    return list(merged.values()), len(rows) - len(merged)


def _tier_key(website_id, customer_group, qty, price, price_type):
    return (
        int(website_id),
        str(customer_group),
        normalize(qty, "decimal"),
        normalize(price, "decimal"),
        price_type,
    )


def _special_key(price, price_from, price_to):
    return (normalize(price, "decimal"), normalize(price_from, "datetime"), normalize(price_to, "datetime"))


def _price_unchanged(row: PriceRow, current: dict, website_ids: dict) -> bool:
    base = current["base"].get(row.store_id)
    if row.price is not None and normalize(base, "decimal") != normalize(row.price, "decimal"):
        return False
    if row.special_price is not None:
        wanted = _special_key(row.special_price, row.special_from, row.special_to)
        found = [
            _special_key(item.get("price"), item.get("price_from"), item.get("price_to"))
            for item in current["special"]
            if item.get("store_id") == row.store_id
        ]
        if wanted not in found:
            return False
    if row.tiers is not None:
        known = {"all": 0, **website_ids}
        if any(tier.website not in known and not tier.website.isdigit() for tier in row.tiers):
            return False
        wanted_tiers = sorted(
            _tier_key(known.get(t.website, t.website), t.customer_group, t.qty, t.price, t.price_type)
            for t in row.tiers
        )
        current_tiers = sorted(
            _tier_key(t["website_id"], t["customer_group"], t["quantity"], t["price"], t["price_type"])
            for t in current["tiers"]
        )
        if wanted_tiers != current_tiers:
            return False
    return True


def import_prices(
    resource, rows, mode: Mode = "sync", diff=True, behavior="upsert", fail_on_error_ratio=None
):
    """Base, special and tier prices through the price-storage list
    endpoints. Always sync: those endpoints take lists and have no bulk
    route, so `mode` is ignored."""
    valid, invalid = _validate(PriceRow, rows, "sku")
    merged, duplicates = _merge_prices(valid)
    snapshot = snapshot_prices(resource, [row.sku for row in merged])
    website_ids = {}
    if any(row.tiers for row in merged):
        website_ids = {item["code"]: item["id"] for item in resource.get("store/websites")}

    changed, skipped = merged, 0
    if diff:
        changed = [
            row
            for row in merged
            if row.sku not in snapshot or not _price_unchanged(row, snapshot[row.sku], website_ids)
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
