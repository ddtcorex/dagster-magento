"""Plans create, update, disable and store-value operations for products.

Create vs. update is decided per row from the `existing` SKU set the diff
snapshot produced (spec 5.4) - this writer never calls Magento to find
out. Type-specific parts (configurable, bundle, grouped, downloadable) are
delegated to `writers/product_types.py:apply_type_parts` so this module
only ever plans the fields every product type shares.
"""

import copy
import urllib.parse
from typing import Any

from dagster_magento.models import ProductRow
from dagster_magento.operation import BulkSpec, Operation, RowError
from dagster_magento.resolvers import ResolveError, boolean_value
from dagster_magento.writers import PlanResult
from dagster_magento.writers.product_types import apply_type_parts

# Keys the writer itself always sets at the top level of the product
# payload. A row whose `attributes` carries any of these would otherwise
# have its value silently overridden by the writer's own field - reject
# the row instead of guessing which value the caller meant.
RESERVED_PRODUCT_KEYS = frozenset(
    {
        "sku",
        "type_id",
        "attribute_set_id",
        "name",
        "price",
        "status",
        "visibility",
        "weight",
        "custom_attributes",
        "extension_attributes",
    }
)

# The only top-level fields a store_values entry may localize. Everything
# else in a store_values dict becomes a custom attribute instead - a
# store-view payload must stay minimal (spec 5.5 "Store scope rules"): a
# full payload there would create a store-level override for every
# attribute, not just the localized ones.
STORE_TOP_LEVEL_KEYS = frozenset({"name", "status", "visibility"})


def plan_products(
    rows: list[ProductRow], resolver, existing: set[str], behavior: str = "upsert"
) -> PlanResult:
    """Plan the common product operations: create/update, disable, and
    per-store-code updates. `existing` decides create vs. update, and
    gates create_only/update_only; `disable` skips everything else."""
    resolver.preload_attributes(_attribute_codes(rows))

    operations: list[Operation] = []
    failed: list[RowError] = []
    skipped: list[str] = []

    for row in rows:
        sku = row.sku
        is_existing = sku in existing

        if behavior == "disable":
            if not is_existing:
                failed.append(RowError(row_ref=sku, message="sku does not exist"))
                continue
            operations.append(_disable_operation(sku))
            continue

        if behavior == "create_only" and is_existing:
            skipped.append(sku)
            continue
        if behavior == "update_only" and not is_existing:
            skipped.append(sku)
            continue

        shadowed = sorted(RESERVED_PRODUCT_KEYS & row.attributes.keys())
        if shadowed:
            failed.append(
                RowError(
                    row_ref=sku,
                    message=f"attributes shadow writer-owned keys: {', '.join(shadowed)}",
                )
            )
            continue

        try:
            row_operations = _plan_row(row, resolver, is_existing)
        except ResolveError as error:
            failed.append(RowError(row_ref=sku, message=str(error)))
            continue

        operations.extend(row_operations)

    return PlanResult(operations=operations, failed=failed, skipped=skipped)


def _attribute_codes(rows: list[ProductRow]) -> set[str]:
    codes: set[str] = set()
    for row in rows:
        codes.update(row.attributes.keys())
        # apply_type_parts resolves these for the configurable options.
        codes.update(row.configurable_attributes)
        for localized in row.store_values.values():
            codes.update(localized.keys() - STORE_TOP_LEVEL_KEYS)
    return codes


def _plan_row(row: ProductRow, resolver, is_existing: bool) -> list[Operation]:
    body = _product_body(row, resolver)
    # apply_type_parts mutates `body` in place, so it must run before the
    # main operation copies `body` into its own payload/bulk dicts - a
    # copy taken first would carry none of the type-specific fields.
    type_part_operations = apply_type_parts(row, body, resolver)
    operations = [_main_operation(row.sku, body, is_existing)]
    operations.extend(type_part_operations)
    operations.extend(_store_value_operations(row, resolver))
    return operations


def _product_body(row: ProductRow, resolver) -> dict[str, Any]:
    body: dict[str, Any] = {
        "sku": row.sku,
        "type_id": row.type,
        "attribute_set_id": _attribute_set_id(row.attribute_set, resolver),
    }
    if row.name is not None:
        body["name"] = row.name
    if row.price is not None:
        body["price"] = row.price
    if row.status is not None:
        body["status"] = row.status
    if row.visibility is not None:
        body["visibility"] = row.visibility
    if row.weight is not None:
        body["weight"] = row.weight
    body["custom_attributes"] = _custom_attributes(row.attributes, resolver)

    extension_attributes: dict[str, Any] = {
        "website_ids": [resolver.website_id(code) for code in row.websites],
    }
    if row.categories:
        extension_attributes["category_links"] = [
            {"position": 0, "category_id": str(resolver.category_id(path))}
            for path in row.categories
        ]
    body["extension_attributes"] = extension_attributes
    return body


def _attribute_set_id(attribute_set: str, resolver) -> int:
    if attribute_set.isdigit():
        return int(attribute_set)
    return resolver.attribute_set_id(attribute_set)


def _product_operation(
    method: str,
    endpoint: str,
    sku: str,
    body: dict[str, Any],
    bulk_endpoint: str,
    bulk_includes_sku: bool,
    store_code: str | None = None,
) -> Operation:
    """Build one Operation plus its paired BulkSpec from a single `body`
    dict, without either side ever sharing a dict object with the other -
    each is its own deep copy, per the immutability contract in
    operation.py ("writers must build a fresh dict per operation ...
    BulkSpec gets its own fresh dict too")."""
    payload = {"product": copy.deepcopy(body)}
    bulk_body = copy.deepcopy(body)
    bulk_payload = {"sku": sku, "product": bulk_body} if bulk_includes_sku else {"product": bulk_body}
    return Operation(
        method=method,
        endpoint=endpoint,
        payload=payload,
        row_refs=(sku,),
        store_code=store_code,
        bulk=BulkSpec(bulk_endpoint, bulk_payload),
    )


def _main_operation(sku: str, body: dict[str, Any], is_existing: bool) -> Operation:
    if not is_existing:
        return _product_operation(
            "POST", "products", sku, body, bulk_endpoint="products", bulk_includes_sku=False
        )
    return _product_operation(
        "PUT",
        f"products/{_quote_sku(sku)}",
        sku,
        body,
        bulk_endpoint="products/bySku",
        bulk_includes_sku=True,
    )


def _disable_operation(sku: str) -> Operation:
    body = {"sku": sku, "status": 2}
    return _product_operation(
        "PUT",
        f"products/{_quote_sku(sku)}",
        sku,
        body,
        bulk_endpoint="products/bySku",
        bulk_includes_sku=True,
    )


def _store_value_operations(row: ProductRow, resolver) -> list[Operation]:
    operations = []
    for store_code, localized in row.store_values.items():
        body: dict[str, Any] = {"sku": row.sku}
        custom_attribute_values: dict[str, Any] = {}
        for key, value in localized.items():
            if key in STORE_TOP_LEVEL_KEYS:
                body[key] = value
            else:
                custom_attribute_values[key] = value
        body["custom_attributes"] = _custom_attributes(custom_attribute_values, resolver)
        operations.append(
            _product_operation(
                "PUT",
                f"products/{_quote_sku(row.sku)}",
                row.sku,
                body,
                bulk_endpoint="products/bySku",
                bulk_includes_sku=True,
                store_code=store_code,
            )
        )
    return operations


def _custom_attributes(attributes: dict[str, Any], resolver) -> list[dict[str, Any]]:
    return [
        {"attribute_code": code, "value": _resolve_attribute_value(code, value, resolver)}
        for code, value in attributes.items()
    ]


def _resolve_attribute_value(code: str, value: Any, resolver) -> Any:
    meta = resolver.attribute(code)
    if meta.frontend_input == "boolean":
        return boolean_value(code, value)
    if meta.frontend_input == "select":
        # A non-string value is already an option id (the file adapter
        # emits bundle flags that way); a string, digits included, is a
        # label, since option labels can be numbers ("32").
        if not isinstance(value, str):
            return value
        return resolver.option_id(code, value)
    if meta.frontend_input == "multiselect":
        return ",".join(resolver.option_id(code, label) for label in _multiselect_labels(value))
    return value


def _multiselect_labels(value: Any) -> list[str]:
    if isinstance(value, str):
        return [part.strip() for part in value.split(",")]
    return [str(item).strip() for item in value]


def _quote_sku(sku: str) -> str:
    return urllib.parse.quote(sku, safe="")
