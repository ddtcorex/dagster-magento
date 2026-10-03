"""Map native import rows onto the canonical catalog row models.

Every `*_from_rows` function takes the `(line_number, dict)` pairs
`readers.read_rows` yields and returns `(models, list[RowError])`. The
strategy throughout is the one the brief asks for: build a plain dict per
row (doing only the semantic remapping pydantic cannot do on its own -
text-to-enum mapping, grouping sibling rows, column dropping) and let
`model_validate` do the actual validation, so a bad row becomes a
`RowError`, never an exception.
"""

from collections.abc import Iterable
from datetime import datetime
from typing import Any

from dagster import get_dagster_logger
from pydantic import ValidationError

from dagster_magento.formats import columns
from dagster_magento.models import (
    AttributeRow,
    AttributeSetRow,
    CategoryRow,
    PriceRow,
    ProductRow,
    SourceItemRow,
)
from dagster_magento.operation import RowError

_LOGGER = get_dagster_logger()

Rows = Iterable[tuple[int, dict]]


def _warn_once(key: str, warn, seen: set[str]) -> None:
    if key in seen:
        return
    seen.add(key)
    warn(f"dropping unsupported column {key!r}")


def _validate_row(model, raw: dict, line: int, ref: str, errors: list[RowError]):
    """model_validate one row; on failure append a RowError (row_ref
    'line <n>: <ref>' per the readers contract) and return None."""
    try:
        return model.model_validate(raw)
    except ValidationError as exc:
        errors.append(RowError(row_ref=f"line {line}: {ref}", message=_format_validation_error(exc)))
        return None


def _format_validation_error(exc: ValidationError) -> str:
    parts = []
    for err in exc.errors():
        field = ".".join(str(part) for part in err["loc"])
        parts.append(f"{field}: {err['msg']}" if field else err["msg"])
    return "; ".join(parts)


def _parse_float(column: str, value: str) -> float:
    """Convert one column's raw value to float, or raise a plain
    ValueError naming the column and the bad value - callers catch
    ValueError per row (columns.py's ColumnParseError is itself a
    ValueError, so the same except clause covers both)."""
    try:
        return float(value)
    except ValueError as error:
        raise ValueError(f"{column}: invalid number {value!r}") from error


def _parse_int(column: str, value: str) -> int:
    """Same contract as `_parse_float`, for an integer-valued column."""
    try:
        return int(value)
    except ValueError as error:
        raise ValueError(f"{column}: invalid number {value!r}") from error


def _normalize_global_store_codes(global_store_codes: tuple[str, ...]) -> set[str]:
    """Case-insensitively normalize a `global_store_codes` tuple (stripped,
    lowercased) into a set for membership checks, shared by
    products_from_rows and categories_from_rows."""
    return {code.strip().lower() for code in global_store_codes}


# -- products -----------------------------------------------------------------

# Native inventory/stock columns: stock is imported through
# SourceItemRow/source_items_from_rows, never through the product row.
_STOCK_COLUMNS = frozenset(
    {
        "qty", "out_of_stock_qty", "use_config_min_qty", "is_qty_decimal",
        "allow_backorders", "use_config_backorders", "min_cart_qty",
        "use_config_min_sale_qty", "max_cart_qty", "use_config_max_sale_qty",
        "is_in_stock", "notify_on_stock_below", "use_config_notify_stock_qty",
        "manage_stock", "use_config_manage_stock", "use_config_qty_increments",
        "qty_increments", "use_config_enable_qty_inc", "enable_qty_increments",
        "is_decimal_divided", "website_id", "deferred_stock_update",
        "use_config_deferred_stock_update",
    }
)

_DROPPED_PRODUCT_COLUMNS = _STOCK_COLUMNS | frozenset(
    {
        # Firebear-only, no native equivalent.
        "group", "tier_prices",
        # special price columns go through prices_from_rows instead.
        "special_price", "special_price_from_date", "special_price_to_date",
        # labels come from the configurable attribute itself.
        "configurable_variation_labels",
        "created_at", "updated_at",
        # phase 1 out of scope.
        "related_skus", "crosssell_skus", "upsell_skus", "custom_options",
        "hide_from_product_page",
        # msrp_enabled no longer exists on 2.4; product_options_container
        # is a non-native duplicate of display_product_options_in.
        "map_enabled", "product_options_container",
    }
)

# Recomputed by Magento on save (has_options, required_options) or stock
# (quantity_and_stock_status, written through source items in phase 1).
# The export lists them, often inside additional_attributes; writing them
# is ignored or overridden, so a rerun would never compare unchanged.
_DERIVED_PRODUCT_ATTRIBUTES = ("has_options", "required_options", "quantity_and_stock_status")

# Export column -> attribute code, the inverse of Magento's
# Import\Product::$_fieldsMap for the columns that are plain attributes.
# REST knows only the attribute codes. Values stay as exported: the
# product writer resolves select labels ("Taxable Goods") to option ids.
_RENAMED_PRODUCT_COLUMNS = {
    "tax_class_name": "tax_class_id",
    "new_from_date": "news_from_date",
    "new_to_date": "news_to_date",
    "display_product_options_in": "options_container",
    "map_price": "minimal_price",
    "msrp_price": "msrp",
    "meta_keywords": "meta_keyword",
}

_IMAGE_ROLE_COLUMNS = [
    ("base_image", "image", "base_image_label"),
    ("small_image", "small_image", "small_image_label"),
    ("thumbnail_image", "thumbnail", "thumbnail_image_label"),
    ("swatch_image", "swatch_image", "swatch_image_label"),
]
_IMAGE_COLUMNS = frozenset(
    {column for column, _, _ in _IMAGE_ROLE_COLUMNS}
    | {label for _, _, label in _IMAGE_ROLE_COLUMNS}
    | {"additional_images", "additional_image_labels"}
)

_BUNDLE_PRICE_TYPE = {"dynamic": 0, "fixed": 1}
_BUNDLE_PRICE_VIEW = {"Price range": 0, "As low as": 1}
_BUNDLE_FLAG_COLUMNS = {
    "bundle_price_type": ("price_type", _BUNDLE_PRICE_TYPE),
    "bundle_sku_type": ("sku_type", _BUNDLE_PRICE_TYPE),
    "bundle_weight_type": ("weight_type", _BUNDLE_PRICE_TYPE),
    "bundle_price_view": ("price_view", _BUNDLE_PRICE_VIEW),
}

_VISIBILITY_TEXT = {
    "not visible individually": 1,
    "catalog": 2,
    "search": 3,
    "catalog, search": 4,
}


def _is_dropped_product_column(column: str) -> bool:
    return (
        column in _DROPPED_PRODUCT_COLUMNS
        or column in _DERIVED_PRODUCT_ATTRIBUTES
        or column.startswith("attribute|")
    )


def _parse_visibility(value: str) -> int:
    normalized = " ".join(value.strip().lower().split())
    if normalized in _VISIBILITY_TEXT:
        return _VISIBILITY_TEXT[normalized]
    if normalized in {"1", "2", "3", "4"}:
        return int(normalized)
    raise ValueError(f"unknown visibility: {value!r}")


def _parse_status(value: str) -> int | None:
    if value == "1":
        return 1
    if value == "0":
        return 2
    return None


def _configurable_attribute_order(variations: list[dict[str, Any]]) -> list[str]:
    order: list[str] = []
    for variation in variations:
        for key in variation["attributes"]:
            if key not in order:
                order.append(key)
    return order


def _build_downloadable_links(value: str) -> list[dict[str, Any]]:
    links = []
    for fields in columns.parse_pipe_groups(value):
        link_type = fields.get("type", "url").strip().lower()
        if link_type and link_type != "url":
            raise ValueError(f"unsupported downloadable link type: {fields.get('type')!r}")
        price_text = fields.get("price", "")
        downloads_text = fields.get("downloads", "")
        links.append(
            {
                "title": fields.get("title", ""),
                "url": fields.get("url", ""),
                "price": _parse_float("downloadable_links", price_text) if price_text else 0,
                "downloads": _parse_int("downloadable_links", downloads_text) if downloads_text else None,
            }
        )
    return links


def _build_images(fields: dict[str, str]) -> list[dict[str, Any]]:
    sources: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for column, role, label_column in _IMAGE_ROLE_COLUMNS:
        source = fields.get(column, "")
        if not source:
            continue
        if source not in sources:
            sources[source] = {"roles": [], "label": None}
            order.append(source)
        entry = sources[source]
        if role not in entry["roles"]:
            entry["roles"].append(role)
        if not entry["label"]:
            entry["label"] = fields.get(label_column, "") or None

    additional_sources = [s.strip() for s in fields.get("additional_images", "").split(",") if s.strip()]
    additional_labels = [l.strip() for l in fields.get("additional_image_labels", "").split(",") if l.strip()]
    for index, source in enumerate(additional_sources):
        if source not in sources:
            sources[source] = {"roles": [], "label": None}
            order.append(source)
        entry = sources[source]
        if not entry["label"] and index < len(additional_labels):
            entry["label"] = additional_labels[index]

    return [
        {"source": source, "position": position, "roles": sources[source]["roles"], "label": sources[source]["label"]}
        for position, source in enumerate(order, start=1)
    ]


def _assign_product_column(
    column: str, value: str, kwargs: dict[str, Any], attributes: dict[str, Any]
) -> None:
    """Map one non-dropped, non-image column onto `kwargs`/`attributes` in
    place. May raise ValueError (or columns.py's ColumnParseError, itself a
    ValueError) for a malformed cell - the caller catches it per column and
    turns it into this row's single error, so a bad cell never escapes as
    an uncaught exception."""
    if column == "attribute_set_code":
        kwargs["attribute_set"] = value
    elif column == "product_type":
        kwargs["type"] = value
    elif column == "product_websites":
        kwargs["websites"] = [w.strip() for w in value.split(",") if w.strip()]
    elif column == "categories":
        kwargs["categories"] = columns.parse_categories(value)
    elif column == "name":
        kwargs["name"] = value
    elif column == "price":
        kwargs["price"] = _parse_float("price", value)
    elif column == "weight":
        kwargs["weight"] = _parse_float("weight", value)
    elif column == "product_online":
        status = _parse_status(value)
        if status is not None:
            kwargs["status"] = status
    elif column == "visibility":
        kwargs["visibility"] = _parse_visibility(value)
    elif column == "additional_attributes":
        attributes.update(columns.parse_additional_attributes(value))
    elif column == "configurable_variations":
        variations = columns.parse_configurable_variations(value)
        kwargs["variations"] = variations
        kwargs["configurable_attributes"] = _configurable_attribute_order(variations)
    elif column == "bundle_values":
        kwargs["bundle_options"] = columns.parse_bundle_values(value)
    elif column in _BUNDLE_FLAG_COLUMNS:
        attribute_key, mapping = _BUNDLE_FLAG_COLUMNS[column]
        attributes[attribute_key] = mapping.get(value, value)
    elif column == "associated_skus":
        kwargs["grouped_links"] = [
            {"sku": sku, "qty": qty, "position": position}
            for position, (sku, qty) in enumerate(columns.parse_associated_sku_pairs(value))
        ]
    elif column == "downloadable_links":
        kwargs["downloadable_links"] = _build_downloadable_links(value)
    elif column == "bundle_shipment_type":
        # Applied after every column (see _parse_row_fields), so it wins
        # over a shipment_type carried in additional_attributes.
        kwargs["_bundle_shipment_type"] = value
    else:
        attributes[_RENAMED_PRODUCT_COLUMNS.get(column, column)] = value


def _parse_row_fields(row: dict[str, str], warn, dropped_seen: set[str]) -> dict[str, Any] | str:
    """Map one product row's columns (already stripped of sku/store_view_code)
    onto ProductRow kwargs. Returns an error message (str) instead of a dict
    when a column's value cannot be mapped at all - every malformed cell
    (a bad float, an unmapped visibility text, a non-url downloadable link,
    ...) is caught here as a plain ValueError, never left to escape as an
    exception out of products_from_rows."""
    kwargs: dict[str, Any] = {}
    attributes: dict[str, Any] = {}
    image_fields: dict[str, str] = {}

    for column, raw_value in row.items():
        value = (raw_value or "").strip()
        if not value:
            continue
        if _is_dropped_product_column(column):
            _warn_once(column, warn, dropped_seen)
            continue
        if column in _IMAGE_COLUMNS:
            image_fields[column] = value
            continue
        try:
            _assign_product_column(column, value, kwargs, attributes)
        except ValueError as error:
            return str(error)

    for code in _DERIVED_PRODUCT_ATTRIBUTES:
        if attributes.pop(code, None) is not None:
            _warn_once(code, warn, dropped_seen)
    if "_bundle_shipment_type" in kwargs:
        attributes["shipment_type"] = kwargs.pop("_bundle_shipment_type")
    if image_fields:
        kwargs["images"] = _build_images(image_fields)
    if attributes:
        kwargs["attributes"] = attributes
    return kwargs


def products_from_rows(
    rows: Rows,
    warn=_LOGGER.warning,
    global_store_codes: tuple[str, ...] = ("", "default"),
) -> tuple[list[ProductRow], list[RowError]]:
    """Map native product rows onto ProductRow, folding store_view_code
    rows into the matching global SKU's store_values.

    `store_view_code` values in `global_store_codes` (compared
    case-insensitively after stripping) are treated as the global row
    rather than a store-view fold. The default, `("", "default")`, treats
    "default" as global because both the Firebear sample export
    (product_all_types.csv) and a native single-store-view export tag
    every row - including the only, global one per sku - with
    store_view_code "default" rather than leaving it blank. "default" is
    also a real, addressable store view code on any multi-store Magento
    install, so on one of those pass `global_store_codes=("",)` to make
    "default" rows fold into store_values["default"] (a real per-store
    override) instead of being written globally through `/rest/all/`."""
    dropped_seen: set[str] = set()
    products: dict[str, ProductRow] = {}
    order: list[str] = []
    store_rows: list[tuple[int, str, str, dict]] = []
    errors: list[RowError] = []
    global_codes = _normalize_global_store_codes(global_store_codes)

    for line, row in rows:
        sku = (row.get("sku") or "").strip()
        store_view_code = (row.get("store_view_code") or "").strip()
        rest = {k: v for k, v in row.items() if k not in ("sku", "store_view_code")}
        if not sku:
            errors.append(RowError(row_ref=f"line {line}", message="missing sku"))
            continue
        if store_view_code.lower() not in global_codes:
            store_rows.append((line, sku, store_view_code, rest))
            continue

        parsed = _parse_row_fields(rest, warn, dropped_seen)
        if isinstance(parsed, str):
            errors.append(RowError(row_ref=f"line {line}: {sku}", message=parsed))
            continue
        product = _validate_row(ProductRow, {"sku": sku, **parsed}, line, sku, errors)
        if product is None:
            continue
        products[sku] = product
        order.append(sku)

    for line, sku, store_view_code, rest in store_rows:
        if sku not in products:
            errors.append(
                RowError(
                    row_ref=f"line {line}: {sku}",
                    message=f"store-view row for unknown sku (store_view_code={store_view_code})",
                )
            )
            continue
        parsed = _parse_row_fields(rest, warn, dropped_seen)
        if isinstance(parsed, str):
            errors.append(RowError(row_ref=f"line {line}: {sku}", message=parsed))
            continue
        store_entry: dict[str, Any] = {}
        attrs = parsed.pop("attributes", {})
        for key in ("name", "status", "visibility"):
            if key in parsed:
                store_entry[key] = parsed.pop(key)
        store_entry.update(attrs)
        store_entry.update(parsed)
        products[sku].store_values[store_view_code] = store_entry

    return [products[sku] for sku in order], errors


# -- categories -----------------------------------------------------------------

# landing_page, custom_design and image carry admin labels or remote URLs
# for records a catalog import cannot resolve natively (a CMS block by
# title, a theme by label, a media file Magento expects on disk).
_DROPPED_CATEGORY_COLUMNS = frozenset(
    {"entity_id", "url_path", "group", "landing_page", "custom_design", "image"}
)
_CATEGORY_YES_NO = frozenset(
    {"include_in_menu", "is_active", "is_anchor", "custom_apply_to_products", "custom_use_parent_settings"}
)
# Export labels -> native option codes, compared case-insensitively. A
# value that is already a code (or unknown) passes through unchanged.
_CATEGORY_LABELS = {
    "display_mode": {
        "products only": "PRODUCTS",
        "static block only": "PAGE",
        "static block and products": "PRODUCTS_AND_PAGE",
    },
    "page_layout": {
        "empty": "empty",
        "1 column": "1column",
        "2 columns with left bar": "2columns-left",
        "2 columns with right bar": "2columns-right",
        "3 columns": "3columns",
    },
    "default_sort_by": {"position": "position", "product name": "name", "price": "price"},
}
_CATEGORY_DATES = frozenset({"custom_design_from", "custom_design_to"})


def _category_date(value: str) -> str:
    # The export writes dates as m/d/y; ISO dates pass through unchanged.
    try:
        return datetime.strptime(value, "%m/%d/%y").strftime("%Y-%m-%d")
    except ValueError:
        return value


_FLAG_TRUE = frozenset({"yes", "1", "true"})
_FLAG_FALSE = frozenset({"no", "0", "false"})


def _category_flag(column: str, value: str) -> int:
    lowered = value.lower()
    if lowered in _FLAG_TRUE:
        return 1
    if lowered in _FLAG_FALSE:
        return 0
    raise ValueError(f"{column}: expected yes, no, 1, 0, true or false, got {value!r}")


def _parse_category_fields(row: dict[str, str], warn, dropped_seen: set[str]) -> dict[str, Any]:
    attributes: dict[str, Any] = {}
    for column, raw_value in row.items():
        value = (raw_value or "").strip()
        if not value:
            continue
        if column in _DROPPED_CATEGORY_COLUMNS:
            _warn_once(column, warn, dropped_seen)
            continue
        if column in _CATEGORY_YES_NO:
            attributes[column] = _category_flag(column, value)
        elif column in _CATEGORY_LABELS:
            attributes[column] = _CATEGORY_LABELS[column].get(value.lower(), value)
        elif column in _CATEGORY_DATES:
            attributes[column] = _category_date(value)
        else:
            attributes[column] = value
    return attributes


def categories_from_rows(
    rows: Rows,
    warn=_LOGGER.warning,
    global_store_codes: tuple[str, ...] = ("", "default"),
) -> tuple[list[CategoryRow], list[RowError]]:
    """Map Firebear categories.csv rows onto CategoryRow, folding non-global
    store_view rows into the matching global path's store_values.

    `store_view` values in `global_store_codes` (compared case-insensitively
    after stripping) are treated as the global row rather than a store-view
    fold. The default, `("", "default")`, treats "default" as global because
    the Firebear sample export (categories.csv) tags its global rows with
    store_view "default" rather than leaving it blank. "default" is also a
    real, addressable store view code on any multi-store Magento install, so
    on one of those pass `global_store_codes=("",)` to make "default" rows
    fold into store_values["default"] (a real per-store override) instead of
    being written globally."""
    dropped_seen: set[str] = set()
    categories: dict[str, CategoryRow] = {}
    order: list[str] = []
    store_rows: list[tuple[int, str, str, dict]] = []
    errors: list[RowError] = []
    global_codes = _normalize_global_store_codes(global_store_codes)

    for line, row in rows:
        path = (row.get("name") or "").strip()
        store_view = (row.get("store_view") or "").strip()
        rest = {k: v for k, v in row.items() if k not in ("name", "store_view")}
        if not path:
            errors.append(RowError(row_ref=f"line {line}", message="missing name"))
            continue
        if store_view.lower() not in global_codes:
            store_rows.append((line, path, store_view, rest))
            continue
        try:
            attributes = _parse_category_fields(rest, warn, dropped_seen)
        except ValueError as error:
            errors.append(RowError(row_ref=f"line {line}: {path}", message=str(error)))
            continue
        category = _validate_row(CategoryRow, {"path": path, "attributes": attributes}, line, path, errors)
        if category is None:
            continue
        categories[path] = category
        order.append(path)

    for line, path, store_view, rest in store_rows:
        if path not in categories:
            errors.append(
                RowError(
                    row_ref=f"line {line}: {path}",
                    message=f"store-view row for unknown category (store_view={store_view})",
                )
            )
            continue
        try:
            categories[path].store_values[store_view] = _parse_category_fields(rest, warn, dropped_seen)
        except ValueError as error:
            errors.append(RowError(row_ref=f"line {line}: {path}", message=str(error)))

    return [categories[path] for path in order], errors


# -- attributes -------------------------------------------------------------------

_ATTRIBUTE_BOOL_FLAGS = frozenset(
    {
        "is_required", "is_unique", "is_searchable", "is_filterable", "is_comparable",
        "is_visible_on_front", "is_html_allowed_on_front",
        "is_filterable_in_search", "used_in_product_listing", "used_for_sort_by",
        "is_visible_in_advanced_search", "is_wysiwyg_enabled", "is_used_for_promo_rules",
        "is_used_in_grid", "is_visible_in_grid", "is_filterable_in_grid",
    }
)
_ATTRIBUTE_INT_FLAGS = frozenset({"position"})
_ATTRIBUTE_OTHER_FLAGS = frozenset({"default_value", "note", "apply_to"})
# Export columns with no field on the REST attribute DTO: POST
# products/attributes rejects them with "field is not supported"
# (verified live on 2.4.9), so they are dropped with one warning each.
_ATTRIBUTE_UNSUPPORTED_COLUMNS = ("search_weight", "is_used_for_price_rules")
_ATTRIBUTE_FLAG_COLUMNS = _ATTRIBUTE_BOOL_FLAGS | _ATTRIBUTE_INT_FLAGS | _ATTRIBUTE_OTHER_FLAGS
_ATTRIBUTE_SCOPE = {"0": "store", "1": "global", "2": "website"}


def _convert_attribute_flag(column: str, value: str) -> Any:
    if column in _ATTRIBUTE_BOOL_FLAGS:
        return value == "1"
    if column in _ATTRIBUTE_INT_FLAGS:
        return int(value) if value.lstrip("-").isdigit() else value
    if column == "apply_to":
        # The REST DTO types apply_to as string[]; a comma string is rejected.
        return [part.strip() for part in value.split(",") if part.strip()]
    return value


def _build_attribute_kwargs(code: str, group: list[tuple[int, dict]]) -> dict[str, Any]:
    label = ""
    frontend_input = ""
    scope = "store"
    options: list[dict[str, Any]] = []
    seen_labels: set[str] = set()
    flags: dict[str, Any] = {}

    for _, row in group:
        if not label:
            label = (row.get("frontend_label") or "").strip()
        if not frontend_input:
            frontend_input = (row.get("frontend_input") or "").strip()
        is_global = (row.get("is_global") or "").strip()
        if is_global in _ATTRIBUTE_SCOPE:
            scope = _ATTRIBUTE_SCOPE[is_global]
        option_label = (row.get("option:value") or "").strip()
        if option_label and option_label not in seen_labels:
            seen_labels.add(option_label)
            sort_order_text = (row.get("option:sort_order") or "").strip()
            options.append(
                {"label": option_label, "sort_order": int(sort_order_text) if sort_order_text.isdigit() else 0}
            )
        for column in _ATTRIBUTE_FLAG_COLUMNS:
            if column in flags:
                continue
            value = (row.get(column) or "").strip()
            if value:
                flags[column] = _convert_attribute_flag(column, value)

    return {
        "code": code,
        "frontend_input": frontend_input,
        "label": label,
        "scope": scope,
        "options": options,
        "flags": flags,
    }


def attributes_from_rows(rows: Rows, warn=_LOGGER.warning) -> tuple[list[AttributeRow], list[RowError]]:
    """Map Firebear attributes.csv rows (one row per option) onto
    AttributeRow, grouped by attribute_code. Only store_id '0' rows are
    used; any other store_id is skipped with one warning per value."""
    warned: set[str] = set()
    groups: dict[str, list[tuple[int, dict]]] = {}
    order: list[str] = []
    errors: list[RowError] = []

    for line, row in rows:
        store_id = (row.get("store_id") or "0").strip() or "0"
        if store_id != "0":
            _warn_once(f"store_id={store_id}", warn, warned)
            continue
        code = (row.get("attribute_code") or "").strip()
        if not code:
            errors.append(RowError(row_ref=f"line {line}", message="missing attribute_code"))
            continue
        if code not in groups:
            groups[code] = []
            order.append(code)
        groups[code].append((line, row))
        for column in _ATTRIBUTE_UNSUPPORTED_COLUMNS:
            if (row.get(column) or "").strip():
                _warn_once(column, warn, warned)

    attribute_rows: list[AttributeRow] = []
    for code in order:
        group = groups[code]
        kwargs = _build_attribute_kwargs(code, group)
        attribute = _validate_row(AttributeRow, kwargs, group[0][0], code, errors)
        if attribute is not None:
            attribute_rows.append(attribute)
    return attribute_rows, errors


def attribute_set_assignments_from_rows(rows: Rows) -> list[AttributeSetRow]:
    """Aggregate attributes.csv rows into AttributeSetRow(name, groups),
    keyed by attribute_set. Rows without an attribute_set are ignored."""
    sets: dict[str, dict[str, list[str]]] = {}
    order: list[str] = []
    for _, row in rows:
        attribute_set = (row.get("attribute_set") or "").strip()
        code = (row.get("attribute_code") or "").strip()
        group_name = (row.get("group:name") or "").strip()
        if not attribute_set or not code or not group_name:
            continue
        if attribute_set not in sets:
            sets[attribute_set] = {}
            order.append(attribute_set)
        codes = sets[attribute_set].setdefault(group_name, [])
        if code not in codes:
            codes.append(code)
    return [AttributeSetRow(name=name, groups=sets[name]) for name in order]


# -- advanced pricing -------------------------------------------------------------


def _map_tier_price_website(value: str) -> str:
    if not value or value.startswith("All Websites"):
        return "all"
    return value


def _build_tier_price(row: dict[str, str]) -> dict[str, Any]:
    """Build one tier dict, or raise ValueError naming the missing/bad
    column - the caller catches it per row and turns it into a RowError."""
    qty_text = (row.get("tier_price_qty") or "").strip()
    price_text = (row.get("tier_price") or "").strip()
    if not qty_text or not price_text:
        raise ValueError("advanced_pricing: missing tier_price_qty or tier_price")
    tier: dict[str, Any] = {
        "qty": _parse_float("tier_price_qty", qty_text),
        "price": _parse_float("tier_price", price_text),
        "website": _map_tier_price_website((row.get("tier_price_website") or "").strip()),
    }
    customer_group = (row.get("tier_price_customer_group") or "").strip()
    if customer_group:
        tier["customer_group"] = customer_group
    price_type = (row.get("tier_price_value_type") or "").strip().lower()
    if price_type:
        tier["price_type"] = price_type
    return tier


def prices_from_rows(rows: Rows, warn=_LOGGER.warning) -> tuple[list[PriceRow], list[RowError]]:
    """Map advanced_pricing.csv rows onto one PriceRow per sku, each
    carrying every tier row for that sku."""
    groups: dict[str, list[tuple[int, dict]]] = {}
    order: list[str] = []
    errors: list[RowError] = []

    for line, row in rows:
        sku = (row.get("sku") or "").strip()
        if not sku:
            errors.append(RowError(row_ref=f"line {line}", message="missing sku"))
            continue
        if sku not in groups:
            groups[sku] = []
            order.append(sku)
        groups[sku].append((line, row))

    price_rows: list[PriceRow] = []
    for sku in order:
        group = groups[sku]
        tiers: list[dict[str, Any]] = []
        ok = True
        for line, row in group:
            try:
                tiers.append(_build_tier_price(row))
            except ValueError as error:
                errors.append(RowError(row_ref=f"line {line}: {sku}", message=str(error)))
                ok = False
        if not ok:
            continue
        price = _validate_row(PriceRow, {"sku": sku, "tiers": tiers}, group[0][0], sku, errors)
        if price is not None:
            price_rows.append(price)
    return price_rows, errors


# -- MSI source items -------------------------------------------------------------


def _required_cell(row: dict[str, str], column: str) -> str:
    """A blank cell is an error, never a silent 0: for a source item that
    would put the SKU out of stock."""
    value = (row.get(column) or "").strip()
    if not value:
        raise ValueError(f"{column} is empty")
    return value


def source_items_from_rows(rows: Rows, warn=_LOGGER.warning) -> tuple[list[SourceItemRow], list[RowError]]:
    """Map cataloginventory_source_item.csv rows onto SourceItemRow."""
    items: list[SourceItemRow] = []
    errors: list[RowError] = []
    for line, row in rows:
        sku = (row.get("sku") or "").strip()
        source_code = (row.get("source_code") or "").strip()
        ref = sku or source_code or f"line {line}"
        try:
            quantity = float(_required_cell(row, "quantity"))
            status = int(_required_cell(row, "status"))
        except ValueError as error:
            errors.append(RowError(row_ref=f"line {line}: {ref}", message=str(error)))
            continue
        raw = {"sku": sku, "source_code": source_code, "quantity": quantity, "status": status}
        item = _validate_row(SourceItemRow, raw, line, ref, errors)
        if item is not None:
            items.append(item)
    return items, errors
