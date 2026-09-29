"""Snapshots the current Magento catalog state and compares it against
desired rows, so a writer never sends an update for a value that is
already correct.

Every snapshot defaults to plain native Magento REST endpoints
(`GET /V1/products`, `*-price-information`, `GET
/V1/inventory/source-items`, `GET /V1/products/{sku}/media`). The product
snapshot also accepts an optional bridge client and then reads the index and
the store-scoped attribute values instead, per capability. These functions
only ever read; they never write to Magento.
"""

import logging
import re
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Callable, Literal

import requests

from dagster_magento.resolvers import ResolveError, boolean_value
from dagster_magento.upload import chunk_rows

# Production hit "URI too large" above 50 SKUs per URL when filtering by
# `sku in (...)` - see spec 5.4. Price-information endpoints take the SKU
# list in the POST body instead, so they are not bound by this limit and
# use the larger 1000-row chunk size shared with upload_rows.
logger = logging.getLogger(__name__)

_URL_FILTER_CHUNK_SIZE = 50
_PRICE_CHUNK_SIZE = 1000
_SOURCE_ITEMS_PAGE_SIZE = 200


def normalize(value, kind: Literal["decimal", "datetime", "multiselect", "text", "int"]):
    """Normalize a single value so two representations of the same data
    compare equal (a float price vs. its string form, a naive date vs. an
    explicit UTC offset, a differently-ordered multiselect list, ...).

    `None` always stays `None` regardless of `kind` - a missing value never
    normalizes into a placeholder that could spuriously equal a real one.
    """
    if value is None:
        return None

    if kind == "decimal":
        quantized = Decimal(str(value)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        return str(quantized)

    if kind == "datetime":
        return _normalize_datetime(value)

    if kind == "multiselect":
        parts = value.split(",") if isinstance(value, str) else list(value)
        return tuple(sorted(str(part).strip() for part in parts))

    if kind == "int":
        return int(value)

    if kind == "text":
        return str(value).strip()

    raise ValueError(f"Unknown normalize kind: {kind!r}")


def _normalize_datetime(value) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        # `datetime.fromisoformat` (3.10+) already accepts "YYYY-MM-DD",
        # "YYYY-MM-DD HH:MM:SS" and an ISO offset such as
        # "YYYY-MM-DDTHH:MM:SS+02:00" - normalize a trailing "Z" first since
        # that Zulu suffix is only accepted by fromisoformat from 3.11.
        iso_text = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            parsed = datetime.fromisoformat(iso_text)
        except ValueError as error:
            raise ValueError(f"Unrecognized datetime value: {value!r}") from error

    # A naive value is treated as already-UTC; an aware value is converted.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)

    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def _sku_filter_chunks(skus: list[str], size: int):
    """Group SKUs into URL-filter-sized chunks, yielding
    `(condition_type, value, chunk)`.

    A SKU containing a comma cannot share an `in` filter (the comma is the
    `in` separator) so it is always split into its own chunk and queried
    with `condition_type=eq` instead.
    """
    chunk: list[str] = []
    for sku in skus:
        if "," in sku:
            if chunk:
                yield "in", ",".join(chunk), chunk
                chunk = []
            yield "eq", sku, [sku]
            continue
        chunk.append(sku)
        if len(chunk) == size:
            yield "in", ",".join(chunk), chunk
            chunk = []
    if chunk:
        yield "in", ",".join(chunk), chunk


def snapshot_products(
    resource, skus: list[str], fields: list[str], bridge=None, store_id: int = 0
) -> dict[str, dict]:
    """Fetch the current state of `skus`, keyed by SKU.

    `GET /V1/products` is the default source. With a bridge client whose store
    advertises the product index, the index answers the entity fields and the
    attribute endpoint answers the rest, per store, and Magento's own fallback
    applies: the store value wins when the store has one, otherwise the
    default store value is used. Either way a SKU Magento does not know about
    is absent from the result, and the caller reads that as "new row".
    """
    if bridge is not None and bridge.has(bridge.PRODUCT_INDEX):
        try:
            return _products_from_bridge(bridge, skus, fields, store_id)
        except requests.exceptions.HTTPError as error:
            logger.warning(f"bridge snapshot failed ({error}); using the REST snapshot")

    return _products_from_rest(resource, skus, fields)


def _products_from_rest(resource, skus: list[str], fields: list[str]) -> dict[str, dict]:
    """`GET /V1/products`, projected to `fields` and flattened
    `custom_attributes`."""
    result: dict[str, dict] = {}
    field_list = ",".join(["sku", *fields, "custom_attributes"])

    for condition_type, value, chunk in _sku_filter_chunks(skus, _URL_FILTER_CHUNK_SIZE):
        params = {
            "searchCriteria[filterGroups][0][filters][0][field]": "sku",
            "searchCriteria[filterGroups][0][filters][0][value]": value,
            "searchCriteria[filterGroups][0][filters][0][condition_type]": condition_type,
            "searchCriteria[pageSize]": len(chunk),
            "fields": f"items[{field_list}]",
        }
        response = resource.get("products", params=params)
        # A `fields` projection turns an empty match into {"items": null}
        # (verified live on 2.4.9), and drops custom_attributes the same way.
        for item in response.get("items") or []:
            flat = {field: item.get(field) for field in fields}
            for attribute in item.get("custom_attributes") or []:
                flat[attribute["attribute_code"]] = attribute["value"]
            result[item["sku"]] = flat

    return result


def _products_from_bridge(bridge, skus: list[str], fields: list[str], store_id: int) -> dict[str, dict]:
    """The index for the entity fields and the attribute endpoint for the rest.

    Only SKUs the index knows are returned, which is what makes this the same
    answer as the REST snapshot: a product Magento does not have cannot be
    reported as existing.
    """
    index = bridge.index_by_sku()
    present = [sku for sku in skus if sku in index]
    result: dict[str, dict] = {
        sku: {field: index[sku].get(field) for field in fields} for sku in present
    }

    codes = [field for field in fields if field not in bridge.ENTITY_FIELDS]
    if codes and present:
        for sku, per_code in bridge.attribute_values(present, codes, store_id=store_id).items():
            for code, (store_value, default_value) in per_code.items():
                if store_value is None and default_value is None:
                    continue
                result[sku][code] = store_value if store_value is not None else default_value

    return result


def snapshot_prices(resource, skus: list[str]) -> dict[str, dict]:
    """Fetch base, special and tier prices for `skus` from the three
    native `*-price-information` endpoints and merge them per SKU as
    `{"base": {store_id: price}, "special": [...], "tiers": [...]}`.

    A SKU that appears in none of the three responses is absent from the
    result, same as `snapshot_products`.
    """
    result: dict[str, dict] = {}

    def entry(sku: str) -> dict:
        return result.setdefault(sku, {"base": {}, "special": [], "tiers": []})

    for chunk in chunk_rows(skus, _PRICE_CHUNK_SIZE):
        payload = {"skus": chunk}

        for item in resource.post("products/base-prices-information", payload).json():
            entry(item["sku"])["base"][item["store_id"]] = item["price"]

        for item in resource.post("products/special-price-information", payload).json():
            entry(item["sku"])["special"].append(item)

        for item in resource.post("products/tier-prices-information", payload).json():
            entry(item["sku"])["tiers"].append(item)

    return result


def snapshot_source_items(resource, skus: list[str]) -> dict[tuple[str, str], tuple[float, int]]:
    """Fetch `GET /V1/inventory/source-items` for `skus`, keyed on
    `(source_code, sku) -> (quantity, status)`.

    Pagination is delegated to `MagentoResource.get_paginated` (the same
    `searchCriteria[page_size]`/`[current_page]` loop every other paged
    endpoint in this package uses) rather than hand-rolled here.
    """
    result: dict[tuple[str, str], tuple[float, int]] = {}

    for condition_type, value, _chunk in _sku_filter_chunks(skus, _URL_FILTER_CHUNK_SIZE):
        params = {
            "searchCriteria[filterGroups][0][filters][0][field]": "sku",
            "searchCriteria[filterGroups][0][filters][0][value]": value,
            "searchCriteria[filterGroups][0][filters][0][condition_type]": condition_type,
        }
        items = resource.get_paginated(
            "inventory/source-items", params=params, page_size=_SOURCE_ITEMS_PAGE_SIZE
        )
        for item in items:
            result[(item["source_code"], item["sku"])] = (item["quantity"], item["status"])

    return result


def snapshot_media(resource, sku: str) -> list[dict]:
    """Fetch `GET /V1/products/{sku}/media` - Magento returns the media
    gallery as a bare list, not wrapped in `items`."""
    return resource.get(f"products/{sku}/media")


def split_changed(
    rows: list,
    snapshot: dict,
    key: Callable,
    project: Callable,
    project_existing: Callable,
) -> tuple[list, int]:
    """Split `rows` into the ones that differ from `snapshot` and a count
    of the ones that don't.

    `key(row)` looks the row up in `snapshot`; a row whose key is absent
    from `snapshot` is always kept (a new SKU has nothing to diff against).
    Otherwise `project(row)` and `project_existing(snapshot[key])` are
    compared for equality - both are expected to already be normalized
    (see `normalize` above) so equivalent-but-differently-formatted values
    compare equal.
    """
    changed = []
    skipped = 0

    for row in rows:
        existing = snapshot.get(key(row))
        if existing is None or project(row) != project_existing(existing):
            changed.append(row)
        else:
            skipped += 1

    return changed, skipped


# The product fields snapshot_products must read for product_matches_snapshot;
# custom attributes come back on top of these.
PRODUCT_SNAPSHOT_FIELDS = [
    "type_id",
    "attribute_set_id",
    "status",
    "name",
    "price",
    "visibility",
    "weight",
    "extension_attributes",
]
_PRODUCT_FIELD_KINDS = {
    "type_id": "text",
    "attribute_set_id": "int",
    "status": "int",
    "name": "text",
    "price": "decimal",
    "visibility": "int",
    "weight": "decimal",
}
# Row parts no snapshot reads back. A row carrying any of them can never be
# proven unchanged, so it always counts as changed.
_UNSNAPSHOTTED_PRODUCT_PARTS = (
    "store_values",
    "variations",
    "configurable_attributes",
    "bundle_options",
    "grouped_links",
    "downloadable_links",
    "downloadable_samples",
)


def _url_key(value) -> str:
    # Magento stores url_key formatted: lowercase, every run of other
    # characters as one "-" (non-ASCII transliteration is not mirrored, so
    # such a key compares unequal and is conservatively rewritten).
    return re.sub(r"[^a-z0-9]+", "-", str(value).lower()).strip("-")


def _attribute_kind_value(meta, value):
    if meta.frontend_input == "select":
        return str(value)
    if meta.frontend_input == "multiselect":
        return normalize(value, "multiselect")
    if meta.backend_type == "datetime":
        # Read back with seconds ("2017-01-01 12:12:00"); raises ValueError
        # on an unparseable value, which the caller reads as "changed".
        return normalize(value, "datetime")
    if meta.code == "url_key":
        return _url_key(value)
    return normalize(value, "decimal" if meta.backend_type == "decimal" else "text")


def _row_option_ids(code: str, value, meta, resolver):
    # Mirrors the product writer's value rules (writers/products.py).
    if meta.frontend_input == "boolean":
        return boolean_value(code, value)
    if meta.frontend_input == "select":
        if not isinstance(value, str):
            return value
        return resolver.option_id(code, value, create=False)
    if meta.frontend_input == "multiselect":
        labels = value.split(",") if isinstance(value, str) else value
        return [resolver.option_id(code, str(label).strip(), create=False) for label in labels]
    return value


def product_matches_snapshot(row, snap: dict, resolver) -> bool:
    """True only when everything the product writer would send for `row`
    already equals `snap` (one snapshot_products entry read with
    PRODUCT_SNAPSHOT_FIELDS). Compared: the scalar fields the row sets, its
    custom attributes (labels resolved to option ids without creating
    any), its website ids, and its category ids when it sets categories.

    A false "unchanged" would silently drop a real update, so every doubt
    answers False: an unresolvable label, set, website or category, a
    missing extension_attributes, or any row part no snapshot covers
    (store values and type-specific parts). Images are not compared here;
    import_media owns them.
    """
    if any(getattr(row, part) for part in _UNSNAPSHOTTED_PRODUCT_PARTS):
        return False
    extension = snap.get("extension_attributes") or {}
    try:
        desired = {
            "type_id": row.type,
            "attribute_set_id": row.attribute_set
            if row.attribute_set.isdigit()
            else resolver.attribute_set_id(row.attribute_set),
        }
        for field in ("name", "price", "status", "visibility", "weight"):
            if getattr(row, field) is not None:
                desired[field] = getattr(row, field)
        wanted = {field: normalize(value, _PRODUCT_FIELD_KINDS[field]) for field, value in desired.items()}
        current = {field: normalize(snap.get(field), _PRODUCT_FIELD_KINDS[field]) for field in desired}

        for code, value in row.attributes.items():
            meta = resolver.attribute(code)
            wanted[code] = _attribute_kind_value(meta, _row_option_ids(code, value, meta, resolver))
            current[code] = None if snap.get(code) is None else _attribute_kind_value(meta, snap[code])

        wanted["website_ids"] = sorted(resolver.website_id(code) for code in row.websites)
        current["website_ids"] = sorted(int(value) for value in extension.get("website_ids") or [])
        if row.categories:
            wanted["category_ids"] = sorted(resolver.category_id(path) for path in row.categories)
            current["category_ids"] = sorted(
                int(link["category_id"]) for link in extension.get("category_links") or []
            )
    except (ResolveError, KeyError, ValueError):
        return False
    return wanted == current


def _tier_key(website_id, customer_group, qty, price, price_type):
    # Magento reads the group back lowercased ("all groups"), verified live.
    return (
        int(website_id),
        str(customer_group).strip().casefold(),
        normalize(qty, "decimal"),
        normalize(price, "decimal"),
        price_type,
    )


def _special_key(price, price_from, price_to):
    return (normalize(price, "decimal"), normalize(price_from, "datetime"), normalize(price_to, "datetime"))


def price_matches_snapshot(row, snap: dict, website_ids: dict[str, int] | None = None) -> bool:
    """True when every price part `row` sets already equals `snap` (one
    snapshot_prices entry): the base price for row.store_id, the special
    price and dates for row.store_id, and the full tier set (tiers are not
    store scoped). A tier website code resolves as in the pricing writer
    ("all" is 0, digits are an id, else `website_ids`); an unknown code
    means changed so the writer can fail the row."""
    if row.price is not None:
        base = snap["base"].get(row.store_id)
        if normalize(base, "decimal") != normalize(row.price, "decimal"):
            return False
    if row.special_price is not None:
        wanted = _special_key(row.special_price, row.special_from, row.special_to)
        found = [
            _special_key(item.get("price"), item.get("price_from"), item.get("price_to"))
            for item in snap["special"]
            if item.get("store_id") == row.store_id
        ]
        if wanted not in found:
            return False
    if row.tiers is not None:
        known = {"all": 0, **(website_ids or {})}
        wanted_tiers = []
        for tier in row.tiers:
            if tier.website not in known and not tier.website.isdigit():
                return False
            website_id = known.get(tier.website, tier.website)
            wanted_tiers.append(
                _tier_key(website_id, tier.customer_group, tier.qty, tier.price, tier.price_type)
            )
        current_tiers = [
            _tier_key(t["website_id"], t["customer_group"], t["quantity"], t["price"], t["price_type"])
            for t in snap["tiers"]
        ]
        if sorted(wanted_tiers) != sorted(current_tiers):
            return False
    return True
