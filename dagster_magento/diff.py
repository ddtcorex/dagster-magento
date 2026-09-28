"""Snapshots the current Magento catalog state and compares it against
desired rows, so a writer never sends an update for a value that is
already correct.

Bridge-absent path only (see spec 5.4): every snapshot here is built from
plain native Magento REST endpoints (`GET /V1/products`,
`*-price-information`, `GET /V1/inventory/source-items`, `GET
/V1/products/{sku}/media`) - no companion module, no B1/B2/B3 bridge call.
These functions only ever read; they never write to Magento.
"""

from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Callable, Literal

from dagster_magento.upload import chunk_rows

# Production hit "URI too large" above 50 SKUs per URL when filtering by
# `sku in (...)` - see spec 5.4. Price-information endpoints take the SKU
# list in the POST body instead, so they are not bound by this limit and
# use the larger 1000-row chunk size shared with upload_rows.
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


def snapshot_products(resource, skus: list[str], fields: list[str]) -> dict[str, dict]:
    """Fetch the current state of `skus` from `GET /V1/products`, projected
    to `fields` plus flattened `custom_attributes`.

    Returns a dict keyed by SKU. A SKU Magento does not know about is
    simply absent from the result - the caller reads that as "new row".
    """
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
        for item in response.get("items", []):
            flat = {field: item.get(field) for field in fields}
            for attribute in item.get("custom_attributes", []):
                flat[attribute["attribute_code"]] = attribute["value"]
            result[item["sku"]] = flat

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
