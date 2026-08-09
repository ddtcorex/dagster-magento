# dagster-magento

A reusable Dagster resource for talking to Magento 2's REST API: admin-token
authentication, paginated/filtered reads, full CRUD, a resilient chunked/per-row
upload helper, and async/bulk submission for high-volume writes — all with
Dagster-integrated logging.

Targets standard Magento 2 REST endpoints only — no dependency on any
project-specific custom API modules.

## Installation

```
pip install git+https://github.com/ddtcorex/dagster-magento.git@v0.1.0
```

## Usage

```python
from dagster import asset, Definitions, EnvVar
from dagster_magento import MagentoResource

@asset
def store_configs(magento: MagentoResource) -> list:
    return magento.get("store/storeConfigs")

@asset
def all_products(magento: MagentoResource) -> list:
    return magento.get_paginated("products", page_size=1000)

@asset
def source_item_sync(magento: MagentoResource) -> None:
    magento.post("products", {"product": {"sku": "NEW-SKU", "name": "New product"}})

    # Resilient bulk write - one row per request (e.g. linking configurable children)
    result = magento.upload_rows(
        "configurable-products/PARENT-SKU/child",
        rows=[{"sku": "CHILD-1"}, {"sku": "CHILD-2"}],
    )

    # Resilient bulk write - chunked under a wrapper key (e.g. inventory source items)
    result = magento.upload_rows(
        "inventory/source-items",
        rows=[{"sku": "A", "source_code": "default", "quantity": 5, "status": 1}],
        chunk_size=200,
        wrap_key="sourceItems",
    )
    print(result.succeeded, result.failed, result.errors)

defs = Definitions(
    assets=[store_configs, all_products, source_item_sync],
    resources={
        "magento": MagentoResource(
            base_url=EnvVar("MAGENTO_BASE_URL"),
            username=EnvVar("MAGENTO_ADMIN_USERNAME"),
            password=EnvVar("MAGENTO_ADMIN_PASSWORD"),
            store_view=EnvVar("MAGENTO_STORE_VIEW"),
        ),
    },
)
```

Logging goes through `dagster.get_dagster_logger()` — no setup needed; it
shows up in the Dagster UI's run logs when called from inside an asset, and
falls back to standard Python logging otherwise. Set `verbose_logging=True`
on `MagentoResource` to log full request/response bodies at `DEBUG` level
when debugging a specific sync issue — off by default since a full sync can
involve hundreds of thousands of rows. **Note:** a request body sent via
`post`/`upload_rows` to an endpoint like `POST /V1/customers` can legitimately
contain a plaintext customer password field — `verbose_logging=True` will log
that request body as-is, so avoid leaving it enabled on any pipeline that
writes customer records.

## Update & delete

`put()`/`delete()` round out `get()`/`post()` for the two remaining core REST
verbs — updating an existing entity by id/sku, and removing one:

```python
magento.put("products/EXISTING-SKU", {"product": {"price": 24.99}})
magento.delete("configurable-products/PARENT-SKU/children/CHILD-SKU")
magento.delete("categories/42")
```

## Filtering & sorting with search criteria

Magento's `searchCriteria` filter/sort query params are the same shape for
every searchable entity (products, orders, customers, invoices, ...).
`build_search_criteria()` builds them from a plain list of filters instead of
hand-writing `searchCriteria[filter_groups][0][filters][0][field]=...` params:

```python
from dagster_magento import build_search_criteria

@asset
def orders_updated_today(magento: MagentoResource) -> list:
    params = build_search_criteria(
        filters=[("updated_at", "2026-08-08 00:00:00", "gteq")],
        sort_orders=[("created_at", "DESC")],
    )
    return magento.get_paginated("orders", params=params)

@asset
def customer_by_email(magento: MagentoResource) -> list:
    params = build_search_criteria(filters=[("email", "someone@example.com")])
    return magento.get("customers/search", params=params)
```

Every `(field, value, condition_type)` filter is AND'd into a single filter
group — the common case for delta syncs. `condition_type` defaults to `"eq"`;
use Magento's standard values (`gteq`, `lteq`, `like`, `in`, `null`, ...) for
anything else. A list/tuple `value` is comma-joined, matching what `in`/`nin`
expect. `build_search_criteria()` never sets `page_size`/`current_page` —
`get_paginated()` already manages those.

## Async bulk writes for large volumes

For very large write volumes, Magento's core `async/bulk/V1/*` API accepts a
batch of individual operations in one HTTP call and processes them in the
background, returning a `bulk_uuid` immediately instead of waiting for each
row to actually save:

```python
@asset
def bulk_create_products(magento: MagentoResource) -> list:
    rows = [
        {"product": {"sku": "NEW-1", "name": "New 1", "price": 9.99, "attribute_set_id": 4, "type_id": "simple"}},
        {"product": {"sku": "NEW-2", "name": "New 2", "price": 14.99, "attribute_set_id": 4, "type_id": "simple"}},
    ]
    result = magento.upload_rows_async("products", rows, chunk_size=200)
    return result.bulk_uuids  # hand these to a downstream asset/sensor

@asset(deps=[bulk_create_products])
def bulk_create_products_status(magento: MagentoResource, bulk_create_products: list) -> None:
    for bulk_uuid in bulk_create_products:
        status = magento.get_bulk_status(bulk_uuid)
        print(status["operation_count"], status["operations_list"])
```

Each row must already be shaped exactly like the body you'd send to the
*synchronous* single-item endpoint (e.g. `{"product": {...}}` for `products`,
matching `POST /V1/products`) — `async/bulk` queues one operation per array
element, it doesn't wrap rows under a key the way `upload_rows(wrap_key=...)`
does. `result.accepted`/`result.rejected` only mean "Magento queued (or
rejected outright) the operation" — call `get_bulk_status()` to find out
whether queued operations actually succeeded.

## Resolving select/multiselect attribute options

Select and multiselect EAV attributes (`color`, `size`, ...) store an integer
`option_id` internally, but a supplier feed or CSV gives you the human-readable
label instead. `resolve_attribute_options()` looks up each label against the
attribute's existing options (trimmed, case-insensitive) and creates whichever
ones don't exist yet, returning a label → option_id map:

```python
@asset
def color_option_ids(magento: MagentoResource, product_feed: list) -> dict:
    labels = {row["color"] for row in product_feed if row.get("color")}
    return magento.resolve_attribute_options("color", list(labels))

@asset(deps=[color_option_ids])
def products_with_resolved_colors(magento: MagentoResource, product_feed: list, color_option_ids: dict) -> None:
    for row in product_feed:
        option_id = color_option_ids.get(row["color"])
        magento.post("products", {"product": {"sku": row["sku"], "custom_attributes": [
            {"attribute_code": "color", "value": option_id},
        ]}})
```

The returned dict is keyed by the exact label strings you passed in, so
`color_option_ids[row["color"]]` works directly without re-trimming/re-casing
the label yourself. Two calls racing to create the same missing label on the
same `attribute_code` at the same time can create duplicate options — Magento's
add-option endpoint doesn't dedupe — so resolve options for a given attribute
from one place, not from parallel/partitioned runs writing to it concurrently.

## Recipe: core endpoints for common sync tasks

All of the following use only `get`/`get_paginated`/`post`/`put`/`delete`/
`upload_rows`/`upload_rows_async`/`build_search_criteria` — no custom Magento
modules required.

| Task | Core endpoint(s) |
|---|---|
| Create/update/delete a product | `POST\|PUT\|DELETE products/:sku` |
| Bulk-update prices | `POST products/base-prices` / `products/special-price` (wrap_key=`"prices"`) |
| Create/update/delete a category | `POST\|PUT\|DELETE categories/:id`, tree via `GET categories` |
| Filter categories by name | `GET categories/list` + `build_search_criteria` |
| Link a child into a configurable product | `POST configurable-products/:sku/child` (via `upload_rows`) |
| Unlink a configurable child | `DELETE configurable-products/:sku/children/:childSku` |
| Link a bundle option | `POST bundle-products/:sku/links/:optionId` |
| Create/search/update a customer | `POST customers`, `GET customers/search` + `build_search_criteria`, `PUT customers/:id` |
| Search orders (e.g. updated since) | `GET orders` + `build_search_criteria` |
| Add a comment to an order | `POST orders/:id/comments` |
| Manage MSI sources/stocks | `GET\|POST inventory/sources`, `inventory/stocks`, `inventory/stock-source-links` |
| Set stock quantities | `POST inventory/source-items` (via `upload_rows`, wrap_key=`"sourceItems"`) |
| Create/update a product attribute | `POST products/attributes`, `POST products/attributes/:code/options` |
| Resolve/auto-create select or multiselect option labels | `GET\|POST products/attributes/:code/options` (via `resolve_attribute_options`) |
| Create/update an attribute set | `GET\|POST products/attribute-sets` |
