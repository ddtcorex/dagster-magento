# dagster-magento

A reusable Dagster library for Magento 2's REST API: an authenticated
`MagentoResource` with full CRUD, search-criteria filtering and async/bulk
submission, a catalog import layer built on top of it (attribute, attribute
set, category, product, price, MSI stock and media writers), and file
adapters for the native csv/json/xlsx import columns.

Targets standard Magento 2 REST endpoints only: no dependency on any
project-specific custom API module, so the same code runs against Magento
Open Source 2.4.6+ (verified live on 2.4.9) and any Commerce install.

## Installation

```
pip install "dagster-magento[xlsx] @ git+https://github.com/ddtcorex/dagster-magento.git@v0.2.0"
```

`dagster>=1.13.17`, `requests` and `pydantic` v2 come with it. The `xlsx`
extra adds `openpyxl`, needed only to read `.xlsx` sources; csv and json use
the standard library.

## Layers

Each layer is a small, independently tested module. Only `resource.py` and
`importers.py` know about Dagster and Magento at the same time.

| Module | What it does |
|---|---|
| `resource.py` | `MagentoResource`: admin token fetch and refresh, retry with backoff, `get`, `get_paginated`, `post`, `put`, `delete`, `upload_rows`, `upload_rows_async`, `get_bulk_status`, `resolve_attribute_options`, per-call `store_code` |
| `search.py` | `build_search_criteria`: `searchCriteria[filter_groups]...` params from a plain list of filters |
| `upload.py` | `UploadResult`/`run_upload`: synchronous chunked writes with catch-log-continue |
| `models.py` | Pydantic v2 row models (`ProductRow`, `CategoryRow`, `AttributeRow`, `PriceRow`, ...) and `validate_rows` |
| `resolvers.py` | One `GET products/attributes` preload plus cached lookups for attribute sets, store views, websites, category paths and option labels |
| `diff.py` | Snapshots the current catalog (products, prices, source items, media) and skips rows that already match |
| `writers/` | Pure planners: rows in, `Operation` values out. No HTTP. |
| `operation.py` | `Operation`, `BulkSpec`, `RowError` |
| `executor.py` | Runs operations in `sync` or `bulk` mode and folds the responses back onto rows |
| `bulk.py` | `AsyncBulkResult`/`run_async_upload` for submission, `map_detailed_status`/`wait_bulk` for polling |
| `importers.py` | One function per entity that composes snapshot, diff, plan and execute, plus `to_materialize_result` |
| `formats/` | Reads csv/json/xlsx and maps the native import columns onto the models |

## Usage: the resource alone

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
def link_children(magento: MagentoResource) -> None:
    # Resilient per-row write: one request per row, catch, log and continue.
    result = magento.upload_rows(
        "configurable-products/PARENT-SKU/child",
        rows=[{"sku": "CHILD-1"}, {"sku": "CHILD-2"}],
    )
    print(result.succeeded, result.failed, result.errors)

defs = Definitions(
    assets=[store_configs, all_products, link_children],
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

Logging goes through `dagster.get_dagster_logger()`, so it appears in the
Dagster UI's run logs when called from inside an asset and falls back to
standard Python logging otherwise. Set `verbose_logging=True` to log full
request and response bodies at `DEBUG` level when debugging one sync issue;
leave it off for normal runs, because a request body sent to an endpoint
like `POST /V1/customers` can legitimately carry a plaintext customer
password, and verbose mode logs it as-is.

## Update & delete

`put()`/`delete()` round out `get()`/`post()` for the two remaining core REST
verbs: updating an existing entity by id/sku, and removing one.

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
group, the common case for delta syncs. `condition_type` defaults to `"eq"`;
use Magento's standard values (`gteq`, `lteq`, `like`, `in`, `null`, ...) for
anything else. A list/tuple `value` is comma-joined, matching what `in`/`nin`
expect. `build_search_criteria()` never sets `page_size`/`current_page`: `get_paginated()` already manages those.

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
matching `POST /V1/products`), while `async/bulk` queues one operation per array
element, it doesn't wrap rows under a key the way `upload_rows(wrap_key=...)`
does. `result.accepted`/`result.rejected` only mean "Magento queued (or
rejected outright) the operation"; call `get_bulk_status()` to find out
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
same `attribute_code` at the same time can create duplicate options; Magento's
add-option endpoint doesn't dedupe, so resolve options for a given attribute
from one place, not from parallel/partitioned runs writing to it concurrently.

## Recipe: core endpoints for common sync tasks

All of the following use only `get`/`get_paginated`/`post`/`put`/`delete`/
`upload_rows`/`upload_rows_async`/`build_search_criteria`, no custom Magento
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

## Usage: catalog import assets

The library ships no orchestrator. This is a complete `Definitions` for the
eight catalog stages, in dependency order, reading native sample-format
files from a local directory:

```python
import os
from pathlib import Path

from dagster import Definitions, EnvVar, MaterializeResult, asset
from dagster_magento import (
    MagentoResource,
    import_attribute_sets,
    import_attributes,
    import_categories,
    import_media,
    import_prices,
    import_products,
    import_source_items,
    to_materialize_result,
)
from dagster_magento.formats import (
    attribute_set_assignments_from_rows,
    attributes_from_rows,
    categories_from_rows,
    prices_from_rows,
    products_from_rows,
    read_rows,
    source_items_from_rows,
)

SOURCE_DIR = Path(os.environ.get("MAGENTO_IMPORT_DIR", "sample-data"))
PARENT_TYPES = {"configurable", "bundle", "grouped", "downloadable"}


def parse(file_name, mapper):
    rows, errors = mapper(list(read_rows(SOURCE_DIR / file_name)))
    if errors:
        raise ValueError(f"{file_name}: {[error.message for error in errors]}")
    return rows


def product_rows():
    return parse("products.csv", products_from_rows)


@asset
def attributes(magento: MagentoResource) -> MaterializeResult:
    rows = parse("attributes.csv", attributes_from_rows)
    return to_materialize_result(import_attributes(magento, rows))


@asset(deps=[attributes])
def attribute_sets(magento: MagentoResource) -> MaterializeResult:
    rows = attribute_set_assignments_from_rows(list(read_rows(SOURCE_DIR / "attributes.csv")))
    return to_materialize_result(import_attribute_sets(magento, rows))


@asset(deps=[attribute_sets])
def categories(magento: MagentoResource) -> MaterializeResult:
    rows = parse("categories.csv", categories_from_rows)
    return to_materialize_result(import_categories(magento, rows))


@asset(deps=[categories])
def child_products(magento: MagentoResource) -> MaterializeResult:
    rows = [row for row in product_rows() if row.type not in PARENT_TYPES]
    return to_materialize_result(import_products(magento, rows))


@asset(deps=[child_products])
def parent_products(magento: MagentoResource) -> MaterializeResult:
    rows = [row for row in product_rows() if row.type in PARENT_TYPES]
    return to_materialize_result(import_products(magento, rows))


@asset(deps=[parent_products])
def prices(magento: MagentoResource) -> MaterializeResult:
    rows = parse("advanced_pricing.csv", prices_from_rows)
    return to_materialize_result(import_prices(magento, rows))


@asset(deps=[parent_products])
def stock(magento: MagentoResource) -> MaterializeResult:
    rows = parse("source_items.csv", source_items_from_rows)
    return to_materialize_result(import_source_items(magento, rows))


@asset(deps=[parent_products])
def media(magento: MagentoResource) -> MaterializeResult:
    rows = [row for row in product_rows() if row.images]
    return to_materialize_result(import_media(magento, rows))


defs = Definitions(
    assets=[
        attributes,
        attribute_sets,
        categories,
        child_products,
        parent_products,
        prices,
        stock,
        media,
    ],
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

MSI source and stock definitions are separate importers (`import_sources`,
`import_stocks`, `import_stock_source_links`) because most catalogs define
them once and then only write quantities through `import_source_items`.

## Behaviour

- **Behaviours:** `behavior="upsert"` (default), `create_only`,
  `update_only`, `disable` (writes `status = 2`). There is no hard delete.
- **Store scope:** global values are written through `/rest/all/`; each
  `store_values` entry is a separate request through `/rest/<store_code>/`
  carrying only the localized attributes, so a store view never gets
  overrides for price, status or anything else it did not ask for.
- **Diff:** every importer except the plan-only ones takes a snapshot first
  and reports `skipped_unchanged` for rows that already match. Pass
  `diff=False` to rewrite everything.
- **Results:** `UploadResult(succeeded, failed, pending, skipped_unchanged,
  errors)`; `to_materialize_result` turns it into `MaterializeResult`
  metadata and applies `fail_on_error_ratio` (default `None`), which raises
  when `failed / (succeeded + failed + pending)` exceeds the ratio.
  `MagentoAuthError` always aborts the run; a per-row rejection never does.
- **Chunking:** 200 rows per bulk request, 1000 rows per list endpoint in
  sync mode (`inventory/source-items`, `products/base-prices` and friends),
  50 SKUs per snapshot URL.

### Bulk mode

`mode="bulk"` submits non-GET operations to `async/bulk/V1/...` and polls
the detailed status. It is the same endpoint "Async bulk writes for large
volumes" above uses through `upload_rows_async`; the difference is that the
importers own the chunking, the retry of a retriably failed operation and
the mapping of each operation back onto its row. It is opt-in per run, and
the target needs:

- a running consumer:
  `bin/magento queue:consumers:start async.operations.all`, ideally several
  processes for throughput (`multiple_processes` under
  `cron_consumers_runner` in `app/etc/env.php`);
- indexers in `schedule` mode and cron running, so the price and inventory
  mviews catch up after the writes;
- a store view created **before** the consumers start, because a running
  consumer caches the store list.

Without a consumer, bulk operations stay at status `4` and are reported as
`pending` after the timeout (600 s by default) with a log line naming the
consumer. `pending` is never counted as success.

That also covers the opposite failure: a consumer that is running but never
executes an operation. On the Magento 2.4.9 test sandbox a variable subset
of the published operations was measured to be dropped by the consumer
(publish 20, deliver 20, ack 14 for one 18-operation bulk, with the lost
rows left at status 4 and nothing logged by Magento), and bulk mode reports
exactly those rows as `pending`. See
`tests/live/test_catalog_e2e.py::test_same_catalog_imports_in_bulk_mode`
for the measurement.

## File adapters

`read_rows(path)` yields `(line_number, row_dict)` for `.csv`, `.json` and
`.xlsx`, and the `*_from_rows` mappers turn those into models, reporting a
bad row as a `RowError` with its source line instead of raising. The
mappers understand the native import columns: `additional_attributes`,
`configurable_variations`, `bundle_values`, `associated_skus`,
`downloadable_links`, `categories`, the image columns and the
`advanced_pricing` tier columns. Unknown columns are passed through into
`attributes`; Firebear-only columns (`group`, `tier_prices`, `attribute|*`)
are dropped with one warning each. Sources are read from the local
filesystem or an http(s) URL (for images); nothing is sent to Magento as a
bare path.

## Local sandbox

`scripts/sandbox.sh` brings up a disposable Magento 2.4.9 through
[govard](https://github.com/ddtcorex/govard) and configures it the way bulk
mode needs (indexers on schedule, full page cache, 2FA off for the admin
token, four `async.operations.all` consumers and `cron_consumers_runner`):

```
scripts/sandbox.sh up            # bootstrap (default 2.4.9)
scripts/sandbox.sh consumers     # start 4 async.operations.all consumers
scripts/sandbox.sh cron-run      # run bin/magento cron:run twice
scripts/sandbox.sh reset         # down -v, then a fresh install
scripts/sandbox.sh env           # export MAGENTO_BASE_URL/USERNAME/PASSWORD/STORE_VIEW
scripts/sandbox.sh down
```

The project lives in `sandbox/dagster-magento-sandbox/` (gitignored) and the
generated admin password in `sandbox/.admin-password`.

## Tests

```
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/pytest -q              # hermetic: requests_mock only, no network
.venv/bin/pytest -m samples -q   # downloads the public sample files and parses them
eval "$(scripts/sandbox.sh env)"
MAGENTO_CA_BUNDLE=~/.govard/ssl/root.crt .venv/bin/pytest -m live -q
```

The default run is hermetic. The `samples` marker downloads the public
sample files at a pinned commit into `tests/.samples/` (gitignored, never
vendored), and `live` needs the sandbox above. The live suite imports a full
sample catalog in dependency order in sync mode, reruns it to prove the diff
skips what is already there, resets the sandbox and repeats in bulk mode,
then checks that a price write reaches a full page cached storefront after
cron, that a bulk store-view update keeps its store scope, and that special
price dates round trip.

## Native-only limits

- Swatch attributes (visual, text, image) are not created; only the
  standard inputs.
- Very large catalogs should be split at the asset level rather than pushed
  through one run: a run is planned in memory before it executes.
- Media is uploaded through the REST media endpoint, one image at a time.
- Customers, CMS content, URL rewrites, cart and catalog rules are out of
  scope; use `post`/`upload_rows` directly for those.
- No hard delete anywhere.

## License

MIT
