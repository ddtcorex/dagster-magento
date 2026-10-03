# Dagster Assets

The library ships no assets and no orchestrator: you write the assets, and
each one calls one importer with a `MagentoResource`. This page gives a
complete `Definitions` for a catalog load, explains the order the assets must
run in and why (grounded in what the writers and the executor actually do),
shows how to expose `mode`, `use_bridge` and the error ratio as run
configuration, how `to_materialize_result` reports counts, and what changes
when you run in bulk mode with a Magento consumer.

## A complete catalog definition

This is a complete eight-stage example with run configuration and a pending
check. It reads native import files from `MAGENTO_IMPORT_DIR`. It
was loaded with Dagster 1.13 and the `prices` asset was materialized against
a mocked Magento to check the wiring.

```python
import os
from pathlib import Path
from typing import Literal, Optional

from dagster import Config, Definitions, EnvVar, Failure, MaterializeResult, asset
from dagster_magento import (
    MagentoResource,
    UploadResult,
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
PARENT_TYPES = {"configurable", "bundle", "grouped"}


class CatalogImportConfig(Config):
    mode: Literal["sync", "bulk"] = "sync"
    use_bridge: Literal["auto", "never", "require"] = "auto"
    diff: bool = True
    fail_on_error_ratio: Optional[float] = None
    fail_on_pending: bool = True


def parse(file_name, mapper):
    rows, errors = mapper(list(read_rows(SOURCE_DIR / file_name)))
    if errors:
        raise ValueError(f"{file_name}: {[error.message for error in errors]}")
    return rows


def product_rows():
    return parse("products.csv", products_from_rows)


def finish(result: UploadResult, config: CatalogImportConfig) -> MaterializeResult:
    if config.fail_on_pending and result.pending:
        raise Failure(
            description=f"{result.pending} row(s) still pending: is async.operations.all consuming?",
            metadata=result.to_metadata(),
        )
    return to_materialize_result(result, fail_on_error_ratio=config.fail_on_error_ratio)


def run(importer, magento, rows, config: CatalogImportConfig) -> MaterializeResult:
    result = importer(magento, rows, mode=config.mode, diff=config.diff, use_bridge=config.use_bridge)
    return finish(result, config)


@asset
def attributes(config: CatalogImportConfig, magento: MagentoResource) -> MaterializeResult:
    return run(import_attributes, magento, parse("attributes.csv", attributes_from_rows), config)


@asset(deps=[attributes])
def attribute_sets(config: CatalogImportConfig, magento: MagentoResource) -> MaterializeResult:
    rows = attribute_set_assignments_from_rows(list(read_rows(SOURCE_DIR / "attributes.csv")))
    return run(import_attribute_sets, magento, rows, config)


@asset(deps=[attribute_sets])
def categories(config: CatalogImportConfig, magento: MagentoResource) -> MaterializeResult:
    return run(import_categories, magento, parse("categories.csv", categories_from_rows), config)


@asset(deps=[categories])
def child_products(config: CatalogImportConfig, magento: MagentoResource) -> MaterializeResult:
    rows = [row for row in product_rows() if row.type not in PARENT_TYPES]
    return run(import_products, magento, rows, config)


@asset(deps=[child_products])
def parent_products(config: CatalogImportConfig, magento: MagentoResource) -> MaterializeResult:
    rows = [row for row in product_rows() if row.type in PARENT_TYPES]
    return run(import_products, magento, rows, config)


@asset(deps=[parent_products])
def prices(config: CatalogImportConfig, magento: MagentoResource) -> MaterializeResult:
    return run(import_prices, magento, parse("advanced_pricing.csv", prices_from_rows), config)


@asset(deps=[parent_products])
def stock(config: CatalogImportConfig, magento: MagentoResource) -> MaterializeResult:
    return run(import_source_items, magento, parse("source_items.csv", source_items_from_rows), config)


@asset(deps=[parent_products])
def media(config: CatalogImportConfig, magento: MagentoResource) -> MaterializeResult:
    rows = [row for row in product_rows() if row.images]
    return run(import_media, magento, rows, config)


defs = Definitions(
    assets=[attributes, attribute_sets, categories, child_products, parent_products, prices, stock, media],
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

Notes on the example:

- Each `*_from_rows` mapper returns `(rows, errors)`, except
  `attribute_set_assignments_from_rows`, which returns a list. `parse()` turns
  file mapping errors into an asset failure before anything is sent; you can
  instead pass the valid rows on and report the errors yourself.
- `PARENT_TYPES` here lists the types the library saves after other products.
  A downloadable product references no other product, so it goes with the
  children.
- Every asset re-reads `products.csv`. For large files, read once into an
  upstream asset or a cached helper.
- MSI sources, stocks and stock-source links have their own importers
  (`import_sources`, `import_stocks`, `import_stock_source_links`). Add them
  before `stock` if the target does not have the sources yet; most catalogs
  define them once.

## Run configuration

Each asset gets its own `CatalogImportConfig`. Launchpad or `run_config`:

```yaml
ops:
  child_products:
    config:
      mode: bulk
      use_bridge: never
      fail_on_error_ratio: 0.01
  parent_products:
    config:
      mode: bulk
  prices:
    config:
      diff: false
```

Why `Literal` types: the importers do not validate these strings at runtime.
Any `mode` other than `"bulk"` runs sync, and a misspelled `use_bridge` (for
example `"Require"`) behaves like `"auto"`. Typing the config fields as
`Literal` makes Dagster reject a typo before the run starts (verified:
`Value at path root:ops:a:config:mode not in enum type Literal['sync', 'bulk']`).

| Option | Values | Effect |
| --- | --- | --- |
| `mode` | `sync`, `bulk` | Bulk applies to product saves, store-value saves, configurable options and links, and media adds. Prices and inventory are always sync. See [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution). |
| `use_bridge` | `auto`, `never`, `require` | See [Optional-Bridge](Optional-Bridge). `require` raises `MagentoImportError` when a needed capability is missing. |
| `diff` | `true`, `false` | `false` rewrites rows the snapshot proves unchanged. Ignored by importers that do not snapshot. |
| `fail_on_error_ratio` | float or null | Fails the asset when `failed / (succeeded + failed + pending)` is above it. |

## `to_materialize_result`

```python
to_materialize_result(result: UploadResult, fail_on_error_ratio: float | None = None) -> MaterializeResult
```

It returns `MaterializeResult(metadata=result.to_metadata())`, so every
materialization carries `succeeded`, `failed`, `pending`,
`skipped_unchanged` and `error_count`, and you can chart them across runs in
the asset's metadata plots. It logs `Magento import result: {...}` and then
applies the ratio. When the ratio raises, the asset fails without metadata;
the logged line is where the counts are. The error details
(`result.errors`) are not attached as metadata; log or store them yourself
if you need them per run, for example
`MaterializeResult(metadata={**result.to_metadata(), "first_errors": MetadataValue.json(result.errors[:20])})`.

Passing `fail_on_error_ratio` to the importer instead has the same effect
(the importer checks before returning). Use one place, not both.

## Ordering: why the dependencies look like this

| Upstream | Downstream | Reason in the code |
| --- | --- | --- |
| `attributes` | `attribute_sets` | Set assignments reference attribute codes. |
| `attribute_sets` | `categories`, products | The product writer resolves `attribute_set` by name and fails the row with `unknown attribute set: ...` if it is missing. |
| `attributes` | products | The resolver fails an unknown code with `unknown attribute: ...`. A missing select option is created on the fly during planning, but the attribute itself must exist. |
| `categories` | products | `categories` paths are resolved with `Resolver.category_id`, which never creates a node: an unknown path fails the product row with `unknown category path: ...`. Only `import_categories` creates categories. |
| child products | parent products | See below. |
| products | `prices`, `stock`, `media` | Price storage rejects an unknown SKU (`Invalid attribute SKU = ...`); a media gallery read on a missing product fails the row. |

### Parents after children

Within one `import_products` call the library already orders things:

- Grouped and bundle parents that carry links are in phase 1 and are saved
  after every phase 0 save, in sync and in bulk mode.
- Configurable options and child links run in a second execution step after
  every product save of the call, and only for parents that succeeded.

So a single call with parents and children together works; the live suite
imports the whole sample catalog that way. The split into `child_products`
and `parent_products` assets matters when the rows are split across calls:
if a parent's children are imported by another asset, partition or run, that
other call must finish first, because phase ordering only covers the
operations of one call.

### Partitions

The library has no partition support of its own; a partition is just a
smaller list of rows passed to an importer. Advice grounded in how the code
behaves:

- Keep a composite parent and all its children in the same partition, or run
  every child partition before any parent partition.
- Each call builds its own resolver and snapshot, so partitions do not share
  caches, and each call probes the bridge again.
- Do not run partitions that may create options for the same select
  attribute at the same time. Option creation is a POST per missing label,
  and Magento's add-option endpoint does not deduplicate, so two concurrent
  calls can create the same label twice. Import attributes with their options
  first (`import_attributes` creates missing options from `AttributeRow.options`),
  or limit the concurrency of the product assets.
- Very large catalogs should be split at the asset or partition level anyway:
  a call plans every row in memory before it sends anything, and media plans
  hold base64 image data.
- Splitting by SKU range is safe for prices and stock: those writes are per
  SKU (and per source or store), with no cross-row references.

## Running in bulk mode with a consumer

Bulk mode hands product and media writes to Magento's asynchronous bulk API.
The asset only succeeds in the sense you want if a consumer actually runs
the operations while the asset waits:

1. On the Magento host, keep `bin/magento queue:consumers:start
   async.operations.all` running (several processes for throughput), either
   under your process supervisor or through `cron_consumers_runner`.
2. Create any store view the run writes to before starting the consumers.
3. Indexers in `schedule` mode and cron running.
4. On MariaDB, set the session isolation to `READ COMMITTED` (see
   [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution)).

While the asset runs, it submits 200 operations at a time and polls
`GET bulk/{uuid}/detailed-status` every 2 seconds for up to 600 seconds per
bulk. These limits are fixed when you go through an importer.

`pending` never counts as success, but it is not counted as failed by the
ratio either. The example's `finish()` turns any pending row into a
`dagster.Failure` with the counts as metadata, so a missing consumer cannot
produce a green asset. Drop `fail_on_pending` if you prefer to accept pending
rows and let the next run's diff re-send what did not land.

Dagster's own run timeout, if you set one, must leave room for the waits: in
the worst case each 200-operation chunk can wait 600 s, plus one resubmission
of retriable operations.

## Running from a sensor or schedule

Nothing in the library is tied to a trigger. A schedule that materializes
`prices` every hour is cheap on reruns: the price snapshot reads current
values first, and rows that already match are reported as
`skipped_unchanged` without a write.

## Gotchas

- Do not share one `MagentoResource` configuration with `store_view` set to a
  store code for catalog imports; use `all` so global saves land in the
  global scope. See [Getting-Started](Getting-Started).
- `use_bridge="require"` fails the asset before anything is written only in
  `import_products` and `import_categories`, the two importers that name a
  bridge capability. `import_attributes`, `import_attribute_sets` and
  `import_stocks` accept it but name none, and `import_prices`, `import_sources`,
  `import_source_items`, `import_media` and `import_stock_source_links` ignore it.
  Checked by materializing the `prices` asset with `require` against a store that
  answers 404 to the capability probe: it succeeded and never probed.
- Validation failures from file mapping are your choice to handle (`parse()`
  raises); validation failures inside an importer become `failed` rows.

## See also

- [Home](Home)
- [Getting-Started](Getting-Started)
- [Catalog-Importers](Catalog-Importers)
- [File-Formats](File-Formats)
- [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution)
- [Results-and-Errors](Results-and-Errors)
- [Optional-Bridge](Optional-Bridge)
- [Troubleshooting](Troubleshooting)
