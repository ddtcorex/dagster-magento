# Changelog

All notable changes to this project are documented in this file. The format
follows Keep a Changelog and this project adheres to Semantic Versioning.

## [Unreleased]

### Added

- Optional client for the `DDTCoreX_DagsterBridge` Magento module
  (`BridgeClient`): one capability probe per run, the product index for
  existence, store-scoped attribute values with Magento's own store fallback,
  and the category upsert. Every importer takes `use_bridge`
  (`auto`/`never`/`require`), each capability falls back to the native REST path
  on its own, and the probe is best effort so an optional module can never fail
  an import.
- `scripts/bench_prices.py`: times the async bulk `products/bySku` price path
  against `import_prices` over the same catalog.

### Fixed

- Category imports no longer fail on Magento 2.4.6 over `default_sort_by`.
  2.4.6 types that attribute as `string[]` while 2.4.9 types it as `string`,
  so no single payload shape works on both - and 2.4.6 stores nothing for
  either shape, so there is nothing to negotiate. Rows failing with exactly
  that type error are now re-planned without the key and executed again,
  with a warning naming them, instead of failing the whole import over one
  attribute the store cannot persist through REST.

- Bulk imports no longer let a composite parent save race its children's
  saves. A grouped parent carries its links inline (`product_links`) and a
  bundle parent its selections (`bundle_product_options`), so Magento
  validates the referenced SKUs while saving the parent; submitting every
  save of one bulk together lets concurrent consumers run the parent first
  and fail it with 'The Product with ... doesn't exist' (seen live on 2.4.6,
  timing hid it on 2.4.9). Configurable option and child-link operations need
  the same ordering and failed with "The product can't be saved.". All of
  them now plan into a later bulk phase that the executor submits only after
  every earlier phase has completed.

- The live bulk test no longer loses operations on the sandbox. Magento
  publishes an async bulk on the broker before it commits the rows that bulk
  belongs to, so a consumer can reach its row while the insert is still
  uncommitted; on MariaDB under its default `REPEATABLE READ` that consumer
  fails with SQLSTATE 1020 ("Record has changed since last read") and
  Magento drops the message without requeue, leaving the operation open
  forever. `scripts/sandbox.sh` now sets the session transaction isolation
  to `READ COMMITTED`, and the README documents the setting for MariaDB
  deployments. No library code changed.

## [0.2.0] - 2026-09-28

Native catalog import. The v0.1.0 public API is unchanged and still works.

### Added

- Catalog import layer, one function per entity, each composing snapshot,
  diff, plan and execute: `import_attributes`, `import_attribute_sets`,
  `import_categories`, `import_products`, `import_prices`, `import_sources`,
  `import_stocks`, `import_stock_source_links`, `import_source_items`,
  `import_media`, plus `to_materialize_result`.
- Pydantic v2 row models (`ProductRow`, `CategoryRow`, `AttributeRow`,
  `AttributeSetRow`, `PriceRow`, `SourceItemRow`, `SourceRow`, `StockRow`,
  `StockSourceLinkRow`) and `validate_rows`, which turns a bad row into a
  `RowError` with its row reference instead of raising.
- Resolver cache: one `GET products/attributes` preload, HTML-entity aware
  option label matching, attribute set, website, store view and category
  path lookup, and parent-first creation of missing category nodes.
- Diff layer: snapshots products, prices, source items and media, and skips
  rows that already match (`skipped_unchanged`).
- Writers for attributes, attribute sets, categories, products of every
  type (configurable, bundle, grouped, downloadable), price storage, MSI
  inventory and media. Writers are pure: they return `Operation` values
  with an optional `BulkSpec` and never make an HTTP call.
- Executor with `sync` and `bulk` modes: 1000-row chunks for list
  endpoints, 200-row chunks for bulk, per-operation id mapping of the
  detailed status, one retry for status 2, and `pending` plus a consumer
  hint when the timeout expires with the queue still open.
- `MagentoResource`: retry with backoff on 429, 502, 503 and 504 honouring
  `Retry-After`, new `put` and `delete`, and a per-call `store_code`
  override for store-view writes.
- File adapters: `read_rows` for csv, json and xlsx, and mappers for the
  native import columns (`additional_attributes`,
  `configurable_variations`, `bundle_values`, `associated_skus`,
  `downloadable_links`, `categories`, image columns and the
  `advanced_pricing` tier columns).
- `xlsx` extra (openpyxl) and the `samples` and `live` pytest markers,
  both excluded from the default run.
- `scripts/sandbox.sh`: a disposable govard Magento 2.4.9 sandbox with
  indexers on schedule, full page cache, four `async.operations.all`
  consumers and `cron_consumers_runner` configured for bulk runs.
- Live end-to-end suite on that sandbox: full sample catalog import in sync
  mode, a rerun that is skipped by the diff, the same catalog in bulk mode
  after a reset, price to full-page-cached storefront after cron,
  store-scoped bulk update, and special price date round trip. The bulk
  test also documents a measured sandbox defect: Magento's consumer drops a
  variable subset of the published operations, and the library reports
  those rows `pending` instead of success (see
  `tests/live/test_catalog_e2e.py`).

### Changed

- `UploadResult` gained `pending` and `skipped_unchanged` (defaults keep
  v0.1.0 construction working), a `merge`, and error dicts carrying
  `row_ids`, `status`, `status_code` and `message`.
- `get`, `post` and `get_paginated` take a `store_code` keyword.
- HTTP retries: every 429, 502, 503 and 504 is retried up to three times
  with 0.5 s, 1 s and 2 s backoff plus jitter, and a 401 refresh is scoped
  to the request that saw it.

## [0.1.0] - 2026-08-08

- `MagentoResource` with admin token authentication, `get`,
  `get_paginated`, `post` and the chunked, catch-log-continue
  `upload_rows`.
- `UploadResult(succeeded, failed, errors)` and `MagentoAuthError`, which
  always aborts instead of being absorbed as a per-row failure.
- `password` is declared `repr=False` with `dagster__is_secret`, so it never
  appears in a `repr()` or in the Dagster launchpad.
