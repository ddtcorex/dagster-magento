# Changelog

All notable changes to this project are documented in this file. The format
follows Keep a Changelog and this project adheres to Semantic Versioning.

## [Unreleased]

### Changed

- The documentation moved to the GitHub wiki. Its source is `docs/` (flat markdown,
  one file per page), published by `.github/workflows/sync-wiki.yml` when it lands
  on `master`, checked by `scripts/check-docs.sh` and `tests/test_docs.py`. The
  README is now short: what it is, install, a quick start and links.
- The compatibility results table is generated into `docs/Compatibility.md`
  instead of the README (`scripts/compat-matrix.sh --write`).

### Fixed

- `compat_record.py table` no longer rewrites a page from a marker quoted inside a
  sentence: a marker only counts when it stands alone on its line.
- The usage text of `scripts/sandbox.sh` lists `bridge`, `bridge-off` and
  `deploy-mode`.

## [0.4.0] - 2026-10-02

### Changed

- Category paths and attribute set names now match existing ones without
  regard to case, with the bridge on or off. Magento's own category
  processor (which the bridge reuses) compares names that way, and Magento
  refuses to create an attribute set whose name differs from an existing one
  only by case, so the old exact match made the same file import differently
  with and without the bridge and tried to create sets that cannot exist.
  Behaviour change: on the native path two sibling categories that differ only
  by case now collapse to the first one in tree order, with a warning (the bridge
  leaves that choice to Magento's processor). New categories keep the caller's
  spelling, and names are lower-cased, not case-folded, like the bridge.

### Added

- `scripts/compat-matrix.sh`: resets the sandbox to each supported Magento
  line (2.4.6-p15, 2.4.7-p10, 2.4.8-p5, 2.4.9), runs the live suite, and writes
  one JSON record per version under `compat/results/`; the README
  compatibility table is generated from them. It restores govard's shared
  proxy when another session removes it mid run and retries that version once.
- `scripts/sandbox.sh deploy-mode <developer|production>`.
- Live probes that record real Magento response shapes as scrubbed fixtures
  (`tests/fixtures/magento/`): a bulk rejected whole when one item is
  malformed, a per operation consumer failure, price storage failed items,
  and production mode error bodies. Hermetic tests read the fixtures.
- `PriceRow` rejects a special price range whose end precedes its start;
  Magento stores such a range and reports success.

### Fixed

- Failed price storage items fill their named placeholders ("Invalid
  attribute Price = -5.") instead of leaving `%fieldName` in every row error,
  and Magento's `trace` key is dropped from the response body copied into
  a row message.
- `scripts/sandbox.sh reset` no longer loses the bridge checkout when
  provisioning fails half way: the stash lives at a fixed path and is kept
  until it has been restored.
- A bridge checkout's `vendor/` and PHPStan cache no longer break
  `setup:di:compile`, which blocked production mode.

### Verified

- Live suite 15 of 15 on 2.4.6-p15, 2.4.8-p5 and 2.4.9 on the final code. The
  bulk catalog test failed intermittently on 2.4.6-p15 (twice in one run) and
  once on 2.4.8-p5 before passing on reruns, unexplained; 2.4.9 never failed.
  2.4.7 is not verified because it cannot be installed through govard. See
  "Compatibility" in the README for the exact patches, PHP and database
  versions.

## [0.3.1] - 2026-09-30

### Fixed

- Updating an existing product with a partial row no longer resets its type,
  attribute set and websites. `ProductRow` defaults those to `simple`,
  `Default` and `base`, and the writer sent them on every update; now an
  update sends them, and the diff compares them, only when the row set them
  explicitly. Creation keeps the defaults.
- Sync mode now honours operation phases like bulk mode does, so a grouped or
  bundle parent listed before its children is saved after them instead of
  failing with "The Product with the ... SKU doesn't exist".
- The bridge product snapshot works. It sent `extension_attributes` to the
  module as an attribute code, the module answered 400, and the library fell
  back to REST silently even with `use_bridge="require"`. It now sends only
  attribute codes (including the custom attributes the rows use), reads
  website ids and category links from REST, raises `MagentoImportError` on a
  bridge failure in `require` mode, and falls back with a warning in `auto`.
- A rejected list item that names no row, such as a negative price answered
  with `{"fieldName": "Price", "fieldValue": -5}`, is no longer reported as
  succeeded: it fails every row of its request. Items are matched only on
  parameters holding a SKU, never on price or quantity values, and a failure
  naming a store no longer fails the same SKU's other store operation.
- An HTTP error when submitting one bulk chunk no longer aborts the import:
  that chunk's rows fail and the run continues. A partial rejection that
  still returns a `bulk_uuid` is polled for its accepted operations.
- A failing bridge category upsert (an HTTP error, or an answer missing a
  requested path) no longer crashes `import_categories`: `auto` falls back to
  the native parent-first creation, `require` raises `MagentoImportError`.
- POST requests are no longer retried on 502/503/504, where a gateway error
  can hide a committed write and a retry would duplicate a category or
  option, or run a bulk twice. POST retries only on 429; GET, PUT and DELETE
  keep the full retry set.

## [0.3.0] - 2026-09-29

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
