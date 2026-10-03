# Troubleshooting

Symptoms seen when running dagster-magento against a real Magento, with the cause and the fix. Most bulk mode problems are store configuration (no consumer, MariaDB isolation, a stale consumer), not library bugs, and the library is deliberately pessimistic: a row it cannot prove succeeded is reported `pending` or `failed`, never `succeeded`. Every entry names where the behaviour is documented or implemented, so you can check it against the version you run. The last section covers the development sandbox.

Quick index:

| Symptom | Section |
| --- | --- |
| Rows reported `pending`, warning about `async.operations.all` | [Operations stay pending](#operations-stay-pending) |
| Bulk loses some operations although consumers run | [Bulk drops operations on MariaDB (1020)](#bulk-drops-operations-on-mariadb-1020) |
| A bulk stays open, a consumer is gone | [A consumer died](#a-consumer-died) |
| "The store that was requested wasn't found" in bulk | [Store view created after consumers started](#store-view-created-after-consumers-started) |
| `MagentoAuthError` | [MagentoAuthError](#magentoautherror) |
| Many token refreshes in the log | [401 loops](#401-loops) |
| "The Product with ... doesn't exist" | [Products rejected with "doesn't exist"](#products-rejected-with-doesnt-exist) |
| "The product can't be saved." | [The product can't be saved.](#the-product-cant-be-saved) |
| Category rows fail on `default_sort_by` | [default_sort_by on 2.4.6](#default_sort_by-on-246) |
| Every row of a price request failed with one message | [Prices failed for every row in a request](#prices-failed-for-every-row-in-a-request) |
| A product is rewritten on every run | [Diff says changed every run](#diff-says-changed-every-run) |
| Categories differing only by case | [Category duplicates and the case rule](#category-duplicates-and-the-case-rule) |
| Bridge 404, `use_bridge="require"` errors | [Bridge 404 and require errors](#bridge-404-and-require-errors) |
| Storefront still shows the old price | [FPC does not show the new price](#fpc-does-not-show-the-new-price) |

## Store and library

### Operations stay pending

**Symptom.** `mode="bulk"` returns `pending > 0`, and the log says `N row(s) on <endpoint> still pending after 600s - check that the 'async.operations.all' message queue consumer is running`. Row errors carry `status: "pending"` and `operation still open at the bulk wait timeout`.

**Cause.** No consumer processed the operations before the wait timeout, so they stayed at Magento status 4 (open). `pending` is never counted as success. Other causes with the same symptom: the MariaDB race and a dead consumer below.

**Fix.**

- Start consumers on the store, ideally several processes:

  ```bash
  bin/magento queue:consumers:start async.operations.all
  ```

  or configure `multiple_processes` under `cron_consumers_runner` in `app/etc/env.php` (the sandbox uses 4 processes with `cron_run => false`).
- If the consumers are simply slow, the default wait is 600 s (`bulk_timeout_s` on `dagster_magento.executor.execute`; the importers use the default).
- `fail_on_error_ratio` counts `pending` in the denominator, `failed / (succeeded + failed + pending)`.

Documented in: [Sync and Bulk Execution](Sync-and-Bulk-Execution), `dagster_magento/executor.py` (`_execute_bulk`), CHANGELOG 0.2.0.

### Bulk drops operations on MariaDB (1020)

**Symptom.** Consumers are running, yet some operations stay open forever with `started_at` NULL and the rows are reported `pending`. `var/log/system.log` has one line per lost operation like `Message has been rejected: ... 1020 ...`.

**Cause.** Magento publishes an async bulk on the broker before it commits the `magento_operation` rows (`MassSchedule::publishMass`, then `SaveMultipleOperations::execute`). A consumer can reach its row while the insert is uncommitted; under MariaDB's default REPEATABLE READ it fails with `SQLSTATE[HY000]: General error: 1020 Record has changed since last read in table 'magento_operation'`, and `MassConsumerEnvelopeCallback::execute` rejects the message without requeue. No client can recover the operation. Measured on 2.4.9 with MariaDB 11.8 and four consumers: 30 of 88 published operations lost.

**Fix.** Set READ COMMITTED for Magento's database sessions in `app/etc/env.php` (Magento passes `driver_options` to PDO; `1002` is `PDO::MYSQL_ATTR_INIT_COMMAND`):

```php
$config['db']['connection']['default']['driver_options'][1002] =
    'SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED';
```

With it, none were lost in a different measurement: three runs of 20 operations with the insert deliberately held open for 400 ms, then the full catalog import in bulk mode (`scripts/sandbox.sh`, `write_db_isolation_config`). Rerun the import for the pending rows afterwards; the diff skips what already landed.

Documented in: [Sync and Bulk Execution](Sync-and-Bulk-Execution), `scripts/sandbox.sh` (`write_db_isolation_config`), docstring of `tests/live/test_catalog_e2e.py::test_same_catalog_imports_in_bulk_mode`, CHANGELOG 0.3.0.

### A consumer died

**Symptom.** A bulk stays open and one or more `async.operations.all` processes are no longer running.

**Cause.** On Magento 2.4.6 an async bulk operation for a product without a SKU raises a `TypeError` the consumer does not catch: the process exits and the operation stays open (observed on 2.4.6-p15; 2.4.9 answers a normal failure). The library never sends such an item because `sku` is required by the row model, but a hand written bulk through `upload_rows_async` or `post` can.

**Fix.** Check that all `async.operations.all` consumers are still running and restart them; fix the bulk payload so every product item has a SKU.

Documented in: [Compatibility](Compatibility), comment in `tests/live/test_probes.py`.

### Store view created after consumers started

**Symptom.** A store scoped bulk (`store_values`) fails with "The store that was requested wasn't found" although the store view exists.

**Cause.** A running consumer caches the store list when it starts.

**Fix.** Create store views before starting the consumers, or restart the consumers after creating one. The live test helper does exactly that.

Documented in: [Sync and Bulk Execution](Sync-and-Bulk-Execution), `tests/live/live_support.py::prepare_sandbox`.

### MagentoAuthError

**Symptom.** The run aborts with `MagentoAuthError: Failed to fetch Magento admin token (store_view=...): <HTTP error>`.

**Cause.** `POST integration/admin/token` failed: wrong username or password, a locked admin account, or a store that blocks the admin token (Magento 2.4.9 enables two factor auth by default, which blocks the REST admin token until a provider is configured). The error is deliberately not an `HTTPError` subclass, so it is never absorbed as per row failures; one bad credential must not become hundreds of thousands of failed rows.

**Fix.** Check the credentials (`MagentoResource.username`, `password`), unlock the account, and make sure the admin token endpoint works for that user. The sandbox disables `Magento_TwoFactorAuth` and `Magento_AdminAdobeImsTwoFactorAuth` for this reason; on a real store, decide how the import user obtains an admin token under your 2FA policy.

Documented in: `dagster_magento/resource.py` (`MagentoAuthError`, `_fetch_token`), `AGENTS.md` "Auth failures vs data failures", `scripts/sandbox.sh` (`cmd_up`).

### 401 loops

**Symptom.** The log repeats `Magento token expired (401 on <endpoint>), refreshing and retrying`, once per row or chunk, and those rows still fail.

**Cause.** `_request` refreshes the token once on any 401 and retries that request. A token that really expired recovers. A 401 that is permanent, such as an endpoint the admin user's ACL role denies, refetches the token once per call and still fails. This is a known open issue in `AGENTS.md`.

**Fix.** Give the admin user's role access to every resource the import calls. If the refetch itself fails, you get `MagentoAuthError` instead.

Documented in: `dagster_magento/resource.py` (`_request`), `AGENTS.md` "Auth failures vs data failures".

### Products rejected with "doesn't exist"

**Symptom.** A grouped or bundle parent fails with `The Product with the ... SKU doesn't exist` (or similar) while its children import fine.

**Cause.** The parent carries its links inline (`product_links` for grouped, `bundle_product_options` for bundle), and Magento validates the referenced SKUs while saving the parent. If the parent is saved first it fails. In bulk mode concurrent consumers could run the parent first (seen live on 2.4.6, hidden by timing on 2.4.9); in sync mode a parent listed before its children did the same before 0.3.1.

**Fix.** Upgrade to 0.3.1 or newer: such parents are planned into a later phase that both modes run only after every earlier phase has completed. If it still happens, the child is really missing or failed earlier in the run: check that row's own error. In a hand written bulk, submit children and parents as separate bulks.

Documented in: CHANGELOG 0.3.0 and 0.3.1, [Catalog Importers](Catalog-Importers), `dagster_magento/writers/products.py` (`_save_phase`), `dagster_magento/executor.py` (`_execute_sync`).

### The product can't be saved.

**Symptom.** A row fails with Magento's generic `The product can't be saved.`.

**Cause.** Magento gives no detail with this message. Two documented occurrences: configurable option and child link operations racing their products in a bulk (fixed in 0.3.0 by the same phase ordering as above), and one media upload during the unexplained intermittent 2.4.6-p15 matrix failure on 2026-10-02.

**Fix.** Make sure you run 0.3.0 or newer. If it remains, rerun the import (the diff skips what landed) and look for the real reason in Magento's own logs: the REST answer carries no detail, and production mode masks error detail further (see the captured bodies in `tests/fixtures/magento/production_error_bodies.json`).

Documented in: CHANGELOG 0.3.0, [Compatibility](Compatibility).

### default_sort_by on 2.4.6

**Symptom.** On Magento 2.4.6, category rows carrying `default_sort_by` log `default_sort_by is not writable through this store's REST API (Magento 2.4.6 types it as string[] and stores nothing for it); retrying N row(s) without it: ...`, and the value stays unset in Magento.

**Cause.** 2.4.6 types the attribute as `string[]` and rejects the string shape with 400; it also stores nothing for the array shape. 2.4.9 types it as `string`. No payload works on 2.4.6.

**Fix.** None needed for the import: since 0.3.0 rows failing with exactly that type error are re-planned without the key and executed once more, and the rest of the row is written. An unrelated type error is not retried and still fails its row.

Documented in: `dagster_magento/importers.py` (`_retry_without_default_sort_by`), [Compatibility](Compatibility), CHANGELOG 0.3.0.

### Prices failed for every row in a request

**Symptom.** Every row of one price (or source item) request fails with `rejected item(s) in this request name no row, failing every row: Invalid attribute Price = -5.` or similar.

**Cause.** List endpoints answer 200 with the items they rejected. An item is attributed to the row whose SKU it names (and whose store or source, when it names one). Magento answers a negative price with only `{"fieldName": "Price", "fieldValue": -5}`, which names no row, so the library cannot tell which row it belongs to and fails all of them rather than report a real rejection as success.

**Fix.** Find the bad value in that chunk (the message shows it, placeholders are filled since 0.4.0) and fix the source data. Items naming a SKU fail only their own row.

Documented in: [Results and Errors](Results-and-Errors), `dagster_magento/executor.py`, `tests/test_captured_magento_bodies.py`, CHANGELOG 0.3.1 and 0.4.0.

### Diff says changed every run

**Symptom.** A rerun reports some products as succeeded instead of `skipped_unchanged`, every time.

**Cause.** The diff answers "unchanged" only when it can prove it; any doubt means "changed". A product row carrying any part no snapshot reads back is always rewritten: `store_values`, `variations`, `configurable_attributes`, `bundle_options`, `grouped_links`, `downloadable_links`, `downloadable_samples`. Other doubts also count as changed: an option label, attribute set, website or category that cannot be resolved, missing `extension_attributes`, or a `url_key` with non ASCII characters (Magento's transliteration is not mirrored). Some importers take no snapshot at all: attributes (always PUT), attribute set assignments, categories (idempotent by path) and sources. Images are compared by `import_media`, not the product diff.

**Fix.** Expected behaviour; the live test asserts exactly this split. Pass `diff=False` to rewrite everything deliberately.

Documented in: `dagster_magento/diff.py` (`_UNSNAPSHOTTED_PRODUCT_PARTS`, `product_matches_snapshot`, `_url_key`), `tests/live/test_catalog_e2e.py` (`NOT_DIFFED`, `test_second_run_is_all_skipped`), [Catalog Importers](Catalog-Importers).

### Category duplicates and the case rule

**Symptom.** `men/tops` lands in an existing `Men/Tops` instead of creating a new node; or the log warns `category '<path>' differs only by case from '<path>'; keeping id X and ignoring id Y`.

**Cause.** Since 0.4.0 category paths and attribute set names match existing ones without regard to case (lower cased like Magento's own category processor, not case folded, so `ß` does not match `ss`), with the bridge on or off. Magento itself allows sibling categories that differ only by case; on the native path the library keeps the first one in tree order and warns. With the bridge, Magento's processor picks one and logs nothing. Attribute set names also follow the database collation, which ignores accents by default.

**Fix.** Avoid sibling categories that differ only by case if the target matters; merge or rename them in Magento. New nodes keep the caller's spelling.

Documented in: [Catalog Importers](Catalog-Importers), CHANGELOG 0.4.0, `dagster_magento/resolvers.py`, `tests/live/test_category_case.py`.

### Bridge 404 and require errors

**Symptom.** One of:

- warning `bridge probe failed (...); continuing without the bridge`;
- `MagentoImportError: the bridge is required for this import but the store does not offer: <capabilities>`;
- `MagentoImportError: bridge product snapshot failed: ...` or `bridge category upsert failed: ...`.

**Cause.** The capability probe `GET /V1/dagster-bridge/capabilities` runs once per run. A 404 (module not installed) or any probe failure means no capability, which is not an error in `auto`. With `use_bridge="require"` a missing capability raises before anything runs, and a capability that fails during the run raises instead of falling back (`auto` falls back with a warning). `MagentoAuthError` is never swallowed by the probe.

**Fix.** Install and enable `DDTCoreX_DagsterBridge` on the store (`bin/magento module:enable DDTCoreX_DagsterBridge`, `setup:upgrade`, `cache:flush`), or use `use_bridge="auto"` or `"never"`. An older module advertises fewer capabilities; `require` needs every capability the path uses.

Documented in: `dagster_magento/bridge.py`, `dagster_magento/importers.py` (`_bridge`), `dagster_magento/diff.py`, `dagster_magento/resolvers.py`, the [Optional Bridge](Optional-Bridge) page, CHANGELOG 0.3.1. See [Optional-Bridge](Optional-Bridge).

### FPC does not show the new price

**Symptom.** `import_prices` reports success, but the full page cached product page still shows the old price.

**Cause.** With indexers on schedule, the price change reaches the storefront only when cron runs the mview price reindex, which invalidates the cached page. `cron:run` executes only jobs whose scheduled minute has come, so it can take one more cron minute.

**Fix.** Make sure cron runs on the store and indexers are in `schedule` mode ([Sync and Bulk Execution](Sync-and-Bulk-Execution) lists both as requirements of bulk mode). The live test runs cron up to four times, 30 s apart, before declaring the path broken.

Documented in: `tests/live/test_price_storefront.py`, [Sync and Bulk Execution](Sync-and-Bulk-Execution).

## Development sandbox

### The sandbox domain stopped resolving

**Symptom.** `dagster-magento-sandbox.test` no longer resolves, live tests fail with connection errors, or a compat record says `govard's shared proxy was removed during the run and had to be restored`.

**Cause.** The domain is served by govard's shared proxy, which other govard sessions on the same machine can remove or recreate; a torn down environment can also lose its proxy registration.

**Fix.**

```bash
govard svc up --no-trust                                  # restore govard's global services
(cd sandbox/dagster-magento-sandbox && govard up)         # re-register the domain
```

`compat-matrix.sh` does the first automatically (and watches every 15 s during a run, retrying a version once if the proxy was lost); `live_support.wait_until_ready` does the second once on a `ConnectionError`. Keep other govard sessions off the machine during the matrix.

Documented in: `scripts/compat-matrix.sh`, `tests/live/live_support.py`.

### govard bootstrap refused by Composer security blocking

**Symptom.** `scripts/sandbox.sh up` or `reset --version ...` fails, the log contains `affected by security advisories`, and the matrix records the version as `not provisioned`.

**Cause.** Composer refuses a dependency that carries security advisories, and govard's Magento bootstrap does not pass `--no-security-blocking`. This is why 2.4.7 is not verified, and why the matrix uses the newest patch of each line rather than base releases.

**Fix.** Use the newest patch of the line. There is no sandbox side workaround in the scripts.

Documented in: `scripts/compat-matrix.sh`, [Compatibility](Compatibility). See [Compatibility](Compatibility).

### Production mode compile fails on the bridge checkout

**Symptom.** `setup:di:compile` fails with `Phar wrapper is not registered` out of PHPStan's cache.

**Cause.** Magento scans every PHP file under `app/code`, including the bridge checkout's `vendor/` and analysis caches.

**Fix.** Switch modes with `scripts/sandbox.sh deploy-mode production`, which parks those leftovers during the compile (fixed in 0.4.0).

Documented in: `scripts/sandbox.sh`, CHANGELOG 0.4.0.

## See also

- [Home](Home)
- [Getting-Started](Getting-Started)
- [MagentoResource](MagentoResource)
- [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution)
- [Price-Import](Price-Import)
- [Results-and-Errors](Results-and-Errors)
- [Catalog-Importers](Catalog-Importers)
- [Optional-Bridge](Optional-Bridge)
- [Compatibility](Compatibility)
- [Sandbox-and-Testing](Sandbox-and-Testing)
