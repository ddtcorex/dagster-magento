# Sandbox and Testing

The test suite has three layers: a hermetic default run that needs no network and no Magento, a `samples` layer that downloads public sample import files, and a `live` layer that drives a real Magento through a disposable govard sandbox. This page shows how to run each layer, how `tests/` is laid out, how live probes record real Magento responses as fixtures the hermetic tests read, and what every `scripts/sandbox.sh` subcommand does. Only the hermetic layer is expected from every contributor; the live layer is what the [Compatibility](Compatibility) matrix runs.

## The pyramid

| Layer | Marker | Needs | Default run |
| --- | --- | --- | --- |
| Hermetic | none | nothing: `requests_mock` and stubs only | yes |
| Samples | `samples` | network: downloads the public sample import files | no |
| Live | `live` | a real Magento, `MAGENTO_*` env vars, usually the local sandbox | no |

`pyproject.toml` declares both markers and sets `addopts = "-m 'not live and not samples'"`, so a plain `pytest` never touches the network. CI runs only that default (see [Compatibility](Compatibility)).

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/pytest -q                      # hermetic
.venv/bin/pytest -m samples -q           # samples, needs network
.venv/bin/pytest tests/test_bulk.py -q   # one file
```

If `python3 -m venv` fails with `ensurepip is not available`, install the system `-venv` package first (for example `python3.14-venv` on Debian), as `AGENTS.md` notes.

## Layout of `tests/`

The convention is one test module per source module.

| Path | Content |
| --- | --- |
| `tests/test_*.py` | Hermetic tests per module: `test_resource.py`, `test_resource_http.py`, `test_bulk.py`, `test_upload.py`, `test_search.py`, `test_executor.py`, `test_diff.py`, `test_resolvers.py`, `test_importers.py`, `test_models.py`, `test_operation.py`, `test_bridge.py` |
| `tests/writers/` | One module per writer (attributes, attribute sets, categories, inventory, media, pricing, products, product types) |
| `tests/formats/` | Readers, column parsers, catalog mappers; `test_samples.py` is the `samples` layer |
| `tests/test_captured_magento_bodies.py`, `tests/test_production_bodies.py` | Executor behaviour pinned to bodies captured from a real Magento (see Fixtures below) |
| `tests/test_compat_record.py`, `tests/test_compat_matrix_script.py`, `tests/test_compat_facts.py` | The compatibility tooling, hermetically |
| `tests/test_sandbox_stash.py`, `tests/test_live_support.py`, `tests/test_fixture_capture.py` | Sandbox and live helpers that can be proven without a sandbox |
| `tests/test_docs.py` | Wiki sources: links resolve, every page is in the sidebar and on Home, the generated table has its markers, no em or en dashes |
| `tests/samples.py` | Downloader for the sample files |
| `tests/live/` | The live suite and its helpers |
| `tests/fixtures/magento/` | Scrubbed real Magento responses |
| `tests/.samples/` | Download cache, gitignored |

## Samples: `tests/samples.py`

`fetch(name)` downloads one file from the public `firebearstudio/magento2-import-export-sample-files` repository at a pinned commit (`7fec061a837288718c1d84455ae8f1a0acde00c4`) into `tests/.samples/<name>`, or returns the cached copy. The download is atomic (temporary file plus rename). Files are never vendored into the repo. Known names: `product_all_types.csv`, `products_all_types.xlsx`, `categories.csv`, `attributes.csv`, `advanced_pricing.csv`, `msi_source_qty.csv`, `catalog_product.csv`. The live catalog test uses the same downloader.

## Live tests

### What runs

| Module | Tests |
| --- | --- |
| `test_catalog_e2e.py` | Full sample catalog import in sync mode, parametrized `use_bridge` = `never`, `require`; a second run in which every row the diff can prove unchanged is skipped (importers without a diff, such as attributes, attribute sets, categories and sources, write again); the same catalog in bulk mode after a sandbox reset, for both `use_bridge` values |
| `test_category_case.py` | Category names match without regard to case with the bridge off and on; attribute set names likewise |
| `test_price_storefront.py` | A price change reaches a full page cached storefront after cron; a bulk store view update keeps its store scope; special price dates round trip |
| `test_probes.py` | Four probes that record Magento response shapes (see Fixtures) |

That is 15 test cases, the number the compatibility table reports.

### Environment

`tests/live/conftest.py` skips the whole directory when `MAGENTO_BASE_URL` is unset. Credentials come only from the environment:

| Variable | Purpose |
| --- | --- |
| `MAGENTO_BASE_URL` | Store base URL |
| `MAGENTO_ADMIN_USERNAME`, `MAGENTO_ADMIN_PASSWORD` | Admin credentials for the token |
| `MAGENTO_STORE_VIEW` | Default store code, `all` when unset |
| `MAGENTO_CA_BUNDLE` | Test only: an extra CA (the sandbox's local development CA). The session fixture concatenates certifi plus this file and points requests at it through `REQUESTS_CA_BUNDLE`. Verification stays on and library defaults are untouched. |

A plain script outside pytest has no conftest to build that bundle: concatenate certifi and the local CA into one file and set `REQUESTS_CA_BUNDLE` yourself (`AGENTS.md`, Testing).

The live suite is written for the local sandbox: any test that drives cron, `bin/magento`, a reset or `prepare_sandbox()` through govard skips when the sandbox project (`sandbox/dagster-magento-sandbox/`) is absent (`live_support.require_sandbox()` and the module level `skipif` marks).

### Running against the sandbox

```bash
scripts/sandbox.sh up && scripts/sandbox.sh consumers
eval "$(scripts/sandbox.sh env)"
MAGENTO_CA_BUNDLE=<govard local root CA> .venv/bin/pytest -m live -q
```

The repo docs give the CA as `~/.govard/ssl/root.crt`, which is also the default `compat-matrix.sh` uses.

One live test or one parametrized case:

```bash
.venv/bin/pytest -m live -q tests/live/test_price_storefront.py::test_special_price_dates_round_trip
.venv/bin/pytest -m live -q "tests/live/test_catalog_e2e.py::test_full_sample_catalog_imports_in_sync_mode[never]"
```

Live helpers worth knowing (`tests/live/live_support.py`):

- `prepare_sandbox()` is idempotent setup: whitelists the sample downloadable domain, sets the price scope to website (`catalog/price/scope 1`), creates store view `fr`, flushes cache, reindexes `catalogsearch_fulltext` so concurrent first saves do not race index creation, and starts consumers. If it had to create the store view while consumers were running, it stops and restarts them, because a running consumer caches the store list.
- `reload_env()` re-reads `MAGENTO_*` from `scripts/sandbox.sh env`. The bulk catalog test resets the sandbox, which rotates the admin password.
- `wait_until_ready()` polls the admin token endpoint for up to 180 s (a reset or cache flush briefly answers 503). On a `ConnectionError` it runs `govard up` once to re-register the domain with the proxy.

### Timings stated in code

| What | Bound | Where |
| --- | --- | --- |
| Bulk wait before rows are reported `pending` | 600 s default | `dagster_magento/executor.py`, `bulk.py` |
| Probe bulk poll | 180 s, every 3 s | `tests/live/test_probes.py` |
| Cron until the storefront shows the new price | 4 cron runs, 30 s apart | `tests/live/test_price_storefront.py` |
| Sandbox ready after reset | 180 s | `live_support.wait_until_ready` |
| Sandbox reset and deploy mode switch, from tests | 3600 s subprocess timeout | `test_catalog_e2e.py`, `test_probes.py` |
| Switch to production mode | "several minutes" | `scripts/sandbox.sh` `cmd_deploy_mode` |
| Elasticsearch first boot on 2.4.6 | up to a couple of minutes, waited for at most 3 minutes | `scripts/sandbox.sh` `use_supported_search_backend` |
| Full compatibility matrix | about an hour and a half | `AGENTS.md`, Release |

## Fixtures from live probes

`tests/live/test_probes.py` records real Magento responses so hermetic tests read captured bodies instead of hand written payloads. Each probe also asserts the keys the library reads, so another Magento line that answers differently fails loudly in the matrix.

| Probe | Fixture | What it records |
| --- | --- | --- |
| `test_probe_bulk_rejects_the_whole_submission_when_an_item_is_malformed` | `bulk_rejected_submission.json` | A bulk with one malformed item is rejected whole with 400: no `bulk_uuid`, no per item list, nothing saved |
| `test_probe_bulk_reports_a_consumer_failure_per_operation` | `bulk_consumer_failure.json` | A consumer failure (unknown attribute set) reported per operation id: statuses 1, 3, 1 |
| `test_probe_price_storage_failed_items` | `price_failed_items.json` | Failed items for a negative base price and an unknown SKU, and the empty answer for an inverted special price range |
| `test_probe_production_mode_error_bodies` | `production_error_bodies.json` | What a rejected library write and a rejected bridge write still say in production mode; always switches back to developer mode |

Bodies are written only when `DAGSTER_CAPTURE_FIXTURES=1`; a normal live run only asserts. `tests/live/fixture_capture.py` scrubs before anything reaches disk: the store host becomes `magento.example`, bearer tokens and 32 character tokens become `<token>`, and `trace` keys are dropped.

```bash
DAGSTER_CAPTURE_FIXTURES=1 MAGENTO_CA_BUNDLE=<govard local root CA> \
  .venv/bin/pytest -m live -q tests/live/test_probes.py
```

The captured fixtures were recorded on Magento 2.4.9 (docstrings of `test_captured_magento_bodies.py` and `test_production_bodies.py`). Review the diff of `tests/fixtures/magento/` before committing a recapture.

## The govard sandbox: `scripts/sandbox.sh`

A disposable Magento built through [govard](https://github.com/ddtcorex/govard). Every container command goes through govard (`govard tool magento ...` or `govard shell -c '...'`), never a raw `docker exec`. The domain is `https://dagster-magento-sandbox.test`, the admin user `dagster`, the default version 2.4.9.

| Path | Content |
| --- | --- |
| `sandbox/` | Gitignored root of everything below |
| `sandbox/dagster-magento-sandbox/` | The Magento project |
| `sandbox/.admin-password` | Generated admin password, mode 600, never committed |
| `sandbox/.bridge-stash/` | Where `reset` keeps the bridge checkout while the project is rebuilt |
| `sandbox/compat-logs/` | Full pytest logs kept by the compatibility matrix |

### Subcommands

| Subcommand | What it does |
| --- | --- |
| `up [--version V]` | Refuses if the project directory is not empty. Runs `govard bootstrap --framework magento2 --fresh --framework-version V --yes`; on 2.4.6 switches search to Elasticsearch 7.17.28; runs `setup:upgrade` (so `async.operations.all` exists); sets indexers to `schedule`; enables `full_page` cache; sets `dev/grid/async_indexing 1`; disables `Magento_TwoFactorAuth` and `Magento_AdminAdobeImsTwoFactorAuth` (2FA blocks the REST admin token); creates admin `dagster` with a random password; writes `cron_consumers_runner` and READ COMMITTED isolation into `app/etc/env.php`; enables the bridge module if its checkout is present. |
| `down` | `govard down`: stops containers, keeps volumes. |
| `reset [--version V]` | Defaults to the `framework_version` of the existing `.govard.yml`, so a plain reset rebuilds the same version. Stashes the bridge checkout, runs `govard down -v`, deletes the project and the password file, runs `up`, then restores the bridge checkout and enables it. |
| `consumers` | Starts four `bin/magento queue:consumers:start async.operations.all` processes in the background, logging to `var/log/dagster-consumer-<n>.log` inside the project. |
| `env` | Prints `export` lines for `MAGENTO_BASE_URL`, `MAGENTO_ADMIN_USERNAME`, `MAGENTO_ADMIN_PASSWORD` and `MAGENTO_STORE_VIEW=all`. Use with `eval`. |
| `cron-run` | Runs `bin/magento cron:run` twice. |
| `deploy-mode developer\|production` | Sets the deploy mode, flushes cache, prints `deploy mode: <mode>`. Production compiles and deploys static content; the bridge checkout's dev leftovers are parked during the compile and put back on exit. |
| `bridge` | Enables `DDTCoreX_DagsterBridge` (`module:enable`, `setup:upgrade`, `cache:flush`) if `app/code/DDTCoreX/DagsterBridge/registration.php` exists; otherwise logs that it runs without it. |
| `bridge-off` | Disables the module, `setup:upgrade`, `cache:flush`, to prove the native fallback. |

### What `up` writes into `app/etc/env.php`

`cron_consumers_runner`:

```php
'cron_consumers_runner' => [
    'cron_run' => false,
    'max_messages' => 0,
    'consumers' => ['async.operations.all'],
    'multiple_processes' => ['async.operations.all' => 4],
],
```

and the MariaDB isolation fix the bulk path needs (`1002` is `PDO::MYSQL_ATTR_INIT_COMMAND`):

```php
$config['db']['connection']['default']['driver_options'][1002] =
    'SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED';
```

The script comment records the measurement: with the operation insert held open for a forced 400 ms on 2.4.9 and MariaDB 11.8, 4 of 20 operations were dropped before, 0 of 20 over three runs after. See [Troubleshooting](Troubleshooting).

### The bridge checkout and `.bridge-stash`

The optional bridge module is a separate git checkout that lives inside the gitignored project at `app/code/DDTCoreX/DagsterBridge`. A reset would delete it, so `reset` first copies `app/code/DDTCoreX` to `sandbox/.bridge-stash/` (without `DagsterBridge/vendor`, `.phpstan.cache`, `.phpunit.cache`, `.phpcs-cache`). The stash is at a fixed path and is removed only after a successful restore; if a reset dies half way (for example a Composer failure while provisioning), the next reset keeps the existing stash instead of overwriting it with nothing. `tests/test_sandbox_stash.py` pins this. If you find a leftover `sandbox/.bridge-stash/DDTCoreX`, it is the only copy of that checkout.

The dev leftovers matter because Magento scans every PHP file under `app/code`: `setup:di:compile` loads them and fails ("Phar wrapper is not registered" out of PHPStan's cache). That is why `deploy-mode production` parks them.

## See also

- [Home](Home)
- [Getting-Started](Getting-Started)
- [Architecture](Architecture)
- [Compatibility](Compatibility)
- [Contributing-and-Releases](Contributing-and-Releases)
- [Troubleshooting](Troubleshooting)
- [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution)
- [Optional-Bridge](Optional-Bridge)
