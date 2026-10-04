# Compatibility

dagster-magento targets Magento Open Source 2.4.6 and newer through the standard REST API, but a Magento line counts as supported only when there is a recorded run of the whole live suite against a fresh sandbox of that exact patch. This page explains how those runs are produced (`scripts/compat-matrix.sh` and `scripts/compat_record.py`), what the records under `compat/results/` contain, how to read the results table generated from them, where the evidence stops, and which version specific Magento behaviours the live runs uncovered. It also lists the Python, Dagster and pydantic versions the package declares and tests.

## The matrix at a glance

```
scripts/compat-matrix.sh [--versions 2.4.6-p15,2.4.7-p10,2.4.8-p5,2.4.9] [--write] [--dry-run]
```

| Flag | Effect |
| --- | --- |
| `--versions` | Comma separated list, run in the order given. Default `2.4.6-p15,2.4.7-p10,2.4.8-p5,2.4.9`. |
| `--write` | After the runs, regenerate the results table at the end of this page (`docs/Compatibility.md`) from `compat/results/`. |
| `--dry-run` | Print `would run <version>` per version (and `would write the results table` with `--write`) and touch nothing. |

Any other argument prints the usage and exits with status 2. The dry run and flag handling are pinned hermetically by `tests/test_compat_matrix_script.py`.

For each version the script:

1. Runs `scripts/sandbox.sh reset --version <version>`. If the reset fails, the version is recorded as `not provisioned`. When the reset log contains `affected by security advisories`, the reason is the fixed sentence "Composer security blocking refused a dependency of <version> and govard bootstrap cannot disable it"; otherwise it is `reset failed:` plus the last three log lines.
2. Loads the sandbox credentials with `eval "$(scripts/sandbox.sh env)"`.
3. Makes sure `dagster-magento-sandbox.test` resolves, restoring govard's shared services (`govard svc up --no-trust`) if it does not, and starts a watcher that checks every 15 seconds and restores the proxy again if another govard session removed it.
4. Runs `pytest -m live -q -p no:cacheprovider --junitxml=...` over `tests/` with a CA bundle built from certifi plus `MAGENTO_CA_BUNDLE`.
5. Copies the full pytest log to `sandbox/compat-logs/<version>-<date>.log` (gitignored, kept so a failure can be diagnosed later).
6. If the run failed and the proxy had been lost during it, resets and runs that version once more before recording a failure.
7. Re-reads the environment (a test inside the suite resets the sandbox, which rotates the admin password) and collects the sandbox facts through `tests/live/live_support.py::sandbox_facts()`.
8. Writes `compat/results/<version>-<YYYY-MM-DD>.json` through `scripts/compat_record.py record`.

No version stops the loop. The exit code is non-zero when any version was not verified (or the table write failed). If the last version in the list is not 2.4.9, the sandbox is reset back to 2.4.9 at the end.

The matrix owns the sandbox for about an hour and a half (stated in `AGENTS.md`, release step 2): run nothing else against the sandbox meanwhile and keep other govard sessions off the machine.

Why patches and not base releases: the script header notes that a base release such as 2.4.7 is refused by Composer's security blocking once advisories exist for it, so the newest patch of each line is also the one that installs. The patch list is updated by hand when a newer patch ships.

## The records: `compat/results/*.json`

`scripts/compat_record.py` has two subcommands:

```
python scripts/compat_record.py record --version V --date YYYY-MM-DD --status STATUS --out FILE [--junit FILE] [--facts FILE] [--reason TEXT]
python scripts/compat_record.py table --results compat/results --readme docs/Compatibility.md
```

`--status` is one of `verified`, `failed`, `not provisioned`. A record holds:

| Key | Content |
| --- | --- |
| `version` | The version string the matrix was asked for, for example `2.4.8-p5`. |
| `date` | The run date. |
| `status` | `verified`, `failed` or `not provisioned`. |
| `reason` | Present only when a reason was passed. |
| `facts` | `magento`, `php`, `database`, `search`, `bridge`, read from the running sandbox. Empty for a version that was not provisioned. |
| `tests` | One entry per JUnit test case: `name` (`classname::name`), `outcome` (`passed`, `failed`, `skipped`) and `seconds`. |

`compat/README.md` says records are never edited by hand.

Records on master (all 15 live tests listed in each provisioned record):

| File | Status | Notes |
| --- | --- | --- |
| `2.4.6-p15-2026-09-30.json` | verified | search fact reads `elasticsearch7 7.17.28` |
| `2.4.6-p15-2026-10-02.json` | failed | both `test_same_catalog_imports_in_bulk_mode` cases failed; search fact reads `opensearch 7.17.28` |
| `2.4.6-p15-2026-10-03.json` | verified | search fact reads `elasticsearch 7.17.28` |
| `2.4.7-p10-2026-09-30.json` | not provisioned | Composer security blocking reason, empty facts |
| `2.4.8-p5-2026-09-30.json` | failed | `test_same_catalog_imports_in_bulk_mode[never]` failed |
| `2.4.8-p5-2026-10-02.json` | verified | |
| `2.4.9-2026-09-30.json` | verified | |
| `2.4.9-2026-10-02.json` | verified | |

The three different search labels for the same Elasticsearch 7.17.28 container on 2.4.6 come from how the fact was read: `sandbox_facts()` now takes both engine and version from the sandbox `.govard.yml`, because, as its comment says, Magento's engine setting can name opensearch on a 2.4.6 sandbox whose search container is Elasticsearch 7.17. The older records predate that.

## Results table

The table sits between the two `compat:start` and `compat:end` comment markers (each alone on its line) and is rewritten by `scripts/compat-matrix.sh --write`, which runs `compat_record.py table`. It keeps only the newest record per version (newest `date`, file name as tie break) and sorts versions numerically (`2.4.7-p10` before `2.4.8-p5`, a base release `2.4.9` as patch 0). Do not edit it by hand: the next `--write` replaces everything between the markers.

<!-- compat:start -->
| Version | Magento patch | PHP | Database | Search | Bridge | Date | Result |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2.4.6-p15 | 2.4.6-p15 | 8.2.26 | MariaDB 10.11.18 | elasticsearch 7.17.28 | 1.0.0 | 2026-10-04 | verified (15 of 15 passed) |
| 2.4.7-p10 | - | - | - | - | - | 2026-10-04 | not provisioned: Composer security blocking refused a dependency of 2.4.7-p10 and govard bootstrap cannot disable it |
| 2.4.8-p5 | 2.4.8-p5 | 8.4.1 | MariaDB 11.4.10 | opensearch 3.0 | 1.0.0 | 2026-10-04 | verified (15 of 15 passed) |
| 2.4.9 | 2.4.9 | 8.5.8 | MariaDB 11.8.8 | opensearch 3.0 | 1.0.0 | 2026-10-04 | verified (15 of 15 passed) |
<!-- compat:end -->

Columns:

| Column | Meaning |
| --- | --- |
| Version | The version the matrix requested. |
| Magento patch | `ProductMetadataInterface::getVersion()` as reported by the running store. |
| PHP | `PHP_VERSION` inside the sandbox. |
| Database | `SELECT VERSION()` normalized to `MariaDB x.y.z` or `MySQL x.y.z`. |
| Search | Search engine and version from the sandbox `.govard.yml` (`search` and `search_version`). |
| Bridge | The `version` the optional bridge module reports on `GET dagster-bridge/capabilities`; `not installed` only on a 404, `unknown (HTTP <status>)`, `unknown (authentication failed)` or `unknown (connection failed)` on those failures. See [Optional-Bridge](Optional-Bridge). |
| Date | Date of the record shown. |
| Result | `verified (P of N passed)`, `failed (F of N failed)` with an optional reason, or `not provisioned: <reason>`. Skipped tests are left out of N. |

What one row proves: the whole `pytest -m live` suite passed on that patch, which covers the sample catalog in sync and bulk mode with the bridge off (`never`) and required (`require`), the rerun in which every row the diff can prove unchanged is skipped, a price change reaching a full page cached storefront after cron, a store scoped bulk update, special price dates, the category and attribute set case rules, and four probes that record Magento response shapes. See [Sandbox-and-Testing](Sandbox-and-Testing).

## Honest limits

- **2.4.7 is not verified.** It could not be provisioned: Composer's security blocking refuses a dependency (the failing resolution named `league/flysystem` 2.x for 2.4.7-p10; the record itself only says a dependency was refused) and govard's Magento bootstrap does not pass `--no-security-blocking` (the comment in `compat-matrix.sh` notes its Laravel bootstrap does). The library uses only standard REST endpoints, but there is no recorded run on that line, so it is not claimed.
- **Intermittent bulk failures on 2.4.6-p15 and 2.4.8-p5, unexplained.** 2.4.8-p5 failed the bulk catalog test once (2026-09-30) and passed on every run after. 2.4.6-p15 failed it twice in one run (2026-10-02: the sandbox reset inside the test exited non-zero, and one media upload answered "The product can't be saved.") and passed all 15 the next day on the same code. One run's log was lost and the other's reset error was not captured. 2.4.9 passed all 15 on each of its two runs. The table shows only the newest record per version; the failed records stay in `compat/results/`.
- **Patches, not ranges.** A row names the exact patch the run used. A newer patch is expected to behave the same but is unverified until the matrix is rerun.
- **Commerce is not in the table.** Commerce installs use the same REST API but are not part of the table.
- **One database family.** Every recorded run used MariaDB (10.11, 11.4, 11.8). `normalize_database` would label MySQL, but no MySQL run is recorded.
- **Sandbox configuration matters.** The sandbox sets READ COMMITTED isolation, indexers on schedule, full page cache and four consumers (see [Sandbox-and-Testing](Sandbox-and-Testing)). A store without those settings can behave differently in bulk mode, see [Troubleshooting](Troubleshooting).

## Version specific behaviours found live

| Behaviour | Lines | What the library does | Source |
| --- | --- | --- | --- |
| `default_sort_by` category attribute typed `string[]` on 2.4.6 and `string` on 2.4.9; 2.4.6 stores nothing for either shape | 2.4.6 | Rows rejected with exactly that type error are re-planned without the key and run again once, with a warning naming them | `dagster_magento/importers.py` (`_retry_without_default_sort_by`), CHANGELOG 0.3.0 |
| An async bulk product without SKU raises an uncaught `TypeError`: the consumer process exits and the operation stays open | 2.4.6 (observed on 2.4.6-p15; 2.4.9 answers a normal failure) | Never sends such an item (`sku` is required by the row model), but a hand written bulk can | this page, comment in `tests/live/test_probes.py` |
| Async bulk ordering race: consumers may run a grouped or bundle parent before its children, failing with "The Product with ... doesn't exist"; configurable option and child link operations failed with "The product can't be saved." | seen on 2.4.6, hidden by timing on 2.4.9 | Composite parents and those operations are planned into a later bulk phase submitted only after earlier phases complete; sync mode honours the same phases since 0.3.1 | CHANGELOG 0.3.0 and 0.3.1, `dagster_magento/writers/products.py`, `dagster_magento/executor.py` |
| MariaDB under default REPEATABLE READ: a consumer can reach its `magento_operation` row before the insert commits, fails with SQLSTATE 1020 and the message is dropped without requeue | measured on 2.4.9 with MariaDB 11.8 | Reports such rows `pending`; READ COMMITTED removes the race (configuration, no library change) | [Sync and Bulk Execution](Sync-and-Bulk-Execution), `scripts/sandbox.sh` `write_db_isolation_config`, docstring of `test_same_catalog_imports_in_bulk_mode` |
| No OpenSearch adapter on 2.4.6: its elasticsearch7 engine is rejected by OpenSearch 2.x | 2.4.6 | Sandbox only: `sandbox.sh` switches 2.4.6 to Elasticsearch 7.17.28 | `scripts/sandbox.sh` `use_supported_search_backend` |
| Magento stores an inverted special price range and reports success | captured on 2.4.9 | `PriceRow` rejects an end before the start | CHANGELOG 0.4.0, `tests/live/test_probes.py` |

## Python, Dagster and pydantic

From `pyproject.toml`:

| Requirement | Declared |
| --- | --- |
| Python | `>=3.10` |
| dagster | `>=1.13.17` (the comment says this is the version the test suite is actually verified against; the earlier `>=1.5` floor was an unverified guess) |
| requests | `>=2.28` |
| pydantic | `>=2` |
| openpyxl (extra `xlsx`) | `>=3.1` |
| dev extra | `pytest>=7`, `requests-mock>=1.11`, `openpyxl>=3.1` |

CI (`.github/workflows/ci.yml`) runs the hermetic suite (`python -m pytest -q`, after `pip install -e ".[dev]"`) on every push to `master` and every pull request:

| Job | Python | Role |
| --- | --- | --- |
| `verify` | 3.10 | The required status check: the declared floor plus the dev extras |
| `python 3.11` ... `python 3.14` | 3.11, 3.12, 3.13, 3.14 | Compatibility matrix, `fail-fast: false` |

CI never runs `live` or `samples`: the `addopts` in `pyproject.toml` deselects them, and the workflow comment states the job must not depend on the network or on a Magento instance. Only the floor of each dependency is declared; CI installs whatever pip resolves on the day, so a Dagster or pydantic version other than the one in a contributor's environment is covered only by those CI runs.

## See also

- [Home](Home)
- [Getting-Started](Getting-Started)
- [Architecture](Architecture)
- [Sandbox-and-Testing](Sandbox-and-Testing)
- [Contributing-and-Releases](Contributing-and-Releases)
- [Troubleshooting](Troubleshooting)
- [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution)
- [Optional-Bridge](Optional-Bridge)
