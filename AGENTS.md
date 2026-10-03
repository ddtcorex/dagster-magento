# AGENTS.md (dagster-magento)

> `CLAUDE.md` in this directory is a symlink to this file. Edit `AGENTS.md` only,
> never the symlink or a copy.

Reusable Dagster library for Magento 2's REST API: `MagentoResource` (admin
token auth, CRUD, paginated and search-criteria reads, chunked synchronous
writes, async/bulk submission), a catalog import layer built on top of it
(models, resolvers, diff, writers, executor, importers) and file adapters for
the native csv/json/xlsx import columns.

Standalone installable library, not a pipeline and not a DSH plugin: no
Dagster assets ship here, no Docker in the package, no `.env`. Consumers add
it as a dependency and build their own assets around it. `sandbox/`
(gitignored) and `scripts/sandbox.sh` exist only so the live tests can run
against a real Magento. Distribution is git tag plus URL install; this
package is never published to PyPI.

## Quick reference

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"   # first-time setup
.venv/bin/pytest -q                      # hermetic suite: requests_mock, no network
.venv/bin/pytest -m samples -q           # downloads the public sample import files
.venv/bin/pytest tests/test_bulk.py -q   # a single file
scripts/sandbox.sh up && scripts/sandbox.sh consumers   # disposable Magento 2.4.9
eval "$(scripts/sandbox.sh env)"
MAGENTO_CA_BUNDLE=~/.govard/ssl/root.crt .venv/bin/pytest -m live -q
```

The two network-touching markers (`samples`, `live`) are excluded from the
default run by `addopts`. `sandbox/`, `tests/.samples/` and `.venv/` are
gitignored; never commit them or the generated admin password.

If `python3 -m venv` fails with `ensurepip is not available`, install the
system `-venv` package first (for example `python3.14-venv` on Debian).

## Scope: standard Magento REST, with one optional module

Never add a dependency on a bespoke, project-specific companion-module
endpoint (`products/skus`, `products/ean-sku-mapping`, ...): those belong in
the consuming project. Keep `MagentoResource` a generic REST client: it never
depends on any module, including the optional bridge below. Only the import
layer may use `DDTCoreX_DagsterBridge`, only through `BridgeClient`, and
always optionally: every path it offers must also work with the native REST
endpoints (see "Optional bridge module"). A new
method must encode non-obvious Magento wire-format behaviour, not alias an
endpoint path: `resolve_attribute_options` earns its place because the
option-create endpoint returns a bare id string and existing option labels
must be matched trimmed and case-insensitively; `upsert_product()` would not.

The catalog import layer is the one deliberate exception, and it is scoped by
its own design record: `<workspace-root>/docs/specs/2026-09-28-cross-repo-dagster-magento-catalog-import-design.md`.
Read it before widening that layer, and update it if the scope changes.

## Architecture

Everything lives in `dagster_magento/`, split by concern. `resource.py` owns
HTTP and auth; every other module is pure request/response shaping, planning
or orchestration that `resource.py` and `importers.py` delegate to.

### Resource primitives

- `resource.py`: `MagentoResource`. Admin token via
  `POST integration/admin/token`, refreshed once on a 401, retried three
  times with 0.5/1/2 s backoff plus jitter and `Retry-After` honoured: GET,
  PUT and DELETE on 429/502/503/504, POST on 429 only, because a gateway
  error after a POST may hide a committed write that a retry would repeat. `get`/`get_paginated`/`post`/`put`/`delete`/
  `upload_rows`/`upload_rows_async`/`get_bulk_status`/
  `resolve_attribute_options` are thin wrappers over `_request()`, which
  takes `api_prefix` (default `"V1"`, `"async/bulk/V1"` for bulk submission)
  and `store_code` (per-call store scope, default `self.store_view`).
- `search.py`: `build_search_criteria()`, pure: Magento's
  `searchCriteria[filter_groups]...`/`[sortOrders]...` params from a plain
  list of filters. Single AND'd group only, and it never touches
  `page_size`/`current_page` (that is `get_paginated()`'s job).
- `upload.py`: `chunk_rows`/`run_upload`/`UploadResult`, used by
  `upload_rows()`. Catches exactly `requests.exceptions.HTTPError`, so
  anything else (including a bug in a caller's own `send`) propagates.
- `bulk.py`: two halves with different semantics: `AsyncBulkResult` and
  `run_async_upload()` (submission, where `accepted` means "Magento queued
  it", not "it finished") and `map_detailed_status()`/`wait_bulk()` (polling
  and per-operation id mapping).
- `bridge.py`: `BridgeClient`, the client for the optional companion module
  `DDTCoreX_DagsterBridge` (its own public repo). One capability probe per run
  (`GET dagster-bridge/capabilities`, cached), product index paging on
  `next_after`, attribute values chunked to the module's 1000 SKU and 50 code
  caps, category upsert, and a separator chosen to appear in none of the paths
  it is given. See "Optional bridge module" below for the rules.

### Catalog import layer: models -> resolvers -> diff -> writers -> executor

- `models.py`: pydantic v2 row models and `validate_rows`; a bad row becomes
  a `RowError`, never an exception.
- `resolvers.py`: `Resolver`: one `GET products/attributes` preload, option
  label matching, attribute set / website / store / category path lookup,
  parent-first category creation. With a bridge client that advertises
  `categories.upsert`, the module creates the missing nodes in one transaction
  instead, and the resolver cache is updated the same way. A failing upsert
  (an HTTP error or an answer missing a requested path) falls back to the
  native creation for that call in `auto` and raises `MagentoImportError` in
  `require`.
- `diff.py`: snapshots products, prices, source items and media; skips rows
  that already match, which is what `skipped_unchanged` reports. The product
  snapshot takes an optional bridge client and store id, reads the index for the
  entity fields, the attribute endpoint for real EAV codes only (the module
  rejects anything else) and `GET /V1/products` for what the module cannot
  answer (website ids and category links in `extension_attributes`), and
  applies Magento's store fallback (store value first, default store value
  otherwise), so a store without its own value never reads as a difference.
  With `use_bridge="require"` a bridge failure raises `MagentoImportError`;
  only `auto` falls back to REST, with a warning.
- `operation.py`: `Operation`, `BulkSpec`, `RowError`.
- `writers/`: pure planners, one module per entity: rows in, `Operation`
  values out, no HTTP.
- `executor.py`: `execute(resource, operations, mode)` plus
  `check_error_ratio`; sync chunks list endpoints at 1000, bulk at 200,
  resubmits a retriably failed operation once, and maps `async/bulk`
  statuses back onto rows by operation id.
- `importers.py`: one function per entity composing snapshot, diff, plan and
  execute, plus `to_materialize_result`. Dagster is used only for logging
  and results, here and in `resource.py`, `executor.py` and
  `formats/catalog.py`; only `resource.py` sends HTTP to Magento.
- `formats/`: csv/json/xlsx readers (`readers.py`, openpyxl behind the
  `xlsx` extra) and the native column mappers (`columns.py` pure string
  parsing, `catalog.py` onto models).

## Optional bridge module

`dagster_magento/bridge.py` talks to `DDTCoreX_DagsterBridge`, an optional
companion module that lives in its own public repo. The library must keep
working without it, so:

- every importer takes `use_bridge` (`auto`, `never`, `require`); `require`
  raises `MagentoImportError` naming the missing capability rather than quietly
  running a slower path, and also when a capability it uses fails, where
  `auto` falls back to the native path with a warning;
- each capability is used on its own, so a store with an older module gets the
  capabilities it has and the native paths for the rest;
- the probe is best effort: a 404 or a probe that cannot answer at all leaves
  the native paths in charge, because an optional module must never be able to
  fail an import;
- `MagentoAuthError` is re-raised and never swallowed by that fallback, exactly
  as it is not caught anywhere else in this library.

## Non-negotiable: secrets never leak

`MagentoResource.password` is declared
`Field(repr=False, json_schema_extra={"dagster__is_secret": True})`. Never
remove or weaken that: it is what keeps the password out of `repr()` and out
of the Dagster launchpad. `SecretStr` does not work on
`ConfigurableResource` (`DagsterInvalidPythonicConfigDefinitionError`, tracked
upstream as dagster-io/dagster#31718) - check whether that changed before
relying on it. Apply the same field pattern to any future secret-bearing
field. `model_dump()` and `ValidationError.errors()` still expose the raw
value on this Dagster/Pydantic version: accepted residual gaps, narrower than
`repr()` and the UI.

**Logging redaction is enforced by construction, not by a filter.** The token
and password are only ever placed into the request inside `_send`/`_request`,
never passed to a `logger.*` call. If you add a method that builds a request,
keep it that way: no log line may interpolate `self.password`, `self._token`
or raw `kwargs` from a call site that could carry either. The regression test
`test_token_value_never_appears_in_logs_even_with_verbose_logging_and_401_retry`
pins the token case; extend it rather than replacing it.

`verbose_logging=True` logs full request and response bodies. That is a
documented trade-off, not a bug: a `POST /V1/customers` body can legitimately
contain a plaintext customer password, so leave verbose mode off in a
pipeline that writes customer records.

## Auth failures vs data failures

`_fetch_token()` raises `MagentoAuthError`, deliberately **not** a
`requests.exceptions.HTTPError` subclass, so the catch-log-continue loops in
`run_upload`/the executor cannot swallow it: a bad credential or a locked
account must abort the run, not turn into one failed login attempt per
row/chunk (hundreds of thousands at this library's stated scale) reported as
"N data rows rejected". Anything that means "the whole operation cannot
proceed" must propagate past those catches.

Known, still open: `_request` refetches the token on any 401 from a data
endpoint, including a permanently ACL-denied one, so that path also refetches
once per row/chunk. Worth a distinct non-`HTTPError` exception or a
consecutive-failure circuit breaker if it ever bites a real consumer.

## Testing

- TDD: write the failing test, confirm the failure reason, implement, confirm
  green. Keep the current shape: one test module per source module
  (`test_resource.py`, `test_bulk.py`, `test_search.py`, `tests/writers/*`).
- The default suite is hermetic. `requests_mock` only, no live HTTP, no real
  Magento: a test that needs one to pass is a design mistake.
- `samples` downloads the public sample import files at a pinned commit into
  `tests/.samples/` (gitignored, never vendored).
- `tests/test_bridge.py` pins the optional module hermetically: the 404 probe,
  the capability cache, `next_after` paging, 1000 SKU chunking, the store
  fallback, per-capability fallback, the separator choice, and the auth failure
  that must not be swallowed by the best-effort probe.
- `live` drives the govard sandbox from `scripts/sandbox.sh` and needs
  `MAGENTO_BASE_URL` plus `MAGENTO_CA_BUNDLE` for the local development CA. A
  plain script has no conftest to build that bundle: cat certifi plus the local
  CA into one file and point `REQUESTS_CA_BUNDLE` at it.
  A live test that resets the sandbox rotates the admin password, which is
  why `tests/live/live_support.py` reloads the environment.
- The live e2e suite runs the catalog import as a matrix over `use_bridge`
  (`never`, `require`) in `sync` and `bulk` mode. The sandbox keeps its bridge
  checkout across `scripts/sandbox.sh reset` and re-enables the module, which is
  what makes the `require` combinations real assertions that the module is
  installed and answering.
- When pinning Dagster behaviour (`get_dagster_logger()` output,
  `AssetExecutionContext`), verify the installed version's API instead of
  assuming: it changed during this package's own development
  (`materialize(..., input_values=...)` does not exist on 1.13.17).
- `test_same_catalog_imports_in_bulk_mode` used to be red on the sandbox for
  a measured environment defect, not a library bug, and it is now green: on
  MariaDB, Magento publishes an async bulk on the broker before it commits
  the rows that bulk belongs to, so a consumer can reach its row while the
  insert is still uncommitted, fail with SQLSTATE 1020 ("Record has changed
  since last read"), and have its message dropped without requeue. The
  sandbox sets `READ COMMITTED` for it. Its docstring carries the
  measurement, and the assertion must not be weakened to make it green.

## Workflow

- Branch off `master`: `feat/<topic>`, `fix/<topic>`, `docs/<topic>` or
  `chore/<topic>`. Never commit to `master`, and never push `master`.
- Conventional Commits, imperative mood, subject at most 72 characters:
  `feat(resource): add backoff retries`, `fix(formats): map category labels`.
  One logical change per commit; body explains why, not what.
- When the base moves, rebase (`git fetch origin && git rebase origin/master`),
  never merge `master` into the branch.
- Cross-repo or contract-changing work follows the workspace Superpowers
  three-phase workflow (brainstorming, writing-plans, executing-plans); its
  specs and plans live at the workspace root, never in this repo.
- Do not claim verified, done or green without the command output behind it.
  Paste the exact commands and results in the PR body.
- Merge and release need explicit human approval.

## Release

No PyPI, no registry: `pip install "dagster-magento[xlsx] @ git+https://github.com/ddtcorex/dagster-magento.git@vX.Y.Z"`.

1. Update `CHANGELOG.md`, bump `version` in `pyproject.toml`, and move the
   `@vX.Y.Z` pin in the README install command to the new tag.
2. `.venv/bin/pytest -q` and `.venv/bin/pytest -m samples -q` must be green.
   Run `scripts/compat-matrix.sh --write` and commit the new `compat/results/`
   records and the regenerated results table in `docs/Compatibility.md`: a release never states a Magento
   line that has no current row. The matrix owns the sandbox for about an
   hour and a half; do not run other live work meanwhile, and keep other
   govard sessions off the machine.
3. Commit, then `git tag vX.Y.Z` and `git push origin vX.Y.Z` after the
   release PR is merged and approved (never push `master`). Pushing the tag runs `.github/workflows/release.yml`, the same flow
   as the other repos in the harness: it checks the tag against `pyproject.toml`,
   turns the matching CHANGELOG section into the GitHub Release, and fails if the
   section is missing. To release a tag pushed before that workflow existed, run it
   by hand: `gh workflow run release.yml -R ddtcorex/dagster-magento -f tag=vX.Y.Z`.
4. Verify the README install command from a clean environment before calling
   the release done; it shipped wrong once already.

## Docs and wiki

The user documentation is the wiki. Its source is `docs/`: flat markdown, one
file per page named `Page-Name.md`, links written `[Text](Page-Name)`, plus
`_Sidebar.md` and `_Footer.md`. A change to `docs/**` reaches the GitHub wiki
through `.github/workflows/sync-wiki.yml` when it lands on `master`; the sync
replaces the whole wiki, so never edit the wiki by hand. `scripts/check-docs.sh`
(also run by `tests/test_docs.py` and by the workflow) fails on a missing `Home.md`,
a link to a page that does not exist, or a non-markdown file. The wiki repository
must have been created once by saving a first page on the wiki tab.

`README.md` stays basic: what it is, install, a quick start, links to the wiki.
Put anything longer in `docs/`. The compatibility results table is generated into
`docs/Compatibility.md` by `scripts/compat-matrix.sh --write`; do not edit it by
hand. A behaviour change updates the page that documents it in the same PR.

## Privacy gate

This repository is public. No private project or client names, no local
absolute paths, no credentials in code, docs, tests or commit messages. Run
the workspace blacklist gate (`node <workspace-root>/scripts/check-public-blacklist.mjs`)
before pushing, and keep the pre-commit hook installed.
