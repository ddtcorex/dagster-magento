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

## Scope: standard Magento REST only

Never add a dependency on a bespoke, project-specific companion-module
endpoint (`products/skus`, `products/ean-sku-mapping`, ...): those belong in
the consuming project. Keep `MagentoResource` a generic REST client. A new
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
  times on 429/502/503/504 with 0.5/1/2 s backoff plus jitter and
  `Retry-After` honoured. `get`/`get_paginated`/`post`/`put`/`delete`/
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

### Catalog import layer (v0.2.0): models -> resolvers -> diff -> writers -> executor

- `models.py`: pydantic v2 row models and `validate_rows`; a bad row becomes
  a `RowError`, never an exception.
- `resolvers.py`: `Resolver`: one `GET products/attributes` preload, option
  label matching, attribute set / website / store / category path lookup,
  parent-first category creation.
- `diff.py`: snapshots products, prices, source items and media; skips rows
  that already match, which is what `skipped_unchanged` reports.
- `operation.py`: `Operation`, `BulkSpec`, `RowError`.
- `writers/`: pure planners, one module per entity: rows in, `Operation`
  values out, no HTTP.
- `executor.py`: `execute(resource, operations, mode)` plus
  `check_error_ratio`; sync chunks list endpoints at 1000, bulk at 200,
  resubmits a retriably failed operation once, and maps `async/bulk`
  statuses back onto rows by operation id.
- `importers.py`: one function per entity composing snapshot, diff, plan and
  execute, plus `to_materialize_result`. Together with `resource.py` it is the
  only module that knows both Dagster and Magento.
- `formats/`: csv/json/xlsx readers (`readers.py`, openpyxl behind the
  `xlsx` extra) and the native column mappers (`columns.py` pure string
  parsing, `catalog.py` onto models).

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
- `live` drives the govard sandbox from `scripts/sandbox.sh` and needs
  `MAGENTO_BASE_URL` plus `MAGENTO_CA_BUNDLE` for the local development CA.
  A live test that resets the sandbox rotates the admin password, which is
  why `tests/live/live_support.py` reloads the environment.
- When pinning Dagster behaviour (`get_dagster_logger()` output,
  `AssetExecutionContext`), verify the installed version's API instead of
  assuming: it changed during this package's own development
  (`materialize(..., input_values=...)` does not exist on 1.13.17).
- One live test is known red on the sandbox for a measured environment
  defect, not a library bug: `test_same_catalog_imports_in_bulk_mode`. Its
  docstring carries the measurement (the Magento consumer drops a variable
  subset of published bulk operations and leaves the rows at status 4 with
  nothing logged). Do not weaken the assertion to make it green.

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

1. Update `CHANGELOG.md` and bump `version` in `pyproject.toml`.
2. `.venv/bin/pytest -q`, `.venv/bin/pytest -m samples -q` and, when the
   sandbox is up, `-m live` must be green.
3. Commit, then `git tag vX.Y.Z` and `git push origin master vX.Y.Z` after
   approval.
4. Verify the README install command from a clean environment before calling
   the release done; it shipped wrong once already.

## Privacy gate

This repository is public. No private project or client names, no local
absolute paths, no credentials in code, docs, tests or commit messages. Run
the workspace blacklist gate (`node <workspace-root>/scripts/check-public-blacklist.mjs`)
before pushing, and keep the pre-commit hook installed.
