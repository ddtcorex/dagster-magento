# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-purpose Dagster resource package: `MagentoResource`, a `ConfigurableResource` for talking to Magento 2's REST API. It targets **standard Magento 2 REST endpoints only** — no dependency on any project-specific custom API modules (e.g. it deliberately does not replicate the custom endpoints a downstream ETL project like bebe9-etl's Magento module exposes).

## Commands

```bash
# Set up (this repo's own tests are verified against dagster>=1.13.17, not just any >=1.5)
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run all tests
pytest -q

# Run a single test file / test
pytest tests/test_resource.py
pytest tests/test_resource.py::test_put_sends_json_body_with_bearer_token
```

No linter/formatter is configured (no ruff/black config, no CI workflow in this repo). The existing code is hand-formatted at ~100 columns — match that rather than running a formatter with default settings, which will reformat the whole codebase to 88 columns and produce a huge unrelated diff.

If `python3 -m venv` fails with `ensurepip is not available`, the system Python needs its `-venv` package (e.g. `python3.14-venv` on Debian/Ubuntu) installed via the system package manager first.

## Architecture

Everything lives in `dagster_magento/`, split by concern — `resource.py` owns HTTP/auth, `upload.py`/`bulk.py`/`search.py` own pure request/response shaping logic that `resource.py` delegates to:

- **`resource.py`** — `MagentoResource`. Handles admin-token auth (`POST integration/admin/token`), auto-refreshes the token once on a `401`, retries once on a `503`, and exposes `get`/`get_paginated`/`post`/`put`/`delete` as thin wrappers over a shared `_request()`. `_request()`/`_url()` take an `api_prefix` (default `"V1"`) so the same plumbing serves both `V1/...` and `async/bulk/V1/...` paths — that's how `upload_rows_async()` reuses it instead of duplicating request/retry/logging logic.
- **`upload.py`** — `chunk_rows`/`run_upload`/`UploadResult`, used by `MagentoResource.upload_rows()`. Synchronous, resilient bulk writes: continues past a failing chunk (HTTPError only — anything else propagates), reports per-chunk errors with the offending row ids.
- **`bulk.py`** — `run_async_upload`/`AsyncBulkResult`, used by `MagentoResource.upload_rows_async()`. Submits through Magento's core `async/bulk/V1/*` API. Critically different semantics from `upload.py`: `accepted`/`rejected` mean "Magento queued (or rejected outright) the operation", not "it finished" — the actual outcome requires polling `get_bulk_status()` (`GET V1/bulk/:uuid/detailed-status`) separately, since queue consumers run asynchronously (and in a typical dev setup, on a cron-triggered schedule rather than a persistent daemon, so status can lag noticeably behind the operation actually completing).
- **`search.py`** — `build_search_criteria()`, a pure function building Magento's `searchCriteria[filter_groups]...`/`[sortOrders]...` query params from a plain list of `(field, value, condition_type)` filters. Only ever builds a single AND'd filter group (no OR-across-groups) — that covers every sync task this library targets. Never touches `page_size`/`current_page`; `get_paginated()` owns those.

### Design boundary: primitives only, not entity-specific methods

`MagentoResource` intentionally stays a generic REST client — `get`/`put`/`post`/`delete`/`upload_rows`/`upload_rows_async`/`build_search_criteria` — rather than growing dedicated methods per entity (`upsert_product()`, `set_source_items()`, etc.). README.md's "Recipe" table documents how to compose the primitives for common sync tasks (products, categories, customers, orders, MSI inventory, configurable/bundle linking, attributes) instead. Before adding a new method, ask whether it's really encoding non-obvious Magento wire-format/API behavior (e.g. `resolve_attribute_options()` exists because Magento's option-create endpoint returns a bare id string, existing options must be matched case-insensitively, and getting that wrong silently creates duplicate EAV options) versus just aliasing an endpoint path — only the former belongs here.

### Secrets and logging

`password` is a Pydantic field marked `repr=False` + `dagster__is_secret` — never remove that. Request/response *bodies* are only logged when `verbose_logging=True` (off by default) because a single sync can carry hundreds of thousands of rows, and because a `POST /V1/customers` body can legitimately contain a plaintext customer password — don't add new unconditional body logging.

### Testing

Every test is hermetic — `requests_mock`, no real Magento instance, no live network — organized one test file per source module (`test_resource.py`, `test_upload.py`, `test_bulk.py`, `test_search.py`). Follow that pairing for new modules.
