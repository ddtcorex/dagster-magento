# MagentoResource

`MagentoResource` (`dagster_magento/resource.py`) is a Dagster
`ConfigurableResource` and the only part of the library that sends HTTP
requests to Magento. It fetches an admin bearer token, retries transient
failures, scopes each call to a store view, and wraps the core REST verbs,
paginated reads, chunked synchronous writes and the async bulk API. The
catalog importers use it underneath, but it is a generic REST client you can
use on its own for anything the standard API exposes (orders, customers,
MSI, ...). This page documents every public method, its defaults, and the
retry, auth and logging rules.

## Configuration fields

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `base_url` | `str` | required | No trailing slash. |
| `username` | `str` | required | Admin user for the token request. |
| `password` | `str` | required | `Field(repr=False, json_schema_extra={"dagster__is_secret": True})`. |
| `store_view` | `str` | required | Default scope segment in `/rest/{scope}/...`. |
| `verbose_logging` | `bool` | `False` | See "Logging and secrets". |

Every request URL is built as:

```text
{base_url}/rest/{store_code or store_view}/{api_prefix}/{endpoint}
```

`api_prefix` is `V1` for everything except bulk submission, which uses
`async/bulk/V1`. Every request has a 30 second timeout.

## Methods

### `get(endpoint, params=None, store_code=None)`

`GET` and return the decoded JSON body (`response.json()`).

```python
configs = magento.get("store/storeConfigs")
fr_name = magento.get("products/TEE-RED-M", store_code="fr")["name"]
```

### `get_paginated(endpoint, params=None, page_size=1000, response_key="items", store_code=None)`

Loops `searchCriteria[page_size]` and `searchCriteria[current_page]`
(starting at 1) over `params` and returns a flat `list` of
`response[response_key]` items.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `params` | `None` | Extra query params, typically from `build_search_criteria`. |
| `page_size` | `1000` | Items per page. |
| `response_key` | `"items"` | Key holding the page's list. |
| `store_code` | `None` | Scope override. |

Stopping rules, in order:

1. The response is not a dict: logs a warning and stops.
2. `response_key` is not in the response: logs a warning (with the keys found) and stops.
3. The page has fewer items than `page_size`: stops after adding them.

`total_count` is not read: the loop stops only on a short page. A collection
whose size is an exact multiple of `page_size` therefore costs one extra
request for the page after the last one, and the loop relies on Magento
answering that page with fewer than `page_size` items.

```python
from dagster_magento import build_search_criteria

params = build_search_criteria(
    filters=[("updated_at", "2026-08-08 00:00:00", "gteq")],
    sort_orders=[("created_at", "DESC")],
)
orders = magento.get_paginated("orders", params=params)
```

### `post(endpoint, payload, store_code=None)`, `put(endpoint, payload, store_code=None)`, `delete(endpoint, store_code=None)`

Send JSON (`json=payload`) and return the `requests.Response` (not the
decoded body). A non-2xx status, after retries, raises
`requests.exceptions.HTTPError`.

```python
magento.put("products/EXISTING-SKU", {"product": {"price": 24.99}})
magento.delete("configurable-products/PARENT-SKU/children/CHILD-SKU")
new_id = magento.post("categories", {"category": {"parent_id": 2, "name": "Sale", "is_active": True}}).json()["id"]
```

URL-encode SKUs yourself when they can contain `/` or other reserved
characters; the importers do it with `urllib.parse.quote(sku, safe="")`.

### `upload_rows(endpoint, rows, chunk_size=200, wrap_key=None, row_id_field="sku")`

Resilient synchronous writes with catch, log and continue. Always `POST`.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `endpoint` | required | For example `configurable-products/PARENT/child`. |
| `rows` | required | List of dicts. |
| `chunk_size` | `200` | Rows per request. Used only when `wrap_key` is set. |
| `wrap_key` | `None` | `None`: one request per row, body is the row itself. Set: body is `{wrap_key: [chunk]}`. |
| `row_id_field` | `"sku"` | Key read from each row to fill `errors[].row_ids`. |

Returns an `UploadResult`. Only `requests.exceptions.HTTPError` is caught, so a
`MagentoAuthError` or any other exception aborts the whole call.

```python
result = magento.upload_rows(
    "inventory/source-items",
    rows=[{"sku": "A", "source_code": "default", "quantity": 5, "status": 1}],
    wrap_key="sourceItems",
)
```

```json
{
  "succeeded": 0,
  "failed": 1,
  "errors": [
    {
      "chunk_index": 0,
      "row_ids": ["A"],
      "status_code": 400,
      "message": "400 Client Error: Bad Request for url: ... - response body: {\"message\": \"...\"}"
    }
  ]
}
```

Caution: a list endpoint such as `inventory/source-items` or
`products/base-prices` can answer 200 with a list of rejected items.
`upload_rows` counts a 2xx chunk as fully succeeded and does not read that
list. The importers do read it; see [Results-and-Errors](Results-and-Errors).

### `upload_rows_async(endpoint, rows, chunk_size=200, row_id_field="sku")`

Submits each chunk as a bare JSON array to `POST
/rest/{store_view}/async/bulk/V1/{endpoint}` and returns an
`AsyncBulkResult(bulk_uuids, accepted, rejected, errors)` right away, before
Magento has run anything. Each row must already be the body of the
synchronous endpoint, for example `{"product": {...}}` for `products`.

| Field | Meaning |
| --- | --- |
| `bulk_uuids` | One per chunk that was accepted for submission. |
| `accepted` | Request items Magento queued. Not "succeeded". |
| `rejected` | Rows of chunks that failed to submit, plus request items marked `rejected`. |
| `errors` | `{"chunk_index", "row_ids", "status_code", "message"}` per failure. |

`to_metadata()` returns `bulk_uuids`, `accepted`, `rejected`, `error_count`.
Poll `get_bulk_status(uuid)` to learn the real outcome. `upload_rows_async`
always uses `POST` and has no `store_code`; use `submit_bulk` for other verbs
or scopes.

### `submit_bulk(method, bulk_endpoint, items, store_code=None)`

Low level bulk submission used by the executor. Sends `items` as a bare JSON
array with `method` to `async/bulk/V1/{bulk_endpoint}` and returns
`response.json()["bulk_uuid"]`.

```python
uuid = magento.submit_bulk(
    "PUT",
    "products/bySku",
    [{"sku": "A", "product": {"sku": "A", "status": 2}}],
    store_code="all",
)
```

```json
{"bulk_uuid": "92c4131a-0ad6-4670-a009-d6e7f56ac57a", "request_items": [{"id": 0, "status": "accepted"}], "errors": false}
```

(Abridged. `submit_bulk` reads only `bulk_uuid` from a 2xx answer.)

A wrapped body (`{"items": [...]}`) is refused by Magento with 400 "Request
body must be an array". Because bulk submission is a `POST` or `PUT`, see the
retry rules below: a `POST` submission is not repeated on a gateway error, but
a `PUT` submission is (see the gotchas).

### `get_bulk_status(bulk_uuid)` and `bulk_detailed_status(bulk_uuid)`

`GET bulk/{bulk_uuid}/detailed-status`. `bulk_detailed_status` is an alias
used by `wait_bulk`. Operation status values:

| Status | Constant in `bulk.py` | Meaning |
| --- | --- | --- |
| 1 | `STATUS_COMPLETE` | Done. |
| 2 | `STATUS_RETRIABLY_FAILED` | Failed, retriable. |
| 3 | `STATUS_NOT_RETRIABLY_FAILED` | Failed. |
| 4 | `STATUS_OPEN` | Not processed yet. |
| 5 | `STATUS_REJECTED` | Rejected. |

### `resolve_attribute_options(attribute_code, labels)`

Maps select or multiselect option labels to integer option ids, creating the
missing ones.

1. `GET products/attributes/{attribute_code}` and index existing options by
   `label.strip().casefold()`, skipping options whose value is empty.
2. For each label (blank labels are skipped): reuse an existing id, reuse one
   created earlier in the same call, or `POST
   products/attributes/{attribute_code}/options` with
   `{"option": {"label": "<label stripped>"}}` and read the id as
   `int(response.json())`.
3. Return a dict keyed by the labels exactly as passed in.

```python
ids = magento.resolve_attribute_options("color", ["Red", " red ", "Navy"])
# illustrative ids: {"Red": 49, " red ": 49, "Navy": 112}
# "Red" and " red " match the same option; "Navy" was created.
```

Gotchas: two runs creating the same missing label concurrently can create two
options, because Magento's add-option endpoint does not deduplicate; resolve
options for one attribute from one place. This method converts the create
response with `int()`; the import layer's resolver deliberately does not trust
that response and re-reads the attribute instead, noting the response can be
`"id_<n>"` on some 2.4.x versions. If you hit a `ValueError` here, that is the
likely cause. HTML entities in labels are not unescaped here (the import
layer's resolver does unescape them).

### `build_search_criteria(filters=None, sort_orders=None)`

A module function (`from dagster_magento import build_search_criteria`), not a
method. Builds query params for one AND'd filter group:

```python
build_search_criteria(filters=[("sku", ["A", "B"], "in"), ("status", 1)], sort_orders=[("sku", "ASC")])
```

```json
{
  "searchCriteria[filter_groups][0][filters][0][field]": "sku",
  "searchCriteria[filter_groups][0][filters][0][value]": "A,B",
  "searchCriteria[filter_groups][0][filters][0][condition_type]": "in",
  "searchCriteria[filter_groups][0][filters][1][field]": "status",
  "searchCriteria[filter_groups][0][filters][1][value]": 1,
  "searchCriteria[filter_groups][0][filters][1][condition_type]": "eq",
  "searchCriteria[sortOrders][0][field]": "sku",
  "searchCriteria[sortOrders][0][direction]": "ASC"
}
```

`condition_type` defaults to `"eq"`; a list or tuple value is comma joined.
It never sets page size or current page; `get_paginated` owns those. OR
across groups is not supported.

## Store scoping

| Method | Accepts `store_code` |
| --- | --- |
| `get`, `get_paginated`, `post`, `put`, `delete`, `submit_bulk` | Yes |
| `upload_rows`, `upload_rows_async`, `get_bulk_status`, `resolve_attribute_options` | No, always `store_view` |
| token request | No, always `store_view` |

`store_code=None` means "use `store_view`". Pass `"all"` for the global scope
and a store code (`"fr"`) for a store view. In Magento, a product save through
a store view scope writes store-level values for that view, which is why the
importers send only localized attributes through a store scope.

## Authentication

- The first request of a resource instance fetches a token with
  `POST /rest/{store_view}/V1/integration/admin/token` and body
  `{"username": ..., "password": ...}`. The token is cached on the instance.
- Every request sends `Authorization: Bearer <token>`.
- On a `401` from any endpoint, the resource logs a warning, fetches a new
  token and repeats that request once. The refresh is scoped to the one call;
  a second call that gets a 401 does its own refresh.
- If the repeat still answers 401, it goes through the normal error path and
  raises `HTTPError`, which the importers count as failed rows. Note this also
  happens for a permanent ACL denial: each such request refetches a token
  once.

### `MagentoAuthError`

`from dagster_magento.resource import MagentoAuthError`

Raised when the token request answers a non-2xx status. It is deliberately
not a subclass of `requests.exceptions.HTTPError`, so no catch, log and
continue loop in the library (upload, executor, media snapshot, bridge probe)
can swallow it. A bad password or a locked account aborts the run at once
instead of becoming one failed login per row. The token request itself is not
retried. A network failure during the token request (connection refused,
timeout) raises the `requests` exception, not `MagentoAuthError`.

## Retries

```text
send
 |-- 401 -> fetch token, send once more
 |-- status in retry set? -> sleep, send again (up to 3 retries)
 `-- raise_for_status()
```

| Method | Retried statuses | Max retries |
| --- | --- | --- |
| `GET`, `PUT`, `DELETE` | 429, 502, 503, 504 | 3 |
| `POST` | 429 only | 3 |

Delay before retry `n` (0 based) is
`max(Retry-After, 0.5 * 2**n) + uniform(0, 0.1)` seconds, so 0.5 s, 1 s, 2 s
plus jitter, or longer when the server sends a larger `Retry-After`.
`Retry-After` is parsed as a number of seconds; a missing or unparseable
header (including the HTTP-date form) counts as 0.

Why POST retries only on 429: a 502, 503 or 504 can come from a gateway that
timed out while Magento committed the write. Repeating it would create a
second category or option, or schedule a bulk twice. Magento answers 429
before processing anything, so that one is safe to repeat. This includes the
read-only POSTs (price information reads, bridge attribute values): they fail
on a gateway error instead of retrying.

Not retried at all: 400, 404, 500 and other statuses, and transport errors
(`requests.ConnectionError`, `requests.Timeout`). Transport errors are not
`HTTPError`, so they also escape every catch in the library and abort an
import.

## Logging and secrets

| Level | What | When |
| --- | --- | --- |
| `INFO` | `Fetching Magento admin token (store_view=...)` | Each token fetch. |
| `DEBUG` | `{method} {endpoint} store_view=... params=...` | Every request. |
| `DEBUG` | `{method} {endpoint} -> {status} in {s}s` (with ` retry` for repeats) | Every send. |
| `WARNING` | token expired, retry attempt with delay | On 401 and retried statuses. |
| `DEBUG` | full request body | `verbose_logging=True` and a JSON body. |
| `DEBUG` | response body, first 2000 characters | `verbose_logging=True`. |
| `INFO` | page progress | `get_paginated`. |

The token and password are added to the request only inside `_send` and the
token request; they are never passed to a log call, with or without
`verbose_logging`. A regression test pins that for the token. The request
body, however, is logged as is in verbose mode: a body sent to `POST
customers` can carry a plaintext customer password. Keep verbose mode for
debugging one issue.

The password field is hidden from `repr()` and marked secret in the Dagster
UI. `model_dump()` and pydantic validation errors can still expose it on the
Dagster and pydantic versions this library is tested with; do not log those.

Row error messages built by the library (`http_error_details`) embed the first
1000 characters of the response body and drop Magento's `trace` key, so
developer mode stack traces with server paths do not end up in row errors.

## Gotchas

- `post`, `put` and `delete` return a `Response`; call `.json()` yourself.
- `get_paginated` returns a list, not the search result envelope.
- A `POST` (product creates, media adds, configurable links, the price
  information reads) fails at once on a 502 or 504. A `PUT` is retried up to
  three times on 429, 502, 503 and 504, and bulk updates of existing products go
  out as `PUT async/bulk/V1/products/bySku`: a gateway timeout that Magento had
  already accepted can schedule that bulk twice. Checked with `submit_bulk("PUT",
  "products/bySku", ...)` against a mocked 502.
- A token is cached for the life of the resource instance and only replaced
  after a 401.
- There is no client side rate limiting; only Magento's 429 slows you down.

## See also

- [Home](Home)
- [Getting-Started](Getting-Started)
- [Architecture](Architecture)
- [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution)
- [Results-and-Errors](Results-and-Errors)
- [Optional-Bridge](Optional-Bridge)
- [Troubleshooting](Troubleshooting)
