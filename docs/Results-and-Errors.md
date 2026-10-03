# Results and Errors

Every importer returns an `UploadResult` that counts one outcome per input
row: succeeded, failed, pending or skipped as unchanged, plus an `errors`
list that says which rows failed and why. A rejected row never raises; an
authentication failure always does, and a failure ratio you configure can
turn too many rejected rows into an exception. This page documents the result
types, how a Magento answer is attributed to rows (including the cases where
it cannot be), the ratio check, and a symptom table.

## `UploadResult`

`from dagster_magento import UploadResult` (defined in `upload.py`)

| Field | Type | Default | Meaning in an importer result |
| --- | --- | --- | --- |
| `succeeded` | `int` | required | Rows whose every operation was accepted (sync) or completed (bulk). |
| `failed` | `int` | required | Rows rejected by validation, planning, or Magento. |
| `errors` | `list[dict]` | `[]` | One entry per failure or pending group, see below. |
| `pending` | `int` | `0` | Rows with an operation still open when the bulk wait ended. |
| `skipped_unchanged` | `int` | `0` | Rows not sent: proven unchanged by the diff, folded into a later duplicate, left alone by a behavior (`create_only`, `update_only`), or planned to nothing. |

For an importer, `succeeded + failed + skipped_unchanged + pending` is meant
to equal the number of input rows: the live suite asserts exactly that for
every importer over the sample catalog (with `pending == 0`).

### `to_metadata()`

```json
{"succeeded": 120, "failed": 2, "pending": 0, "skipped_unchanged": 878, "error_count": 2}
```

`error_count` is `len(errors)`, the number of error entries, not the number of
failed rows: one entry can name many rows.

### `merge(other)`

Sums the counts and concatenates `errors`. Useful to combine several importer
results into one asset's metadata.

### Error entries

Entries produced by the executor and the importers:

```json
{
  "row_ids": ["TEE-RED-M"],
  "status": "failed",
  "status_code": 400,
  "message": "400 Client Error: Bad Request for url: https://magento.example.com/rest/all/V1/products - response body: {\"message\": \"...\"}"
}
```

| Key | Meaning |
| --- | --- |
| `row_ids` | Row references this entry covers (SKU, path, code, `source/sku`, `stock/source` depending on the importer). |
| `status` | `"failed"` or `"pending"`. |
| `status_code` | HTTP status of the failed request; `None` for items rejected inside a 2xx answer, bulk operation failures, validation and planning errors. |
| `message` | Human readable reason. For HTTP errors it embeds the first 1000 characters of the response body, without Magento's `trace` key. |

`MagentoResource.upload_rows` produces a slightly different shape: `chunk_index`,
`row_ids`, `status_code`, `message`, and no `status`.

## `RowError`

`from dagster_magento import RowError`

A frozen dataclass `RowError(row_ref: str, message: str)` for a row that never
reached Magento: a validation error from the row model, a reference the
resolver cannot resolve (`unknown attribute: color`, `unknown attribute set:
Shoes`, `unknown category path: ...`), a writer rule (`attributes shadow
writer-owned keys: price`), or an unreadable media gallery. Importers turn
each into an error entry with `status_code: None`. The file mappers in
`dagster_magento.formats` return `RowError` values as well; see
[File-Formats](File-Formats).

## `MagentoImportError` and the error ratio

```python
from dagster_magento.executor import MagentoImportError, check_error_ratio
```

`check_error_ratio(result, fail_on_error_ratio)`:

| `fail_on_error_ratio` | Behaviour |
| --- | --- |
| `None` (default everywhere) | Never raises. |
| a float, for example `0.05` | Raises `MagentoImportError` when `failed / (succeeded + failed + pending) > fail_on_error_ratio`. |

- `skipped_unchanged` is not in the denominator.
- `pending` is in the denominator but is not counted as failed.
- A result with nothing in the denominator never raises.
- The comparison is strict: a ratio equal to the threshold passes.

Message: `Error ratio 12.50% (5 failed of 40) exceeds fail_on_error_ratio=5.00%`.

The ratio is applied in two places, both defaulting to `None`:

1. Every importer takes `fail_on_error_ratio` and checks it before returning.
   `import_categories` checks it once, on the result merged with its
   `default_sort_by` retry, so a row the retry repairs on 2.4.6 never counts as
   a failure. It logs `Magento import result: {...}` first, so the counts are
   in the run log even when it raises.
2. `to_materialize_result(result, fail_on_error_ratio=None)` builds a
   `MaterializeResult(metadata=result.to_metadata())`, logs the counts, then
   checks. If it raises, the asset fails and the metadata is not attached;
   the counts are only in the log.

`MagentoImportError` is also raised outside the ratio check, always about the
optional bridge with `use_bridge="require"`: a missing capability, a failed
bridge product snapshot, or a failed category upsert. See
[Optional-Bridge](Optional-Bridge).

## `MagentoAuthError` always aborts

```python
from dagster_magento.resource import MagentoAuthError
```

Raised when the admin token request fails. It is not an `HTTPError`, so none
of the library's per-row catches handle it: the import stops immediately,
the asset fails, and no partial `UploadResult` is returned. Rows already sent
before it stay written. See [MagentoResource](MagentoResource).

## Other exceptions that escape an importer

| Exception | Typical cause |
| --- | --- |
| `requests.ConnectionError`, `requests.Timeout` | Network failure or a request longer than 30 s. Not retried. |
| `requests.exceptions.HTTPError` from a snapshot or lookup | The snapshot reads (`GET products`, the price information POSTs, `GET inventory/source-items`) and some lookups (`GET store/websites` in `import_prices`, `GET inventory/stocks`, `GET configurable-products/{sku}/children`) are not wrapped per row. A failure there raises out of the importer. The media gallery read is the exception: it fails only that SKU's row. |
| `MagentoImportError` | Ratio exceeded, or a bridge failure in `require` mode. |

## How Magento answers become row outcomes

### Single requests

`POST products`, `PUT products/{sku}`, `PUT categories/{id}`, media and the
like: a 2xx means every row in the operation's `row_refs` succeeded; an
`HTTPError` (after retries) fails them with the status code and body.

### List endpoints

Price storage (`products/base-prices`, `special-price`, `tier-prices`,
`tier-prices-delete`) and inventory list endpoints take many items in one
request. A transport level `HTTPError` fails every row of that request. A 2xx
answer is a list of rejected items, which the executor attributes:

1. **Named parameters.** `parameters` may be a dict
   (`{"SKU": "X", "storeId": "1"}`) or a positional list. For a list, values
   map to the distinct `%name` placeholders in the order the message first
   names them; a repeated placeholder takes one value. The generic form
   `Invalid attribute %fieldName = %fieldValue.` also maps `fieldName`'s value
   to `fieldValue`, so `["SKU", "X"]` reads as `{"SKU": "X"}`. With numbered
   placeholders (`%1`), every string value is a SKU candidate.
2. **Match on SKU only.** Only a parameter named `SKU` (any case) is compared
   with the operation's SKU. Price and quantity values never are, so a
   rejected price of `5` cannot fail the row whose SKU is `"5"`.
3. **Scope must match.** If the item also names `storeId`/`store_id` or
   `sourceCode`/`source_code`, that value must equal the operation's own
   `store_id` or `source_code`. A failure for SKU X in store 1 leaves X's
   store 0 item succeeded.
4. **Pessimistic rule.** An item that names no row (for example a negative
   price, answered as `["Price", "-5"]`) cannot be pinned on one row. Every
   row of that request not already failed is then failed with
   `rejected item(s) in this request name no row, failing every row: <message>`.
   Reporting them as succeeded could hide a real rejection.

The message is filled from the parameters:

```text
"Requested store is not found. Row ID: SKU = %SKU, Store ID: %storeId." + ["X", "1"]
-> "Requested store is not found. Row ID: SKU = X, Store ID: 1."

"Invalid attribute SKU = %SKU. Row ID: SKU = %SKU, Website ID: %websiteId, Customer Group: %customerGroup, Quantity: %qty."
+ ["ABC", "1", "ALL GROUPS", "2"]
-> "Invalid attribute SKU = ABC. Row ID: SKU = ABC, Website ID: 1, Customer Group: ALL GROUPS, Quantity: 2."
```

### Bulk operations

| Bulk answer | Row outcome |
| --- | --- |
| Submission `HTTPError`, no `bulk_uuid` in the body | All rows of the chunk failed. |
| Submission `HTTPError` with `bulk_uuid` | Rejected `request_items` failed with their errors; accepted ones polled. |
| Operation status 1 | succeeded |
| Status 2 | resubmitted once; status 2 again means failed |
| Status 3 or 5 | failed with `result_message` |
| Status 4 or missing at the 600 s deadline | pending |

See [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution).

### Folding to one outcome per row

A product row can become several operations (main save, store-value saves,
configurable options and links); a price row up to three parts plus tiers.
The importer folds them: a row is **failed** if any of its operations failed
or planning failed it, else **pending** if any is pending, else
**succeeded**. Rows are matched by reference, so two price rows for one SKU
in two stores share the reference `SKU`: a failure on either store marks both
rows failed in the counts.

## What is never counted as success

- `pending` rows. An open bulk operation may still run later, or never.
- A row in a list request that contained a rejected item naming no row.
- A follow-up (configurable option or child link) is not even sent when its
  parent save failed or is pending; the row is counted once, from the parent.

## Symptom to meaning

| Symptom | Meaning | Where to look |
| --- | --- | --- |
| Run fails with `MagentoAuthError: Failed to fetch Magento admin token` | Wrong credentials, locked admin, admin 2FA enforced on the token endpoint, or `base_url`/`store_view` wrong. | [MagentoResource](MagentoResource) |
| `pending` > 0 and log says `check that the 'async.operations.all' message queue consumer is running` | Bulk operations still open after 600 s: no consumer, too few consumers, a crashed consumer, or MariaDB under `REPEATABLE READ` dropping messages. | [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution) |
| Every row of a price request failed with `rejected item(s) in this request name no row` | One item in that request was invalid (negative price, ...) and Magento did not say which. | Validate the values; rerun. |
| `Invalid attribute SKU = X.` on a price row | SKU `X` does not exist in Magento. Import products first. | [Price-Import](Price-Import) |
| `unknown attribute: <code>` | The code is not an attribute in Magento. Import attributes first, or fix the column. | [Catalog-Importers](Catalog-Importers) |
| `unknown attribute set: <name>` | The set does not exist (names match case-insensitively). | [Catalog-Importers](Catalog-Importers) |
| `attributes shadow writer-owned keys: ...` | `attributes` contains a key the writer sets itself (`price`, `name`, `status`, ...). Use the model field. | [Row-Models](Row-Models) |
| `special_to is before special_from` | Inverted special price range, rejected before sending. | [Price-Import](Price-Import) |
| `unknown website code: <code>` | A tier price website code is not `all`, a number, or a website code. | [Price-Import](Price-Import) |
| `The Product with the ... SKU doesn't exist` on a bundle or grouped parent | A child is missing from Magento or failed in the same run. | [Dagster-Assets](Dagster-Assets) |
| `attribute set not converged after 3 passes` | The set's groups or assignments kept planning new operations. | [Catalog-Importers](Catalog-Importers) |
| `cannot read media gallery: ...` | `GET products/{sku}/media` failed for that SKU (often: product does not exist). | [Catalog-Importers](Catalog-Importers) |
| `MagentoImportError: the bridge is required ...` | `use_bridge="require"` but the module does not offer a capability. | [Optional-Bridge](Optional-Bridge) |
| `MagentoImportError: Error ratio ...` | Your `fail_on_error_ratio` was exceeded; the counts are in the log line before it. | this page |
| `skipped_unchanged` equals the row count on a rerun | Expected: the diff proved every row already matches. | [Architecture](Architecture) |
| Second run still reports products as succeeded | Rows with `store_values` or type-specific parts are never proven unchanged and are always rewritten. | [Catalog-Importers](Catalog-Importers) |

## Example: inspecting a result

```python
from dagster_magento import import_source_items

result = import_source_items(magento, rows)
for error in result.errors:
    if error["status"] == "failed":
        print(error["row_ids"][:10], error["status_code"], error["message"][:200])
print(result.to_metadata())
```

## See also

- [Home](Home)
- [MagentoResource](MagentoResource)
- [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution)
- [Price-Import](Price-Import)
- [Catalog-Importers](Catalog-Importers)
- [Dagster-Assets](Dagster-Assets)
- [Troubleshooting](Troubleshooting)
