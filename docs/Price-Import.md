# Price Import

`import_prices` writes base prices, special prices and tier prices through
Magento's native price storage API: list endpoints under `/V1/products/...`
that take up to thousands of prices per request and do not save the product
entity. It merges rows per SKU and store, reads the current prices from the
matching `*-information` endpoints, skips rows that already match, and sends
the rest in 1000-item requests, always synchronously. This page documents the
endpoints, the merge and diff rules, store scope, how the change reaches the
storefront, the project's benchmark against full product saves, and the
validation the library adds on top of Magento.

## Signature

```python
from dagster_magento import import_prices

result = import_prices(
    resource,
    rows,                      # list of PriceRow or dicts
    mode="sync",               # ignored: price storage has no bulk route
    diff=True,
    behavior="upsert",         # accepted for a uniform signature, not used
    fail_on_error_ratio=None,
    use_bridge="auto",         # accepted, not used by this importer
)
```

## The row

`PriceRow` fields (see [Row-Models](Row-Models) for the full model):

| Field | Type | Default | Sent as |
| --- | --- | --- | --- |
| `sku` | `str` | required | `sku` |
| `price` | `float` or `None` | `None` | base price; `None` means "do not touch" |
| `store_id` | `int` | `0` | `store_id` of base and special price items |
| `special_price` | `float` or `None` | `None` | special price; `None` means "do not touch" |
| `special_from`, `special_to` | `str` or `None` | `None` | `price_from`, `price_to`; omitted when `None` |
| `tiers` | `list[TierPrice]` or `None` | `None` | tier prices; `None` means "do not touch", `[]` means "remove all" |

`TierPrice`: `qty` (required), `price` (required), `customer_group`
(default `"ALL GROUPS"`), `website` (default `"all"`), `price_type`
(`"fixed"` or `"discount"`, default `"fixed"`).

### Inverted special range is rejected

If both `special_from` and `special_to` parse with
`datetime.fromisoformat` and `special_to` is earlier than `special_from`, the
row fails validation with `special_to is before special_from`. Magento itself
stores such a range and answers success (a recorded probe returned `200 []`),
so the library is the only gate. Dates that do not parse are not checked here.

```python
result = import_prices(magento, [{"sku": "S", "special_price": 5, "special_from": "2027-02-01", "special_to": "2027-01-01"}])
# result.failed == 1, nothing sent for that row
```

## Endpoints used

| Purpose | Method and endpoint | Body |
| --- | --- | --- |
| Read base prices | `POST products/base-prices-information` | `{"skus": [...]}`, 1000 SKUs per call |
| Read special prices | `POST products/special-price-information` | same |
| Read tier prices | `POST products/tier-prices-information` | same |
| Write base prices | `POST products/base-prices` | `{"prices": [{"sku", "price", "store_id"}, ...]}` |
| Write special prices | `POST products/special-price` | `{"prices": [{"sku", "price", "store_id", "price_from"?, "price_to"?}, ...]}` |
| Replace tier prices | `PUT products/tier-prices` | `{"prices": [{"sku", "price", "price_type", "website_id", "customer_group", "quantity"}, ...]}` |
| Add tier prices | `POST products/tier-prices` | same shape (only when you call the writer with `tier_mode="add"`) |
| Delete tier prices | `POST products/tier-prices-delete` | `{"prices": [<items as read from tier-prices-information>]}` |
| Website codes | `GET store/websites` | only when a row carries tiers |

`products/special-price-delete` is not used by the library: there is no way to
remove a special price through `import_prices`. A row without
`special_price` leaves existing special prices alone. If you need removal,
call `POST products/special-price-delete` yourself with
`resource.post("products/special-price-delete", {"prices": [...]})`.

Example of one request the executor builds (two rows, same endpoint, same
scope):

```json
POST /rest/all/V1/products/special-price
{
  "prices": [
    {"sku": "TEE-RED-M", "price": 14.9, "store_id": 0, "price_from": "2027-01-15 08:30:00", "price_to": "2027-02-15 23:59:59"},
    {"sku": "TEE-RED-L", "price": 15.9, "store_id": 0}
  ]
}
```

A 200 answer is the list of rejected items, `[]` when everything was accepted.

## Why not full product saves

Writing a price with `PUT products/{sku}` (or its bulk form
`PUT async/bulk/V1/products/bySku`) is a full product save: the product
repository loads and saves the product with all its attributes and plugins,
one product per operation. The price storage endpoints write only price rows,
many per request. The project measured the difference with
`scripts/bench_prices.py --rows 10000`. These are the numbers recorded when the
script was added (the 0.3.0 work); rerun the script to measure your own setup:

Setup of that measurement: Magento 2.4.9, PHP 8.5, MariaDB 11.8, 12 cores and
30.5 GiB RAM on the host, four `async.operations.all` consumers, 10,000 base
prices over a seeded catalog, three runs, wall clock of the whole write with
consumer time included. Path A submitted price-only payloads to
`PUT async/bulk/V1/products/bySku` without a diff; path B was `import_prices`
with `diff=False`.

| Run | Path A: async bulk `products/bySku` | Path B: `import_prices` | Ratio |
| --- | --- | --- | --- |
| 1 | 507.3 s (19.7 rows/s) | 15.2 s (657.9 rows/s) | 33x |
| 2 | 587.6 s (17.0 rows/s) | 7.9 s (1261.4 rows/s) | 74x |
| 3 | 228.6 s (43.7 rows/s) | 7.8 s (1276.4 rows/s) | 29x |
| median | 507.3 s | 7.9 s | 64x |

Path A's wall clock follows consumer throughput, which is why it varies
between runs. No other figures are published by the project; measure on your
own hardware before sizing.

## Merging rows

Before anything is read, rows are merged (`_merge_prices`):

- Base and special parts merge per `(sku, store_id)`. A later row's non-None
  `price`, `special_price`, `special_from` or `special_to` overrides an
  earlier one's. A later `None` never erases an earlier value.
- Tiers are not store scoped: the last non-None `tiers` list per SKU wins and
  rides on that SKU's first merged row.
- Rows for one SKU in different stores stay separate rows and separate items.
- Every row folded into another counts as `skipped_unchanged`.

```python
rows = [
    {"sku": "A", "price": 10},
    {"sku": "A", "special_price": 8},                 # merges into the row above
    {"sku": "A", "price": 11, "store_id": 1},         # separate row (store 1)
    {"sku": "A", "tiers": [{"qty": 5, "price": 9}]},  # tiers ride on the store 0 row
]
# merged: (A, 0) price 10, special 8, tiers [5 @ 9]; (A, 1) price 11
# skipped_unchanged starts at 2
```

## Diff against the information endpoints

`snapshot_prices` merges the three information answers per SKU as
`{"base": {store_id: price}, "special": [items], "tiers": [items]}`. A merged
row is skipped only when every part it sets matches:

| Part | Compared with | Normalization |
| --- | --- | --- |
| base price | `base[row.store_id]` | decimal to 4 places |
| special price | any special item with the same `store_id` whose `(price, price_from, price_to)` equals the row's | price to 4 places; dates as UTC `YYYY-MM-DD HH:MM:SS`, a naive date taken as UTC |
| tiers | the full tier set of the SKU, order ignored | website code resolved (`all` is 0, digits are an id, otherwise from `GET store/websites`); customer group case-insensitive; qty and price to 4 places |

A SKU absent from all three answers is always written. An unknown tier
website code counts as changed so the writer can fail the row with
`unknown website code: <code>`. With `diff=False` every merged row is written.

The live suite checks that special price dates round trip: the dates are
stored and read back as given, with no shift to or from the store timezone.

## Tier prices: replace versus add

`import_prices` always plans tiers in `replace` mode (the writer's default;
the importer has no parameter for it):

| `tiers` on the row | Operations |
| --- | --- |
| `None` | none |
| non-empty | one `PUT products/tier-prices` item per tier, all with `chunk_key=sku` so they always travel in the same request |
| `[]` | one `POST products/tier-prices-delete` item per tier the snapshot found for that SKU; nothing if there were none |

The replace call overwrites the tier prices of the SKUs in the request, which
is why the executor never splits one SKU's tiers across two requests.

`add` mode exists on the writer, `dagster_magento.writers.pricing.plan_prices(rows,
tier_mode="add", ...)`: it `POST`s each tier and never deletes. To use it,
build the plan and run it with `execute()` yourself:

```python
from dagster_magento.executor import execute
from dagster_magento.models import PriceRow
from dagster_magento.writers.pricing import plan_prices

rows = [PriceRow(sku="A", tiers=[{"qty": 10, "price": 7.5}])]
plan = plan_prices(rows, tier_mode="add", website_ids={"base": 1})
result = execute(magento, plan.operations, mode="sync")
```

That path skips the merge, the diff and the per-row fold.

## Store scope

- Base and special prices carry `store_id` in each item; the importer does not
  use a REST store scope for them (requests go through the resource's
  `store_view`).
- Tier prices carry `website_id` (0 for all websites).
- Whether a non-zero `store_id` takes effect is decided by Magento's own price
  scope configuration. The library does not read that setting.
- Result counting is per SKU: the executor attributes a rejected item to the
  exact store it names, but the importer folds outcomes by SKU, so if the
  store 1 item of SKU `A` is rejected, every changed row of `A` (store 0
  included) is reported failed in `import_prices`' counts. The `errors` entry
  names the store in its message.

## Getting the change to the storefront

The library does not flush caches or reindex. Price storage writes are picked
up by Magento's price indexer through its materialized view when indexers are
in `schedule` mode and cron runs. The live test
`tests/live/test_price_storefront.py::test_price_change_reaches_storefront_after_cron_with_fpc`
checks this on a sandbox with full page cache: it confirms the product page is
a cache hit, writes a new base price with `import_prices`, then runs cron up
to four times, 30 s apart, and asserts the new price is rendered and the old
one is gone. It does not assert how many cron runs that takes.

If your store runs indexers in `realtime` mode, or without cron, the behavior
is Magento's and was not measured by the project.

## Rejections

Price storage answers 200 with a list of rejected items. Recorded on the
sandbox:

```json
[{"message": "Invalid attribute %fieldName = %fieldValue.", "parameters": ["SKU", "probe-price-...-does-not-exist"]}]
```

```json
[{"message": "Invalid attribute %fieldName = %fieldValue.", "parameters": ["Price", "-5"]}]
```

The first names a SKU and fails only that row, with the message filled as
`Invalid attribute SKU = probe-price-...-does-not-exist.`. The second names no
row, so every row of that request fails with it. See
[Results-and-Errors](Results-and-Errors) for the attribution rules.

## Gotchas

- `mode="bulk"` has no effect here; prices are always sync, so no consumer is
  needed for prices.
- A negative or otherwise invalid price fails the whole 1000-item request in
  the result, because Magento's answer does not say which row it was. Validate
  prices upstream if partial success matters.
- Price information reads are POSTs: a 502, 503 or 504 on them is not retried
  and fails the import call.
- `special_price` cannot be removed through `import_prices`.
- Tiers in a row for a non-zero store still apply to the whole SKU.
- The native `advanced_pricing.csv` mapper (`prices_from_rows`) produces one
  `PriceRow` per SKU carrying only `tiers`. Importing such a file therefore
  replaces each listed SKU's whole tier set with the file's tiers. See
  [File-Formats](File-Formats).

## See also

- [Home](Home)
- [Catalog-Importers](Catalog-Importers)
- [Row-Models](Row-Models)
- [File-Formats](File-Formats)
- [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution)
- [Results-and-Errors](Results-and-Errors)
- [Troubleshooting](Troubleshooting)
