# Sync and Bulk Execution

`dagster_magento.executor.execute()` is the step that actually sends a plan of
`Operation` values to Magento. It has two modes. `sync` sends ordinary REST
requests and knows the outcome as soon as each response arrives. `bulk`
submits operations to Magento's asynchronous bulk API (`async/bulk/V1/...`),
then polls the bulk status until every operation has run, so the outcome
depends on Magento's message queue consumers. Both modes order work by
phase, so a composite parent is saved only after its children. This page
explains chunking, phases, retries, polling, pending semantics, and what the
Magento side needs for bulk mode to work.

## The function

```python
from dagster_magento.executor import execute

result = execute(
    resource,
    operations,          # list[Operation], usually PlanResult.operations
    mode="sync",         # "sync" or "bulk"
    chunk_size=None,     # None: 1000 for sync list endpoints, 200 for bulk
    bulk_timeout_s=600,  # per bulk wait
    poll_interval_s=2.0, # between status polls
)
```

| Parameter | Default | Applies to |
| --- | --- | --- |
| `mode` | `"sync"` | Both. |
| `chunk_size` | `None`, meaning 1000 (sync list endpoints) or 200 (bulk) | Sync list operations, bulk submissions. Single sync operations always go one per request. |
| `bulk_timeout_s` | `600` | Each `wait_bulk` call in bulk mode. |
| `poll_interval_s` | `2.0` | Sleep between status polls. |

The importers call `execute(resource, plan.operations, mode=mode)` and expose
only `mode`: through an importer, chunk sizes, the 600 s timeout and the 2 s
poll interval are fixed. Call `execute()` yourself (with a writer's plan) if
you need different values.

The result is an `UploadResult` counted per operation `row_refs`; the
importers then fold it per row. See [Results-and-Errors](Results-and-Errors).

## Which operations can go bulk

An operation runs through the bulk API only when its writer gave it a
`BulkSpec`. Everything else runs synchronously, even with `mode="bulk"`.

| Writer | Operation | Bulk endpoint | Phase |
| --- | --- | --- | --- |
| products | create `POST products` | `products` | 0, or 1 for a grouped or bundle parent with links |
| products | update `PUT products/{sku}` | `products/bySku` | same as above |
| products | store value `PUT products/{sku}` with `store_code` | `products/bySku` | same as the row's main save |
| products | disable (`status = 2`) | `products/bySku` | 0 |
| product types | configurable option `POST configurable-products/{sku}/options` | `configurable-products/bySku/options` | 1 |
| product types | configurable child link `POST configurable-products/{sku}/child` | `configurable-products/bySku/child` | 1 |
| media | add image `POST products/{sku}/media` | `products/bySku/media` | 0 |
| media | delete image | none, always sync | - |
| attributes, attribute sets, categories, sources, stocks, links, source items | all | none, always sync | - |
| prices | all | none; `import_prices` forces sync and ignores `mode` | - |

In bulk mode the operations without a `BulkSpec` run first, through the sync
path, with a log line `N operation(s) without a bulk spec running through sync
mode`. For media that means old images are deleted before new ones are
queued.

## Sync mode

```text
for each phase, ascending:
    list operations   -> group by (method, endpoint, store_code, list_key)
                      -> chunk to 1000, keeping chunk_key units together
                      -> one request per chunk: {list_key: [payload, ...]}
    single operations -> one request each, in plan order
```

- **List endpoints** (`products/base-prices`, `products/special-price`,
  `products/tier-prices`, `products/tier-prices-delete`,
  `inventory/source-items`, `inventory/stock-source-links`) take many items in
  one body. Operations that share `(method, endpoint, store_code, list_key)`
  are merged:

  ```json
  POST /rest/all/V1/products/base-prices
  {"prices": [{"sku": "A", "price": 9.99, "store_id": 0}, {"sku": "B", "price": 5, "store_id": 0}]}
  ```

  A 2xx answer is a list of rejected items (empty when everything was
  accepted), which the executor maps back onto rows; an HTTP error fails every
  row in the chunk. See [Results-and-Errors](Results-and-Errors).
- **chunk_key**: list operations with the same non-None `chunk_key` that are
  consecutive in the plan form a unit that is never split across two
  requests. A unit that would overflow the current chunk starts a new one; a
  unit larger than the chunk size is sent alone. The pricing writer sets
  `chunk_key=sku` on tier prices in replace mode, because a replace call
  overwrites a SKU's tiers with what that one request carries.
- **Single operations** go one request each. An `HTTPError` fails that
  operation's rows and the loop continues.
- **Phases**: sync mode honours `BulkSpec.phase` too. A grouped or bundle
  parent that carries its links inline is in phase 1, so it is saved after
  every phase 0 save even if it comes first in the file. Within a phase, all
  list operations are sent before the single operations, and each kind keeps
  the plan order.

## Bulk mode

```text
non-bulk ops ------------------------------------------------> sync path
bulk ops -> group by (phase, method, bulk endpoint, store_code)
         -> for each group in ascending phase order:
              for each chunk of 200:
                  submit_bulk(...)  -> bulk_uuid
                  wait_bulk(...)    -> status per operation
                  status 2 ops      -> resubmit once as a new bulk, wait again
```

Each chunk is submitted and waited on before the next chunk is submitted.
Throughput therefore comes from Magento processing the 200 operations of one
bulk in parallel (several consumer processes), not from overlapping bulks.

### Request shape

`submit_bulk` sends a bare JSON array, one element per operation, to the bulk
route with the same verb as the synchronous call:

```json
PUT /rest/all/async/bulk/V1/products/bySku
[
  {"sku": "A", "product": {"sku": "A", "name": "Tee A", "custom_attributes": []}},
  {"sku": "B", "product": {"sku": "B", "status": 2}}
]
```

Store value updates are grouped separately by `store_code`, so a bulk sent to
`/rest/fr/async/bulk/V1/products/bySku` carries only `fr` values. The live
suite checks that such an update keeps its store scope.

### Why phases (composite parents after children)

A grouped parent carries its links inline (`product_links`) and a bundle
parent its selections (`extension_attributes.bundle_product_options`).
Magento validates the referenced SKUs while it saves the parent. In one bulk,
consumers process operations concurrently, so the parent can run before its
child exists and fail with "The Product with the ... SKU doesn't exist" (seen
live on 2.4.6). Configurable options and child links act on a parent that
must already be saved and failed the same way ("The product can't be
saved."). Those operations are planned into phase 1, and the executor submits
phase 1 only after every phase 0 bulk has finished waiting.

`import_products` adds one more ordering on top: it executes all
`products...` operations first, then the configurable follow-ups in a second
`execute()` call, and drops follow-ups for parents that failed or are pending.

### Polling: `wait_bulk`

`wait_bulk(resource, bulk_uuid, count, timeout_s=600, poll_interval_s=2.0, skip_ids=frozenset())`
calls `GET bulk/{uuid}/detailed-status` until no operation (except those in
`skip_ids`) is open (status 4) or missing from `operations_list`, or until the
deadline. It never raises on timeout; it returns the last statuses.

Statuses are matched by operation `id`, not list order: operation id `i` is
the `i`th item submitted. Magento does not sort `operations_list` by id
(the recorded fixture lists ids 0, 2, 1).

```json
{
  "bulk_id": "92c4131a-0ad6-4670-a009-d6e7f56ac57a",
  "operation_count": 3,
  "operations_list": [
    {"id": 0, "status": 1, "result_message": "Service execution success ..."},
    {"id": 2, "status": 1, "result_message": "Service execution success ..."},
    {"id": 1, "status": 3, "result_message": "The \"sku\" attribute value is empty. Set the attribute and try again."}
  ]
}
```

### Outcome per operation

| Final status | Outcome |
| --- | --- |
| 1 complete | succeeded |
| 2 retriably failed, first time | resubmitted once in a new bulk with only those operations |
| 2 retriably failed, on the resubmission | failed, message from `result_message` or `bulk operation status 2` |
| 3 not retriably failed, 5 rejected | failed with `result_message` |
| 4 open, or missing, at the deadline | pending, message `operation still open at the bulk wait timeout` unless Magento gave one |
| item rejected at submission | failed with that item's errors |

When any row is pending the executor logs: `N row(s) on <endpoint> still
pending after 600s - check that the 'async.operations.all' message queue
consumer is running`.

### Pending semantics

`pending` means "Magento did not report an outcome before the deadline". It
is never counted as success, it is not retried, and the library does not come
back to it later. The operation may still run after the import returned (a
slow consumer), or never run (no consumer, or the MariaDB race below). Treat
a non-zero `pending` as "state unknown"; rerunning the import will diff and
re-send whatever did not land. `check_error_ratio` counts pending in the
denominator but not as failed.

### Submission errors and partial submission

| Submission answer | Handling |
| --- | --- |
| 2xx with `bulk_uuid` | Poll the bulk. |
| HTTP error with no `bulk_uuid` in the body | Every row of the chunk fails with the error message. The run continues with the next chunk and the next phases. |
| HTTP error whose body carries `bulk_uuid` (top level or under `parameters`) | Magento scheduled the accepted items. Items in `request_items` with `status: "rejected"` fail with their `errors`; the accepted ones are polled as usual, skipping the rejected ids. |
| `MagentoAuthError` | Aborts the run. |

A malformed item can also make Magento refuse the whole submission. Recorded
on the sandbox: one item `{"foo": 1}` among two good products produced a 400
with no `bulk_uuid`, and neither good product was created:

```json
{"message": "\"%fieldName\" is required. Enter and try again.", "parameters": {"fieldName": "product"}}
```

The executor does not inspect `request_items` in a 2xx answer: it assumes
Magento reports any rejected item through an error status, as above.

## What the Magento side needs for bulk mode

### A running consumer

Bulk operations are executed by the `async.operations.all` consumer.
Without one they stay at status 4 and end as `pending` after the timeout.

```
bin/magento queue:consumers:start async.operations.all
```

Run several processes for throughput. Two ways:

- Start several processes yourself under a supervisor. The library's sandbox
  starts four in the background and sets `cron_run` to `false` in
  `cron_consumers_runner`, so cron does not start more.
- Let Magento's cron start them: `cron_consumers_runner` in `app/etc/env.php`
  with `cron_run` true and `multiple_processes`, for example:

  ```php
  'cron_consumers_runner' => [
      'cron_run' => true,
      'max_messages' => 0,
      'consumers' => ['async.operations.all'],
      'multiple_processes' => ['async.operations.all' => 4],
  ],
  ```

  This is standard Magento configuration; the library does not depend on
  which way you choose.

A store view used by a bulk (a `store_values` entry) must exist before the
consumers start: a running consumer caches the store list. Restart consumers
after creating a store view.

### Indexers in schedule mode and cron

Bulk writes and price storage writes are picked up by the indexers'
materialized views. Set the indexers to `schedule`
(`bin/magento indexer:set-mode schedule`) and run cron, so the price and
inventory indexes catch up after the writes. The library never reindexes or
flushes caches itself. See [Price-Import](Price-Import) for what the live
suite measured on the storefront.

### MariaDB: READ COMMITTED

On MariaDB under its default `REPEATABLE READ` isolation, a bulk can lose
operations, and no client can recover them. Magento publishes the bulk's
messages before it commits the rows they belong to:
`MassSchedule::publishMass` publishes through `BulkManagement::scheduleBulk`
and commits, and only then does `SaveMultipleOperations::execute` insert the
`magento_operation` rows. A consumer that reaches its row while that insert
is still uncommitted fails with `SQLSTATE[HY000]: General error: 1020 Record
has changed since last read in table 'magento_operation'`, and
`MassConsumerEnvelopeCallback::execute` rejects the message without requeue.
The operation keeps status 4 with `started_at` NULL forever, and each loss
shows as one `Message has been rejected: ... 1020 ...` line in
`var/log/system.log`. The library reports those rows as `pending`.

Setting the session isolation to `READ COMMITTED` removes the race:

```php
// app/etc/env.php: Magento passes driver_options to PDO.
// 1002 is PDO::MYSQL_ATTR_INIT_COMMAND.
$config['db']['connection']['default']['driver_options'][1002] =
    'SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED';
```

Measured by the project on Magento 2.4.9 with MariaDB 11.8 and four consumers
(recorded in the docstring of
`tests/live/test_catalog_e2e.py::test_same_catalog_imports_in_bulk_mode`):
30 of 88 published operations were lost before the setting, none after it.
Whether MySQL is affected the same way is not recorded by the project.

## Choosing a mode

| Situation | Mode |
| --- | --- |
| Prices | Irrelevant: always sync through the price storage list endpoints. |
| Stock quantities | Irrelevant: source items are always sync list calls (1000 per request). |
| A few hundred products, no consumer available | `sync` |
| Large product or media volumes with consumers, schedule indexers and cron in place | `bulk` |
| You need the outcome of every row before the asset finishes | `sync`, or `bulk` with a generous consumer capacity so nothing ends `pending` |

## Gotchas

- `mode="bulk"` without a consumer does not fail fast: each chunk waits the
  full 600 s before reporting pending.
- A consumer process that crashes leaves its operations open. On 2.4.6 an
  item without a SKU crashes the consumer with an uncaught `TypeError`; the
  row model makes `sku` mandatory, but a hand written `submit_bulk` can send
  one. Check the consumers if a bulk stays open.
- `upload_rows_async` on the resource is a separate, lower level path: it
  submits and returns `bulk_uuids` without polling, retrying or mapping
  statuses to rows.

## See also

- [Home](Home)
- [Architecture](Architecture)
- [MagentoResource](MagentoResource)
- [Results-and-Errors](Results-and-Errors)
- [Price-Import](Price-Import)
- [Catalog-Importers](Catalog-Importers)
- [Compatibility](Compatibility)
- [Troubleshooting](Troubleshooting)
