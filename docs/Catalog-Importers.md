# Catalog Importers

`dagster_magento` ships one import function per catalog entity:
`import_attributes`, `import_attribute_sets`, `import_categories`,
`import_products`, `import_prices`, `import_sources`, `import_stocks`,
`import_stock_source_links`, `import_source_items` and `import_media`. Each one
validates your rows against a [row model](Row-Models), reads what it needs from
Magento first (a snapshot or a resolver cache), plans plain REST operations,
executes them in `sync` or `bulk` mode, and returns one `UploadResult` that
counts every input row exactly once as `succeeded`, `failed`, `pending` or
`skipped_unchanged`. This page documents, per importer, what it reads, what it
writes, which parameters it honours and when a row counts as unchanged. All
statements below were checked against `dagster_magento/importers.py`,
`writers/*`, `diff.py`, `resolvers.py` and `executor.py` at version 0.4.1.

## Common signature

Every importer has the same keyword parameters:

```python
from dagster_magento import import_products

result = import_products(
    resource,                 # a MagentoResource
    rows,                     # list of row models or plain dicts
    mode="sync",              # "sync" or "bulk"
    diff=True,                # skip rows a snapshot proves unchanged (where supported)
    behavior="upsert",        # "upsert", "create_only", "update_only", "disable" (where supported)
    fail_on_error_ratio=None, # float in [0, 1], or None to never raise
    use_bridge="auto",        # "auto", "never", "require"
)
```

The one exception is `import_stock_source_links`, which takes an extra
`stock_ids` parameter in third position:

```python
import_stock_source_links(resource, rows, stock_ids=None, mode="sync", diff=True,
                          behavior="upsert", fail_on_error_ratio=None, use_bridge="auto")
```

Always pass `mode` by keyword to that function: a positional third argument is
`stock_ids`.

### Parameters

| Parameter | Default | Meaning |
| --- | --- | --- |
| `rows` | required | A list of pydantic row models or dicts. Model instances are dumped with `exclude_unset=True` and re-validated, so the set of fields you explicitly set survives (this matters for [partial product updates](Row-Models#partial-update-semantics)). |
| `mode` | `"sync"` | `"sync"` sends ordinary REST calls. `"bulk"` sends operations that have a bulk route through `async/bulk/V1/...` and polls them; operations without one still run synchronously. See [Sync and Bulk Execution](Sync-and-Bulk-Execution). |
| `diff` | `True` | Honoured by `import_products`, `import_prices` and `import_source_items` only. Every other importer accepts it for a uniform signature and ignores it. |
| `behavior` | `"upsert"` | Fully honoured by `import_products` only. `import_attributes` honours `create_only`. Every other importer ignores it. |
| `fail_on_error_ratio` | `None` | When set, the importer raises `MagentoImportError` after logging its counts if `failed / (succeeded + failed + pending)` is strictly greater than the ratio. `None` never raises. |
| `use_bridge` | `"auto"` | How to treat the optional `DDTCoreX_DagsterBridge` module. See [Optional Bridge](Optional-Bridge). |

### What every importer returns

An `UploadResult` with `succeeded`, `failed`, `pending`, `skipped_unchanged`
and `errors`. Each error is a dict
`{"row_ids": [...], "status": "failed" | "pending", "status_code": int | None, "message": str}`.
Counting rules (from `importers._fold_by_row` and `_complete`):

- A row that becomes several operations (a configurable with options and child
  links, a price row with base, special and tier parts) is `failed` if any of
  its operations failed, else `pending` if any is still pending, else
  `succeeded`.
- A row that fails validation counts as `failed`, with the pydantic message.
- A row the writer planned no operation for, that neither failed nor was
  skipped by a behaviour, is already in the desired state and counts as
  `skipped_unchanged`. Rows skipped by `create_only`/`update_only` also count
  as `skipped_unchanged`, and so do duplicate rows that `import_prices` and
  `import_source_items` fold into a later one. `import_products` does not fold:
  every duplicate SKU row is written in order and counted, and the last one wins
  in Magento.
- `MagentoAuthError` (bad credentials, locked account) is never caught: it
  aborts the importer. A per-row HTTP rejection never aborts it.

See [Results and Errors](Results-and-Errors) for the full error contract.

## Recommended order

Each importer only looks up references created by the importers before it;
products in particular never create categories or attribute sets. Run them in
this order (it is the order the live end-to-end test uses, with stocks and
links added where they fit):

1. `import_attributes`: attributes and their options.
2. `import_attribute_sets`: sets, groups, attribute assignments. Needs the attributes.
3. `import_categories`: creates missing category paths. Products only look paths up.
4. `import_sources`, then `import_stocks`, then `import_stock_source_links`.
5. `import_products`: needs sets, attributes, categories and websites.
6. `import_prices`: price storage rejects unknown SKUs.
7. `import_source_items`: needs the SKUs and the sources.
8. `import_media`: reads each SKU's gallery first, which fails for a SKU that does not exist.

Within one `import_products` call, child and parent products may be mixed: the
importer orders them itself (see [Two-phase product execution](#two-phase-product-execution)).

## Name matching rules

### Category paths

- Paths use `/` between levels. Each segment is stripped, and empty segments
  from doubled separators are dropped, so `Default Category//  Men /Tops`
  and `Default Category/Men/Tops` are the same path.
- Paths are matched case-insensitively (lower-cased with `str.lower()`, not
  case-folded). `men/tops` finds an existing `Men/Tops`.
- A path that does not start with the root category is prefixed with it. The
  importers always use the root `"Default Category"` (they build their
  `Resolver` with its default `root_category`), so `Men/Tops` means
  `Default Category/Men/Tops`, and a category under a different root cannot be
  addressed through the importers: `Other Root/Men` becomes
  `Default Category/Other Root/Men`.
- New categories keep the spelling you gave. They are created parent first
  with `is_active: true` and `include_in_menu: true`.
- Native path: the resolver reads the tree once with
  `GET categories?depth=1000`. Two siblings that differ only by case collapse to
  the first one in tree order, and a warning names both ids.
- Bridge path (`categories.upsert`): the module decides which of such siblings
  a path resolves to and the library logs nothing. Avoid such siblings if the
  choice matters.
- If the root itself is missing from the tree, the row fails with
  `root category '<name>' not found in the category tree`.

### Attribute sets

- Set names are stripped and matched case-insensitively (`str.lower()`):
  a row naming `apparel` reuses an existing `Apparel` instead of creating a
  second set (Magento refuses a set differing only by case).
- An `attribute_set` value made only of digits is used as the numeric id
  without lookup (products only).
- Attribute group names inside a set are matched exactly after stripping, so
  group names are case-sensitive.

### Attribute option labels

Option labels for `select` and `multiselect` values are HTML-unescaped,
stripped and case-folded before matching, so `Black &amp; White` matches
`black & white`.

## import_attributes

| | |
| --- | --- |
| Row model | `AttributeRow`, row reference `code` |
| Reads first | `GET products/attributes` filtered by `attribute_code in (...)` for all row codes in one request; `GET store/storeViews` when store labels are used |
| Writes | New code: `POST products/attributes` with the full payload including options. Existing code: `PUT products/attributes/{code}` (payload without `attribute_code` and `options`, with `attribute_id`) plus one `POST products/attributes/{code}/options` per option label not yet present |
| Sync or bulk | Always synchronous; these operations have no bulk route |
| `diff` | Ignored. Missing options are decided against the preloaded attribute metadata |
| `behavior` | `create_only` skips existing codes (counted `skipped_unchanged`). Any other value acts as upsert |
| Unchanged | Never, for an existing attribute: it is always PUT again and counts as `succeeded` |

Behaviours and gotchas:

- Every payload carries `is_user_defined: true`, the row `scope`, the default
  label and the `frontend_labels` resolved from store codes to store ids.
- `flags` are spread into the payload as is. A flag named `attribute_code`,
  `attribute_id`, `frontend_input`, `default_frontend_label`,
  `frontend_labels`, `scope`, `is_user_defined` or `options` fails the row with
  `flags shadow writer-owned keys: ...`.
- Existing options are never updated: a changed `sort_order` or store label on
  an option that already exists is not sent. Only missing labels are created.
- An unknown store code in `store_labels` fails the row.
- Magento accepts only letters, digits and underscores in attribute codes; a
  code with a hyphen is rejected by Magento (the live test renames the
  sample's hyphenated codes for that reason).
- Swatch attributes are not created by this library.

## import_attribute_sets

| | |
| --- | --- |
| Row model | `AttributeSetRow`, row reference `name` |
| Reads first | `GET eav/attribute-sets/list` (catalog_product sets), `GET products/attribute-sets/groups/list` per set |
| Writes | `POST products/attribute-sets` (with `skeletonId` from `based_on`), `POST products/attribute-sets/groups`, `POST products/attribute-sets/attributes` |
| Sync or bulk | Always synchronous |
| `diff`, `behavior` | Ignored |
| Unchanged | A row that plans nothing (existing set, no `groups`) counts as `skipped_unchanged` |

The importer is pass based, because a group can only be created once its set
exists and an attribute assigned once its group exists:

1. Pass 1 creates missing sets.
2. Pass 2 creates missing groups in sets that now exist.
3. Pass 3 assigns attributes to groups that now exist.

The resolver's set and group caches are refreshed between passes. A pass
converges when it plans nothing that has not already succeeded. After 3
executed passes, any row that still plans a new operation fails with
`attribute set not converged after 3 passes`. Assignments are re-planned on
every run because they are idempotent, so on a rerun a set with groups counts
as `succeeded`, not `skipped_unchanged`.

- An unknown `based_on` set fails the row with `unknown based_on attribute set: <name>`.
- A row whose set or groups failed is dropped from later passes.

## import_categories

| | |
| --- | --- |
| Row model | `CategoryRow`, row reference `path` |
| Reads first | The category tree (`GET categories?depth=1000`), unless the bridge `categories.upsert` capability is used |
| Writes | Missing nodes: `POST categories` per node, parent first (native) or one `POST dagster-bridge/categories/upsert` (bridge). Then one `PUT categories/{id}` for `attributes` (global, through the resource's store view) and one `PUT categories/{id}` per `store_values` entry through `/rest/<store_code>/` |
| Sync or bulk | Category creation happens while planning. The PUTs have no bulk route and run synchronously in both modes |
| `diff`, `behavior` | Ignored. The writer is idempotent by path |
| Unchanged | Never: a path resolved or created during planning counts as `succeeded` even with no attribute to write |

Payload rules (`writers/categories.py`):

- `name`, `is_active`, `position`, `include_in_menu` and `available_sort_by`
  sit at the top level of the category payload. Every other key goes into
  `custom_attributes` (Magento rejects `url_key`, `image` and similar at the
  top level).
- `available_sort_by` given as a comma string is split into a list.
- `attributes` may not contain `id`, `parent_id`, `path` or `name`; a
  `store_values` entry may not contain `id`, `parent_id` or `path` (a localized
  `name` is allowed). A violation fails the row.
- If creating the paths in one call fails, the importer retries path by path
  so only the paths that really fail become row errors.
- Magento 2.4.6 types `default_sort_by` as `string[]` and rejects the plain
  string with HTTP 400. Rows failing with exactly that error are retried once
  without `default_sort_by`, with a warning naming them; any other 400 stays a
  failure.

## import_products

| | |
| --- | --- |
| Row model | `ProductRow`, row reference `sku` |
| Reads first | Attribute metadata for every code in the rows (a filtered `GET products/attributes`, issued once by the importer and again by the writer for changed rows), then the product snapshot for every SKU (always, even with `diff=False`, because it decides create versus update) |
| Writes | New SKU: `POST products`. Existing SKU: `PUT products/{sku}`. Store values: `PUT products/{sku}` through `/rest/<store_code>/`. Configurable follow-ups: `POST configurable-products/{sku}/options` and `POST configurable-products/{sku}/child` |
| Bulk routes | `products` (create), `products/bySku` (update and store values), `configurable-products/bySku/options`, `configurable-products/bySku/child` |
| `diff` | Honoured |
| `behavior` | `upsert`, `create_only`, `update_only`, `disable` |

### Snapshot and unchanged rule

Native snapshot: `GET products` filtered by `sku in (...)` in chunks of 50
SKUs (a SKU containing a comma is queried alone with `eq`), projected to
`type_id`, `attribute_set_id`, `status`, `name`, `price`, `visibility`,
`weight`, `extension_attributes` and all custom attributes. With the bridge,
see [Optional Bridge](Optional-Bridge).

A row is skipped as unchanged only when everything the writer would send
already equals the snapshot (`diff.product_matches_snapshot`):

- the scalar fields the row sets (`name`, `price`, `status`, `visibility`,
  `weight`), normalized (prices and weights to 4 decimals, text stripped);
- `type`, `attribute_set` and `websites` only when the row set them explicitly;
- each custom attribute in `attributes`, with select labels resolved to option
  ids without creating any, multiselect compared as a sorted set, datetimes
  normalized to UTC, `url_key` compared as Magento formats it;
- the category ids when the row sets `categories`.

Any doubt answers "changed": an unknown label, set, website or category, or a
row that carries any of `store_values`, `variations`,
`configurable_attributes`, `bundle_options`, `grouped_links`,
`downloadable_links` or `downloadable_samples`. Such rows are rewritten on
every run. `images` are not compared here: `import_media` owns them.

### Behaviours

| `behavior` | New SKU | Existing SKU |
| --- | --- | --- |
| `upsert` | created | updated |
| `create_only` | created | skipped (`skipped_unchanged`) |
| `update_only` | skipped (`skipped_unchanged`) | updated |
| `disable` | fails with `sku does not exist` | `PUT` with only `{"sku", "status": 2}`; with `diff=True`, skipped when the snapshot already has status 2 (the row's other fields are irrelevant) |

There is no hard delete.

### What the main payload contains

- `sku`; `type_id`, `attribute_set_id` and `extension_attributes.website_ids`
  on create, and on update only when the row set `type`, `attribute_set` or
  `websites` explicitly;
- `name`, `price`, `status`, `visibility`, `weight` when not `None`;
- `custom_attributes` from `attributes`: booleans mapped from
  yes/no/true/false/1/0 to 1/0, select labels resolved (and created if
  missing) to option ids, multiselect labels (comma string or list) resolved
  and joined with commas, everything else sent as given;
- `extension_attributes.category_links` (each at position 0) when
  `categories` is non-empty. An empty list sends no category links at all;
- the type parts described below.

`attributes` may not contain `sku`, `type_id`, `attribute_set_id`, `name`,
`price`, `status`, `visibility`, `weight`, `custom_attributes` or
`extension_attributes`: the row fails with
`attributes shadow writer-owned keys: ...`. An unknown attribute code, an
unknown set, website or category path fails the row with the resolver's
message (`unknown attribute: <code>`, `unknown category path: <path>`, ...).

### Store-view scoped values

Main operations go through the resource's own `store_view` (`/rest/all/`
when `store_view="all"`). Each `store_values[store_code]` entry becomes one
extra minimal `PUT products/{sku}` through `/rest/<store_code>/`, containing
`sku`, any of `name`, `status`, `visibility` the entry sets at the top level,
and every other key as a custom attribute. The store view therefore gets an
override only for what it names. Store values are never diffed.

### Type parts

| Type | Where it goes |
| --- | --- |
| `configurable` | Follow-up operations: one options POST per code in `configurable_attributes` (values are the distinct labels of the variations, resolved to option ids), then one child POST per variation |
| `bundle` | Inline in the main payload: `extension_attributes.bundle_product_options` with `product_links` per selection (`price_type` fixed=0, percent=1; `can_change_quantity: 0`) |
| `grouped` | Inline: `product_links` with `link_type: "associated"`, `linked_product_type: "simple"`, `position`, `extension_attributes.qty` |
| `downloadable` | Inline: `extension_attributes.downloadable_product_links` (url type only) and `downloadable_product_samples` |
| other (`simple`, `virtual`, ...) | Nothing extra |

### Two-phase product execution

The importer executes in two calls, and each call orders its operations by
phase:

1. **Main saves and store values.** Operations on `products` and
   `products/...` run first. Within them, a grouped parent with
   `grouped_links` and a bundle parent with at least one selection are in
   phase 1, so they are saved after every phase 0 product of the run, in both
   sync and bulk mode: Magento validates the referenced child SKUs while saving
   the parent. Their store-value PUTs wait with them.
2. **Follow-ups.** Configurable options and child links run only after all
   main saves, because the parent and the child must both exist. A follow-up
   whose parent failed or is still pending is dropped (the row already counts
   from the parent). For a configurable that existed before the run, the
   importer reads `GET configurable-products/{sku}/children` once and skips
   children already attached (Magento answers a re-link with 400 "The product
   is already attached.").

Downloadable and configurable parents are not in phase 1: downloadable links
reference URLs, not SKUs, and configurable links are follow-ups. A child that
is neither in this run nor already in Magento makes its parent's link or save
fail with Magento's message.

## import_prices

| | |
| --- | --- |
| Row model | `PriceRow`, row reference `sku` |
| Reads first | `POST products/base-prices-information`, `special-price-information` and `tier-prices-information` in chunks of 1000 SKUs (always, even with `diff=False`: current tiers are needed to replace them); `GET store/websites` when any row has tiers |
| Writes | `POST products/base-prices`, `POST products/special-price`, `PUT products/tier-prices` (replace), `POST products/tier-prices-delete` (an empty `tiers` list) |
| Sync or bulk | Always sync: `mode` is ignored, these list endpoints have no bulk route. Chunks of 1000 items, all tiers of one SKU kept in one request |
| `diff` | Honoured per row: base price for the row's store, special price and dates for the row's store, and the full tier set |
| `behavior` | Ignored |

Rows with the same `(sku, store_id)` are merged, a later non-`None` value
winning; tiers are merged per SKU (they are not store scoped). Folded rows
count as `skipped_unchanged`. `tiers=None` leaves tiers alone; `tiers=[]`
deletes all current tiers. Details in [Price Import](Price-Import).

## import_sources

| | |
| --- | --- |
| Row model | `SourceRow`, row reference `source_code` |
| Reads first | Nothing |
| Writes | One `POST inventory/sources` per row with `source_code`, `name`, `enabled`, `country_id`, `postcode` |
| Sync or bulk | Always synchronous |
| `diff`, `behavior` | Ignored. The live test relies on the POST overwriting an existing source on a rerun |

## import_stocks

| | |
| --- | --- |
| Row model | `StockRow`, row reference `name` |
| Reads first | `GET store/websites` to check every website code |
| Writes | One `POST inventory/stocks` per row with `extension_attributes.sales_channels` = `[{"type": "website", "code": ...}]` |
| Sync or bulk | Always synchronous |
| `diff`, `behavior` | Ignored |

An unknown website code fails the row with `unknown website code: <code>`. The
payload carries no `stock_id`, so it is always a create. What Magento does when
a stock with the same name already exists is not covered by the library's tests;
check before rerunning this importer.

## import_stock_source_links

| | |
| --- | --- |
| Row model | `StockSourceLinkRow`, row reference `"<stock>/<source_code>"` |
| Reads first | `GET inventory/stocks` to map stock names to ids, unless you pass `stock_ids={"name": id}` |
| Writes | `POST inventory/stock-source-links` with a `links` list (chunks of 1000) |
| Sync or bulk | Always synchronous |
| `diff`, `behavior` | Ignored |

The stock is matched by exact name. An unknown name fails the row with
`unknown stock name: <name>`. Rerun behaviour on an existing link is Magento's
and is not covered by the library's tests.

## import_source_items

| | |
| --- | --- |
| Row model | `SourceItemRow`, row reference `"<source_code>/<sku>"` |
| Reads first | With `diff=True`: `GET inventory/source-items` filtered by SKU (50 SKUs per URL, paged by 200) |
| Writes | `POST inventory/source-items` with a `sourceItems` list (chunks of 1000) |
| Sync or bulk | Always synchronous |
| `diff` | Honoured: a pair whose `(quantity, status)` already match (quantity to 4 decimals) is skipped |
| `behavior` | Ignored |

The last row per `(source_code, sku)` pair wins; earlier duplicates count as
`skipped_unchanged`. A rejected item that Magento names by SKU and source fails
only that row; a rejected item that names no row fails every row of its request.

## import_media

| | |
| --- | --- |
| Row model | `ProductRow` (only `sku` and `images` are used), row reference `sku` |
| Reads first | `GET products/{sku}/media` for every SKU that has images (always, whatever `diff` says) |
| Writes | `DELETE products/{sku}/media/{entry_id}` for a replaced image, `POST products/{sku}/media` with base64 content per new image |
| Bulk routes | Adds use `products/bySku/media`; deletes have none and run synchronously first |
| `diff`, `behavior` | Ignored |

### Media identity rule

An existing gallery entry is identified by `(position, label)`, where `None`
and `""` are the same "no label":

- same position, same label: already matches, no load, no operation;
- same position, different label: the old entry is deleted and the new image
  added at that position;
- a position the row does not mention: left alone;
- a position with no existing entry: added.

File content is never compared, and roles (`types`) and `disabled` are not
compared either: changing only the roles of an image at the same position and
label changes nothing in Magento. Change the label or position to force a
re-upload.

Image loading: an `http`/`https` source is downloaded with `requests.get`
(60 s timeout, no credentials sent); anything else is a local path, relative
paths resolving against the process working directory. The MIME type comes from
the bytes (PNG, JPEG, GIF, WebP), never from the file name; other bytes fail
with `not an image`. One image failing to load fails the whole row and plans
nothing for it. A SKU whose gallery cannot be read (for example a SKU that does
not exist) fails with `cannot read media gallery: ...`. Rows without images
count as `skipped_unchanged`.

## Gotchas

- `import_products` does not create categories, attribute sets, websites or
  attributes. It does create missing select and multiselect option labels.
- `import_products` ignores `images`; run `import_media` with the same rows.
- A row with any type part or store values is never skipped as unchanged, so
  configurable, bundle, grouped and downloadable parents are rewritten on every
  run (they succeed again; the live rerun test checks that).
- The bridge `require` mode raises before anything is written when a
  capability is missing: `the bridge is required for this import but the store does not offer: ...`.
- `fail_on_error_ratio` raises after the counts are logged, so a failed run
  still leaves them in the Dagster log.

## See also

- [Home](Home)
- [Getting Started](Getting-Started)
- [Row Models](Row-Models)
- [File Formats](File-Formats)
- [Optional Bridge](Optional-Bridge)
- [Sync and Bulk Execution](Sync-and-Bulk-Execution)
- [Price Import](Price-Import)
- [Results and Errors](Results-and-Errors)
- [Dagster Assets](Dagster-Assets)
- [Architecture](Architecture)
- [MagentoResource](MagentoResource)
- [Compatibility](Compatibility)
- [Troubleshooting](Troubleshooting)
