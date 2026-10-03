# Optional Bridge

`DDTCoreX_DagsterBridge` is an optional Magento module, released from its own
public repository, that gives the catalog importers three faster or atomic
endpoints. The library never needs it: every importer works against Magento's
own REST API, and with the module installed it switches to the module one
capability at a time. This page describes the bridge from the library's side:
the `BridgeClient`, the capability probe, exactly what each capability replaces
on the native path, the `use_bridge` modes and how failures behave. Installing
and configuring the module itself is covered in the
[module documentation](https://github.com/ddtcorex/module-dagster-bridge/blob/master/docs/README.md). All
statements were checked against `dagster_magento/bridge.py`, `diff.py`,
`resolvers.py`, `importers.py` and `tests/test_bridge.py` at version 0.4.0.

## Who uses it

Only the catalog import layer, and only through `BridgeClient`.
`MagentoResource` never depends on the module. Two importers can use it:

| Importer | Capabilities it can use |
| --- | --- |
| `import_products` | `products.index`, `products.attribute_values` (product snapshot) |
| `import_categories` | `categories.upsert` (creating missing category paths) |
| every other importer | none |

`import_products` does not use `categories.upsert`: product category paths are
looked up in the native category tree, never created.

## The capability probe

```
GET /rest/<store_view>/V1/dagster-bridge/capabilities
{"version": "1.2.3", "capabilities": ["products.index", "products.attribute_values", "categories.upsert"]}
```

- The probe is lazy: it runs the first time an importer asks whether a
  capability is available, and its answer is cached on the `BridgeClient`
  instance. Each importer call builds its own client, so it is one probe per
  importer call (`import_categories` builds a second client for its
  `default_sort_by` retry). Importers that use no capability never probe.
- The probe is best effort. A 404 (module not installed), a 500, a timeout or
  any other error that is not an authentication failure logs
  `bridge probe failed (<error>); continuing without the bridge` (through the
  standard `dagster_magento.bridge` Python logger) and leaves the
  capability set empty, so the native paths run. An optional module can never
  fail an import through its probe.
- `MagentoAuthError` (the admin token cannot be obtained) is re-raised by the
  probe and aborts the run, as everywhere else in the library.
- Capabilities are independent: a store with an older module that offers only
  some of them gets those, and the native path for the rest.

## The three capabilities

| Capability | Bridge endpoint | Native path it replaces |
| --- | --- | --- |
| `products.index` | `GET dagster-bridge/products/index?after=<n>&limit=5000` | The SKU-filtered `GET products` snapshot (50 SKUs per request) that tells which SKUs exist and reads their entity fields |
| `products.attribute_values` | `POST dagster-bridge/products/attribute-values` | Reading attribute values for the diff from `GET products` |
| `categories.upsert` | `POST dagster-bridge/categories/upsert` | Reading the category tree (`GET categories?depth=1000`) and one `POST categories` per missing node |

### products.index

- The client pages through the whole product index with `after` and
  `limit=5000`, following `next_after` until it is `null`, and caches it by
  SKU for the importer call. It reads every product in the store, not only the
  SKUs of the run.
- The index answers which SKUs exist (a SKU missing from it is a new row,
  exactly like the native snapshot) and the entity fields `entity_id`, `sku`,
  `type_id`, `attribute_set_id`, `status`, `updated_at`.
- Website ids and category links live only in `extension_attributes`, which
  the module does not answer. For SKUs the index has, the library still reads
  them with the native SKU-filtered `GET products` (50 SKUs per URL). A SKU the
  index has but REST does not answer reads as `None`, which the diff treats as
  changed.

### products.attribute_values

Used only together with `products.index` (the bridge snapshot runs only when
the index capability is present).

- Request body: `{"skus": [...], "attribute_codes": [...], "store_id": <int>}`.
  The client splits requests to the module's caps: at most 1000 SKUs and 50
  attribute codes per call (`BridgeClient.MAX_SKUS_PER_CALL`,
  `MAX_ATTRIBUTE_CODES_PER_CALL`).
- Codes asked: `name`, `price`, `visibility`, `weight` (the snapshot fields the
  index does not answer) plus the row attribute codes that exist in Magento.
  Unknown codes are left out on purpose, because the module rejects a request
  with an unknown code, which would otherwise fail the snapshot for every row.
- `store_id` is 0 when the resource's `store_view` is `all` or empty, otherwise
  the numeric id of that store view (`GET store/storeViews`).
- Answer: a list of `{"sku", "attribute_code", "store_value", "default_value"}`.
  The library applies Magento's own fallback: the store value when there is
  one, otherwise the default value. A pair with neither stays `None`. A store
  view without its own value therefore never reads as a difference.
- Without this capability (an older module), the attribute codes are read
  from `GET products` like the extension attributes.

### categories.upsert

- Request body: `{"paths": [...], "root": "Default Category", "separator": "<sep>"}`.
  The module creates whatever is missing in one transaction and answers a list
  of `{"path", "id"}`; the library maps the answer back onto the paths you gave
  and fills the resolver cache, so later lookups need no extra request.
- The separator is `/` unless a path contains one of `|`, `>`, `^`, `~`; then
  the first of `|`, `>`, `^`, `~` that appears in no path is used, and the
  levels (split on `/`) are re-joined with it. Example: `Default Category/Men|Women`
  is sent as `Default Category>Men|Women` with separator `>`.
- On this path the library does not strip level names (only empty levels from
  doubled `/` are dropped); the native path strips each segment.
- Case: on the native path two sibling categories that differ only by case
  collapse to the first one in tree order with a warning. With the bridge the
  module's processor picks one and the library logs nothing. New categories keep
  your spelling either way.

## use_bridge modes

Every importer takes `use_bridge`, default `"auto"`.

| Mode | Missing capability | Capability that fails while used |
| --- | --- | --- |
| `"auto"` | Native path for that capability | Warning, then the native path (see below) |
| `"never"` | No client is built and nothing is probed; native paths only, even with the module installed | Not applicable |
| `"require"` | `MagentoImportError` before anything is written | `MagentoImportError` |

The `require` check lists only the capabilities the importer's path needs
(`import_products`: `products.index` and `products.attribute_values`;
`import_categories`: `categories.upsert`). The message names what is missing,
sorted:

```
MagentoImportError: the bridge is required for this import but the store does not offer: categories.upsert
```

For the other importers (attributes, attribute sets, prices, sources, stocks,
stock source links, source items and media) `require` has nothing to check, so
they log a warning that `use_bridge='require'` has no effect and run the native
path.

Failures while a capability is in use:

| Where | Caught errors | `auto` | `require` |
| --- | --- | --- | --- |
| Product snapshot through the bridge | `requests.exceptions.HTTPError`, `KeyError`, `ValueError`, `TypeError` | Warning `bridge snapshot failed (...); using the REST snapshot`, then the full native snapshot | `MagentoImportError: bridge product snapshot failed: ...` |
| Category upsert | the same four (an answer missing a requested path is a `KeyError`) | Warning `bridge category upsert failed (...); creating the paths natively`, then native parent-first creation for that call | `MagentoImportError: bridge category upsert failed: ...` |

`MagentoImportError` raised in `require` mode aborts the importer call; it is
not turned into row errors. `MagentoAuthError` is in none of the caught tuples:
an authentication failure during the probe, the snapshot or the upsert always
aborts the run and is never mistaken for "module absent".

## Using BridgeClient directly

```python
from dagster_magento import BridgeClient, MagentoResource

resource = MagentoResource(base_url="https://shop.example.test", username="api-user",
                           password="...", store_view="all")
client = BridgeClient(resource)

client.capabilities()                      # frozenset({'products.index', ...}) or frozenset()
client.has(BridgeClient.CATEGORIES_UPSERT) # True / False
index = client.index_by_sku()              # {sku: {"entity_id", "type_id", "attribute_set_id", "status", "updated_at", ...}}
values = client.attribute_values(["SKU-1"], ["name", "price"], store_id=0)
# {"SKU-1": {"name": ("store value or None", "default value or None"), ...}}
ids = client.upsert_categories(["Default Category/Men/Tops"], "Default Category")
# {"Default Category/Men/Tops": 42}
```

| Member | Value |
| --- | --- |
| `CAPABILITIES_ENDPOINT` | `dagster-bridge/capabilities` |
| `PRODUCT_INDEX_ENDPOINT` | `dagster-bridge/products/index` |
| `ATTRIBUTE_VALUES_ENDPOINT` | `dagster-bridge/products/attribute-values` |
| `CATEGORIES_UPSERT_ENDPOINT` | `dagster-bridge/categories/upsert` |
| `PRODUCT_INDEX`, `ATTRIBUTE_VALUES`, `CATEGORIES_UPSERT` | `"products.index"`, `"products.attribute_values"`, `"categories.upsert"` |
| `MAX_SKUS_PER_CALL`, `MAX_ATTRIBUTE_CODES_PER_CALL` | 1000, 50 |
| `product_index(limit=5000)` | generator over every index item |
| `pick_separator(paths)` | class method, the separator rule above |

All requests go through the `MagentoResource`, so they use its admin token,
retries and store view in the URL. `attribute_values` and `upsert_categories`
are POSTs, which the resource retries on 429 only.

## Installing the module

The module is installed into Magento, not into Python. Installation,
supported Magento versions, ACL and upgrade notes live in the
[module documentation](https://github.com/ddtcorex/module-dagster-bridge/blob/master/docs/README.md). From the
library's side the only requirements are that the three endpoints above are
reachable for the admin user the `MagentoResource` logs in with, and that the
capabilities endpoint lists what the module offers.

## Verifying that the bridge is active

1. Ask the probe directly (the snippet above): `client.capabilities()` should
   list the three capabilities. An empty set means the module is absent or the
   probe failed; the `dagster_magento.bridge` Python logger then emits a `bridge probe failed`
   warning with the reason (it reaches the Dagster run log only if
   `python_logs.managed_python_loggers` includes `dagster_magento`).
2. Run one importer with `use_bridge="require"`. It raises
   `MagentoImportError` naming any capability the store does not offer, before
   writing anything:

   ```python
   from dagster_magento import import_categories

   import_categories(resource, [{"path": "Default Category"}], use_bridge="require")
   ```

3. Compare the two paths on the same data with `use_bridge="never"` and
   `use_bridge="require"`. The library's live end-to-end suite runs the full
   sample catalog exactly that way, in sync and bulk mode, and expects the same
   outcome from both.

## Gotchas

- `auto` hides a broken module behind a warning: if you rely on the bridge for
  performance, use `require` so a missing or failing capability stops the run.
- The product index is read in full for every `import_products` call. On a
  very large store with a small delta, that can cost more than the native
  50-SKU snapshot; measure both with `never` and `require`.
- The category root sent to the module is always `Default Category`, the same
  root the native path uses (see
  [Catalog Importers](Catalog-Importers#category-paths)).
- `use_bridge="never"` is the way to prove the native paths on a store that has
  the module installed.

## See also

- [Home](Home)
- [Catalog Importers](Catalog-Importers)
- [Architecture](Architecture)
- [MagentoResource](MagentoResource)
- [Compatibility](Compatibility)
- [Troubleshooting](Troubleshooting)
- [Results and Errors](Results-and-Errors)
- [Module documentation](https://github.com/ddtcorex/module-dagster-bridge/blob/master/docs/README.md)
