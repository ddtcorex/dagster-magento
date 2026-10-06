# Getting Started

`dagster-magento` is a Python library, not a pipeline: it gives you a Dagster
resource (`MagentoResource`) that talks to the standard Magento 2 REST API, and
a catalog import layer (`import_products`, `import_prices`, ...) that turns
rows into Magento writes, skips rows that already match, and reports one
outcome per row. You install it as a dependency and write your own assets
around it. This page covers installation, the resource configuration, a first
import run end to end, and where to read next.

## Requirements

| Item | Value | Source |
| --- | --- | --- |
| Python | `>=3.10` | `pyproject.toml` `requires-python` |
| Dagster | `>=1.13.17` | `pyproject.toml` dependencies |
| requests | `>=2.28` | `pyproject.toml` dependencies |
| pydantic | `>=2` | `pyproject.toml` dependencies |
| openpyxl | `>=3.1`, only with the `xlsx` extra | `pyproject.toml` optional dependencies |
| Magento | Open Source 2.4.6 and newer, see [Compatibility](Compatibility) for the lines actually verified | `pyproject.toml`, [Compatibility](Compatibility) |

No custom Magento module is required. The optional companion module is
described in [Optional-Bridge](Optional-Bridge); nothing on this page needs it.

## Install

The package is not on PyPI. It is installed from a git tag:

```
pip install "dagster-magento[xlsx] @ git+https://github.com/ddtcorex/dagster-magento.git@v0.5.0"
```

The `xlsx` extra pulls in `openpyxl`, which is needed only to read `.xlsx`
source files. CSV and JSON sources use the standard library, so you can drop
the extra when you never read spreadsheets:

```
pip install "dagster-magento @ git+https://github.com/ddtcorex/dagster-magento.git@v0.5.0"
```

If you later call `read_rows()` on an `.xlsx` file without the extra, it
raises `ImportError` with the install command in the message.

## Configure the resource

`MagentoResource` is a Dagster `ConfigurableResource` with five fields:

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `base_url` | `str` | required | Store root, for example `https://magento.example.com`. Do not add a trailing slash: URLs are built as `{base_url}/rest/{scope}/V1/{endpoint}`. |
| `username` | `str` | required | Admin user used for `POST /rest/{store_view}/V1/integration/admin/token`. |
| `password` | `str` | required | Admin password. Declared `repr=False` and marked secret for the Dagster UI. |
| `store_view` | `str` | required | The default REST scope segment for every call that does not pass `store_code`. |
| `verbose_logging` | `bool` | `False` | Log full request bodies and the first 2000 characters of response bodies at `DEBUG`. |

Use `EnvVar` so credentials never land in code or in the launchpad:

```python
from dagster import Definitions, EnvVar
from dagster_magento import MagentoResource

magento = MagentoResource(
    base_url=EnvVar("MAGENTO_BASE_URL"),
    username=EnvVar("MAGENTO_ADMIN_USERNAME"),
    password=EnvVar("MAGENTO_ADMIN_PASSWORD"),
    store_view=EnvVar("MAGENTO_STORE_VIEW"),
)
```

### Which `store_view` to pick

For catalog imports, set `store_view` to `all`. Every catalog operation that
is not explicitly store scoped (the main product save, a category attribute
update, a price list call) is sent through `/rest/{store_view}/`, so with
`all` it lands in the global (admin) scope. With a store code such as
`default` those same saves go through that store view's scope instead. The
library's own live tests use `all` (`scripts/sandbox.sh env` exports
`MAGENTO_STORE_VIEW=all`). Store-specific values are written with an explicit
`store_code` per operation regardless of this setting; see
[Catalog-Importers](Catalog-Importers).

The admin token endpoint must be reachable with plain username and password.
If the store enforces admin two-factor authentication for REST tokens, the
token request fails and every call raises `MagentoAuthError` (the library's
sandbox turns 2FA off for exactly this reason).

## A first import, end to end

The resource is an ordinary object, so you can try it from a Python shell
before wiring any asset. Importers accept plain dicts or row model instances.

```python
from dagster_magento import MagentoResource, import_products, import_prices, import_source_items

magento = MagentoResource(
    base_url="https://magento.example.com",
    username="api-admin",
    password="change-me",
    store_view="all",
)

result = import_products(
    magento,
    [
        {
            "sku": "TEE-RED-M",
            "name": "Tee red M",
            "price": 19.9,
            "status": 1,
            "visibility": 4,
            "attributes": {"url_key": "tee-red-m"},
        }
    ],
)
print(result.to_metadata())
# {'succeeded': 1, 'failed': 0, 'pending': 0, 'skipped_unchanged': 0, 'error_count': 0}

print(import_source_items(magento, [{"sku": "TEE-RED-M", "source_code": "default", "quantity": 100, "status": 1}]).to_metadata())
print(import_prices(magento, [{"sku": "TEE-RED-M", "price": 17.5}]).to_metadata())
```

What happens on the first `import_products` call:

1. Each row is validated against `ProductRow`. A bad row becomes a failed row,
   never an exception.
2. The attribute metadata for every code the rows use is fetched with one
   filtered `GET /V1/products/attributes`.
3. The snapshot step probes the bridge once (`GET /V1/dagster-bridge/capabilities`;
   a store without the module answers 404 and the native REST paths are used),
   then reads the current state of the SKUs with `GET /V1/products` filtered by
   `sku in (...)`, 50 SKUs per URL.
4. The writer asks for the attribute metadata of the rows that changed once
   more before it plans.
5. `TEE-RED-M` is not in the snapshot, so the writer plans a create:
   `POST /V1/products` with `type_id` `simple`, attribute set `Default` and
   website `base` (the model defaults). Resolving those names costs one
   `GET /V1/eav/attribute-sets/list` and one `GET /V1/store/websites`, cached
   for the call.
6. The executor sends it, and the result is folded to one outcome per row.

Run the same `import_products` call again (before the price change) and you
get `{'succeeded': 0, ..., 'skipped_unchanged': 1}`: the snapshot proves the
row already matches, so nothing is sent. Pass `diff=False` to rewrite anyway.

### Reading the result

Every importer returns an `UploadResult`:

| Field | Meaning |
| --- | --- |
| `succeeded` | Rows whose every operation Magento accepted. |
| `failed` | Rows rejected by validation, planning or Magento. |
| `pending` | Bulk mode only: rows whose operations were still open when the wait timed out. Never success. |
| `skipped_unchanged` | Rows the diff proved already correct, duplicates folded into another row, and rows a behavior left alone. |
| `errors` | One dict per failure, with `row_ids`, `status`, `status_code` and `message`. |

A per-row rejection never raises. A bad credential does: `MagentoAuthError`
aborts the run. To fail a run above a failure rate, pass
`fail_on_error_ratio`. Details are in [Results-and-Errors](Results-and-Errors).

## Inside Dagster

Declare the resource under a key and take it as an asset parameter:

```python
from dagster import Definitions, EnvVar, MaterializeResult, asset
from dagster_magento import MagentoResource, import_prices, to_materialize_result


@asset
def prices(magento: MagentoResource) -> MaterializeResult:
    rows = [{"sku": "TEE-RED-M", "price": 17.5}]
    return to_materialize_result(import_prices(magento, rows), fail_on_error_ratio=0.05)


defs = Definitions(
    assets=[prices],
    resources={
        "magento": MagentoResource(
            base_url=EnvVar("MAGENTO_BASE_URL"),
            username=EnvVar("MAGENTO_ADMIN_USERNAME"),
            password=EnvVar("MAGENTO_ADMIN_PASSWORD"),
            store_view=EnvVar("MAGENTO_STORE_VIEW"),
        )
    },
)
```

`to_materialize_result` attaches the counts as asset metadata. Logging goes
through `dagster.get_dagster_logger()`, so it shows in the run log; the bridge
probe, the bridge snapshot fallback and the category upsert fallback warnings use
the standard `logging` module instead, and reach the run log only if
`python_logs.managed_python_loggers` includes `dagster_magento`. A full
eight-stage catalog definition is on [Dagster-Assets](Dagster-Assets).

## Gotchas

- `base_url` with a trailing slash produces `//rest/` in every URL. Leave it off.
- A store view used by bulk mode must exist before the Magento consumers
  start; see [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution).
- Importers default to `mode="sync"`. Bulk mode needs a running consumer.
- On a store without the optional bridge module, each importer that probes it
  logs a warning such as `bridge probe failed (404 Client Error ...);
  continuing without the bridge`. That is harmless; pass `use_bridge="never"`
  to skip the probe entirely.
- `verbose_logging=True` logs request bodies as they are. Leave it off in a
  pipeline that writes anything sensitive (customer records can carry
  passwords).
- The whole run is planned in memory before it executes, including base64
  image data for media. Split very large catalogs at the asset level.

## Where to go next

- [Architecture](Architecture): the layers and what one `import_*` call does.
- [MagentoResource](MagentoResource): every resource method, retries, auth.
- [Catalog-Importers](Catalog-Importers): one page per entity and its options.
- [Row-Models](Row-Models) and [File-Formats](File-Formats): what a row looks like
  and how CSV, JSON and XLSX map onto it.
- [Price-Import](Price-Import): how prices are written without full product saves.
- [Sync-and-Bulk-Execution](Sync-and-Bulk-Execution): the two execution modes.

## See also

- [Home](Home)
- [Architecture](Architecture)
- [MagentoResource](MagentoResource)
- [Results-and-Errors](Results-and-Errors)
- [Dagster-Assets](Dagster-Assets)
- [Compatibility](Compatibility)
- [Troubleshooting](Troubleshooting)
