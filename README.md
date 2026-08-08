# dagster-magento

A reusable Dagster resource for talking to Magento 2's REST API: admin-token
authentication, paginated reads, and a resilient chunked/per-row upload
helper with Dagster-integrated logging.

Targets standard Magento 2 REST endpoints only — no dependency on any
project-specific custom API modules.

## Installation

```
pip install git+https://git.sutunam.com/PATH/TO/dagster-magento.git@v0.1.0
```

## Usage

```python
from dagster import asset, Definitions, EnvVar
from dagster_magento import MagentoResource

@asset
def store_configs(magento: MagentoResource) -> list:
    return magento.get("store/storeConfigs")

@asset
def all_products(magento: MagentoResource) -> list:
    return magento.get_paginated("products", page_size=1000)

@asset
def source_item_sync(magento: MagentoResource) -> None:
    magento.post("products", {"product": {"sku": "NEW-SKU", "name": "New product"}})

    # Resilient bulk write - one row per request (e.g. linking configurable children)
    result = magento.upload_rows(
        "configurable-products/PARENT-SKU/child",
        rows=[{"sku": "CHILD-1"}, {"sku": "CHILD-2"}],
    )

    # Resilient bulk write - chunked under a wrapper key (e.g. inventory source items)
    result = magento.upload_rows(
        "inventory/source-items",
        rows=[{"sku": "A", "source_code": "default", "quantity": 5, "status": 1}],
        chunk_size=200,
        wrap_key="sourceItems",
    )
    print(result.succeeded, result.failed, result.errors)

defs = Definitions(
    assets=[store_configs, all_products, source_item_sync],
    resources={
        "magento": MagentoResource(
            base_url=EnvVar("MAGENTO_BASE_URL"),
            username=EnvVar("MAGENTO_ADMIN_USERNAME"),
            password=EnvVar("MAGENTO_ADMIN_PASSWORD"),
            store_view=EnvVar("MAGENTO_STORE_VIEW"),
        ),
    },
)
```

Logging goes through `dagster.get_dagster_logger()` — no setup needed; it
shows up in the Dagster UI's run logs when called from inside an asset, and
falls back to standard Python logging otherwise. Set `verbose_logging=True`
on `MagentoResource` to log full request/response bodies at `DEBUG` level
when debugging a specific sync issue — off by default since a full sync can
involve hundreds of thousands of rows. **Note:** a request body sent via
`post`/`upload_rows` to an endpoint like `POST /V1/customers` can legitimately
contain a plaintext customer password field — `verbose_logging=True` will log
that request body as-is, so avoid leaving it enabled on any pipeline that
writes customer records.
