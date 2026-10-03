# dagster-magento

[![CI](https://github.com/ddtcorex/dagster-magento/actions/workflows/ci.yml/badge.svg)](https://github.com/ddtcorex/dagster-magento/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/ddtcorex/dagster-magento)](https://github.com/ddtcorex/dagster-magento/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A Dagster resource and a native catalog import layer for the Magento 2 REST API.
Feed it rows from an ERP, a PIM, a database or csv/json/xlsx files; it reads what
Magento already has, writes only what changed, and reports exactly which rows
succeeded, failed or are still pending.

- **Standard REST only.** No custom Magento module is required.
- **Catalog importers.** Attributes, attribute sets, categories, products of every
  type, prices, MSI sources, stocks, source items and media.
- **Diff first.** A rerun of the same input skips every row that already matches.
- **Sync or async bulk.** Write chunk by chunk, or submit Magento's async bulk API
  and poll it.
- **Fast prices.** Prices use Magento's native price storage endpoints, not full
  product saves.
- **File adapters.** csv, json and xlsx in Magento's native import column format.

## Install

Python 3.10 or newer. The package is installed from a git tag; it is not on PyPI.

```
pip install "dagster-magento[xlsx] @ git+https://github.com/ddtcorex/dagster-magento.git@v0.4.0"
```

The `xlsx` extra adds `openpyxl`, needed only to read `.xlsx` files.

## Quick start

```python
from dagster import Definitions, EnvVar, MaterializeResult, asset

from dagster_magento import MagentoResource, import_products, to_materialize_result


@asset
def products(magento: MagentoResource) -> MaterializeResult:
    rows = [{"sku": "TSHIRT-1", "name": "T-shirt", "price": 19.9}]
    return to_materialize_result(import_products(magento, rows))


defs = Definitions(
    assets=[products],
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

`import_products` reads the current state, plans the writes and returns a result
you can hand to Dagster. Run it against a Magento admin account.

## Documentation

The full documentation is in the [wiki](https://github.com/ddtcorex/dagster-magento/wiki):

| | |
| --- | --- |
| [Getting Started](https://github.com/ddtcorex/dagster-magento/wiki/Getting-Started) | install, configure, run a first import |
| [Architecture](https://github.com/ddtcorex/dagster-magento/wiki/Architecture) | how one import call flows |
| [Catalog Importers](https://github.com/ddtcorex/dagster-magento/wiki/Catalog-Importers) | every importer and its options |
| [Row Models](https://github.com/ddtcorex/dagster-magento/wiki/Row-Models) | every field of every row |
| [File Formats](https://github.com/ddtcorex/dagster-magento/wiki/File-Formats) | csv, json and xlsx mapping |
| [Sync and Bulk Execution](https://github.com/ddtcorex/dagster-magento/wiki/Sync-and-Bulk-Execution) | consumers, chunking, MariaDB |
| [Troubleshooting](https://github.com/ddtcorex/dagster-magento/wiki/Troubleshooting) | symptoms and fixes |

## Compatibility

Magento Open Source 2.4.6 and newer over the standard REST API. The versions that
were actually run end to end are listed, with their patch, PHP and database, on the
[Compatibility](https://github.com/ddtcorex/dagster-magento/wiki/Compatibility) page.

## Optional bridge module

[`DDTCoreX_DagsterBridge`](https://github.com/ddtcorex/module-dagster-bridge) is a
companion Magento module that makes the product index and store-scoped attribute
reads cheaper and the category upsert atomic. The library uses it capability by
capability when it is installed and falls back to plain REST when it is not.

## Contributing

Pull requests are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md) and
[Contributing and Releases](https://github.com/ddtcorex/dagster-magento/wiki/Contributing-and-Releases).
Release notes are in [CHANGELOG.md](CHANGELOG.md).

## License

MIT. See [LICENSE](LICENSE).
