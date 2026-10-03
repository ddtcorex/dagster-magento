# dagster-magento

A Dagster resource and a native catalog import layer for the Magento 2 REST API. You give it rows (from an ERP, a PIM, a database, or csv/json/xlsx files); it works out what already exists, writes only what changed, and tells you exactly which rows succeeded, failed or are still pending.

It talks to standard Magento REST endpoints only, so it needs no custom module. An optional companion module, [DDTCoreX_DagsterBridge](Optional-Bridge), makes a few reads cheaper and the category upsert atomic when it is installed; nothing requires it.

## What it does

- **A Magento client you can trust in a pipeline.** `MagentoResource` fetches and refreshes the admin token, retries only what is safe to retry, scopes every call to a store view, and never logs the password. See [MagentoResource](MagentoResource).
- **Importers for the catalog.** Attributes, attribute sets, categories, products of every type, prices, MSI sources, stocks, source items and media. See [Catalog Importers](Catalog-Importers) and [Row Models](Row-Models).
- **Diff first.** Each importer reads the current state and skips rows that already match, so a rerun of the same input reports every row as `skipped_unchanged`. See [Architecture](Architecture).
- **Sync or async bulk.** Write row by row and chunk by chunk, or submit Magento's async bulk API and poll it. See [Sync and Bulk Execution](Sync-and-Bulk-Execution).
- **Fast prices.** Prices go through Magento's native price storage endpoints instead of full product saves. See [Price Import](Price-Import).
- **Files in, rows out.** Adapters for csv, json and xlsx in Magento's native import column format. See [File Formats](File-Formats).
- **Honest results.** A row is succeeded, failed, pending or skipped, never guessed. See [Results and Errors](Results-and-Errors).

## Where to start

| I want to | Read |
| --- | --- |
| Install it and run a first import | [Getting Started](Getting-Started) |
| Understand how a call flows | [Architecture](Architecture) |
| Use it from Dagster assets | [Dagster Assets](Dagster-Assets) |
| Know every field of a product, category or price row | [Row Models](Row-Models) |
| Load a csv, json or xlsx file | [File Formats](File-Formats) |
| Make bulk imports work (consumers, MariaDB) | [Sync and Bulk Execution](Sync-and-Bulk-Execution) |
| See which Magento versions were tested | [Compatibility](Compatibility) |
| Fix something that went wrong | [Troubleshooting](Troubleshooting) |
| Run the live tests or cut a release | [Sandbox and Testing](Sandbox-and-Testing), [Contributing and Releases](Contributing-and-Releases) |

## All pages

**Guides:** [Getting Started](Getting-Started), [Architecture](Architecture), [Dagster Assets](Dagster-Assets)

**Reference:** [MagentoResource](MagentoResource), [Catalog Importers](Catalog-Importers), [Row Models](Row-Models), [File Formats](File-Formats), [Price Import](Price-Import), [Sync and Bulk Execution](Sync-and-Bulk-Execution), [Results and Errors](Results-and-Errors), [Optional Bridge](Optional-Bridge)

**Operate and contribute:** [Compatibility](Compatibility), [Troubleshooting](Troubleshooting), [Sandbox and Testing](Sandbox-and-Testing), [Contributing and Releases](Contributing-and-Releases)

These pages are generated from the `docs/` folder of the repository. To change one, open a pull request there; edits made on the wiki itself are overwritten by the next sync.
