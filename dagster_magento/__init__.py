"""Reusable Dagster resource for Magento 2 REST integrations."""

from dagster_magento.bridge import BridgeClient
from dagster_magento.bulk import AsyncBulkResult
from dagster_magento.importers import (
    import_attribute_sets,
    import_attributes,
    import_categories,
    import_media,
    import_prices,
    import_products,
    import_source_items,
    import_sources,
    import_stock_source_links,
    import_stocks,
    to_materialize_result,
)
from dagster_magento.operation import BulkSpec, Operation, RowError
from dagster_magento.resource import MagentoResource
from dagster_magento.search import build_search_criteria
from dagster_magento.upload import UploadResult

__all__ = [
    "AsyncBulkResult",
    "BridgeClient",
    "MagentoResource",
    "UploadResult",
    "build_search_criteria",
    "Operation",
    "BulkSpec",
    "RowError",
    "import_attributes",
    "import_attribute_sets",
    "import_categories",
    "import_products",
    "import_prices",
    "import_sources",
    "import_stocks",
    "import_stock_source_links",
    "import_source_items",
    "import_media",
    "to_materialize_result",
]
