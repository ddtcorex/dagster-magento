"""Reusable Dagster resource for Magento 2 REST integrations."""

from dagster_magento.bulk import AsyncBulkResult
from dagster_magento.resource import MagentoResource
from dagster_magento.search import build_search_criteria
from dagster_magento.upload import UploadResult

__all__ = ["AsyncBulkResult", "MagentoResource", "UploadResult", "build_search_criteria"]
