"""Reusable Dagster resource for Magento 2 REST integrations."""

from dagster_magento.resource import MagentoResource
from dagster_magento.upload import UploadResult

__all__ = ["MagentoResource", "UploadResult"]
