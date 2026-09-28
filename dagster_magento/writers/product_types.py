"""Type-specific product operations (configurable, bundle, grouped,
downloadable links).

Placeholder for this task: every product type returns no additional
operations. A later task fills in the real per-type planning; until then
plan_products still calls apply_type_parts for every row so that wiring
does not have to change again when the real behaviour lands.
"""

from typing import Any

from dagster_magento.models import ProductRow
from dagster_magento.operation import Operation


def apply_type_parts(row: ProductRow, product: dict[str, Any], resolver) -> list[Operation]:
    """Return the type-specific operations for one product row. No-op for
    every type until a later task implements configurable/bundle/grouped/
    downloadable planning."""
    return []
