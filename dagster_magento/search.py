def build_search_criteria(filters: list | None = None, sort_orders: list | None = None) -> dict:
    """Build Magento REST searchCriteria query params for `get`/`get_paginated`.

    `filters` is a list of `(field, value, condition_type)` tuples (condition_type
    defaults to "eq"), all combined into a single AND'd filter group - Magento's
    filter_groups[]/filters[] nesting supports OR-across-groups too, but every
    sync task this library targets (products/customers/orders updated since a
    date, low-stock items, a customer by email) only needs a flat AND, so that's
    all this builds. A list/tuple `value` is comma-joined, matching what Magento
    expects for "in"/"nin" condition_types.

    `sort_orders` is a list of `(field, direction)` tuples, direction being "ASC"
    or "DESC".

    Does not set searchCriteria[page_size]/[current_page] - get_paginated() owns
    those.
    """
    params = {}

    for index, condition in enumerate(filters or []):
        field, value, *rest = condition
        condition_type = rest[0] if rest else "eq"
        if isinstance(value, (list, tuple)):
            value = ",".join(str(item) for item in value)
        prefix = f"searchCriteria[filter_groups][0][filters][{index}]"
        params[f"{prefix}[field]"] = field
        params[f"{prefix}[value]"] = value
        params[f"{prefix}[condition_type]"] = condition_type

    for index, (field, direction) in enumerate(sort_orders or []):
        prefix = f"searchCriteria[sortOrders][{index}]"
        params[f"{prefix}[field]"] = field
        params[f"{prefix}[direction]"] = direction

    return params
