"""Type-specific product operations (configurable, bundle, grouped,
downloadable links).

Every function here mutates the `product` body dict in place (adding the
type's fields to `extension_attributes` or `product_links`) and returns
any follow-up Operations the type needs beyond the main product payload -
configurable is the only type with follow-ups (options + child links);
bundle, grouped and downloadable fold entirely into the body mutation.
"""

import copy
import urllib.parse
from typing import Any

from dagster_magento.models import ProductRow
from dagster_magento.operation import BulkSpec, Operation

# price_type on a bundle selection is a string in the row model but an
# integer flag in the Magento payload - map once, here, rather than
# duplicating the mapping at each call site.
_BUNDLE_PRICE_TYPE = {"fixed": 0, "percent": 1}

# DownloadableLink.shareable is an optional bool; Magento's is_shareable is
# a three-way flag where 2 means "use the store config default" - None
# maps to 2, not to a missing key, because the field is always present in
# a downloadable link payload.
_SHAREABLE = {True: 1, False: 0, None: 2}


def apply_type_parts(row: ProductRow, product: dict[str, Any], resolver) -> list[Operation]:
    """Return the type-specific operations for one product row, mutating
    `product` in place with that type's fields. No-op for simple/virtual."""
    if row.type == "configurable":
        return _configurable_operations(row, resolver)
    if row.type == "bundle":
        _apply_bundle(row, product)
        return []
    if row.type == "grouped":
        _apply_grouped(row, product)
        return []
    if row.type == "downloadable":
        _apply_downloadable(row, product)
        return []
    return []


# -- configurable -------------------------------------------------------------


def _configurable_operations(row: ProductRow, resolver) -> list[Operation]:
    operations = [
        _configurable_option_operation(row, resolver, code, position)
        for position, code in enumerate(row.configurable_attributes)
    ]
    operations.extend(_configurable_child_operation(row, variation) for variation in row.variations)
    return operations


def _configurable_option_operation(row: ProductRow, resolver, code: str, position: int) -> Operation:
    meta = resolver.attribute(code)
    option = {
        "attribute_id": str(meta.id),
        "label": meta.code,
        "position": position,
        "is_use_default": True,
        "values": [
            {"value_index": int(resolver.option_id(code, label))}
            for label in _distinct_variation_labels(row, code)
        ],
    }
    sku = row.sku
    return Operation(
        method="POST",
        endpoint=f"configurable-products/{_quote_sku(sku)}/options",
        payload={"option": option},
        row_refs=(sku,),
        # Phase 1: the option acts on the configurable product, which must
        # have been saved already. In bulk mode consumers run concurrently, so
        # a same-phase option reaches a product that does not exist yet.
        bulk=BulkSpec(
            "configurable-products/bySku/options",
            {"sku": sku, "option": copy.deepcopy(option)},
            phase=1,
        ),
    )


def _distinct_variation_labels(row: ProductRow, code: str) -> list[str]:
    seen: list[str] = []
    for variation in row.variations:
        label = variation.attributes[code]
        if label not in seen:
            seen.append(label)
    return seen


def _configurable_child_operation(row: ProductRow, variation) -> Operation:
    sku = row.sku
    payload = {"childSku": variation.sku}
    return Operation(
        method="POST",
        endpoint=f"configurable-products/{_quote_sku(sku)}/child",
        payload=payload,
        row_refs=(sku,),
        # Phase 1: a link needs both the configurable parent and the child
        # saved, and concurrent consumers must not reach either before its
        # save has finished.
        bulk=BulkSpec(
            "configurable-products/bySku/child",
            {"sku": sku, "childSku": variation.sku},
            phase=1,
        ),
    )


# -- bundle ---------------------------------------------------------------------


def _apply_bundle(row: ProductRow, product: dict[str, Any]) -> None:
    extension_attributes = product.setdefault("extension_attributes", {})
    extension_attributes["bundle_product_options"] = [
        _bundle_option_payload(row.sku, position, option)
        for position, option in enumerate(row.bundle_options)
    ]


def _bundle_option_payload(sku: str, position: int, option) -> dict[str, Any]:
    return {
        "title": option.title,
        "type": option.type,
        "required": option.required,
        "position": position,
        "sku": sku,
        "product_links": [
            _bundle_selection_payload(position, selection)
            for position, selection in enumerate(option.selections)
        ],
    }


def _bundle_selection_payload(position: int, selection) -> dict[str, Any]:
    link: dict[str, Any] = {
        "sku": selection.sku,
        "qty": selection.qty,
        "price": selection.price,
        "is_default": selection.is_default,
        "can_change_quantity": 0,
        "position": position,
    }
    if selection.price_type is not None:
        link["price_type"] = _BUNDLE_PRICE_TYPE[selection.price_type]
    return link


# -- grouped ----------------------------------------------------------------------


def _apply_grouped(row: ProductRow, product: dict[str, Any]) -> None:
    product["product_links"] = [
        {
            "sku": row.sku,
            "link_type": "associated",
            "linked_product_sku": link.sku,
            "linked_product_type": "simple",
            "position": link.position,
            "extension_attributes": {"qty": link.qty},
        }
        for link in row.grouped_links
    ]


# -- downloadable -------------------------------------------------------------------


def _apply_downloadable(row: ProductRow, product: dict[str, Any]) -> None:
    extension_attributes = product.setdefault("extension_attributes", {})
    extension_attributes["downloadable_product_links"] = [
        {
            "title": link.title,
            "sort_order": index,
            "is_shareable": _SHAREABLE[link.shareable],
            "price": link.price,
            "number_of_downloads": link.downloads or 0,
            "link_type": "url",
            "link_url": link.url,
        }
        for index, link in enumerate(row.downloadable_links)
    ]
    extension_attributes["downloadable_product_samples"] = [
        {
            "title": sample.title,
            "sort_order": index,
            "sample_type": "url",
            "sample_url": sample.url,
        }
        for index, sample in enumerate(row.downloadable_samples)
    ]


def _quote_sku(sku: str) -> str:
    return urllib.parse.quote(sku, safe="")
