"""Plans create/update operations for EAV product attributes and their
options.

An attribute that the resolver does not know about yet is planned as a
create; one it does know about is planned as an update plus one options
POST per option the resolver's cached metadata is still missing. Missing
is decided with normalize_label(), the same encoding- and
case-insensitive comparison the resolver itself uses, so a label already
present in a different HTML-escaped form never gets re-created.
"""

from typing import Any

from dagster_magento.models import AttributeOption, AttributeRow
from dagster_magento.operation import Operation, RowError
from dagster_magento.resolvers import AttributeMeta, ResolveError, normalize_label
from dagster_magento.writers import PlanResult


def plan_attributes(
    rows: list[AttributeRow], resolver, behavior: str = "upsert"
) -> PlanResult:
    """Plan `POST products/attributes` for every row the resolver does not
    already know, `PUT products/attributes/{code}` plus missing-option
    creates for every row it does, unless `behavior` is "create_only", in
    which case an already-known code is skipped entirely."""
    resolver.preload_attributes(row.code for row in rows)

    operations: list[Operation] = []
    failed: list[RowError] = []

    for row in rows:
        try:
            meta = resolver.attribute(row.code)
        except ResolveError:
            meta = None

        try:
            if meta is None:
                operations.append(_create_operation(row, resolver))
                continue

            if behavior == "create_only":
                continue

            operations.append(_update_operation(row, resolver, meta))
            operations.extend(_missing_option_operations(row, meta, resolver))
        except ResolveError as error:
            # A ResolveError here (for example an unknown store view in
            # store_labels) is a genuine row failure, unlike the
            # "attribute not found" case above, which just means "create".
            failed.append(RowError(row_ref=row.code, message=str(error)))

    return PlanResult(operations=operations, failed=failed)


def _create_operation(row: AttributeRow, resolver) -> Operation:
    return Operation(
        method="POST",
        endpoint="products/attributes",
        payload=_attribute_payload(row, resolver),
        row_refs=(row.code,),
    )


def _update_operation(row: AttributeRow, resolver, meta: AttributeMeta) -> Operation:
    payload = _attribute_payload(row, resolver)
    attribute = payload["attribute"]
    del attribute["attribute_code"]
    del attribute["options"]
    attribute["attribute_id"] = meta.id
    return Operation(
        method="PUT",
        endpoint=f"products/attributes/{row.code}",
        payload=payload,
        row_refs=(row.code,),
    )


def _missing_option_operations(
    row: AttributeRow, meta: AttributeMeta, resolver
) -> list[Operation]:
    operations = []
    for option in row.options:
        if normalize_label(option.label) in meta.options:
            continue
        operations.append(
            Operation(
                method="POST",
                endpoint=f"products/attributes/{row.code}/options",
                payload={
                    "option": {
                        "label": option.label,
                        "sort_order": option.sort_order,
                        "is_default": False,
                        "store_labels": _store_labels(option.store_labels, resolver),
                    }
                },
                row_refs=(row.code,),
            )
        )
    return operations


def _attribute_payload(row: AttributeRow, resolver) -> dict[str, Any]:
    # A fresh dict every call - never shared between operations.
    return {
        "attribute": {
            "attribute_code": row.code,
            "frontend_input": row.frontend_input,
            "default_frontend_label": row.label,
            "frontend_labels": _store_labels(row.store_labels, resolver),
            "scope": row.scope,
            "is_user_defined": True,
            "options": [_option_entry(option, resolver) for option in row.options],
            **row.flags,
        }
    }


def _option_entry(option: AttributeOption, resolver) -> dict[str, Any]:
    return {
        "label": option.label,
        "sort_order": option.sort_order,
        "store_labels": _store_labels(option.store_labels, resolver),
    }


def _store_labels(store_labels: dict[str, str], resolver) -> list[dict[str, Any]]:
    return [
        {"store_id": resolver.store_id(store_code), "label": label}
        for store_code, label in store_labels.items()
    ]
