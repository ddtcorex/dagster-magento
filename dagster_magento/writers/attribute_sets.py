"""Plans create operations for attribute sets, their groups and the
attribute-to-group assignments inside them.

This writer is pass-based (see spec 5.5 / 5.6): a set that does not exist
yet only ever gets its own create operation in one pass - its groups and
attribute assignments are planned in a later pass, once the importer has
re-run the resolver and the set actually exists. Assigning an attribute
that is already in a group is idempotent in Magento, so no membership
check is needed before emitting an assignment.
"""

from dagster_magento.models import AttributeSetRow
from dagster_magento.operation import Operation, RowError
from dagster_magento.resolvers import ResolveError
from dagster_magento.writers import PlanResult


def plan_attribute_sets(rows: list[AttributeSetRow], resolver) -> PlanResult:
    """Plan `POST products/attribute-sets` for a set the resolver does not
    know about yet (skeletonId from `based_on`, itself a row failure when
    unknown). For a known set, plan a group create for each group in
    `row.groups` that does not exist yet, and an attribute-assignment
    create for each attribute code in a group that already exists."""
    operations: list[Operation] = []
    failed: list[RowError] = []

    for row in rows:
        try:
            set_id = resolver.attribute_set_id(row.name)
        except ResolveError:
            set_id = None

        if set_id is None:
            operation = _create_set_operation(row, resolver, failed)
            if operation is not None:
                operations.append(operation)
            continue

        operations.extend(_group_operations(row, set_id, resolver))

    return PlanResult(operations=operations, failed=failed)


def _create_set_operation(row: AttributeSetRow, resolver, failed: list[RowError]) -> Operation | None:
    try:
        skeleton_id = resolver.attribute_set_id(row.based_on)
    except ResolveError:
        failed.append(
            RowError(
                row_ref=row.name,
                message=f"unknown based_on attribute set: {row.based_on}",
            )
        )
        return None

    return Operation(
        method="POST",
        endpoint="products/attribute-sets",
        payload={
            "attributeSet": {"attribute_set_name": row.name, "sort_order": 0},
            "skeletonId": skeleton_id,
        },
        row_refs=(row.name,),
    )


def _group_operations(row: AttributeSetRow, set_id: int, resolver) -> list[Operation]:
    operations: list[Operation] = []
    for group_name, codes in row.groups.items():
        group_id = resolver.attribute_group_id(set_id, group_name)
        if group_id is None:
            operations.append(
                Operation(
                    method="POST",
                    endpoint="products/attribute-sets/groups",
                    payload={
                        "group": {
                            "attribute_group_name": group_name,
                            "attribute_set_id": set_id,
                        }
                    },
                    row_refs=(row.name,),
                )
            )
            continue

        for index, code in enumerate(codes):
            operations.append(
                Operation(
                    method="POST",
                    endpoint="products/attribute-sets/attributes",
                    payload={
                        "attributeSetId": set_id,
                        "attributeGroupId": group_id,
                        "attributeCode": code,
                        "sortOrder": index,
                    },
                    row_refs=(row.name,),
                )
            )
    return operations
