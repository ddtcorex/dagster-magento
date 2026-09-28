from conftest import FakeResolver

from dagster_magento.models import AttributeSetRow
from dagster_magento.writers.attribute_sets import plan_attribute_sets


def test_attribute_set_based_on_resolves_skeleton_id():
    resolver = FakeResolver(attribute_sets={"Default": 4})
    row = AttributeSetRow(name="Shirts", based_on="Default")

    result = plan_attribute_sets([row], resolver)

    assert result.failed == []
    assert len(result.operations) == 1
    operation = result.operations[0]
    assert operation.method == "POST"
    assert operation.endpoint == "products/attribute-sets"
    assert operation.row_refs == ("Shirts",)
    assert operation.payload == {
        "attributeSet": {"attribute_set_name": "Shirts", "sort_order": 0},
        "skeletonId": 4,
    }


def test_unknown_based_on_set_is_row_failure():
    resolver = FakeResolver()
    row = AttributeSetRow(name="Shirts", based_on="Ghost")

    result = plan_attribute_sets([row], resolver)

    assert result.operations == []
    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "Shirts"
    assert "Ghost" in result.failed[0].message


def test_existing_set_creates_missing_group_and_assigns_existing_group_attributes():
    resolver = FakeResolver(
        attribute_sets={"Shirts": 9},
        attribute_groups={9: {"General": 70}},
    )
    row = AttributeSetRow(
        name="Shirts",
        groups={
            "General": ["color", "size"],
            "Fabric": ["material"],
        },
    )

    result = plan_attribute_sets([row], resolver)

    assert result.failed == []
    assert len(result.operations) == 3

    color_assign, size_assign, group_create = result.operations
    assert color_assign.method == "POST"
    assert color_assign.endpoint == "products/attribute-sets/attributes"
    assert color_assign.row_refs == ("Shirts",)
    assert color_assign.payload == {
        "attributeSetId": 9,
        "attributeGroupId": 70,
        "attributeCode": "color",
        "sortOrder": 0,
    }
    assert size_assign.payload == {
        "attributeSetId": 9,
        "attributeGroupId": 70,
        "attributeCode": "size",
        "sortOrder": 1,
    }

    assert group_create.method == "POST"
    assert group_create.endpoint == "products/attribute-sets/groups"
    assert group_create.row_refs == ("Shirts",)
    assert group_create.payload == {
        "group": {"attribute_group_name": "Fabric", "attribute_set_id": 9}
    }
