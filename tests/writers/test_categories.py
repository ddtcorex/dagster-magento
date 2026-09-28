from conftest import FakeResolver

from dagster_magento.models import CategoryRow
from dagster_magento.writers.categories import plan_categories


def test_categories_are_ensured_before_attribute_updates():
    """ensure_categories is called once with all paths, operations follow."""
    resolver = FakeResolver(
        categories={"Default Category/Men": 3, "Default Category/Women": 4},
        stores={"fr": 2, "en": 1},
    )
    rows = [
        CategoryRow(path="Default Category/Men", attributes={"description": "Men"}),
        CategoryRow(path="Default Category/Women", attributes={"description": "Women"}),
    ]

    result = plan_categories(rows, resolver)

    assert result.failed == []
    assert len(result.operations) == 2

    # Both operations should be PUT to categories/{id}
    men_op = result.operations[0]
    assert men_op.method == "PUT"
    assert men_op.endpoint == "categories/3"
    assert men_op.row_refs == ("Default Category/Men",)
    assert men_op.store_code is None
    assert men_op.payload == {
        "category": {"id": 3, "custom_attributes": [{"attribute_code": "description", "value": "Men"}]}
    }

    women_op = result.operations[1]
    assert women_op.method == "PUT"
    assert women_op.endpoint == "categories/4"
    assert women_op.row_refs == ("Default Category/Women",)
    assert women_op.payload == {
        "category": {"id": 4, "custom_attributes": [{"attribute_code": "description", "value": "Women"}]}
    }


def test_category_store_values_use_store_code_and_minimal_payload():
    """Store values emit PUT per store code with that store_code."""
    resolver = FakeResolver(
        categories={"Default Category/Shoes": 5},
        stores={"fr": 2, "en": 1},
    )
    row = CategoryRow(
        path="Default Category/Shoes",
        attributes={},
        store_values={
            "fr": {"name": "Chaussures"},
            "en": {"name": "Shoes", "description": "Footwear"},
        },
    )

    result = plan_categories([row], resolver)

    assert result.failed == []
    # One per store (no global attribute update since attributes is empty)
    assert len(result.operations) == 2

    fr_op = result.operations[0]
    assert fr_op.method == "PUT"
    assert fr_op.endpoint == "categories/5"
    assert fr_op.store_code == "fr"
    assert fr_op.row_refs == ("Default Category/Shoes",)
    assert fr_op.payload == {"category": {"id": 5, "name": "Chaussures"}}

    en_op = result.operations[1]
    assert en_op.method == "PUT"
    assert en_op.endpoint == "categories/5"
    assert en_op.store_code == "en"
    assert en_op.row_refs == ("Default Category/Shoes",)
    assert en_op.payload == {
        "category": {
            "id": 5,
            "name": "Shoes",
            "custom_attributes": [{"attribute_code": "description", "value": "Footwear"}],
        }
    }


def test_ensure_failure_marks_row_failed():
    """When ensure_categories raises ResolveError, retry per row."""
    resolver = FakeResolver(
        categories={"Default Category/Found": 10},
    )
    rows = [
        CategoryRow(path="Default Category/Found", attributes={"color": "blue"}),
        CategoryRow(path="Default Category/NotFound", attributes={"color": "red"}),
        CategoryRow(path="Default Category/AlsoFound", attributes={"color": "green"}),
    ]

    # Populate the resolver with the missing path so we can test partial failures
    resolver._categories["Default Category/AlsoFound"] = 11

    result = plan_categories(rows, resolver)

    # Only the NotFound row should fail
    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "Default Category/NotFound"
    assert "unknown category path" in result.failed[0].message

    # The other two rows should have operations
    assert len(result.operations) == 2
    assert result.operations[0].row_refs == ("Default Category/Found",)
    assert result.operations[1].row_refs == ("Default Category/AlsoFound",)


def test_reserved_keys_in_attributes_fail_the_row():
    """Attributes shadowing id, parent_id, path, or name fail the row."""
    resolver = FakeResolver(
        categories={"Default Category/Good": 1, "Default Category/Bad": 2},
    )
    rows = [
        CategoryRow(path="Default Category/Good", attributes={"description": "OK"}),
        CategoryRow(path="Default Category/Bad", attributes={"id": 999, "name": "Hacked"}),
    ]

    result = plan_categories(rows, resolver)

    # Good row succeeds, Bad row fails
    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "Default Category/Bad"
    assert "id" in result.failed[0].message
    assert "name" in result.failed[0].message
    assert result.failed[0].message.index("id") < result.failed[0].message.index("name")
    assert "shadow writer-owned keys" in result.failed[0].message

    # Only the good row has operations
    assert len(result.operations) == 1
    assert result.operations[0].row_refs == ("Default Category/Good",)


def test_localized_name_is_allowed_but_id_is_rejected():
    """Store values can have "name" but not "id", "parent_id", or "path"."""
    resolver = FakeResolver(
        categories={"Default Category/Shirts": 5},
    )
    row = CategoryRow(
        path="Default Category/Shirts",
        attributes={},
        store_values={
            "fr": {"name": "Chemises"},  # OK - localized name is legitimate.
            "en": {"id": 999, "name": "Shirts"},  # BAD - id shadows writer-owned field.
        },
    )

    result = plan_categories([row], resolver)

    # Row should fail due to reserved key in store_values[en]
    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "Default Category/Shirts"
    assert "store_values[en]" in result.failed[0].message
    assert "id" in result.failed[0].message
    assert "shadow writer-owned keys" in result.failed[0].message

    # No operations emitted
    assert len(result.operations) == 0


def test_category_payload_keeps_dto_fields_top_level_and_the_rest_as_custom_attributes():
    # Verified live on 2.4.9: PUT categories/{id} rejects EAV attributes
    # such as image or url_key at the top level ("field is not
    # supported"); only CategoryInterface fields live there, and
    # available_sort_by is a string array.
    resolver = FakeResolver(categories={"Default Category/Men": 3})
    row = CategoryRow(
        path="Default Category/Men",
        attributes={
            "is_active": 1,
            "include_in_menu": 0,
            "position": "7",
            "available_sort_by": "position, name",
            "url_key": "men",
            "is_anchor": 1,
        },
    )

    result = plan_categories([row], resolver)

    assert result.operations[0].payload == {
        "category": {
            "id": 3,
            "is_active": 1,
            "include_in_menu": 0,
            "position": "7",
            "available_sort_by": ["position", "name"],
            "custom_attributes": [
                {"attribute_code": "url_key", "value": "men"},
                {"attribute_code": "is_anchor", "value": 1},
            ],
        }
    }
