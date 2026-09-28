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
    assert men_op.payload == {"category": {"id": 3, "description": "Men"}}

    women_op = result.operations[1]
    assert women_op.method == "PUT"
    assert women_op.endpoint == "categories/4"
    assert women_op.row_refs == ("Default Category/Women",)
    assert women_op.payload == {"category": {"id": 4, "description": "Women"}}


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
    assert en_op.payload == {"category": {"id": 5, "name": "Shoes", "description": "Footwear"}}


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
