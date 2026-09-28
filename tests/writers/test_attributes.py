from conftest import FakeResolver

from dagster_magento.models import AttributeOption, AttributeRow
from dagster_magento.resolvers import AttributeMeta
from dagster_magento.writers.attributes import plan_attributes


def test_new_attribute_emits_create_with_frontend_labels_and_scope():
    resolver = FakeResolver(stores={"fr": 2})
    row = AttributeRow(
        code="color",
        frontend_input="select",
        label="Color",
        scope="global",
        options=[AttributeOption(label="Red", sort_order=1)],
        store_labels={"fr": "Couleur"},
        flags={"is_required": True},
    )

    result = plan_attributes([row], resolver)

    assert result.failed == []
    assert len(result.operations) == 1
    operation = result.operations[0]
    assert operation.method == "POST"
    assert operation.endpoint == "products/attributes"
    assert operation.row_refs == ("color",)
    assert operation.payload == {
        "attribute": {
            "attribute_code": "color",
            "frontend_input": "select",
            "default_frontend_label": "Color",
            "frontend_labels": [{"store_id": 2, "label": "Couleur"}],
            "scope": "global",
            "is_user_defined": True,
            "options": [{"label": "Red", "sort_order": 1, "store_labels": []}],
            "is_required": True,
        }
    }
    # preload_attributes must be called with every code up front, never
    # skipped and never called once per row.
    assert resolver.preloaded_codes == ["color"]


def test_existing_attribute_only_adds_missing_options():
    meta = AttributeMeta(
        id=93,
        code="color",
        frontend_input="select",
        backend_type="int",
        scope="global",
        options={"red": "10"},
    )
    resolver = FakeResolver(attributes={"color": meta})
    row = AttributeRow(
        code="color",
        frontend_input="select",
        label="Color",
        scope="global",
        options=[
            AttributeOption(label="Red", sort_order=0),
            AttributeOption(label="Blue", sort_order=1),
        ],
    )

    result = plan_attributes([row], resolver)

    assert result.failed == []
    assert len(result.operations) == 2

    update = result.operations[0]
    assert update.method == "PUT"
    assert update.endpoint == "products/attributes/color"
    assert update.row_refs == ("color",)
    assert update.payload == {
        "attribute": {
            "attribute_id": 93,
            "frontend_input": "select",
            "default_frontend_label": "Color",
            "frontend_labels": [],
            "scope": "global",
            "is_user_defined": True,
        }
    }

    option_create = result.operations[1]
    assert option_create.method == "POST"
    # "Red" already exists (normalize_label-matched) - only "Blue" is missing.
    assert option_create.endpoint == "products/attributes/color/options"
    assert option_create.row_refs == ("color",)
    assert option_create.payload == {
        "option": {
            "label": "Blue",
            "sort_order": 1,
            "is_default": False,
            "store_labels": [],
        }
    }


def test_create_only_skips_existing_codes():
    meta = AttributeMeta(
        id=93,
        code="color",
        frontend_input="select",
        backend_type="int",
        scope="global",
        options={},
    )
    resolver = FakeResolver(attributes={"color": meta})
    existing_row = AttributeRow(code="color", frontend_input="select", label="Color")
    new_row = AttributeRow(code="size", frontend_input="select", label="Size")

    result = plan_attributes([existing_row, new_row], resolver, behavior="create_only")

    assert result.failed == []
    assert len(result.operations) == 1
    assert result.operations[0].endpoint == "products/attributes"
    assert result.operations[0].row_refs == ("size",)


def test_flags_overlapping_reserved_keys_fail_the_row():
    resolver = FakeResolver()
    # "scope" and "options" both shadow keys the writer sets itself -
    # letting the spread win would silently override the writer's own
    # scope/options with whatever the row's flags happen to carry.
    row = AttributeRow(
        code="color",
        frontend_input="select",
        label="Color",
        flags={"scope": "website", "options": [], "is_searchable": True},
    )

    result = plan_attributes([row], resolver)

    assert result.operations == []
    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "color"
    # sorted, and only the actually-reserved keys - "is_searchable" is a
    # legitimate flag and must not be named.
    assert "options" in result.failed[0].message
    assert "scope" in result.failed[0].message
    assert result.failed[0].message.index("options") < result.failed[0].message.index("scope")
    assert "is_searchable" not in result.failed[0].message
