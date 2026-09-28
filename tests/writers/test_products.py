from conftest import FakeResolver

from dagster_magento.models import GroupedLink, ProductRow
from dagster_magento.operation import BulkSpec
from dagster_magento.resolvers import AttributeMeta
from dagster_magento.writers.products import plan_products


def _resolver(**overrides):
    defaults = dict(attribute_sets={"Default": 4}, websites={"base": 1, "fr": 2})
    defaults.update(overrides)
    return FakeResolver(**defaults)


def test_new_sku_emits_post_with_bulk_spec():
    """A SKU absent from `existing` plans a POST, with a bulk spec that
    reuses the same payload, and resolves categories/websites/digit
    attribute_set along the way."""
    resolver = _resolver(categories={"Default Category/Shoes": 12})
    row = ProductRow(
        sku="A1",
        type="simple",
        attribute_set="7",  # all-digit shortcut - never calls attribute_set_id.
        name="Widget",
        price=9.99,
        status=1,
        visibility=4,
        weight=1.5,
        websites=["base"],
        categories=["Default Category/Shoes"],
    )

    result = plan_products([row], resolver, existing=set())

    assert result.failed == []
    assert len(result.operations) == 1
    op = result.operations[0]
    assert op.method == "POST"
    assert op.endpoint == "products"
    assert op.row_refs == ("A1",)
    assert op.store_code is None
    expected_product = {
        "sku": "A1",
        "type_id": "simple",
        "attribute_set_id": 7,
        "name": "Widget",
        "price": 9.99,
        "status": 1,
        "visibility": 4,
        "weight": 1.5,
        "custom_attributes": [],
        "extension_attributes": {
            "website_ids": [1],
            "category_links": [{"position": 0, "category_id": "12"}],
        },
    }
    assert op.payload == {"product": expected_product}
    assert op.bulk == BulkSpec("products", {"product": expected_product})


def test_existing_sku_emits_put_bysku_bulk_payload_with_sku():
    """A SKU already in `existing` plans a PUT to the quoted SKU path, and
    the bulk spec carries the extra top-level "sku" key. A SKU with a
    reserved URL character is quoted; no categories omits category_links."""
    resolver = _resolver(attribute_sets={"Special": 9})
    row = ProductRow(
        sku="A/1",
        type="simple",
        attribute_set="Special",
        websites=["base"],
    )

    result = plan_products([row], resolver, existing={"A/1"})

    assert result.failed == []
    assert len(result.operations) == 1
    op = result.operations[0]
    assert op.method == "PUT"
    assert op.endpoint == "products/A%2F1"
    assert op.row_refs == ("A/1",)
    expected_product = {
        "sku": "A/1",
        "type_id": "simple",
        "attribute_set_id": 9,
        "custom_attributes": [],
        "extension_attributes": {"website_ids": [1]},
    }
    assert op.payload == {"product": expected_product}
    assert op.bulk == BulkSpec("products/bySku", {"sku": "A/1", "product": expected_product})


def test_select_and_multiselect_labels_resolve_to_option_ids():
    """Select values resolve to a single option id; multiselect values
    (list or comma string) resolve to comma-joined ids."""
    color = AttributeMeta(
        id=1, code="color", frontend_input="select", backend_type="int",
        scope="global", options={"red": "10"},
    )
    tags = AttributeMeta(
        id=2, code="tags", frontend_input="multiselect", backend_type="varchar",
        scope="global", options={"eco": "5", "sale": "6"},
    )
    resolver = _resolver(attributes={"color": color, "tags": tags})
    row = ProductRow(
        sku="A1",
        attribute_set="Default",
        websites=["base"],
        attributes={"color": "Red", "tags": "Eco, Sale"},
    )

    result = plan_products([row], resolver, existing=set())

    assert result.failed == []
    op = result.operations[0]
    assert op.payload["product"]["custom_attributes"] == [
        {"attribute_code": "color", "value": "10"},
        {"attribute_code": "tags", "value": "5,6"},
    ]
    # preload_attributes must be called with every code up front.
    assert set(resolver.preloaded_codes) == {"color", "tags"}


def test_store_values_emit_minimal_payload_per_store_code():
    """Each store_values entry plans its own PUT with store_code set and
    only the localized keys - never price, websites, categories or the
    row's global attributes."""
    description = AttributeMeta(
        id=3, code="description", frontend_input="textarea", backend_type="text",
        scope="store", options={},
    )
    resolver = _resolver(attributes={"description": description})
    row = ProductRow(
        sku="A1",
        attribute_set="Default",
        price=9.99,
        websites=["base"],
        categories=[],
        attributes={},
        store_values={
            "fr": {"name": "Widget FR", "description": "Desc FR"},
            "en": {"status": 1},
        },
    )

    result = plan_products([row], resolver, existing=set())

    assert result.failed == []
    # Main create operation, plus one PUT per store code.
    assert len(result.operations) == 3
    store_ops = {op.store_code: op for op in result.operations if op.store_code is not None}
    assert set(store_ops) == {"fr", "en"}

    fr_op = store_ops["fr"]
    assert fr_op.method == "PUT"
    assert fr_op.endpoint == "products/A1"
    assert fr_op.row_refs == ("A1",)
    assert fr_op.payload == {
        "product": {
            "sku": "A1",
            "name": "Widget FR",
            "custom_attributes": [{"attribute_code": "description", "value": "Desc FR"}],
        }
    }
    assert fr_op.bulk == BulkSpec("products/bySku", {"sku": "A1", "product": fr_op.payload["product"]})

    en_op = store_ops["en"]
    assert en_op.payload == {
        "product": {"sku": "A1", "status": 1, "custom_attributes": []}
    }


def test_disable_emits_status_only_payload():
    """behavior="disable" plans a status-only PUT for an existing SKU and
    nothing else (no store values, no type parts); a SKU not in `existing`
    is a row error instead."""
    resolver = _resolver()
    row = ProductRow(
        sku="A1",
        attribute_set="Default",
        name="Widget",
        price=9.99,
        websites=["base"],
        store_values={"fr": {"name": "Widget FR"}},
    )

    result = plan_products([row], resolver, existing={"A1"}, behavior="disable")

    assert result.failed == []
    assert len(result.operations) == 1
    op = result.operations[0]
    assert op.method == "PUT"
    assert op.endpoint == "products/A1"
    assert op.row_refs == ("A1",)
    assert op.store_code is None
    assert op.payload == {"product": {"sku": "A1", "status": 2}}
    assert op.bulk == BulkSpec("products/bySku", {"sku": "A1", "product": {"sku": "A1", "status": 2}})

    missing_result = plan_products([row], resolver, existing=set(), behavior="disable")
    assert missing_result.operations == []
    assert len(missing_result.failed) == 1
    assert missing_result.failed[0].row_ref == "A1"
    assert missing_result.failed[0].message == "sku does not exist"


def test_update_only_skips_new_skus():
    resolver = _resolver()
    row = ProductRow(sku="NEW1", attribute_set="Default", websites=["base"])

    result = plan_products([row], resolver, existing=set(), behavior="update_only")

    assert result.operations == []
    assert result.failed == []
    assert result.skipped == ["NEW1"]


def test_create_only_skips_existing():
    resolver = _resolver()
    row = ProductRow(sku="OLD1", attribute_set="Default", websites=["base"])

    result = plan_products([row], resolver, existing={"OLD1"}, behavior="create_only")

    assert result.operations == []
    assert result.failed == []
    assert result.skipped == ["OLD1"]


def test_unresolvable_option_fails_only_that_row():
    """A row whose attributes reference an attribute code the resolver
    does not know about fails only that row - a later row still plans."""
    resolver = _resolver()
    bad_row = ProductRow(
        sku="BAD",
        attribute_set="Default",
        websites=["base"],
        attributes={"ghost_attr": "whatever"},
    )
    good_row = ProductRow(sku="GOOD", attribute_set="Default", websites=["base"])

    result = plan_products([bad_row, good_row], resolver, existing=set())

    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "BAD"
    assert "ghost_attr" in result.failed[0].message

    assert len(result.operations) == 1
    assert result.operations[0].row_refs == ("GOOD",)


def test_operation_and_bulk_payloads_never_alias():
    """Every builder (create, update, disable, store-value) must hand back
    an Operation.payload and a BulkSpec.payload that are independent dict
    objects, down to the nested "product" dict and its nested lists - the
    module contract in operation.py forbids a writer from sharing one dict
    across two Operations/BulkSpecs, and equality-based assertions alone
    cannot see an aliasing bug like this."""
    color = AttributeMeta(
        id=1, code="color", frontend_input="text", backend_type="varchar",
        scope="global", options={},
    )
    resolver = _resolver(attributes={"color": color})
    row = ProductRow(
        sku="A1",
        attribute_set="Default",
        name="Widget",
        websites=["base"],
        attributes={"color": "red"},
        store_values={"fr": {"name": "Widget FR", "color": "rouge"}},
    )

    create_result = plan_products([row], resolver, existing=set())
    update_result = plan_products([row], resolver, existing={"A1"})
    disable_result = plan_products([row], resolver, existing={"A1"}, behavior="disable")

    create_op = create_result.operations[0]
    update_op = update_result.operations[0]
    disable_op = disable_result.operations[0]
    store_op = next(op for op in update_result.operations if op.store_code == "fr")

    for op in (create_op, update_op, disable_op, store_op):
        assert op.payload is not op.bulk.payload
        assert op.payload["product"] is not op.bulk.payload["product"]

    # Nested lists (custom_attributes) must not be shared either.
    assert (
        create_op.payload["product"]["custom_attributes"]
        is not create_op.bulk.payload["product"]["custom_attributes"]
    )
    assert (
        store_op.payload["product"]["custom_attributes"]
        is not store_op.bulk.payload["product"]["custom_attributes"]
    )


def test_type_parts_reach_both_operation_and_bulk_payloads():
    """apply_type_parts mutates the product body in place - plan_products
    must call it before the main operation's body is copied into
    Operation.payload/BulkSpec.payload, or a type part (here: a grouped
    product's product_links) would land in neither side, or only one."""
    resolver = _resolver()
    row = ProductRow(
        sku="GRP1",
        type="grouped",
        attribute_set="Default",
        websites=["base"],
        grouped_links=[GroupedLink(sku="ITEM-A", qty=2, position=1)],
    )

    result = plan_products([row], resolver, existing=set())

    assert result.failed == []
    main_op = next(op for op in result.operations if op.store_code is None)
    expected_links = [
        {
            "sku": "GRP1",
            "link_type": "associated",
            "linked_product_sku": "ITEM-A",
            "linked_product_type": "simple",
            "position": 1,
            "extension_attributes": {"qty": 2},
        }
    ]
    assert main_op.payload["product"]["product_links"] == expected_links
    assert main_op.bulk.payload["product"]["product_links"] == expected_links
    # The two sides never share the same list/dict objects.
    assert (
        main_op.payload["product"]["product_links"]
        is not main_op.bulk.payload["product"]["product_links"]
    )


def test_reserved_keys_in_attributes_fail_the_row():
    """attributes keys that shadow writer-owned top-level payload names
    (e.g. "price", "sku") are a row error naming the offending keys."""
    resolver = _resolver()
    row = ProductRow(
        sku="A1",
        attribute_set="Default",
        websites=["base"],
        attributes={"price": 1, "sku": "hacked"},
    )

    result = plan_products([row], resolver, existing=set())

    assert result.operations == []
    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "A1"
    assert "price" in result.failed[0].message
    assert "sku" in result.failed[0].message
    assert result.failed[0].message.index("price") < result.failed[0].message.index("sku")
    assert "shadow writer-owned keys" in result.failed[0].message
