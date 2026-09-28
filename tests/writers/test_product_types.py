from conftest import FakeResolver

from dagster_magento.models import (
    BundleOption,
    BundleSelection,
    DownloadableLink,
    DownloadableSample,
    GroupedLink,
    ProductRow,
    Variation,
)
from dagster_magento.operation import BulkSpec
from dagster_magento.resolvers import AttributeMeta
from dagster_magento.writers.product_types import apply_type_parts


def _resolver(**overrides):
    defaults = dict(attribute_sets={"Default": 4}, websites={"base": 1})
    defaults.update(overrides)
    return FakeResolver(**defaults)


def test_configurable_emits_options_then_children():
    """A configurable row plans one options operation per configurable
    attribute (in declared order), followed by one child operation per
    variation - options must all come before any child, per the brief."""
    color = AttributeMeta(
        id=93, code="color", frontend_input="select", backend_type="int",
        scope="global", options={"red": "10", "blue": "11"},
    )
    resolver = _resolver(attributes={"color": color})
    row = ProductRow(
        sku="CFG1",
        type="configurable",
        attribute_set="Default",
        websites=["base"],
        configurable_attributes=["color"],
        variations=[
            Variation(sku="CFG1-RED", attributes={"color": "Red"}),
            Variation(sku="CFG1-BLUE", attributes={"color": "Blue"}),
        ],
    )
    product = {"sku": "CFG1"}

    operations = apply_type_parts(row, product, resolver)

    assert [op.endpoint for op in operations] == [
        "configurable-products/CFG1/options",
        "configurable-products/CFG1/child",
        "configurable-products/CFG1/child",
    ]
    options_op = operations[0]
    assert options_op.method == "POST"
    assert options_op.row_refs == ("CFG1",)
    assert options_op.payload == {
        "option": {
            "attribute_id": "93",
            "label": "color",
            "position": 0,
            "is_use_default": True,
            "values": [{"value_index": 10}, {"value_index": 11}],
        }
    }
    assert options_op.bulk == BulkSpec(
        "configurable-products/bySku/options",
        {"sku": "CFG1", "option": options_op.payload["option"]},
    )

    child_ops = operations[1:]
    assert [op.payload for op in child_ops] == [
        {"childSku": "CFG1-RED"},
        {"childSku": "CFG1-BLUE"},
    ]
    for op in child_ops:
        assert op.method == "POST"
        assert op.row_refs == ("CFG1",)
    assert child_ops[0].bulk == BulkSpec(
        "configurable-products/bySku/child", {"sku": "CFG1", "childSku": "CFG1-RED"}
    )


def test_configurable_option_values_are_resolved_ids():
    """Each configurable attribute's `values` list carries the resolved
    option id (via resolver.option_id) for every distinct label seen
    across the variations, in first-seen order - not raw labels, and not
    duplicated when two variations share a label."""
    size = AttributeMeta(
        id=50, code="size", frontend_input="select", backend_type="int",
        scope="global", options={"small": "1", "medium": "2", "large": "3"},
    )
    resolver = _resolver(attributes={"size": size})
    row = ProductRow(
        sku="CFG2",
        type="configurable",
        attribute_set="Default",
        websites=["base"],
        configurable_attributes=["size"],
        variations=[
            Variation(sku="CFG2-S", attributes={"size": "Small"}),
            Variation(sku="CFG2-L", attributes={"size": "Large"}),
            Variation(sku="CFG2-S2", attributes={"size": "Small"}),
        ],
    )
    product = {"sku": "CFG2"}

    operations = apply_type_parts(row, product, resolver)

    options_op = next(op for op in operations if op.endpoint.endswith("/options"))
    assert options_op.payload["option"]["values"] == [
        {"value_index": 1},
        {"value_index": 3},
    ]


def test_bundle_options_map_selection_fields():
    """A bundle row sets extension_attributes.bundle_product_options with
    one entry per BundleOption, each carrying product_links built from its
    selections (qty, price, price_type mapped to 0/1, is_default), and
    leaves any existing extension_attributes content (Task 11's
    website_ids/category_links) untouched."""
    resolver = _resolver()
    row = ProductRow(
        sku="BND1",
        type="bundle",
        attribute_set="Default",
        websites=["base"],
        bundle_options=[
            BundleOption(
                title="Choose one",
                type="radio",
                required=True,
                selections=[
                    BundleSelection(sku="PART-A", qty=1, price=5.0, price_type="fixed", is_default=True),
                    BundleSelection(sku="PART-B", qty=2, price=10.0, price_type="percent"),
                ],
            )
        ],
    )
    product = {"sku": "BND1", "extension_attributes": {"website_ids": [1]}}

    operations = apply_type_parts(row, product, resolver)

    assert operations == []
    assert product["extension_attributes"]["website_ids"] == [1]
    bundle_options = product["extension_attributes"]["bundle_product_options"]
    assert bundle_options == [
        {
            "title": "Choose one",
            "type": "radio",
            "required": True,
            "position": 0,
            "sku": "BND1",
            "product_links": [
                {
                    "sku": "PART-A",
                    "qty": 1,
                    "price": 5.0,
                    "price_type": 0,
                    "is_default": True,
                    "can_change_quantity": 0,
                    "position": 0,
                },
                {
                    "sku": "PART-B",
                    "qty": 2,
                    "price": 10.0,
                    "price_type": 1,
                    "is_default": False,
                    "can_change_quantity": 0,
                    "position": 1,
                },
            ],
        }
    ]


def test_bundle_selection_omits_price_type_when_none():
    """A selection with price_type=None omits the key entirely rather than
    sending null - Magento treats a present-but-null price_type as an
    explicit value, not "unset"."""
    resolver = _resolver()
    row = ProductRow(
        sku="BND2",
        type="bundle",
        attribute_set="Default",
        websites=["base"],
        bundle_options=[
            BundleOption(
                title="Add-on",
                type="checkbox",
                required=False,
                selections=[BundleSelection(sku="ADDON", price=None, price_type=None)],
            )
        ],
    )
    product = {"sku": "BND2", "extension_attributes": {}}

    apply_type_parts(row, product, resolver)

    link = product["extension_attributes"]["bundle_product_options"][0]["product_links"][0]
    assert "price_type" not in link
    assert link["price"] is None


def test_grouped_links_are_associated_with_qty():
    """A grouped row sets product_links with link_type "associated" and
    the qty carried under extension_attributes.qty per link, preserving
    each link's declared position."""
    resolver = _resolver()
    row = ProductRow(
        sku="GRP1",
        type="grouped",
        attribute_set="Default",
        websites=["base"],
        grouped_links=[
            GroupedLink(sku="ITEM-A", qty=3, position=1),
            GroupedLink(sku="ITEM-B", qty=1, position=2),
        ],
    )
    product = {"sku": "GRP1"}

    operations = apply_type_parts(row, product, resolver)

    assert operations == []
    assert product["product_links"] == [
        {
            "sku": "GRP1",
            "link_type": "associated",
            "linked_product_sku": "ITEM-A",
            "linked_product_type": "simple",
            "position": 1,
            "extension_attributes": {"qty": 3},
        },
        {
            "sku": "GRP1",
            "link_type": "associated",
            "linked_product_sku": "ITEM-B",
            "linked_product_type": "simple",
            "position": 2,
            "extension_attributes": {"qty": 1},
        },
    ]


def test_downloadable_links_use_url_type():
    """A downloadable row sets extension_attributes.downloadable_product_links
    (link_type "url", is_shareable mapped True/False/None to 1/0/2, and
    number_of_downloads defaulted to 0) and downloadable_product_samples,
    each ordered with a zero-based sort_order and without disturbing an
    existing extension_attributes dict."""
    resolver = _resolver()
    row = ProductRow(
        sku="DL1",
        type="downloadable",
        attribute_set="Default",
        websites=["base"],
        downloadable_links=[
            DownloadableLink(title="Track 1", url="https://example.com/1.mp3", price=1.5, shareable=True, downloads=3),
            DownloadableLink(title="Track 2", url="https://example.com/2.mp3", shareable=False),
            DownloadableLink(title="Track 3", url="https://example.com/3.mp3"),
        ],
        downloadable_samples=[
            DownloadableSample(title="Sample 1", url="https://example.com/1-sample.mp3"),
        ],
    )
    product = {"sku": "DL1", "extension_attributes": {"website_ids": [1]}}

    operations = apply_type_parts(row, product, resolver)

    assert operations == []
    assert product["extension_attributes"]["website_ids"] == [1]
    links = product["extension_attributes"]["downloadable_product_links"]
    assert links == [
        {
            "title": "Track 1",
            "sort_order": 0,
            "is_shareable": 1,
            "price": 1.5,
            "number_of_downloads": 3,
            "link_type": "url",
            "link_url": "https://example.com/1.mp3",
        },
        {
            "title": "Track 2",
            "sort_order": 1,
            "is_shareable": 0,
            "price": 0,
            "number_of_downloads": 0,
            "link_type": "url",
            "link_url": "https://example.com/2.mp3",
        },
        {
            "title": "Track 3",
            "sort_order": 2,
            "is_shareable": 2,
            "price": 0,
            "number_of_downloads": 0,
            "link_type": "url",
            "link_url": "https://example.com/3.mp3",
        },
    ]
    samples = product["extension_attributes"]["downloadable_product_samples"]
    assert samples == [
        {
            "title": "Sample 1",
            "sort_order": 0,
            "sample_type": "url",
            "sample_url": "https://example.com/1-sample.mp3",
        }
    ]


def test_simple_and_virtual_are_no_ops():
    resolver = _resolver()
    for product_type in ("simple", "virtual"):
        row = ProductRow(sku="S1", type=product_type, attribute_set="Default", websites=["base"])
        product = {"sku": "S1"}

        operations = apply_type_parts(row, product, resolver)

        assert operations == []
        assert product == {"sku": "S1"}
