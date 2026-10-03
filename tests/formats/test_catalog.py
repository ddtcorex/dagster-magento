import pytest

from dagster_magento.formats import catalog


def _warn_spy():
    calls: list[str] = []
    return calls, calls.append


# -- malformed cells never raise, always become one RowError -------------------


@pytest.mark.parametrize(
    "from_rows, rows",
    [
        pytest.param(
            catalog.products_from_rows,
            [
                (2, {"sku": "BAD1", "price": "not-a-number"}),
                (3, {"sku": "GOOD1", "price": "9.99"}),
            ],
            id="price",
        ),
        pytest.param(
            catalog.products_from_rows,
            [
                (2, {"sku": "BAD1", "weight": "not-a-number"}),
                (3, {"sku": "GOOD1", "weight": "1.5"}),
            ],
            id="weight",
        ),
        pytest.param(
            catalog.products_from_rows,
            [
                (2, {"sku": "BAD1", "associated_skus": "SKU1=not-a-number"}),
                (3, {"sku": "GOOD1", "associated_skus": "SKU1=2.0"}),
            ],
            id="associated_skus_qty",
        ),
        pytest.param(
            catalog.products_from_rows,
            [
                (
                    2,
                    {
                        "sku": "BAD1",
                        "bundle_values": "name=Opt1,type=select,required=1,sku=S1,default_qty=not-a-number",
                    },
                ),
                (
                    3,
                    {
                        "sku": "GOOD1",
                        "bundle_values": "name=Opt1,type=select,required=1,sku=S1,default_qty=1.0",
                    },
                ),
            ],
            id="bundle_default_qty",
        ),
        pytest.param(
            catalog.products_from_rows,
            [
                (2, {"sku": "BAD1", "bundle_values": "name=Opt1,type=select,required=1,sku=S1,price=not-a-number"}),
                (3, {"sku": "GOOD1", "bundle_values": "name=Opt1,type=select,required=1,sku=S1,price=15.0"}),
            ],
            id="bundle_price",
        ),
        pytest.param(
            catalog.products_from_rows,
            [
                (2, {"sku": "BAD1", "bundle_values": "name=Opt1,type=not-a-real-type,required=1,sku=S1"}),
                (3, {"sku": "GOOD1", "bundle_values": "name=Opt1,type=select,required=1,sku=S1"}),
            ],
            id="bundle_unknown_type",
        ),
        pytest.param(
            catalog.prices_from_rows,
            [
                (2, {"sku": "BAD1", "tier_price_qty": "not-a-number", "tier_price": "10"}),
                (3, {"sku": "GOOD1", "tier_price_qty": "5", "tier_price": "10"}),
            ],
            id="tier_price_qty",
        ),
        pytest.param(
            catalog.prices_from_rows,
            [
                (2, {"sku": "BAD1", "tier_price_qty": "5", "tier_price": "not-a-number"}),
                (3, {"sku": "GOOD1", "tier_price_qty": "5", "tier_price": "10"}),
            ],
            id="tier_price",
        ),
        pytest.param(
            catalog.products_from_rows,
            [
                (
                    2,
                    {
                        "sku": "BAD1",
                        "downloadable_links": "title=Link1,price=0,url=https://example.test/y.zip,downloads=not-a-number",
                    },
                ),
                (
                    3,
                    {
                        "sku": "GOOD1",
                        "downloadable_links": "title=Link1,price=0,url=https://example.test/y.zip,downloads=5",
                    },
                ),
            ],
            id="downloadable_links_downloads",
        ),
    ],
)
def test_malformed_numeric_cells_become_row_errors(from_rows, rows):
    """A malformed numeric/enum cell never raises - it becomes exactly one
    RowError for its own row, and every other row in the same input still
    parses (the contract every *_from_rows function makes)."""
    results, errors = from_rows(rows)

    assert len(errors) == 1
    assert "BAD1" in errors[0].row_ref
    assert len(results) == 1
    assert results[0].sku == "GOOD1"


# -- products -----------------------------------------------------------------


def test_products_maps_basic_columns_and_websites_and_categories():
    rows = [
        (
            2,
            {
                "sku": "SKU1",
                "store_view_code": "",
                "attribute_set_code": "Default",
                "product_type": "simple",
                "product_websites": "base,eu",
                "categories": "Default Category/Women,Default Category/Sale",
                "name": "Test Product",
                "price": "19.99",
                "weight": "1.5",
                "product_online": "1",
                "visibility": "Catalog, Search",
                "description": "A description",
            },
        )
    ]

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    assert len(products) == 1
    product = products[0]
    assert product.sku == "SKU1"
    assert product.attribute_set == "Default"
    assert product.type == "simple"
    assert product.websites == ["base", "eu"]
    assert product.categories == ["Default Category/Women", "Default Category/Sale"]
    assert product.name == "Test Product"
    assert product.price == 19.99
    assert product.weight == 1.5
    assert product.status == 1
    assert product.visibility == 4
    assert product.attributes == {"description": "A description"}


def test_products_missing_sku_is_row_error():
    rows = [(2, {"sku": "", "name": "No sku"})]

    products, errors = catalog.products_from_rows(rows)

    assert products == []
    assert len(errors) == 1
    assert errors[0].row_ref == "line 2"


def test_products_unknown_visibility_text_is_row_error():
    rows = [(2, {"sku": "SKU1", "visibility": "Nonsense"})]

    products, errors = catalog.products_from_rows(rows)

    assert products == []
    assert len(errors) == 1
    assert "SKU1" in errors[0].row_ref
    assert "visibility" in errors[0].message.lower()


def test_firebear_only_columns_are_dropped_with_one_warning_each():
    """Firebear-only columns (group, tier_prices) are dropped, warned
    exactly once per column name across the whole file, not once per row."""
    calls, warn = _warn_spy()
    rows = [
        (2, {"sku": "SKU1", "group": "1", "tier_prices": "x"}),
        (3, {"sku": "SKU2", "group": "1", "tier_prices": "y"}),
    ]

    products, errors = catalog.products_from_rows(rows, warn=warn)

    assert errors == []
    assert len(products) == 2
    assert len(calls) == 2
    assert any("group" in call for call in calls)
    assert any("tier_prices" in call for call in calls)


def test_products_native_stock_and_special_price_columns_are_dropped():
    calls, warn = _warn_spy()
    rows = [
        (
            2,
            {
                "sku": "SKU1",
                "qty": "10",
                "is_in_stock": "1",
                "special_price": "9.99",
                "created_at": "2020-01-01",
                "attribute|custom_thing": "value",
            },
        )
    ]

    products, errors = catalog.products_from_rows(rows, warn=warn)

    assert errors == []
    assert products[0].attributes == {}
    assert len(calls) == 5


def test_store_view_rows_fold_into_store_values():
    rows = [
        (2, {"sku": "SKU1", "store_view_code": "", "name": "Global Name"}),
        (
            3,
            {
                "sku": "SKU1",
                "store_view_code": "fr",
                "name": "Nom FR",
                "meta_title": "Titre",
            },
        ),
    ]

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    assert len(products) == 1
    product = products[0]
    assert product.name == "Global Name"
    assert product.store_values == {"fr": {"name": "Nom FR", "meta_title": "Titre"}}


def test_store_view_row_for_unknown_sku_is_row_error():
    rows = [(2, {"sku": "SKU1", "store_view_code": "fr", "name": "Nom FR"})]

    products, errors = catalog.products_from_rows(rows)

    assert products == []
    assert len(errors) == 1
    assert "SKU1" in errors[0].row_ref


def test_default_store_code_is_global_by_default():
    # Pins the real shape of the Firebear sample export
    # (product_all_types.csv): every row, including the only one per sku,
    # carries store_view_code "default" rather than leaving it blank.
    # Without this, every product in that file would be rejected as a
    # "store-view row for unknown sku".
    rows = [(2, {"sku": "SKU1", "store_view_code": "default", "name": "Global Name"})]

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    assert len(products) == 1
    assert products[0].name == "Global Name"
    assert products[0].store_values == {}


def test_default_store_code_is_store_level_when_opted_out():
    # On a real multi-store install "default" is a genuine, addressable
    # store view code - passing global_store_codes=("",) makes a
    # store_view_code "default" row fold into store_values["default"]
    # instead of being written globally through /rest/all/.
    rows = [
        (2, {"sku": "SKU1", "store_view_code": "", "name": "Global Name"}),
        (3, {"sku": "SKU1", "store_view_code": "default", "name": "Default Store Name"}),
    ]

    products, errors = catalog.products_from_rows(rows, global_store_codes=("",))

    assert errors == []
    assert len(products) == 1
    product = products[0]
    assert product.name == "Global Name"
    assert product.store_values == {"default": {"name": "Default Store Name"}}


def test_products_configurable_variations_build_attributes_and_order():
    rows = [
        (
            2,
            {
                "sku": "PARENT",
                "product_type": "configurable",
                "configurable_variations": "sku=A,size=S,color=Gray,default=1|sku=B,size=S,color=Green",
            },
        )
    ]

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    product = products[0]
    assert [variation.sku for variation in product.variations] == ["A", "B"]
    assert product.configurable_attributes == ["size", "color"]


def test_products_bundle_values_and_bundle_flags():
    rows = [
        (
            2,
            {
                "sku": "BUNDLE1",
                "product_type": "bundle",
                "bundle_values": (
                    "name=Opt1,type=select,required=1,sku=S1,price=15.0000,default=0,"
                    "default_qty=1.0000,price_type=fixed"
                ),
                "bundle_price_type": "dynamic",
                "bundle_price_view": "As low as",
            },
        )
    ]

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    product = products[0]
    assert len(product.bundle_options) == 1
    assert product.bundle_options[0].selections[0].sku == "S1"
    assert product.attributes["price_type"] == 0
    assert product.attributes["price_view"] == 1


def test_products_rename_native_export_columns_to_attribute_codes():
    # The native export renames several attributes (Magento's
    # Import\Product::$_fieldsMap); REST only knows the attribute codes,
    # so an unmapped column fails the row with "unknown attribute"
    # (verified live on 2.4.9). Labels stay labels: the product writer
    # resolves select labels such as "Taxable Goods" to option ids.
    calls, warn = _warn_spy()
    rows = [
        (
            2,
            {
                "sku": "BUNDLE1",
                "product_type": "bundle",
                "tax_class_name": "Taxable Goods",
                "new_from_date": "2017-01-01 12:12",
                "new_to_date": "2017-02-02 12:12",
                "display_product_options_in": "Block after Info Column",
                "map_price": "10",
                "msrp_price": "11",
                "meta_keywords": "a, b",
                "additional_attributes": "shipment_type=together,color=Red",
                "bundle_shipment_type": "separately",
                "map_enabled": "0",
                "product_options_container": "Block after Info Column",
            },
        )
    ]

    products, errors = catalog.products_from_rows(rows, warn=warn)

    assert errors == []
    assert products[0].attributes == {
        "tax_class_id": "Taxable Goods",
        "news_from_date": "2017-01-01 12:12",
        "news_to_date": "2017-02-02 12:12",
        "options_container": "Block after Info Column",
        "minimal_price": "10",
        "msrp": "11",
        "meta_keyword": "a, b",
        # bundle_shipment_type wins over the additional_attributes copy.
        "shipment_type": "separately",
        "color": "Red",
    }
    # map_enabled has no attribute on 2.4 (msrp_enabled was removed) and
    # product_options_container is a non-native duplicate of options_container.
    assert len(calls) == 2


def test_products_drop_magento_derived_attributes_from_additional_attributes():
    # has_options/required_options are recomputed by Magento on save and
    # quantity_and_stock_status is stock (phase 1 writes stock through
    # source items); the export still lists them in additional_attributes,
    # and live on 2.4.9 they made every rerun compare as changed.
    calls, warn = _warn_spy()
    rows = [
        (
            2,
            {
                "sku": "S1",
                "additional_attributes": (
                    "has_options=1,quantity_and_stock_status=In Stock,required_options=0,color=Red"
                ),
            },
        ),
        (3, {"sku": "S2", "additional_attributes": "has_options=0"}),
    ]

    products, errors = catalog.products_from_rows(rows, warn=warn)

    assert errors == []
    assert products[0].attributes == {"color": "Red"}
    assert products[1].attributes == {}
    assert len(calls) == 3


def test_products_associated_skus_become_grouped_links_with_position():
    rows = [
        (2, {"sku": "GROUPED1", "product_type": "grouped", "associated_skus": "SKU1=2.0000,SKU2"}),
    ]

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    links = products[0].grouped_links
    assert [(link.sku, link.qty, link.position) for link in links] == [
        ("SKU1", 2.0, 0),
        ("SKU2", 0.0, 1),
    ]


def test_products_downloadable_links_url_type():
    rows = [
        (
            2,
            {
                "sku": "DL1",
                "product_type": "downloadable",
                "downloadable_links": "title=Link1,price=0,url=https://example.test/y.zip",
            },
        )
    ]

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    assert products[0].downloadable_links[0].url == "https://example.test/y.zip"


def test_downloadable_links_map_downloads():
    rows = [
        (
            2,
            {
                "sku": "DL1",
                "product_type": "downloadable",
                "downloadable_links": "title=Link1,price=0,url=https://example.test/y.zip,downloads=5",
            },
        ),
        (
            3,
            {
                "sku": "DL2",
                "product_type": "downloadable",
                "downloadable_links": "title=Link2,price=0,url=https://example.test/z.zip",
            },
        ),
    ]

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    by_sku = {product.sku: product for product in products}
    assert by_sku["DL1"].downloadable_links[0].downloads == 5
    assert by_sku["DL2"].downloadable_links[0].downloads is None


def test_products_downloadable_links_non_url_type_is_row_error():
    rows = [
        (
            2,
            {
                "sku": "DL1",
                "downloadable_links": "title=Link1,type=file,url=https://example.test/y.zip",
            },
        )
    ]

    products, errors = catalog.products_from_rows(rows)

    assert products == []
    assert len(errors) == 1
    assert "file" in errors[0].message


def test_products_images_group_by_source_with_roles_and_position():
    rows = [
        (
            2,
            {
                "sku": "IMG1",
                "base_image": "/a.jpg",
                "base_image_label": "Front",
                "small_image": "/a.jpg",
                "thumbnail_image": "/b.jpg",
                "additional_images": "/a.jpg,/c.jpg",
            },
        )
    ]

    products, errors = catalog.products_from_rows(rows)

    assert errors == []
    images = products[0].images
    assert [image.source for image in images] == ["/a.jpg", "/b.jpg", "/c.jpg"]
    first = images[0]
    assert first.position == 1
    assert first.label == "Front"
    assert set(first.roles) == {"image", "small_image"}
    assert images[2].roles == []


# -- categories -----------------------------------------------------------------


def test_categories_from_rows_maps_path_and_attributes():
    rows = [
        (
            2,
            {
                "name": "Default Category/First test category",
                "store_view": "",
                "entity_id": "5",
                "url_path": "first-test-category",
                "include_in_menu": "Yes",
                "is_active": "No",
                "description": "A category",
            },
        )
    ]

    categories, errors = catalog.categories_from_rows(rows)

    assert errors == []
    category = categories[0]
    assert category.path == "Default Category/First test category"
    assert category.attributes == {
        "include_in_menu": 1,
        "is_active": 0,
        "description": "A category",
    }


def test_categories_map_export_labels_to_native_values():
    # The Firebear category export writes admin labels, which Magento
    # rejects or stores as garbage (verified live on 2.4.9): Yes/No for
    # int flags, "Product Name" for a sort code, "3 columns" for a layout
    # code, m/d/y dates. Label-only references to records a catalog
    # import cannot resolve (CMS block title, theme label, a remote image
    # URL where Magento wants a media file) are dropped with a warning.
    calls, warn = _warn_spy()
    rows = [
        (
            2,
            {
                "name": "Default Category/A",
                "custom_apply_to_products": "Yes",
                "custom_use_parent_settings": "No",
                "default_sort_by": "Product Name",
                "available_sort_by": "position,name,price",
                "display_mode": "Static block and products",
                "page_layout": "3 columns",
                "custom_design_from": "2/1/18",
                "custom_design_to": "2018-02-28",
                "landing_page": "Contact us info",
                "custom_design": "Magento Luma",
                "image": "https://example.test/logo.png",
            },
        ),
        (3, {"name": "Default Category/B", "display_mode": "Products only", "default_sort_by": "Position"}),
    ]

    categories, errors = catalog.categories_from_rows(rows, warn=warn)

    assert errors == []
    assert categories[0].attributes == {
        "custom_apply_to_products": 1,
        "custom_use_parent_settings": 0,
        "default_sort_by": "name",
        "available_sort_by": "position,name,price",
        "display_mode": "PRODUCTS_AND_PAGE",
        "page_layout": "3columns",
        "custom_design_from": "2018-02-01",
        "custom_design_to": "2018-02-28",
    }
    assert categories[1].attributes == {"display_mode": "PRODUCTS", "default_sort_by": "position"}
    assert len(calls) == 3


def test_categories_dropped_columns_warn_once_each():
    calls, warn = _warn_spy()
    rows = [
        (2, {"name": "Default Category/A", "entity_id": "1", "url_path": "a", "group": "1"}),
        (3, {"name": "Default Category/B", "entity_id": "2", "url_path": "b", "group": "1"}),
    ]

    categories, errors = catalog.categories_from_rows(rows, warn=warn)

    assert errors == []
    assert len(categories) == 2
    assert len(calls) == 3


def test_categories_store_view_rows_fold_into_store_values():
    rows = [
        (2, {"name": "Default Category/A", "store_view": "default", "description": "Global"}),
        (3, {"name": "Default Category/A", "store_view": "fr", "description": "FR"}),
    ]

    categories, errors = catalog.categories_from_rows(rows)

    assert errors == []
    assert categories[0].store_values == {"fr": {"description": "FR"}}


def test_categories_default_store_code_is_global_by_default():
    # Pins the real shape of the Firebear sample export (categories.csv):
    # its global rows carry store_view "default" rather than being blank.
    rows = [(2, {"name": "Default Category/A", "store_view": "default", "description": "Global"})]

    categories, errors = catalog.categories_from_rows(rows)

    assert errors == []
    assert len(categories) == 1
    assert categories[0].attributes == {"description": "Global"}
    assert categories[0].store_values == {}


def test_categories_default_store_code_is_store_level_when_opted_out():
    # On a real multi-store install "default" is a genuine, addressable
    # store view code - passing global_store_codes=("",) makes a
    # store_view "default" row fold into store_values["default"] instead
    # of being written globally.
    rows = [
        (2, {"name": "Default Category/A", "store_view": "", "description": "Global"}),
        (3, {"name": "Default Category/A", "store_view": "default", "description": "Default Store"}),
    ]

    categories, errors = catalog.categories_from_rows(rows, global_store_codes=("",))

    assert errors == []
    assert len(categories) == 1
    category = categories[0]
    assert category.attributes == {"description": "Global"}
    assert category.store_values == {"default": {"description": "Default Store"}}


# -- attributes -------------------------------------------------------------------


def test_attributes_rows_group_options_by_code():
    rows = [
        (
            2,
            {
                "store_id": "0",
                "attribute_code": "color",
                "frontend_label": "Color",
                "frontend_input": "select",
                "is_global": "1",
                "option:value": "Red",
                "option:sort_order": "1",
                "is_required": "1",
            },
        ),
        (
            3,
            {
                "store_id": "0",
                "attribute_code": "color",
                "frontend_label": "Color",
                "frontend_input": "select",
                "option:value": "Blue",
                "option:sort_order": "2",
            },
        ),
    ]

    attributes, errors = catalog.attributes_from_rows(rows)

    assert errors == []
    assert len(attributes) == 1
    attribute = attributes[0]
    assert attribute.code == "color"
    assert attribute.label == "Color"
    assert attribute.frontend_input == "select"
    assert attribute.scope == "global"
    assert [(option.label, option.sort_order) for option in attribute.options] == [("Red", 1), ("Blue", 2)]
    assert attribute.flags == {"is_required": True}


def test_attributes_rows_skip_non_zero_store_id_with_one_warning():
    calls, warn = _warn_spy()
    rows = [
        (2, {"store_id": "0", "attribute_code": "color", "frontend_input": "select", "option:value": "Red"}),
        (3, {"store_id": "1", "attribute_code": "color", "frontend_input": "select", "option:value": "Rouge"}),
        (4, {"store_id": "1", "attribute_code": "size", "frontend_input": "select", "option:value": "Taille"}),
    ]

    attributes, errors = catalog.attributes_from_rows(rows, warn=warn)

    assert errors == []
    assert len(attributes) == 1
    assert [option.label for option in attributes[0].options] == ["Red"]
    assert len(calls) == 1


def test_attributes_rows_missing_code_is_row_error():
    rows = [(2, {"store_id": "0", "attribute_code": "", "frontend_input": "select"})]

    attributes, errors = catalog.attributes_from_rows(rows)

    assert attributes == []
    assert len(errors) == 1


def test_attribute_set_assignments_from_rows_aggregates_groups():
    rows = [
        (2, {"attribute_set": "Default", "attribute_code": "color", "group:name": "General"}),
        (3, {"attribute_set": "Default", "attribute_code": "size", "group:name": "General"}),
        (4, {"attribute_set": "Default", "attribute_code": "color", "group:name": "General"}),
        (5, {"attribute_set": "", "attribute_code": "ignored", "group:name": "General"}),
    ]

    sets = catalog.attribute_set_assignments_from_rows(rows)

    assert len(sets) == 1
    assert sets[0].name == "Default"
    assert sets[0].based_on == "Default"
    assert sets[0].groups == {"General": ["color", "size"]}


# -- advanced pricing -------------------------------------------------------------


def test_prices_from_rows_groups_tiers_by_sku():
    rows = [
        (
            2,
            {
                "sku": "SKU1",
                "tier_price_website": "All Websites [USD]",
                "tier_price_customer_group": "ALL GROUPS",
                "tier_price_qty": "5",
                "tier_price": "10.00",
                "tier_price_value_type": "Fixed",
            },
        ),
        (
            3,
            {
                "sku": "SKU1",
                "tier_price_website": "Main Website",
                "tier_price_customer_group": "General",
                "tier_price_qty": "10",
                "tier_price": "5",
                "tier_price_value_type": "Discount",
            },
        ),
    ]

    prices, errors = catalog.prices_from_rows(rows)

    assert errors == []
    assert len(prices) == 1
    price = prices[0]
    assert price.sku == "SKU1"
    assert len(price.tiers) == 2
    assert price.tiers[0].website == "all"
    assert price.tiers[0].price_type == "fixed"
    assert price.tiers[1].website == "Main Website"
    assert price.tiers[1].price_type == "discount"


def test_prices_from_rows_missing_qty_or_price_is_row_error():
    rows = [(2, {"sku": "SKU1", "tier_price_qty": "", "tier_price": "10"})]

    prices, errors = catalog.prices_from_rows(rows)

    assert prices == []
    assert len(errors) == 1


# -- MSI source items -------------------------------------------------------------


def test_source_items_from_rows_maps_columns():
    rows = [(2, {"source_code": "default", "sku": "SKU1", "status": "1", "quantity": "42.5"})]

    items, errors = catalog.source_items_from_rows(rows)

    assert errors == []
    assert len(items) == 1
    item = items[0]
    assert item.sku == "SKU1"
    assert item.source_code == "default"
    assert item.quantity == 42.5
    assert item.status == 1


def test_attributes_rows_map_flags_onto_the_rest_attribute_shape():
    # Verified live on 2.4.9: POST products/attributes rejects
    # search_weight and is_used_for_price_rules ("field is not
    # supported") and wants apply_to as a string array.
    calls, warn = _warn_spy()
    rows = [
        (
            2,
            {
                "store_id": "0",
                "attribute_code": "color",
                "frontend_input": "select",
                "apply_to": "simple,virtual, configurable",
                "search_weight": "3",
                "is_used_for_price_rules": "1",
                "is_searchable": "1",
            },
        ),
        (3, {"store_id": "0", "attribute_code": "size", "frontend_input": "select", "search_weight": "1"}),
    ]

    attributes, errors = catalog.attributes_from_rows(rows, warn=warn)

    assert errors == []
    assert attributes[0].flags == {"apply_to": ["simple", "virtual", "configurable"], "is_searchable": True}
    assert attributes[1].flags == {}
    # One warning per dropped column, not per row.
    assert len(calls) == 2


@pytest.mark.parametrize("blank", ["quantity", "status"])
def test_source_items_blank_quantity_or_status_is_a_row_error(blank):
    row = {"sku": "A", "source_code": "default", "quantity": "5", "status": "1", blank: "  "}

    items, errors = catalog.source_items_from_rows([(2, row)])

    assert items == []
    assert [error.row_ref for error in errors] == ["line 2: A"]
    assert blank in errors[0].message
