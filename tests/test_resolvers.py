import pytest
import requests_mock

from dagster_magento.resolvers import ResolveError, Resolver
from dagster_magento.resource import MagentoResource


def make_resource(**overrides):
    defaults = dict(
        base_url="https://shop.test",
        username="admin",
        password="secret-password",
        store_view="all",
    )
    defaults.update(overrides)
    return MagentoResource(**defaults)


def mock_token(m):
    m.post(
        "https://shop.test/rest/all/V1/integration/admin/token",
        json="fake-token-123",
    )


def test_preload_attributes_uses_one_in_filter_for_all_codes():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/products/attributes",
            json={
                "items": [
                    {
                        "attribute_id": 93,
                        "attribute_code": "color",
                        "frontend_input": "select",
                        "backend_type": "int",
                        "scope": "global",
                        "options": [
                            {"label": " ", "value": ""},
                            {"label": "Black &amp; White", "value": "49"},
                        ],
                    },
                    {
                        "attribute_id": 94,
                        "attribute_code": "size",
                        "frontend_input": "select",
                        "backend_type": "int",
                        "scope": "global",
                        "options": [],
                    },
                ],
                "total_count": 2,
            },
        )
        resolver.preload_attributes(["color", "size"])

    attribute_requests = [r for r in m.request_history if r.path.endswith("/products/attributes")]
    assert len(attribute_requests) == 1  # one GET for every code, never one per code

    query = attribute_requests[0].qs
    assert query["searchcriteria[filtergroups][0][filters][0][field]"] == ["attribute_code"]
    assert query["searchcriteria[filtergroups][0][filters][0][value]"] == ["color,size"]
    assert query["searchcriteria[filtergroups][0][filters][0][condition_type]"] == ["in"]

    color = resolver.attribute("color")
    assert color.id == 93
    assert color.code == "color"
    assert color.frontend_input == "select"
    assert color.backend_type == "int"
    assert color.scope == "global"
    # The blank placeholder option (value "") must be skipped.
    assert color.options == {"black & white": "49"}

    size = resolver.attribute("size")
    assert size.id == 94
    assert size.options == {}


def test_attribute_raises_resolve_error_for_unknown_code():
    resolver = Resolver(make_resource())
    with pytest.raises(ResolveError):
        resolver.attribute("does-not-exist")


def test_option_id_matches_html_escaped_existing_label():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/products/attributes",
            json={
                "items": [
                    {
                        "attribute_id": 93,
                        "attribute_code": "color",
                        "frontend_input": "select",
                        "backend_type": "int",
                        "scope": "global",
                        "options": [
                            {"label": " ", "value": ""},
                            {"label": "Black &amp; White", "value": "49"},
                        ],
                    }
                ],
                "total_count": 1,
            },
        )
        resolver.preload_attributes(["color"])
        value_id = resolver.option_id("color", "Black & White")

    assert value_id == "49"
    create_calls = [
        r for r in m.request_history if r.path.endswith("/products/attributes/color/options")
    ]
    assert create_calls == []  # an existing label must never trigger a create


def test_option_id_creates_missing_option_and_refreshes():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/products/attributes",
            json={
                "items": [
                    {
                        "attribute_id": 93,
                        "attribute_code": "color",
                        "frontend_input": "select",
                        "backend_type": "int",
                        "scope": "global",
                        "options": [{"label": " ", "value": ""}],
                    }
                ],
                "total_count": 1,
            },
        )
        m.post(
            "https://shop.test/rest/all/V1/products/attributes/color/options",
            json="id_50",
        )
        m.get(
            "https://shop.test/rest/all/V1/products/attributes/color",
            json={
                "attribute_id": 93,
                "attribute_code": "color",
                "frontend_input": "select",
                "backend_type": "int",
                "scope": "global",
                "options": [
                    {"label": " ", "value": ""},
                    {"label": "Purple", "value": "50"},
                ],
            },
        )
        resolver.preload_attributes(["color"])
        value_id = resolver.option_id("color", "Purple")

    # The real value id comes from the refresh, never trusted from the
    # create response ("id_50").
    assert value_id == "50"

    create_request = next(
        r for r in m.request_history if r.path.endswith("/products/attributes/color/options")
    )
    assert create_request.json() == {
        "option": {"label": "Purple", "sort_order": 0, "is_default": False}
    }

    refresh_requests = [
        r
        for r in m.request_history
        if r.method == "GET" and r.path.endswith("/products/attributes/color")
    ]
    assert len(refresh_requests) == 1


def test_option_id_without_create_raises_on_miss():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/products/attributes",
            json={
                "items": [
                    {
                        "attribute_id": 93,
                        "attribute_code": "color",
                        "frontend_input": "select",
                        "backend_type": "int",
                        "scope": "global",
                        "options": [],
                    }
                ],
                "total_count": 1,
            },
        )
        resolver.preload_attributes(["color"])
        with pytest.raises(ResolveError):
            resolver.option_id("color", "Purple", create=False)


def test_option_create_failure_raises_resolve_error():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/products/attributes",
            json={
                "items": [
                    {
                        "attribute_id": 93,
                        "attribute_code": "color",
                        "frontend_input": "select",
                        "backend_type": "int",
                        "scope": "global",
                        "options": [{"label": " ", "value": ""}],
                    }
                ],
                "total_count": 1,
            },
        )
        m.post(
            "https://shop.test/rest/all/V1/products/attributes/color/options",
            status_code=400,
            json={"message": "Value for Color already exists."},
        )
        resolver.preload_attributes(["color"])
        with pytest.raises(ResolveError) as excinfo:
            resolver.option_id("color", "Purple")

    # The failure must never be swallowed, and its Magento message text
    # must survive into the ResolveError.
    assert "Value for Color already exists." in str(excinfo.value)


def test_ensure_categories_creates_missing_nodes_parent_first():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/categories",
            json={
                "id": 1,
                "parent_id": 0,
                "name": "Root Catalog",
                "children_data": [
                    {
                        "id": 2,
                        "parent_id": 1,
                        "name": "Default Category",
                        "children_data": [
                            {"id": 10, "parent_id": 2, "name": "Men", "children_data": []}
                        ],
                    }
                ],
            },
        )
        m.post(
            "https://shop.test/rest/all/V1/categories",
            [
                {"json": {"id": 20, "parent_id": 10, "name": "Tops"}, "status_code": 200},
                {"json": {"id": 21, "parent_id": 20, "name": "Hoodies"}, "status_code": 200},
            ],
        )
        result = resolver.ensure_categories(["Default Category/Men/Tops/Hoodies"])

    assert result == {"Default Category/Men/Tops/Hoodies": 21}

    create_requests = [
        r for r in m.request_history if r.method == "POST" and r.path.endswith("/categories")
    ]
    assert len(create_requests) == 2  # Tops then Hoodies, parent-first
    assert create_requests[0].json() == {
        "category": {"parent_id": 10, "name": "Tops", "is_active": True, "include_in_menu": True}
    }
    assert create_requests[1].json() == {
        "category": {"parent_id": 20, "name": "Hoodies", "is_active": True, "include_in_menu": True}
    }


def test_category_paths_without_root_get_root_prefix():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/categories",
            json={
                "id": 1,
                "parent_id": 0,
                "name": "Root Catalog",
                "children_data": [
                    {
                        "id": 2,
                        "parent_id": 1,
                        "name": "Default Category",
                        "children_data": [
                            {"id": 10, "parent_id": 2, "name": "Men", "children_data": []}
                        ],
                    }
                ],
            },
        )
        result = resolver.ensure_categories(["Men"])

    assert result == {"Men": 10}
    assert resolver.category_id("Default Category/Men") == 10

    create_requests = [
        r for r in m.request_history if r.method == "POST" and r.path.endswith("/categories")
    ]
    assert create_requests == []  # "Men" already exists once root-prefixed


def test_category_id_raises_on_unknown_path():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/categories",
            json={"id": 1, "parent_id": 0, "name": "Root Catalog", "children_data": []},
        )
        with pytest.raises(ResolveError):
            resolver.category_id("Default Category/Ghost")


def test_website_id_and_store_id_resolve_by_code():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/store/websites",
            json=[{"id": 1, "code": "base"}, {"id": 2, "code": "eu"}],
        )
        m.get(
            "https://shop.test/rest/all/V1/store/storeViews",
            json=[{"id": 1, "code": "default"}, {"id": 2, "code": "fr"}],
        )
        assert resolver.website_id("eu") == 2
        assert resolver.store_id("fr") == 2

    with pytest.raises(ResolveError):
        resolver.website_id("does-not-exist")
    with pytest.raises(ResolveError):
        resolver.store_id("does-not-exist")


def test_attribute_set_id_resolves_by_exact_name():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/eav/attribute-sets/list",
            json={
                "items": [
                    {"attribute_set_id": 4, "attribute_set_name": "Default"},
                    {"attribute_set_id": 9, "attribute_set_name": "Shirts"},
                ],
                "total_count": 2,
            },
        )
        assert resolver.attribute_set_id("Shirts") == 9

    with pytest.raises(ResolveError):
        resolver.attribute_set_id("does-not-exist")
