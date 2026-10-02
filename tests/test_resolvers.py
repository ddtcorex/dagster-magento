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


def test_category_tree_is_read_with_an_explicit_depth():
    # Verified live on 2.4.9: GET categories without `depth` stops at
    # level 3 (Default Category/Women/Tops), so a level-4 node the
    # resolver created in an earlier run looked missing and the create
    # then failed with "URL key for specified store already exists".
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/categories",
            json={"id": 1, "name": "Root Catalog", "children_data": [{"id": 2, "name": "Default Category"}]},
        )
        resolver.category_id("Default Category")

    tree_request = m.request_history[-1]
    assert tree_request.qs == {"depth": [str(Resolver.CATEGORY_TREE_DEPTH)]}
    assert Resolver.CATEGORY_TREE_DEPTH >= 100


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


def test_attribute_set_id_resolves_by_name():
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


def test_category_segments_match_after_whitespace_strip():
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
                            # The tree node name itself carries stray
                            # whitespace, as a real Magento tree sometimes
                            # does - it must still match a stripped request.
                            {"id": 10, "parent_id": 2, "name": " Men ", "children_data": []}
                        ],
                    }
                ],
            },
        )
        m.post(
            "https://shop.test/rest/all/V1/categories",
            json={"id": 20, "parent_id": 10, "name": "Tops"},
        )
        # Deliberately different whitespace than the tree node above (so a
        # raw string comparison would miss), plus a doubled separator and a
        # stray-whitespace missing segment - none of it may survive into
        # the cache key or the create payload.
        result = resolver.ensure_categories(["Default Category//  Men  / Tops "])

    assert result == {"Default Category//  Men  / Tops ": 20}

    create_requests = [
        r for r in m.request_history if r.method == "POST" and r.path.endswith("/categories")
    ]
    assert len(create_requests) == 1  # Men already existed once stripped - only Tops is created
    assert create_requests[0].json() == {
        "category": {"parent_id": 10, "name": "Tops", "is_active": True, "include_in_menu": True}
    }


def test_two_paths_sharing_new_parent_create_it_once():
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
                        "children_data": [],
                    }
                ],
            },
        )
        m.post(
            "https://shop.test/rest/all/V1/categories",
            [
                {"json": {"id": 30, "parent_id": 2, "name": "New"}, "status_code": 200},
                {"json": {"id": 31, "parent_id": 30, "name": "A"}, "status_code": 200},
                {"json": {"id": 32, "parent_id": 30, "name": "B"}, "status_code": 200},
            ],
        )
        result = resolver.ensure_categories(
            ["Default Category/New/A", "Default Category/New/B"]
        )

    assert result == {"Default Category/New/A": 31, "Default Category/New/B": 32}

    create_requests = [
        r for r in m.request_history if r.method == "POST" and r.path.endswith("/categories")
    ]
    assert len(create_requests) == 3  # New created once, then A and B under it
    assert create_requests[0].json()["category"]["name"] == "New"
    assert create_requests[1].json() == {
        "category": {"parent_id": 30, "name": "A", "is_active": True, "include_in_menu": True}
    }
    assert create_requests[2].json() == {
        "category": {"parent_id": 30, "name": "B", "is_active": True, "include_in_menu": True}
    }


def test_attribute_group_id_is_cached_per_set():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/products/attribute-sets/groups/list",
            json={
                "items": [
                    {
                        "attribute_group_id": 7,
                        "attribute_group_name": "General",
                        "attribute_set_id": 4,
                    },
                    {
                        "attribute_group_id": 8,
                        "attribute_group_name": "Prices",
                        "attribute_set_id": 4,
                    },
                ],
                "total_count": 2,
            },
        )
        assert resolver.attribute_group_id(4, "General") == 7
        assert resolver.attribute_group_id(4, "Prices") == 8
        assert resolver.attribute_group_id(4, "Ghost") is None

    group_requests = [
        r for r in m.request_history if r.path.endswith("/attribute-sets/groups/list")
    ]
    assert len(group_requests) == 1  # cached per set_id after the first lookup

    query = group_requests[0].qs
    assert query["searchcriteria[filtergroups][0][filters][0][field]"] == ["attribute_set_id"]
    assert query["searchcriteria[filtergroups][0][filters][0][value]"] == ["4"]
    assert query["searchcriteria[filtergroups][0][filters][0][condition_type]"] == ["eq"]


def test_refresh_attribute_sets_clears_caches():
    resource = make_resource()
    resolver = Resolver(resource)
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/eav/attribute-sets/list",
            json={
                "items": [{"attribute_set_id": 4, "attribute_set_name": "Default"}],
                "total_count": 1,
            },
        )
        m.get(
            "https://shop.test/rest/all/V1/products/attribute-sets/groups/list",
            json={"items": [], "total_count": 0},
        )
        assert resolver.attribute_set_id("Default") == 4
        resolver.attribute_group_id(4, "General")

        resolver.refresh_attribute_sets()

        assert resolver.attribute_set_id("Default") == 4
        resolver.attribute_group_id(4, "General")

    set_requests = [r for r in m.request_history if r.path.endswith("/eav/attribute-sets/list")]
    group_requests = [
        r for r in m.request_history if r.path.endswith("/attribute-sets/groups/list")
    ]
    assert len(set_requests) == 2  # re-fetched once after refresh_attribute_sets
    assert len(group_requests) == 2  # group cache cleared alongside the set cache


def _category_tree(*children):
    return {
        "id": 1,
        "parent_id": 0,
        "name": "Root Catalog",
        "children_data": [{"id": 2, "parent_id": 1, "name": "Default Category", "children_data": list(children)}],
    }


def _node(node_id, name, *children):
    return {"id": node_id, "parent_id": 2, "name": name, "children_data": list(children)}


def _category_posts(m):
    return [r for r in m.request_history if r.method == "POST" and r.path.endswith("/categories")]


def test_category_lookup_ignores_case():
    resolver = Resolver(make_resource())
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get("https://shop.test/rest/all/V1/categories", json=_category_tree(_node(10, "Men")))

        assert resolver.category_id("default category/MEN") == 10
        result = resolver.ensure_categories(["Default Category/men"])

    assert result == {"Default Category/men": 10}
    assert _category_posts(m) == []


def test_root_prefix_differing_by_case_does_not_create_a_second_root():
    resolver = Resolver(make_resource())
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get("https://shop.test/rest/all/V1/categories", json=_category_tree(_node(10, "Men")))
        m.post("https://shop.test/rest/all/V1/categories", json={"id": 20, "parent_id": 10, "name": "Tops"})

        resolver.ensure_categories(["default category/Men/Tops"])

    posts = _category_posts(m)
    assert len(posts) == 1
    assert posts[0].json()["category"]["parent_id"] == 10
    assert posts[0].json()["category"]["name"] == "Tops"


def test_names_fold_like_the_bridge_so_a_sharp_s_is_not_ss():
    """The bridge goes through Magento's CategoryProcessor, which lower-cases with
    mb_strtolower ('Straße' stays 'straße'). A fold that turned ß into ss would
    resolve a name natively that the bridge would create again."""
    resolver = Resolver(make_resource())
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get("https://shop.test/rest/all/V1/categories", json=_category_tree(_node(10, "Straße")))

        assert resolver.category_id("Default Category/STRAßE") == 10
        with pytest.raises(ResolveError):
            resolver.category_id("Default Category/STRASSE")


def test_sibling_categories_differing_by_case_first_in_tree_wins_with_warning(caplog):
    resolver = Resolver(make_resource())
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/categories",
            json=_category_tree(_node(10, "Men"), _node(11, "men")),
        )
        with caplog.at_level("WARNING"):
            found = resolver.category_id("Default Category/MEN")

    assert found == 10
    warning = " ".join(record.getMessage() for record in caplog.records)
    assert "Men" in warning and "men" in warning


def test_attribute_set_lookup_ignores_case_like_magento_uniqueness():
    """Verified live on 2.4.9: creating 'caseset' next to 'CaseSet' is refused
    with 'attribute set name already exists', so a name that differs only by
    case is the same set and must resolve instead of being created again."""
    resolver = Resolver(make_resource())
    with requests_mock.Mocker() as m:
        mock_token(m)
        m.get(
            "https://shop.test/rest/all/V1/eav/attribute-sets/list",
            json={"items": [{"attribute_set_id": 4, "attribute_set_name": "Default"}], "total_count": 1},
        )

        assert resolver.attribute_set_id("default") == 4
        assert resolver.attribute_set_id("  DEFAULT ") == 4
