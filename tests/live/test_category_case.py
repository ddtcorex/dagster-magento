"""Category and attribute set names match without regard to case, with the
bridge off and on. Magento's own category processor (which the bridge reuses)
compares names case-insensitively and refuses a second attribute set that
differs only by case, so the library follows the same rule on every path."""

import time

import pytest
from live_support import SANDBOX_PROJECT, make_resource, prepare_sandbox

from dagster_magento import import_attribute_sets, import_categories

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(not SANDBOX_PROJECT.is_dir(), reason="drives the local sandbox"),
]


def _children(resource, parent_name):
    tree = resource.get("categories", params={"depth": 1000})
    stack = list(tree.get("children_data", []))
    while stack:
        node = stack.pop()
        if node["name"] == parent_name:
            return node.get("children_data", [])
        stack.extend(node.get("children_data", []))
    return []


@pytest.mark.parametrize("use_bridge", ["never", "require"])
def test_category_names_match_the_same_with_and_without_the_bridge(use_bridge):
    prepare_sandbox()
    resource = make_resource()
    label = f"CaseProbe{use_bridge.capitalize()}{int(time.time())}"

    for path in (
        f"Default Category/{label}/Tops",
        f"default category/{label.lower()}/tops",
        f"DEFAULT CATEGORY/{label.upper()}/TOPS",
    ):
        result = import_categories(resource, [{"path": path}], use_bridge=use_bridge)
        assert result.failed == 0, (path, result.errors)

    top_level = [node for node in _children(resource, "Default Category") if node["name"].casefold() == label.casefold()]
    assert len(top_level) == 1, [node["name"] for node in top_level]
    assert top_level[0]["name"] == label  # the first spelling created stays
    children = [node for node in _children(resource, top_level[0]["name"]) if node["name"].casefold() == "tops"]
    assert len(children) == 1, [node["name"] for node in children]


def test_attribute_set_names_match_without_regard_to_case():
    prepare_sandbox()
    resource = make_resource()
    name = f"CaseSet{int(time.time())}"

    assert import_attribute_sets(resource, [{"name": name}]).failed == 0
    again = import_attribute_sets(resource, [{"name": name.lower()}])

    assert again.failed == 0, again.errors
    listing = resource.get(
        "eav/attribute-sets/list",
        params={
            "searchCriteria[filterGroups][0][filters][0][field]": "entity_type_code",
            "searchCriteria[filterGroups][0][filters][0][value]": "catalog_product",
        },
    )
    same = [item for item in listing["items"] if item["attribute_set_name"].casefold() == name.casefold()]
    assert len(same) == 1, same
