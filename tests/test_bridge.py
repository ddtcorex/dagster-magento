"""The optional bridge module client and the library paths that use it.

Everything here is hermetic: `requests_mock` answers the bridge endpoints, so
the tests pin the client's contract (probe, chunking, separator choice,
fallback) without a Magento instance.
"""

import pytest
import requests_mock

from dagster_magento import BridgeClient, MagentoResource
from dagster_magento.bridge import BridgeClient as BridgeClientClass
from dagster_magento.diff import snapshot_products
from dagster_magento.executor import MagentoImportError
from dagster_magento.importers import import_categories
from dagster_magento.models import CategoryRow
from dagster_magento.resolvers import Resolver
from dagster_magento.resource import MagentoAuthError

BASE = "https://shop.test/rest/all/V1"
TOKEN_URL = f"{BASE}/integration/admin/token"
CAPABILITIES_URL = f"{BASE}/dagster-bridge/capabilities"
INDEX_URL = f"{BASE}/dagster-bridge/products/index"
VALUES_URL = f"{BASE}/dagster-bridge/products/attribute-values"
UPSERT_URL = f"{BASE}/dagster-bridge/categories/upsert"

ALL_CAPABILITIES = ["products.index", "products.attribute_values", "categories.upsert"]


def make_resource(**overrides):
    defaults = dict(
        base_url="https://shop.test",
        username="admin",
        password="secret-password",
        store_view="all",
    )
    defaults.update(overrides)
    return MagentoResource(**defaults)


def mock_token(mock):
    mock.post(TOKEN_URL, json="token-value")


def mock_capabilities(mock, capabilities):
    mock.get(CAPABILITIES_URL, json={"version": "1.2.3", "capabilities": capabilities})


def test_capabilities_404_means_no_bridge():
    """A store without the module is not an error: it simply offers nothing."""
    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock.get(CAPABILITIES_URL, status_code=404, json={"message": "not found"})

        client = BridgeClient(make_resource())

        assert client.capabilities() == frozenset()
        assert client.has(BridgeClientClass.PRODUCT_INDEX) is False
        # Cached: the probe happens once per run, not once per capability.
        assert len(mock.request_history) == 2


def test_capabilities_are_probed_once_per_run():
    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock_capabilities(mock, ALL_CAPABILITIES)

        client = BridgeClient(make_resource())

        assert client.has(BridgeClientClass.PRODUCT_INDEX)
        assert client.has(BridgeClientClass.CATEGORIES_UPSERT)
        assert client.has("products.something_else") is False
        probes = [request for request in mock.request_history if "capabilities" in request.url]
        assert len(probes) == 1


def test_product_index_follows_next_after():
    first = {
        "items": [
            {"sku": "sku-1", "entity_id": 1, "type_id": "simple", "attribute_set_id": 4,
             "status": 1, "updated_at": "2026-01-01 00:00:00"},
            {"sku": "sku-2", "entity_id": 2, "type_id": "simple", "attribute_set_id": 4,
             "status": 1, "updated_at": "2026-01-01 00:00:00"},
        ],
        "next_after": 2,
    }
    second = {
        "items": [
            {"sku": "sku-3", "entity_id": 3, "type_id": "simple", "attribute_set_id": 4,
             "status": 2, "updated_at": "2026-01-02 00:00:00"},
        ],
        "next_after": None,
    }

    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock.get(f"{INDEX_URL}?after=0&limit=2", json=first)
        mock.get(f"{INDEX_URL}?after=2&limit=2", json=second)

        items = list(BridgeClient(make_resource()).product_index(limit=2))

    assert [item["sku"] for item in items] == ["sku-1", "sku-2", "sku-3"]


def test_client_picks_separator_absent_from_names():
    # A plain path keeps the library's own level separator.
    assert BridgeClientClass.pick_separator(["Default Category/Men/Tops"]) == "/"
    # A level that carries the pipe makes the pipe unusable.
    assert BridgeClientClass.pick_separator(["Default Category/Men|Women/Tops"]) == ">"
    # A single name that carries ">" moves the wire separator to "|".
    assert BridgeClientClass.pick_separator(["Men > Women"]) == "|"


def test_upsert_categories_sends_the_chosen_separator_and_answers_by_path():
    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock_capabilities(mock, ALL_CAPABILITIES)
        upsert = mock.post(
            UPSERT_URL,
            json=[
                {"path": "Default Category>Men|Women", "id": 12},
                {"path": "Default Category>Tops", "id": 13},
            ],
        )

        resolved = BridgeClient(make_resource()).upsert_categories(
            ["Default Category/Men|Women", "Default Category/Tops"], "Default Category"
        )

    assert resolved == {"Default Category/Men|Women": 12, "Default Category/Tops": 13}
    assert upsert.last_request.json() == {
        "paths": ["Default Category>Men|Women", "Default Category>Tops"],
        "root": "Default Category",
        "separator": ">",
    }


def test_missing_single_capability_falls_back_only_that_path():
    """The bridge offers the product index but no category upsert: the product
    snapshot comes from the bridge and category creation stays native."""
    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock_capabilities(mock, [BridgeClientClass.PRODUCT_INDEX])
        mock.get(
            f"{INDEX_URL}?after=0&limit=5000",
            json={
                "items": [
                    {"sku": "sku-1", "entity_id": 1, "type_id": "simple",
                     "attribute_set_id": 4, "status": 1, "updated_at": "2026-01-01 00:00:00"}
                ],
                "next_after": None,
            },
        )
        tree = mock.get(
            f"{BASE}/categories?depth=1000",
            json={
                "children_data": [
                    {"id": 2, "name": "Default Category", "children_data": []}
                ]
            },
        )
        created = mock.post(f"{BASE}/categories", json={"id": 11})
        upsert = mock.post(UPSERT_URL, json=[])

        resource = make_resource()
        client = BridgeClient(resource)

        snapshot = snapshot_products(resource, ["sku-1"], ["status"], bridge=client)
        resolved = Resolver(resource, bridge=client).ensure_categories(["Default Category/Men"])

    assert snapshot["sku-1"]["status"] == 1
    assert resolved == {"Default Category/Men": 11}
    assert tree.called
    assert created.called
    assert upsert.call_count == 0


def test_category_upsert_capability_is_used_when_present():
    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock_capabilities(mock, [BridgeClientClass.CATEGORIES_UPSERT])
        upsert = mock.post(
            UPSERT_URL, json=[{"path": "Default Category/Men", "id": 11}]
        )
        tree = mock.get(f"{BASE}/categories?depth=1000", json={"children_data": []})

        resolved = Resolver(make_resource(), bridge=BridgeClient(make_resource())).ensure_categories(
            ["Default Category/Men"]
        )

    assert resolved == {"Default Category/Men": 11}
    assert upsert.called
    assert tree.call_count == 0


def test_diff_applies_store_fallback_from_bridge_values():
    """The review focus: a store value that is missing must fall back to the
    default store value instead of reading as a difference."""
    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock_capabilities(mock, ALL_CAPABILITIES)
        mock.get(
            f"{INDEX_URL}?after=0&limit=5000",
            json={
                "items": [
                    {"sku": "sku-1", "entity_id": 1, "type_id": "simple",
                     "attribute_set_id": 4, "status": 1, "updated_at": "2026-01-01 00:00:00"}
                ],
                "next_after": None,
            },
        )
        values = mock.post(
            VALUES_URL,
            json=[
                {"sku": "sku-1", "attribute_code": "name", "default_value": "Default name"},
                {"sku": "sku-1", "attribute_code": "price", "store_value": "9.9900",
                 "default_value": "19.9900"},
                {"sku": "sku-1", "attribute_code": "weight"},
            ],
        )

        resource = make_resource()
        snapshot = snapshot_products(
            resource,
            ["sku-1", "sku-2"],
            ["name", "price", "weight", "status"],
            bridge=BridgeClient(resource),
            store_id=2,
        )

    assert values.last_request.json()["store_id"] == 2
    assert values.last_request.json()["attribute_codes"] == ["name", "price", "weight"]
    assert snapshot["sku-1"]["name"] == "Default name"
    assert snapshot["sku-1"]["price"] == "9.9900"
    assert snapshot["sku-1"]["weight"] is None
    assert snapshot["sku-1"]["status"] == 1
    assert "sku-2" not in snapshot


def test_attribute_values_are_chunked_to_the_module_caps():
    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock_capabilities(mock, [BridgeClientClass.ATTRIBUTE_VALUES])
        calls = mock.post(VALUES_URL, json=[])

        skus = [f"sku-{index}" for index in range(BridgeClientClass.MAX_SKUS_PER_CALL + 1)]
        BridgeClient(make_resource()).attribute_values(skus, ["name"])

    assert calls.call_count == 2
    assert len(calls.request_history[0].json()["skus"]) == BridgeClientClass.MAX_SKUS_PER_CALL
    assert len(calls.request_history[1].json()["skus"]) == 1


def test_require_mode_raises_without_capability():
    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock_capabilities(mock, [BridgeClientClass.PRODUCT_INDEX])

        with pytest.raises(MagentoImportError) as error:
            import_categories(
                make_resource(),
                [CategoryRow(path="Default Category/Men")],
                use_bridge="require",
            )

    assert "categories.upsert" in str(error.value)


def test_never_mode_ignores_an_installed_bridge():
    with requests_mock.Mocker() as mock:
        mock_token(mock)
        probes = mock.get(CAPABILITIES_URL, json={"version": "1.2.3", "capabilities": ALL_CAPABILITIES})
        tree = mock.get(f"{BASE}/categories?depth=1000", json={"children_data": []})

        result = import_categories(
            make_resource(),
            [CategoryRow(path="Default Category/Men")],
            use_bridge="never",
        )

    assert probes.call_count == 0
    assert tree.called
    # The native path ran and reported its own outcome (one failed row here,
    # because the mocked tree has no root), instead of using the module.
    assert result.failed == 1


def test_auth_failure_is_never_swallowed_by_the_probe():
    """The probe is best effort, but a credential problem is about the store:
    it must abort the run instead of quietly disabling the bridge."""
    with requests_mock.Mocker() as mock:
        mock.post(TOKEN_URL, status_code=401, json={"message": "bad credentials"})

        with pytest.raises(MagentoAuthError):
            BridgeClient(make_resource()).capabilities()


# -- product snapshot through the importer's real field set -------------------

# The EAV codes the fake module knows. Anything else (extension_attributes,
# website_ids, ...) is rejected the way the real module rejects it.
KNOWN_ATTRIBUTE_CODES = {"name", "price", "visibility", "weight", "status", "color"}


def mock_product_catalog(mock, rest_items):
    """Everything import_products needs besides the bridge itself."""
    mock.get(
        f"{BASE}/eav/attribute-sets/list",
        json={"items": [{"attribute_set_name": "Default", "attribute_set_id": 4}]},
    )
    mock.get(f"{BASE}/store/websites", json=[{"code": "base", "id": 1}, {"code": "fr", "id": 2}])
    mock.get(
        f"{BASE}/products/attributes",
        json={
            "items": [
                {"attribute_id": 93, "attribute_code": "color", "frontend_input": "select",
                 "backend_type": "int", "options": [{"label": "Red", "value": "12"}]}
            ]
        },
    )
    return mock.get(f"{BASE}/products", json={"items": rest_items})


def fake_attribute_values(store_values):
    """A module answer that 400s on unknown codes, like the real module."""

    def answer(request, context):
        body = request.json()
        unknown = sorted(set(body["attribute_codes"]) - KNOWN_ATTRIBUTE_CODES)
        if unknown:
            context.status_code = 400
            return {"message": "Unknown attribute codes: %1.", "parameters": [", ".join(unknown)]}
        return [
            {"sku": sku, "attribute_code": code, "default_value": store_values.get(sku, {}).get(code)}
            for sku in body["skus"]
            for code in body["attribute_codes"]
        ]

    return answer


def mock_bridge_products(mock, index_items, store_values):
    mock_capabilities(mock, ALL_CAPABILITIES)
    mock.get(f"{INDEX_URL}?after=0&limit=5000", json={"items": index_items, "next_after": None})
    return mock.post(VALUES_URL, json=fake_attribute_values(store_values))


INDEX_A = {"sku": "A", "entity_id": 1, "type_id": "simple", "attribute_set_id": 4, "status": 1}


def test_bridge_snapshot_sends_only_attribute_codes_and_reads_the_rest_from_rest():
    """The importer's real field set goes to a module that rejects unknown
    codes: only EAV codes are asked for, website ids come from REST, and a
    row that already matches is skipped instead of failing or falling back."""
    from dagster_magento.importers import import_products

    rows = [
        {"sku": "A", "name": "Shirt", "websites": ["base"], "attributes": {"color": "Red"}},
        {"sku": "B", "name": "New"},
    ]
    with requests_mock.Mocker() as mock:
        mock_token(mock)
        values = mock_bridge_products(mock, [INDEX_A], {"A": {"name": "Shirt", "color": "12"}})
        rest = mock_product_catalog(
            mock, [{"sku": "A", "extension_attributes": {"website_ids": [1]}}]
        )
        created = mock.post(f"{BASE}/products", json={})

        result = import_products(make_resource(), rows, use_bridge="require")

    codes = values.last_request.json()["attribute_codes"]
    assert "extension_attributes" not in codes
    assert "color" in codes and "name" in codes
    assert rest.called
    assert "extension_attributes" in rest.last_request.qs["fields"][0]
    assert result.skipped_unchanged == 1
    assert result.succeeded == 1 and result.failed == 0
    assert [r.json()["product"]["sku"] for r in created.request_history] == ["B"]


def test_bridge_snapshot_reports_a_website_change_read_from_rest():
    from dagster_magento.importers import import_products

    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock_bridge_products(mock, [INDEX_A], {"A": {"name": "Shirt"}})
        mock_product_catalog(mock, [{"sku": "A", "extension_attributes": {"website_ids": [1]}}])
        updated = mock.put(f"{BASE}/products/A", json={})

        result = import_products(
            make_resource(), [{"sku": "A", "name": "Shirt", "websites": ["base", "fr"]}],
            use_bridge="require",
        )

    assert result.succeeded == 1
    assert updated.called


def test_require_mode_raises_when_the_bridge_snapshot_fails():
    from dagster_magento.importers import import_products

    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock_capabilities(mock, ALL_CAPABILITIES)
        mock.get(f"{INDEX_URL}?after=0&limit=5000", json={"items": [INDEX_A], "next_after": None})
        mock.post(VALUES_URL, status_code=500, json={"message": "boom"})
        rest = mock_product_catalog(mock, [])

        with pytest.raises(MagentoImportError):
            import_products(make_resource(), [{"sku": "A", "name": "Shirt"}], use_bridge="require")

    assert not rest.called


def test_auto_mode_falls_back_to_rest_when_the_bridge_snapshot_fails():
    from dagster_magento.importers import import_products

    with requests_mock.Mocker() as mock:
        mock_token(mock)
        mock_capabilities(mock, ALL_CAPABILITIES)
        mock.get(f"{INDEX_URL}?after=0&limit=5000", json={"items": [INDEX_A], "next_after": None})
        mock.post(VALUES_URL, status_code=500, json={"message": "boom"})
        mock_product_catalog(
            mock,
            [{"sku": "A", "type_id": "simple", "attribute_set_id": 4, "name": "Shirt",
              "extension_attributes": {"website_ids": [1]}, "custom_attributes": []}],
        )

        result = import_products(make_resource(), [{"sku": "A", "name": "Shirt"}], use_bridge="auto")

    assert result.skipped_unchanged == 1
