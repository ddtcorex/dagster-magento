from dagster_magento.search import build_search_criteria


def test_no_filters_or_sort_orders_returns_empty_dict():
    assert build_search_criteria() == {}


def test_single_filter_defaults_condition_type_to_eq():
    params = build_search_criteria(filters=[("email", "a@example.com")])
    assert params == {
        "searchCriteria[filter_groups][0][filters][0][field]": "email",
        "searchCriteria[filter_groups][0][filters][0][value]": "a@example.com",
        "searchCriteria[filter_groups][0][filters][0][condition_type]": "eq",
    }


def test_filter_with_explicit_condition_type():
    params = build_search_criteria(filters=[("updated_at", "2026-01-01", "gteq")])
    assert params["searchCriteria[filter_groups][0][filters][0][condition_type]"] == "gteq"
    assert params["searchCriteria[filter_groups][0][filters][0][value]"] == "2026-01-01"


def test_multiple_filters_are_anded_into_one_filter_group():
    params = build_search_criteria(
        filters=[
            ("status", "processing"),
            ("updated_at", "2026-01-01", "gteq"),
        ]
    )
    assert params["searchCriteria[filter_groups][0][filters][0][field]"] == "status"
    assert params["searchCriteria[filter_groups][0][filters][1][field]"] == "updated_at"
    # both filters stay in filter_group 0 - AND, not OR-across-groups
    assert all("[filter_groups][0]" in key for key in params)


def test_list_value_is_comma_joined_for_in_condition():
    params = build_search_criteria(filters=[("sku", ["A", "B", "C"], "in")])
    assert params["searchCriteria[filter_groups][0][filters][0][value]"] == "A,B,C"


def test_sort_orders_are_indexed_independently_of_filters():
    params = build_search_criteria(
        filters=[("status", "processing")],
        sort_orders=[("created_at", "DESC")],
    )
    assert params["searchCriteria[sortOrders][0][field]"] == "created_at"
    assert params["searchCriteria[sortOrders][0][direction]"] == "DESC"


def test_multiple_sort_orders():
    params = build_search_criteria(sort_orders=[("created_at", "DESC"), ("entity_id", "ASC")])
    assert params["searchCriteria[sortOrders][0][field]"] == "created_at"
    assert params["searchCriteria[sortOrders][1][field]"] == "entity_id"


def test_does_not_set_page_size_or_current_page():
    params = build_search_criteria(filters=[("status", "processing")])
    assert not any("page_size" in key or "current_page" in key for key in params)
