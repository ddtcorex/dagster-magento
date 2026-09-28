import json

import pytest

from dagster_magento.formats.readers import read_rows


def test_read_rows_csv_yields_line_numbers_and_dicts(tmp_path):
    path = tmp_path / "products.csv"
    path.write_text("sku,name\nSKU1,First\nSKU2,Second\n", encoding="utf-8")

    rows = list(read_rows(path))

    assert rows == [
        (2, {"sku": "SKU1", "name": "First"}),
        (3, {"sku": "SKU2", "name": "Second"}),
    ]


def test_read_rows_csv_strips_utf8_bom(tmp_path):
    path = tmp_path / "products.csv"
    path.write_bytes("﻿sku,name\nSKU1,First\n".encode("utf-8"))

    rows = list(read_rows(path))

    assert rows == [(2, {"sku": "SKU1", "name": "First"})]


def test_read_rows_json_top_level_list(tmp_path):
    path = tmp_path / "products.json"
    path.write_text(json.dumps([{"sku": "SKU1"}, {"sku": "SKU2"}]), encoding="utf-8")

    rows = list(read_rows(path))

    assert rows == [(1, {"sku": "SKU1"}), (2, {"sku": "SKU2"})]


def test_read_rows_json_object_with_single_list_value(tmp_path):
    path = tmp_path / "products.json"
    path.write_text(json.dumps({"products": [{"sku": "SKU1"}]}), encoding="utf-8")

    rows = list(read_rows(path))

    assert rows == [(1, {"sku": "SKU1"})]


def test_read_rows_unsupported_suffix_raises(tmp_path):
    path = tmp_path / "products.xlsx"
    path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError):
        list(read_rows(path))
