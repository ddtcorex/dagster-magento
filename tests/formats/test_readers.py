import json
import sys

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
    path = tmp_path / "products.txt"
    path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError):
        list(read_rows(path))


# -- xlsx --------------------------------------------------------------------


def _write_workbook(path, header, *data_rows):
    openpyxl = pytest.importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(header)
    for row in data_rows:
        sheet.append(row)
    workbook.save(path)


def test_read_rows_xlsx_yields_sheet_row_numbers_and_dicts(tmp_path):
    path = tmp_path / "products.xlsx"
    _write_workbook(path, ["sku", "name"], ["SKU1", "First"], ["SKU2", "Second"])

    rows = list(read_rows(path))

    assert rows == [
        (2, {"sku": "SKU1", "name": "First"}),
        (3, {"sku": "SKU2", "name": "Second"}),
    ]


def test_read_rows_xlsx_reads_only_the_first_sheet(tmp_path):
    openpyxl = pytest.importorskip("openpyxl")
    path = tmp_path / "products.xlsx"
    workbook = openpyxl.Workbook()
    first = workbook.active
    first.title = "Sheet1"
    first.append(["sku"])
    first.append(["SKU1"])
    second = workbook.create_sheet("Sheet2")
    second.append(["sku"])
    second.append(["SKU2"])
    workbook.save(path)

    rows = list(read_rows(path))

    assert rows == [(2, {"sku": "SKU1"})]


def test_read_rows_xlsx_converts_none_cells_to_empty_string(tmp_path):
    path = tmp_path / "products.xlsx"
    _write_workbook(path, ["sku", "name"], ["SKU1", None])

    rows = list(read_rows(path))

    assert rows == [(2, {"sku": "SKU1", "name": ""})]


def test_read_rows_xlsx_converts_whole_number_floats_without_trailing_zero(tmp_path):
    path = tmp_path / "products.xlsx"
    _write_workbook(path, ["sku", "price"], ["SKU1", 9.0])

    rows = list(read_rows(path))

    assert rows == [(2, {"sku": "SKU1", "price": "9"})]


def test_read_rows_xlsx_keeps_fractional_floats(tmp_path):
    path = tmp_path / "products.xlsx"
    _write_workbook(path, ["sku", "price"], ["SKU1", 9.99])

    rows = list(read_rows(path))

    assert rows == [(2, {"sku": "SKU1", "price": "9.99"})]


def test_read_rows_xlsx_missing_openpyxl_raises_import_error_with_hint(tmp_path, monkeypatch):
    pytest.importorskip("openpyxl")
    path = tmp_path / "products.xlsx"
    _write_workbook(path, ["sku"], ["SKU1"])
    # Simulate openpyxl not being installed: a None entry in sys.modules
    # makes `import openpyxl` raise ImportError without needing to
    # actually uninstall the dev-extra dependency.
    monkeypatch.setitem(sys.modules, "openpyxl", None)

    with pytest.raises(ImportError, match=r"dagster-magento\[xlsx\]"):
        list(read_rows(path))


def test_read_rows_json_scalars_are_read_as_text(tmp_path):
    path = tmp_path / "rows.json"
    path.write_text('[{"sku": "A", "price": 12, "weight": 1.5, "flag": true, "note": null}]', encoding="utf-8")

    (_, row), = list(read_rows(path))

    assert row == {"sku": "A", "price": "12", "weight": "1.5", "flag": "1", "note": ""}


XML_HAPPY = """<?xml version="1.0" encoding="UTF-8"?>
<export>
  <product>
    <row><field name="sku">SKU1</field><field name="name">First</field></row>
    <row><field name="sku">SKU2</field><field name="price">9.5</field></row>
  </product>
</export>
"""


def _write_xml(tmp_path, text, name="products.xml"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


def test_read_rows_xml_yields_ordinals_and_field_pairs(tmp_path):
    rows = list(read_rows(_write_xml(tmp_path, XML_HAPPY)))

    assert rows == [
        (1, {"sku": "SKU1", "name": "First"}),
        (2, {"sku": "SKU2", "price": "9.5"}),
    ]


def test_read_rows_xml_empty_field_is_an_empty_string(tmp_path):
    rows = list(read_rows(_write_xml(tmp_path, '<export><product><row>'
                                       '<field name="sku">S</field>'
                                       '<field name="name"></field>'
                                       '</row></product></export>')))

    assert rows == [(1, {"sku": "S", "name": ""})]


def test_read_rows_xml_repeated_field_name_keeps_the_last_value(tmp_path):
    rows = list(read_rows(_write_xml(tmp_path, '<export><product><row>'
                                       '<field name="sku">first</field>'
                                       '<field name="sku">second</field>'
                                       '</row></product></export>')))

    assert rows == [(1, {"sku": "second"})]


def test_read_rows_xml_reads_cdata_as_text(tmp_path):
    rows = list(read_rows(_write_xml(tmp_path, '<export><product><row>'
                                       '<field name="sku"><![CDATA[S&1]]></field>'
                                       '</row></product></export>')))

    assert rows == [(1, {"sku": "S&1"})]


def test_read_rows_xml_without_entity_refuses_an_ambiguous_file(tmp_path):
    path = _write_xml(tmp_path, "<export><product><row><field name='sku'>A</field></row></product>"
                          "<product><row><field name='sku'>B</field></row></product></export>")

    with pytest.raises(ValueError, match="pass entity= to choose one"):
        list(read_rows(path))


def test_read_rows_xml_entity_selects_one_of_several(tmp_path):
    path = _write_xml(tmp_path, "<export><product><row><field name='sku'>A</field></row></product>"
                          "<category><row><field name='path'>A/B</field></row></category></export>")

    assert list(read_rows(path, entity="product")) == [(1, {"sku": "A"})]
    assert list(read_rows(path, entity="category")) == [(1, {"path": "A/B"})]


def test_read_rows_xml_entity_that_is_absent_names_the_ones_present(tmp_path):
    path = _write_xml(tmp_path, '<export><product><row><field name="sku">A</field></row></product></export>')

    with pytest.raises(ValueError, match="entity 'category' not in"):
        list(read_rows(path, entity="category"))


def test_read_rows_xml_refuses_a_root_that_is_not_export(tmp_path):
    path = _write_xml(tmp_path, '<feed><product><row><field name="sku">A</field></row></product></feed>')

    with pytest.raises(ValueError, match="expected an 'export' root"):
        list(read_rows(path))


def test_read_rows_xml_refuses_a_nested_field_instead_of_dropping_it(tmp_path):
    path = _write_xml(tmp_path, '<export><product><row><field name="sku">A</field>'
                             '<field name="meta"><inner>x</inner></field>'
                             '</row></product></export>')

    with pytest.raises(ValueError, match="nested element"):
        list(read_rows(path))


def test_read_rows_xml_malformed_reports_the_position(tmp_path):
    path = _write_xml(tmp_path, "<export><product><row></product></export>")

    with pytest.raises(ValueError) as caught:
        list(read_rows(path))

    assert "products.xml" in str(caught.value)


def test_read_rows_xml_reports_the_ordinal_not_a_line_number(tmp_path):
    """csv reports a physical line, xml reports an ordinal in the entity.

    A file whose rows span several lines proves the two numbering schemes are
    not the same thing, which is what the docstring promises.
    """
    path = _write_xml(tmp_path, '<export><product>\n<row>\n<field name="sku">A</field>\n</row>\n'
                             '<row>\n<field name="sku">B</field>\n</row>\n</product></export>')

    assert [number for number, _ in read_rows(path)] == [1, 2]
