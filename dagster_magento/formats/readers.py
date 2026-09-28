"""Read raw catalog import rows from csv or json, by file suffix.

`read_rows` never validates or maps columns - it only yields the source
line number paired with the raw string-keyed dict, so `formats/catalog.py`
can build a `RowError` that points a caller back at the exact source line.
"""

import csv
import json
from collections.abc import Iterator
from pathlib import Path


def read_rows(path: Path) -> Iterator[tuple[int, dict]]:
    """Yield `(line_number, row)` for each record in `path`. csv is read
    with `csv.DictReader`; json must be a top-level list of objects, or an
    object with exactly one list-valued key (that list is used)."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        yield from _read_csv_rows(path)
    elif suffix == ".json":
        yield from _read_json_rows(path)
    elif suffix == ".xlsx":
        yield from _read_xlsx_rows(path)
    else:
        raise ValueError(f"unsupported catalog import file suffix: {suffix}")


def _read_csv_rows(path: Path) -> Iterator[tuple[int, dict]]:
    # utf-8-sig strips a BOM if present without affecting BOM-less files.
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            # DictReader.line_num is the count of physical lines consumed
            # so far; for a single-physical-line record (no embedded
            # newline in a quoted field) that equals the record's own
            # first (and only) physical line.
            yield reader.line_num, dict(row)


def _read_json_rows(path: Path) -> Iterator[tuple[int, dict]]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict):
        data = _single_list_value(data, path)
    if not isinstance(data, list):
        raise ValueError(f"{path}: expected a top-level json list of objects")
    for index, row in enumerate(data, start=1):
        yield index, dict(row)


def _read_xlsx_rows(path: Path) -> Iterator[tuple[int, dict]]:
    # openpyxl is an optional extra (`pip install "dagster-magento[xlsx]"`)
    # so most consumers of csv/json-only imports never need to install it.
    try:
        import openpyxl
    except ImportError as error:
        raise ImportError(
            "reading .xlsx catalog import files requires openpyxl; install it "
            'with pip install "dagster-magento[xlsx]"'
        ) from error

    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        sheet = workbook.worksheets[0]
        rows_iter = sheet.iter_rows(values_only=True)
        try:
            header_cells = next(rows_iter)
        except StopIteration:
            return
        headers = [_xlsx_cell_str(cell) for cell in header_cells]
        # Sheet row numbers start at 1; the header consumed row 1, so the
        # first data row is row 2 - matching csv's DictReader.line_num
        # semantics (the row number of the record itself).
        for row_number, cells in enumerate(rows_iter, start=2):
            yield (
                row_number,
                {headers[index]: _xlsx_cell_str(value) for index, value in enumerate(cells) if index < len(headers)},
            )
    finally:
        workbook.close()


def _xlsx_cell_str(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _single_list_value(data: dict, path: Path) -> list:
    list_values = [value for value in data.values() if isinstance(value, list)]
    if len(list_values) != 1:
        raise ValueError(
            f"{path}: expected a json object with exactly one list-valued key, "
            f"found {len(list_values)}"
        )
    return list_values[0]
