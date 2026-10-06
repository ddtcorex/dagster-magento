"""Read raw catalog import rows from csv, json, xlsx or xml, by file suffix.

`read_rows` never validates or maps columns - it only yields the source
line number paired with the raw string-keyed dict, so `formats/catalog.py`
can build a `RowError` that points a caller back at the exact source line.
"""

import csv
import json
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from pathlib import Path


def read_rows(path: Path, entity: str | None = None) -> Iterator[tuple[int, dict]]:
    """Yield `(row_number, row)` for each record in `path`. csv is read
    with `csv.DictReader`; json must be a top-level list of objects, or an
    object with exactly one list-valued key (that list is used); xlsx is the
    first worksheet; xml is a Magento export file.

    The number is a source line number for csv, json and xlsx. An xml file
    has no line numbers, so it yields the 1-based ordinal of the row inside
    the selected entity instead.

    `entity` only applies to xml: an export file may hold several entities,
    and without it a file that does not hold exactly one is refused rather
    than guessed at."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        yield from _read_csv_rows(path)
    elif suffix == ".json":
        yield from _read_json_rows(path)
    elif suffix == ".xlsx":
        yield from _read_xlsx_rows(path)
    elif suffix == ".xml":
        yield from _read_xml_rows(path, entity)
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
        yield index, {key: _json_cell_str(value) for key, value in dict(row).items()}


def _json_cell_str(value) -> str:
    """Read a json value as the text a csv cell would hold, so the mappers
    only ever see strings. Booleans become 1 and 0, nested values their json."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, str):
        return value
    if isinstance(value, (list, dict)):
        return json.dumps(value)
    return _xlsx_cell_str(value)


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


_XML_ROOT = "export"
_XML_ROW_TAG = "row"
_XML_FIELD_TAG = "field"


def _read_xml_rows(path: Path, entity: str | None) -> Iterator[tuple[int, dict]]:
    """Read a Magento export XML file: `<export><<entity>><row><field
    name="...">`. One `<row>` yields one flat dict, exactly the shape a csv
    header row produces, so `formats/catalog.py` needs no xml branch.

    `iterparse` with a `clear()` after every row keeps peak memory at one row
    instead of the whole tree, which matters for a 10k-row export. A `<field>`
    carrying a `name` is the only child of a row that is read: any other
    element is a nested value this dialect cannot express, so it is refused
    rather than silently dropped.

    An entity element is still open while its own rows close, so `active` is
    set on the entity's start event. That is also where a second entity is
    refused, so a multi-entity file never yields rows before it fails.
    """
    entities: list[str] = []
    active: str | None = None      # the entity element currently open
    root: str | None = None
    ordinal = 0
    depth = 0

    try:
        for event, element in ET.iterparse(path, events=("start", "end")):
            if event == "start":
                if root is None:
                    root = element.tag
                    if root != _XML_ROOT:
                        raise ValueError(
                            f"{path}: expected an '{_XML_ROOT}' root, found <{root}>"
                        )
                    depth = 1
                    continue
                depth += 1
                if depth == 2:
                    name = element.tag
                    if entity is None and name in entities:
                        raise ValueError(
                            f"{path}: this export file holds {len(entities)} entities "
                            f"({', '.join(entities)}); pass entity= to choose one"
                        )
                    if name not in entities:
                        entities.append(name)
                    active = None if entity is not None and name != entity else name
                    element.clear()
                continue

            if depth == 3 and element.tag == _XML_ROW_TAG:
                if active is not None:
                    ordinal += 1
                    yield ordinal, _xml_row_dict(path, active, element, ordinal)
                element.clear()
            depth -= 1
    except ET.ParseError as error:
        # ElementTree reports line and column but never the file, and a
        # RowError built downstream has to name the file it came from.
        raise ValueError(f"{path}: {error}") from error

    if entity is not None and entity not in entities:
        raise ValueError(
            f"{path}: entity {entity!r} not in this export file "
            f"(it holds: {', '.join(entities) or 'nothing'})"
        )


def _xml_row_dict(path: Path, entity: str, row, ordinal: int) -> dict:
    values: dict[str, str] = {}
    for field in row:
        if field.tag != _XML_FIELD_TAG:
            continue
        name = field.get("name")
        if name is None:
            continue
        if len(field):
            raise ValueError(
                f"{path}: row {ordinal} of entity {entity!r} has a nested element "
                f"inside <field name={name!r}>; this dialect reads text values only"
            )
        # A repeated name keeps the last value, the same way csv.DictReader
        # resolves a repeated header.
        values[name] = (field.text or "").strip()
    return values
