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


def _single_list_value(data: dict, path: Path) -> list:
    list_values = [value for value in data.values() if isinstance(value, list)]
    if len(list_values) != 1:
        raise ValueError(
            f"{path}: expected a json object with exactly one list-valued key, "
            f"found {len(list_values)}"
        )
    return list_values[0]
