"""Fetch real Firebear sample import files for `samples`-marked tests.

These files are not vendored into this repo (GPL-3.0, and the task-17
context is explicit that only short inline fixtures belong in the test
suite itself) - `fetch` downloads them on demand from the sample repo at a
pinned commit and caches them under `tests/.samples/` (gitignored). Tests
using this module are marked `@pytest.mark.samples` and are excluded from
the default test run (see pyproject.toml's `addopts`).
"""

import urllib.parse
import urllib.request
from pathlib import Path

_REPO = "firebearstudio/magento2-import-export-sample-files"
_SHA = "7fec061a837288718c1d84455ae8f1a0acde00c4"
_CACHE_DIR = Path(__file__).parent / ".samples"

# name -> folder in the sample repo, at the pinned SHA.
_FOLDERS = {
    "product_all_types.csv": "Improved Import : Export - Sample Files",
    "products_all_types.xlsx": "Improved Import : Export - Sample Files",
    "categories.csv": "Improved Import : Export - Sample Files",
    "attributes.csv": "Improved Import : Export - Sample Files",
    "advanced_pricing.csv": "Improved Import : Export - Sample Files",
    "msi_source_qty.csv": "Improved Import : Export - Sample Files",
    "catalog_product.csv": "Magento 2 Default import sample files",
}


def fetch(name: str) -> Path:
    """Download `<folder>/<name>` at the pinned sample repo SHA into
    `tests/.samples/<name>`, or return the cached copy if already present.
    Safe to call repeatedly (skips the download when cached) and writes
    atomically (temp file + rename) so a partial/interrupted download can
    never be mistaken for a valid cached copy."""
    cached = _CACHE_DIR / name
    if cached.exists():
        return cached

    folder = _FOLDERS[name]
    url = (
        f"https://raw.githubusercontent.com/{_REPO}/{_SHA}/"
        f"{urllib.parse.quote(folder)}/{urllib.parse.quote(name)}"
    )
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = cached.with_suffix(cached.suffix + ".tmp")
    with urllib.request.urlopen(url) as response:  # noqa: S310 (pinned https raw.githubusercontent.com URL)
        data = response.read()
    tmp_path.write_bytes(data)
    tmp_path.rename(cached)
    return cached


def fetch_as_xml(name: str) -> Path:
    """Write the cached csv sample `<name>` out as a Magento export xml file.

    The sample repo ships csv and xlsx only, and its files are GPL-3.0, so
    they are never vendored into this repository. Converting the pinned
    sample in memory instead keeps the xml test honest about the real column
    layout without adding a second licence to the tree.
    """
    csv_path = fetch(name)
    target = _CACHE_DIR / (csv_path.stem + ".xml")
    if target.exists():
        return target

    import csv as _csv
    import xml.etree.ElementTree as ET

    root = ET.Element("export")
    product = ET.SubElement(root, "product")
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for record in _csv.DictReader(handle):
            row = ET.SubElement(product, "row")
            for header, value in record.items():
                # A csv record can carry cells past the last header; the
                # reader drops those, so the generated xml must not invent
                # a column for them.
                if header is None:
                    continue
                ET.SubElement(row, "field", {"name": header}).text = value
    ET.indent(root, space="  ")
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = target.with_suffix(".xml.tmp")
    ET.ElementTree(root).write(tmp_path, encoding="utf-8", xml_declaration=True)
    tmp_path.rename(target)
    return target
