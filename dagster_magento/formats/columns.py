"""Pure string parsers for the native Magento import column encodings.

Every function here takes one column's raw string value and returns a
parsed structure - no file I/O, no logging, no row-to-model mapping. That
lives in `formats/readers.py` and `formats/catalog.py`. Keeping this file
free of those concerns is what makes the parsers independently testable
against short inline strings copied from the sample column shapes.
"""

from typing import Any


class ColumnParseError(ValueError):
    """Raised when a column's raw value cannot be parsed at all (a
    non-numeric qty/price, for example). The message names the column and
    the offending value. `ColumnParseError` is a `ValueError` subclass, so
    an ordinary `except ValueError` in `formats/catalog.py` already catches
    it - a column parser never lets a bad cell escape as anything a caller
    would have to know a special exception type to handle."""


def _parse_float(column: str, field: str, value: str) -> float:
    try:
        return float(value)
    except ValueError as error:
        raise ColumnParseError(f"{column}: invalid {field} {value!r}") from error


def _split_top_level(value: str, separator: str) -> list[str]:
    """Split `value` on `separator`, ignoring occurrences of `separator`
    inside a double-quoted span. A `"` toggles quote state wherever it
    appears, so a quoted span may start mid-token (e.g. `desc="a, b"`)."""
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    for char in value:
        if char == '"':
            in_quotes = not in_quotes
            current.append(char)
        elif char == separator and not in_quotes:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    parts.append("".join(current))
    return parts


def _split_kv(token: str) -> tuple[str, str]:
    """Split one `key=value` token on the first `=`. A value wrapped in
    matching double quotes has the quotes stripped."""
    key, _, raw_value = token.partition("=")
    key = key.strip()
    raw_value = raw_value.strip()
    if len(raw_value) >= 2 and raw_value.startswith('"') and raw_value.endswith('"'):
        raw_value = raw_value[1:-1]
    return key, raw_value


def _parse_kv_group(entry: str) -> dict[str, str]:
    """Parse one `k=v,k=v` group (a single pipe-delimited entry) into a dict."""
    pairs: dict[str, str] = {}
    for token in _split_top_level(entry, ","):
        token = token.strip()
        if not token:
            continue
        key, val = _split_kv(token)
        if key:
            pairs[key] = val
    return pairs


def parse_additional_attributes(value: str) -> dict[str, str]:
    """Parse `additional_attributes`: "color=Gray,size=S" -> {"color":
    "Gray", "size": "S"}. A value may be double-quoted to contain commas,
    e.g. `desc="a, b"`."""
    if not value:
        return {}
    return _parse_kv_group(value)


def parse_pipe_groups(value: str) -> list[dict[str, str]]:
    """Split `value` on `|` into a list of `k=v,k=v` groups, each parsed
    into a dict. Shared shape used by configurable_variations,
    bundle_values and downloadable_links."""
    if not value:
        return []
    return [_parse_kv_group(entry.strip()) for entry in value.split("|") if entry.strip()]


def parse_configurable_variations(value: str) -> list[dict[str, Any]]:
    """Parse `configurable_variations`: pipe-separated variation entries,
    each a `k=v,k=v` group with a `sku` key, into plain dicts shaped like
    `Variation` kwargs (`{"sku": ..., "attributes": {...}}`). `default` is
    dropped - it is not modeled on `Variation`, configurable child
    selection is expressed entirely through the parent's `variations`
    list. Returns dicts, not `Variation` instances: a parser never
    constructs a pydantic model itself, so a bad cell can never escape as
    an uncaught `ValidationError` - `catalog.py` builds the real model and
    turns any validation failure into a `RowError` in one place."""
    variations: list[dict[str, Any]] = []
    for fields in parse_pipe_groups(value):
        sku = fields.pop("sku", "")
        fields.pop("default", None)
        variations.append({"sku": sku, "attributes": fields})
    return variations


def parse_bundle_values(value: str) -> list[dict[str, Any]]:
    """Parse `bundle_values`: pipe-separated selection entries, each a
    `k=v,k=v` group, into plain dicts shaped like `BundleOption` kwargs.
    Entries sharing the same `name` become one option dict, in first-seen
    order. Numeric fields (`default_qty`, `price`) that fail to parse
    raise `ColumnParseError` naming the field and value; an unrecognized
    `type` is passed through as-is and left for `catalog.py`'s
    `model_validate` to reject as a `RowError` - see the module docstring
    on why this parser never constructs a pydantic model itself."""
    options: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for fields in parse_pipe_groups(value):
        name = fields.get("name", "")
        if name not in options:
            options[name] = {
                "title": name,
                "type": fields.get("type", "select"),
                "required": fields.get("required") == "1",
                "selections": [],
            }
            order.append(name)
        options[name]["selections"].append(_bundle_selection(fields))
    return [options[name] for name in order]


def _bundle_selection(fields: dict[str, str]) -> dict[str, Any]:
    selection: dict[str, Any] = {"sku": fields.get("sku", "")}
    qty = fields.get("default_qty", "")
    if qty:
        selection["qty"] = _parse_float("bundle_values", "default_qty", qty)
    price = fields.get("price", "")
    if price:
        selection["price"] = _parse_float("bundle_values", "price", price)
    price_type = fields.get("price_type", "")
    if price_type:
        selection["price_type"] = price_type
    selection["is_default"] = fields.get("default") == "1"
    return selection


def parse_categories(value: str) -> list[str]:
    """Parse `categories`: comma-separated category paths."""
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def parse_associated_sku_pairs(value: str) -> list[tuple[str, float]]:
    """Parse `associated_skus`: "SKU1=2.0000,SKU2" -> ordered `(sku, qty)`
    pairs, keeping the first occurrence of a duplicate SKU and defaulting
    a bare SKU's qty to 0."""
    if not value:
        return []
    pairs: list[tuple[str, float]] = []
    seen: set[str] = set()
    for token in _split_top_level(value, ","):
        token = token.strip()
        if not token:
            continue
        sku, _, qty_text = token.partition("=")
        sku = sku.strip()
        if not sku or sku in seen:
            continue
        seen.add(sku)
        qty_text = qty_text.strip()
        if not qty_text:
            pairs.append((sku, 0.0))
            continue
        pairs.append((sku, _parse_float("associated_skus", f"qty for sku {sku!r}", qty_text)))
    return pairs


def parse_associated_skus(value: str) -> list[str]:
    """Parse `associated_skus` into plain, ordered, deduplicated SKUs."""
    return [sku for sku, _ in parse_associated_sku_pairs(value)]
