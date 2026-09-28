"""Resolves catalog references (attributes, options, sets, stores,
categories) to their Magento ids, creating missing ones on demand.

Named after the production PHP lessons this mirrors: collect attribute
codes from every row up front (never only the first row) so one filtered
GET can preload them all, and never trust a create response over a fresh
read of the resource that created it.
"""

import html
from dataclasses import dataclass
from typing import Iterable

import requests


class ResolveError(Exception):
    """Raised when a resolver cannot resolve or create a catalog reference.
    Never swallowed - a failed create must fail the rows that depend on it,
    not silently continue with a missing id."""


def normalize_label(label: str) -> str:
    """Normalize an option/attribute label for encoding- and case-insensitive
    comparison. Magento can return a label raw, htmlspecialchars-encoded, or
    htmlentities-encoded (for example "Black & White" vs "Black &amp;
    White") - unescape, strip surrounding whitespace, then casefold so
    lookups do not care which form the API happened to return."""
    return html.unescape(label).strip().casefold()


@dataclass(frozen=True)
class AttributeMeta:
    """Cached metadata for one EAV attribute.

    `options` maps a normalize_label()-ed label to its Magento option value
    id (as a string), so a lookup never depends on which raw encoding a
    caller or the API used for the label.
    """
    id: int
    code: str
    frontend_input: str
    backend_type: str
    scope: str
    options: dict[str, str]


class Resolver:
    """Looks up (and, where the brief allows, creates) the Magento ids a
    catalog import needs. Cached for one run and updated in place whenever
    this resolver creates something itself - callers never invalidate the
    cache by hand."""

    def __init__(self, resource, root_category: str = "Default Category"):
        self.resource = resource
        self.root_category = root_category
        self._attributes: dict[str, AttributeMeta] = {}
        self._attribute_sets: dict[str, int] = {}
        self._websites: dict[str, int] = {}
        self._stores: dict[str, int] = {}
        self._categories: dict[str, int] = {}

    # -- attributes and options ------------------------------------------------

    def preload_attributes(self, codes: Iterable[str]) -> None:
        """Fetch metadata for every code in one GET, using an `in` filter -
        this must never turn into one GET per attribute (the production
        bug it fixes was doing exactly that per row)."""
        codes = list(codes)
        if not codes:
            return
        params = {
            "searchCriteria[filterGroups][0][filters][0][field]": "attribute_code",
            "searchCriteria[filterGroups][0][filters][0][value]": ",".join(codes),
            "searchCriteria[filterGroups][0][filters][0][condition_type]": "in",
        }
        response = self.resource.get("products/attributes", params=params)
        for item in response.get("items", []):
            self._store_attribute(item)

    def attribute(self, code: str) -> AttributeMeta:
        if code not in self._attributes:
            raise ResolveError(f"unknown attribute: {code}")
        return self._attributes[code]

    def option_id(self, code: str, label: str, create: bool = True) -> str:
        meta = self.attribute(code)
        key = normalize_label(label)
        if key in meta.options:
            return meta.options[key]
        if not create:
            raise ResolveError(f"unknown option '{label}' for attribute '{code}'")

        endpoint = f"products/attributes/{code}/options"
        payload = {"option": {"label": label, "sort_order": 0, "is_default": False}}
        try:
            self.resource.post(endpoint, payload)
        except requests.exceptions.HTTPError as error:
            raise self._wrap_http_error(error, endpoint) from error

        # Never trust the create response (in 2.4.x it can be "id_<n>" or a
        # bare numeric string) - refresh the attribute and read the real
        # value id back from its options instead.
        meta = self._refresh_attribute(code)
        if key not in meta.options:
            raise ResolveError(
                f"created option '{label}' for attribute '{code}' but it is "
                f"missing after refresh"
            )
        return meta.options[key]

    def _refresh_attribute(self, code: str) -> AttributeMeta:
        item = self.resource.get(f"products/attributes/{code}")
        return self._store_attribute(item)

    def _store_attribute(self, item: dict) -> AttributeMeta:
        options = {}
        for option in item.get("options", []):
            value = option.get("value", "")
            if value == "":
                # products/attributes always includes a blank "please
                # select" placeholder option - never a real option, and it
                # would collide with an empty label if kept.
                continue
            options[normalize_label(option.get("label", ""))] = value
        meta = AttributeMeta(
            id=item["attribute_id"],
            code=item["attribute_code"],
            frontend_input=item.get("frontend_input", ""),
            backend_type=item.get("backend_type", ""),
            scope=item.get("scope", ""),
            options=options,
        )
        self._attributes[meta.code] = meta
        return meta

    # -- attribute sets ---------------------------------------------------------

    def attribute_set_id(self, name: str) -> int:
        name = name.strip()
        if not self._attribute_sets:
            self._load_attribute_sets()
        if name not in self._attribute_sets:
            raise ResolveError(f"unknown attribute set: {name}")
        return self._attribute_sets[name]

    def _load_attribute_sets(self) -> None:
        params = {
            "searchCriteria[filterGroups][0][filters][0][field]": "entity_type_code",
            "searchCriteria[filterGroups][0][filters][0][value]": "catalog_product",
        }
        response = self.resource.get("eav/attribute-sets/list", params=params)
        for item in response.get("items", []):
            # Exact, case-sensitive match after strip() - Magento allows
            # sibling sets differing only in case.
            self._attribute_sets[item["attribute_set_name"].strip()] = item["attribute_set_id"]

    # -- websites and store views ------------------------------------------------

    def website_id(self, code: str) -> int:
        if not self._websites:
            self._websites = {item["code"]: item["id"] for item in self.resource.get("store/websites")}
        if code not in self._websites:
            raise ResolveError(f"unknown website: {code}")
        return self._websites[code]

    def store_id(self, code: str) -> int:
        if not self._stores:
            self._stores = {item["code"]: item["id"] for item in self.resource.get("store/storeViews")}
        if code not in self._stores:
            raise ResolveError(f"unknown store view: {code}")
        return self._stores[code]

    # -- categories ---------------------------------------------------------------

    def ensure_categories(self, paths: Iterable[str]) -> dict[str, int]:
        """Resolve every path, creating any missing node parent-first.
        Returns a dict keyed by the paths exactly as given (not normalized),
        mapping each to its Magento category id."""
        if not self._categories:
            self._load_category_tree()
        return {path: self._ensure_category_path(path) for path in paths}

    def category_id(self, path: str) -> int:
        if not self._categories:
            self._load_category_tree()
        normalized = self._normalize_category_path(path)
        if normalized not in self._categories:
            raise ResolveError(f"unknown category path: {path}")
        return self._categories[normalized]

    def _normalize_category_path(self, path: str) -> str:
        segments = self._split_segments(path)
        root_segments = self._split_segments(self.root_category)
        if segments[: len(root_segments)] == root_segments:
            return "/".join(segments)
        return "/".join(root_segments + segments)

    @staticmethod
    def _split_segments(path: str) -> list[str]:
        # Exact-after-strip matching applies per segment, not to the path
        # as a whole: strip whitespace off each segment (a tree node name
        # or a path segment can carry stray whitespace) and drop any
        # segment left empty by a doubled separator, so
        # "Default Category//  Men  /Tops" and "Default Category/Men/Tops"
        # key on the same cache entry. Matching stays case-sensitive.
        return [segment.strip() for segment in path.split("/") if segment.strip()]

    def _load_category_tree(self) -> None:
        # The tree's own top node (id 1, "Root Catalog" by default) is never
        # part of a path - paths start at its children, the store root(s)
        # such as "Default Category".
        tree = self.resource.get("categories")
        self._categories = {}
        for child in tree.get("children_data", []):
            self._walk_category_tree(child, prefix="")

    def _walk_category_tree(self, node: dict, prefix: str) -> None:
        name = node["name"].strip()
        path = f"{prefix}/{name}" if prefix else name
        self._categories[path] = node["id"]
        for child in node.get("children_data", []):
            self._walk_category_tree(child, path)

    def _ensure_category_path(self, path: str) -> int:
        normalized = self._normalize_category_path(path)
        if normalized in self._categories:
            return self._categories[normalized]

        # normalized is built from _split_segments() above, so every
        # segment here is already stripped and non-empty.
        segments = normalized.split("/")
        parent_id = None
        built = ""
        for segment in segments:
            built = f"{built}/{segment}" if built else segment
            if built in self._categories:
                parent_id = self._categories[built]
                continue
            if parent_id is None:
                # The configured root itself is missing from the tree -
                # ensure_categories cannot fix that by creating a second root.
                raise ResolveError(f"root category '{segments[0]}' not found in the category tree")
            parent_id = self._create_category(segment, parent_id)
            self._categories[built] = parent_id
        return parent_id

    def _create_category(self, name: str, parent_id: int) -> int:
        endpoint = "categories"
        payload = {
            "category": {
                "parent_id": parent_id,
                "name": name,
                "is_active": True,
                "include_in_menu": True,
            }
        }
        try:
            response = self.resource.post(endpoint, payload)
        except requests.exceptions.HTTPError as error:
            raise self._wrap_http_error(error, endpoint) from error
        return response.json()["id"]

    # -- shared helpers ---------------------------------------------------------

    @staticmethod
    def _wrap_http_error(error: requests.exceptions.HTTPError, endpoint: str) -> ResolveError:
        message = str(error)
        response = error.response
        if response is not None:
            try:
                body = response.json()
            except ValueError:
                body = None
            if isinstance(body, dict) and "message" in body:
                message = body["message"]
        return ResolveError(f"{endpoint}: {message}")
