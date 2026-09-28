"""In-memory stand-in for dagster_magento.resolvers.Resolver, covering only
the methods a writer actually calls. Test files in this directory import it
directly (`from conftest import FakeResolver`) - it is a plain helper class,
not a fixture, so each test can seed it with exactly the attributes,
attribute sets and groups the scenario needs.

Grows as later writer tasks need more of the real Resolver's surface -
never duplicate what a plain assignment in a test already covers.
"""

from dagster_magento.resolvers import AttributeMeta, ResolveError, normalize_label


class FakeResolver:
    def __init__(
        self,
        attributes=None,
        attribute_sets=None,
        attribute_groups=None,
        stores=None,
        categories=None,
        websites=None,
    ):
        self._attributes: dict[str, AttributeMeta] = dict(attributes or {})
        self._attribute_sets: dict[str, int] = dict(attribute_sets or {})
        # {set_id: {group_name: group_id}}
        self._attribute_groups: dict[int, dict[str, int]] = {
            set_id: dict(groups) for set_id, groups in (attribute_groups or {}).items()
        }
        self._stores: dict[str, int] = dict(stores or {})
        # {path: id}
        self._categories: dict[str, int] = dict(categories or {})
        self._websites: dict[str, int] = dict(websites or {})
        self.preloaded_codes: list[str] = []

    def preload_attributes(self, codes) -> None:
        self.preloaded_codes.extend(codes)

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
        # Mirrors the real resolver's "create then refresh": synthesize a
        # new id and cache it on the same (mutable) options dict, so a
        # later lookup for the same label is a cache hit, not a re-create.
        new_id = str(1000 + len(meta.options))
        meta.options[key] = new_id
        return new_id

    def website_id(self, code: str) -> int:
        if code not in self._websites:
            raise ResolveError(f"unknown website: {code}")
        return self._websites[code]

    def attribute_set_id(self, name: str) -> int:
        name = name.strip()
        if name not in self._attribute_sets:
            raise ResolveError(f"unknown attribute set: {name}")
        return self._attribute_sets[name]

    def attribute_group_id(self, set_id: int, name: str) -> int | None:
        name = name.strip()
        return self._attribute_groups.get(set_id, {}).get(name)

    def store_id(self, code: str) -> int:
        if code not in self._stores:
            raise ResolveError(f"unknown store view: {code}")
        return self._stores[code]

    def ensure_categories(self, paths) -> dict[str, int]:
        result = {}
        for path in paths:
            if path not in self._categories:
                raise ResolveError(f"unknown category path: {path}")
            result[path] = self._categories[path]
        return result

    def category_id(self, path: str) -> int:
        if path not in self._categories:
            raise ResolveError(f"unknown category path: {path}")
        return self._categories[path]
