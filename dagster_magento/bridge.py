"""Client for the optional `DDTCoreX_DagsterBridge` Magento module.

The module is optional and versioned separately from this library, so nothing
here assumes it is installed or complete: a store without it answers 404 on the
probe and the caller keeps its plain REST paths, and a store with an older
module advertises fewer capabilities than the new client can use. Every
capability is therefore checked on its own, once per run.

See `docs/specs/2026-09-28-cross-repo-dagster-magento-catalog-import-design.md`
at the workspace root for the endpoint contracts.
"""

from __future__ import annotations

import logging
from typing import Iterable, Iterator, Literal

import requests

from dagster_magento.resource import MagentoAuthError, MagentoResource

# How an importer should treat the bridge.
#   auto    - use every capability the store advertises, fall back for the rest
#   never   - ignore the bridge even when it is installed
#   require - refuse to run when a capability this path needs is missing
BridgeMode = Literal["auto", "never", "require"]

logger = logging.getLogger(__name__)


class BridgeClient:
    """Talks to the bridge module, one capability at a time."""

    CAPABILITIES_ENDPOINT = "dagster-bridge/capabilities"
    PRODUCT_INDEX_ENDPOINT = "dagster-bridge/products/index"
    ATTRIBUTE_VALUES_ENDPOINT = "dagster-bridge/products/attribute-values"
    CATEGORIES_UPSERT_ENDPOINT = "dagster-bridge/categories/upsert"

    PRODUCT_INDEX = "products.index"
    ATTRIBUTE_VALUES = "products.attribute_values"
    CATEGORIES_UPSERT = "categories.upsert"

    # Fields the index endpoint already answers, so the attribute endpoint is
    # never asked for them.
    ENTITY_FIELDS = frozenset(
        {"entity_id", "sku", "type_id", "attribute_set_id", "status", "updated_at"}
    )

    # The library writes category paths with "/" between levels.
    LEVEL_SEPARATOR = "/"
    # Separators the module accepts, best first.
    SEPARATOR_CANDIDATES = ("/", "|", ">", "^", "~")

    # Caps the module enforces; the client chunks instead of failing.
    MAX_SKUS_PER_CALL = 1000
    MAX_ATTRIBUTE_CODES_PER_CALL = 50

    def __init__(self, resource: MagentoResource) -> None:
        self.resource = resource
        self._capabilities: frozenset[str] | None = None
        self._index: dict[str, dict] | None = None

    # -- capabilities ----------------------------------------------------------

    def capabilities(self) -> frozenset[str]:
        """Capabilities the store advertises, probed once per run.

        A module that is not installed answers 404, which is not an error: it
        simply means no capability is available.
        """
        if self._capabilities is None:
            try:
                answer = self.resource.get(self.CAPABILITIES_ENDPOINT)
            except MagentoAuthError:
                # A credential problem is about the store, not about this
                # optional module: it must abort the run, never quietly
                # disable the bridge.
                raise
            except Exception as error:
                # The probe is best effort by design: a store without the
                # module answers 404, and a probe that cannot answer at all (a
                # timeout, a 500, no route) means the same thing to the caller,
                # so the native paths stay in charge instead of an import
                # failing over an optional module.
                logger.warning(
                    f"bridge probe failed ({error}); continuing without the bridge"
                )
                self._capabilities = frozenset()
            else:
                self._capabilities = frozenset(answer.get("capabilities") or [])
        return self._capabilities

    def has(self, capability: str) -> bool:
        """Whether this store offers one capability."""
        return capability in self.capabilities()

    # -- product index ---------------------------------------------------------

    def product_index(self, limit: int = 5000) -> Iterator[dict]:
        """Yield every product of the index, following `next_after`."""
        after = 0
        while True:
            page = self.resource.get(
                self.PRODUCT_INDEX_ENDPOINT, params={"after": after, "limit": limit}
            )
            items = page.get("items") or []
            yield from items
            next_after = page.get("next_after")
            if next_after is None:
                return
            after = int(next_after)

    def index_by_sku(self) -> dict[str, dict]:
        """The whole index, keyed by SKU and cached for this run."""
        if self._index is None:
            self._index = {item["sku"]: item for item in self.product_index()}
        return self._index

    # -- attribute values ------------------------------------------------------

    def attribute_values(
        self,
        skus: Iterable[str],
        codes: Iterable[str],
        store_id: int = 0,
    ) -> dict[str, dict[str, tuple[str | None, str | None]]]:
        """Store value and default value per SKU and attribute code.

        Answers `(store_value, default_value)` per pair, both None when the
        product has no value at all. The module caps one call at 1000 SKUs and
        50 codes, so larger requests are chunked here.
        """
        sku_list = list(dict.fromkeys(skus))
        code_list = list(dict.fromkeys(codes))
        result: dict[str, dict[str, tuple[str | None, str | None]]] = {}

        for code_chunk in _chunked(code_list, self.MAX_ATTRIBUTE_CODES_PER_CALL):
            for sku_chunk in _chunked(sku_list, self.MAX_SKUS_PER_CALL):
                answer = self.resource.post(
                    self.ATTRIBUTE_VALUES_ENDPOINT,
                    {
                        "skus": sku_chunk,
                        "attribute_codes": code_chunk,
                        "store_id": store_id,
                    },
                ).json()
                for item in answer or []:
                    result.setdefault(item["sku"], {})[item["attribute_code"]] = (
                        item.get("store_value"),
                        item.get("default_value"),
                    )

        return result

    # -- categories ------------------------------------------------------------

    def upsert_categories(self, paths: Iterable[str], root: str) -> dict[str, int]:
        """Create the categories these paths name and return path -> id."""
        path_list = list(paths)
        separator = self.pick_separator(path_list)
        encoded = [separator.join(_levels(path)) for path in path_list]

        answer = self.resource.post(
            self.CATEGORIES_UPSERT_ENDPOINT,
            {"paths": encoded, "root": root, "separator": separator},
        ).json()

        resolved: dict[str, int] = {}
        for item in answer or []:
            resolved[str(item["path"])] = int(item["id"])

        # The module answers the encoded paths; the caller asked for its own.
        return {path: resolved[encoded[index]] for index, path in enumerate(path_list)}

    @classmethod
    def pick_separator(cls, paths: Iterable[str]) -> str:
        """Separator to send with these paths.

        The library writes category paths with "/" between levels, so "/" is
        the answer as long as no path carries one of the other candidates: the
        module then splits the path exactly as the library wrote it. A path
        that does carry one (a level named "Men > Women", say) moves the wire
        separator to the first candidate that appears in no path at all, and
        `upsert_categories` re-joins the levels with it.
        """
        materialized = [str(path) for path in paths]
        for candidate in cls.SEPARATOR_CANDIDATES:
            if candidate == cls.LEVEL_SEPARATOR:
                if not any(
                    other in path
                    for path in materialized
                    for other in cls.SEPARATOR_CANDIDATES[1:]
                ):
                    return candidate
                continue
            if not any(candidate in path for path in materialized):
                return candidate
        return cls.LEVEL_SEPARATOR


def _levels(path: str) -> list[str]:
    """Level names of one path, as the library writes them."""
    return [level for level in str(path).split(BridgeClient.LEVEL_SEPARATOR) if level]


def _chunked(values: list, size: int) -> Iterator[list]:
    """Split a list into chunks of at most `size` items."""
    for start in range(0, len(values), size):
        yield values[start : start + size]
