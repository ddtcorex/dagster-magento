from dataclasses import dataclass


def _make_hashable(obj):
    """Convert an object to a hashable form for hashing."""
    if isinstance(obj, dict):
        return frozenset((k, _make_hashable(v)) for k, v in obj.items())
    elif isinstance(obj, list):
        return tuple(_make_hashable(item) for item in obj)
    elif isinstance(obj, tuple):
        return tuple(_make_hashable(item) for item in obj)
    else:
        return obj


@dataclass(frozen=True)
class BulkSpec:
    """Specification for a bulk API operation.

    The payload dict is treated as immutable once built. Callers must not
    mutate the dict after construction; executors must copy before wrapping
    or merging chunks.
    """
    endpoint: str
    payload: dict

    def __hash__(self):
        return hash((self.endpoint, _make_hashable(self.payload)))


@dataclass(frozen=True)
class Operation:
    """A REST operation to be sent to Magento.

    The payload dict is treated as immutable once built. Callers must not
    mutate the dict after construction; writers must build a fresh dict
    per operation and never share one dict across operations; executors
    must copy before wrapping or merging chunks.
    """
    method: str
    endpoint: str
    payload: dict | None
    row_refs: tuple[str, ...]
    store_code: str | None = None
    list_key: str | None = None
    bulk: BulkSpec | None = None

    def __hash__(self):
        return hash((
            self.method,
            self.endpoint,
            _make_hashable(self.payload),
            self.row_refs,
            self.store_code,
            self.list_key,
            self.bulk,
        ))


@dataclass(frozen=True)
class RowError:
    """An error associated with a single row."""
    row_ref: str
    message: str
