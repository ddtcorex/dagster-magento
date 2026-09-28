"""Plans product gallery image uploads and loads image bytes for them.

Identity for an existing gallery entry is (position, label): the same
position with the same label (None and "" treated as equal, "no label")
means the row's image already matches Magento and is skipped - no load,
no op. The same position with a different label means the image changed,
so the old entry is deleted and the row's image is added in its place.
A position the row never mentions is left alone. File content itself is
never compared - only position and label decide whether an image is
considered "the same".

A row is planned all-or-nothing: if loading any one of its images fails,
the whole row fails with a single RowError and no operation is planned
for it, even for images that loaded fine.
"""

import base64
import copy
import os
import urllib.parse
from pathlib import Path

import requests

from dagster_magento.models import Image, ProductRow
from dagster_magento.operation import BulkSpec, Operation, RowError
from dagster_magento.writers import PlanResult

# Magic-byte signatures for the image types Magento's media gallery
# accepts. The source string (URL or path) is never trusted for content
# type - only the downloaded/read bytes decide the MIME, so a mislabeled
# or malicious extension can never reach base64_encoded_data as an image.
_MAGIC_SIGNATURES = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
)

# Errors a loader may raise for one bad image; plan_media catches exactly
# these and turns them into a RowError for the whole row.
_LOADER_ERRORS = (requests.RequestException, OSError, ValueError)


def load_image(source: str, base_dir: Path | None = None, timeout_s: float = 60) -> tuple[bytes, str]:
    """Load raw image bytes from `source` and detect their MIME type.

    An http(s) source is downloaded with `requests.get` and never sends
    credentials - this download does not go through MagentoResource.
    Anything else is read as a local file path; a relative path resolves
    against `base_dir` (the current working directory when `base_dir` is
    None). Raises ValueError("not an image") when the bytes match none of
    the known magic-byte signatures.
    """
    if urllib.parse.urlsplit(source).scheme in ("http", "https"):
        response = requests.get(source, timeout=timeout_s)
        response.raise_for_status()
        data = response.content
    else:
        path = Path(source)
        if not path.is_absolute():
            path = (base_dir or Path.cwd()) / path
        data = path.read_bytes()

    return data, _detect_mime(data)


def _detect_mime(data: bytes) -> str:
    for signature, mime in _MAGIC_SIGNATURES:
        if data.startswith(signature):
            return mime
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    raise ValueError("not an image")


def plan_media(rows: list[ProductRow], current: dict[str, list[dict]], loader=load_image) -> PlanResult:
    """Plan gallery add/delete operations for each row's `images`.

    `current` is the per-SKU media snapshot from
    `dagster_magento.diff.snapshot_media`: a list of existing entry dicts
    with at least "id", "label" and "position". A row whose SKU is absent
    from `current` is treated as having no existing images.
    """
    operations: list[Operation] = []
    failed: list[RowError] = []

    for row in rows:
        sku = row.sku
        existing_by_position = {entry["position"]: entry for entry in current.get(sku, [])}

        deletes: list[Operation] = []
        adds: list[Operation] = []
        row_error: RowError | None = None

        for image in row.images:
            label = image.label or ""
            existing = existing_by_position.get(image.position)
            if existing is not None:
                if (existing.get("label") or "") == label:
                    continue  # same position, same label: already matches, no op, no load
                deletes.append(_delete_operation(sku, existing["id"]))

            try:
                data, mime = loader(image.source)
            except _LOADER_ERRORS as error:
                row_error = RowError(row_ref=sku, message=f"{image.source}: {error}")
                break

            adds.append(_add_operation(sku, image, label, data, mime))

        if row_error is not None:
            failed.append(row_error)
            continue

        # Deletes for a row always precede its adds, even when several
        # images in the row each need a delete-then-add.
        operations.extend(deletes)
        operations.extend(adds)

    return PlanResult(operations=operations, failed=failed)


def _delete_operation(sku: str, entry_id) -> Operation:
    return Operation(
        method="DELETE",
        endpoint=f"products/{_quote_sku(sku)}/media/{entry_id}",
        payload=None,
        row_refs=(sku,),
    )


def _add_operation(sku: str, image: Image, label: str, data: bytes, mime: str) -> Operation:
    entry = {
        "media_type": "image",
        "label": label,
        "position": image.position,
        "disabled": image.disabled,
        "types": list(image.roles),
        "content": {
            "base64_encoded_data": base64.b64encode(data).decode("ascii"),
            "type": mime,
            "name": _source_name(image.source),
        },
    }
    # Own fresh dict per side, per the immutability contract in
    # operation.py: the Operation payload and the BulkSpec payload must
    # never share a dict object.
    payload = {"entry": copy.deepcopy(entry)}
    bulk_payload = {"sku": sku, "entry": copy.deepcopy(entry)}
    return Operation(
        method="POST",
        endpoint=f"products/{_quote_sku(sku)}/media",
        payload=payload,
        row_refs=(sku,),
        bulk=BulkSpec("products/bySku/media", bulk_payload),
    )


def _source_name(source: str) -> str:
    return os.path.basename(urllib.parse.urlsplit(source).path)


def _quote_sku(sku: str) -> str:
    return urllib.parse.quote(sku, safe="")
