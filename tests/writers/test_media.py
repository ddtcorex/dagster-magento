"""Tests for the media writer: plan_media turns product image rows plus a
current gallery snapshot into add/delete Operations, and load_image fetches
raw image bytes with the MIME type detected from magic bytes only.
"""

import base64

from dagster_magento.models import Image, ProductRow
from dagster_magento.writers.media import load_image, plan_media

PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"rest-of-png-data"
JPEG_BYTES = b"\xff\xd8\xff" + b"rest-of-jpeg-data"


def fake_loader(known: dict[str, tuple[bytes, str]]):
    """Build a loader that returns fixed (bytes, mime) for known sources
    and raises for anything else, so plan_media tests never touch the
    network or the filesystem."""

    def _load(source, base_dir=None, timeout_s=60):
        if source not in known:
            raise ValueError(f"unexpected source: {source}")
        return known[source]

    return _load


def test_url_image_is_downloaded_and_base64_encoded(requests_mock):
    """load_image downloads an http(s) source with requests and returns
    its raw bytes, MIME detected from PNG magic bytes."""
    requests_mock.get("https://example.com/hero.png", content=PNG_BYTES)

    data, mime = load_image("https://example.com/hero.png")

    assert data == PNG_BYTES
    assert mime == "image/png"


def test_local_file_image_is_read_from_base_dir(tmp_path):
    """A relative local source path resolves against base_dir, MIME
    detected from JPEG magic bytes."""
    (tmp_path / "swatch.jpg").write_bytes(JPEG_BYTES)

    data, mime = load_image("swatch.jpg", base_dir=tmp_path)

    assert data == JPEG_BYTES
    assert mime == "image/jpeg"


def test_non_image_bytes_fail_the_row(tmp_path):
    """A source whose bytes match no known image signature fails the
    whole row with a RowError and plans no operation for it."""
    bad_path = tmp_path / "not-an-image.txt"
    bad_path.write_bytes(b"just some text")

    rows = [
        ProductRow(
            sku="SKU-1",
            images=[Image(source=str(bad_path), position=1, roles=["image"])],
        )
    ]

    result = plan_media(rows, current={})

    assert result.operations == []
    assert len(result.failed) == 1
    assert result.failed[0].row_ref == "SKU-1"
    assert "not an image" in result.failed[0].message
    assert str(bad_path) in result.failed[0].message


def test_existing_same_position_and_label_is_skipped():
    """An image whose position and label match an existing gallery entry
    produces no operation and never calls the loader."""
    rows = [
        ProductRow(
            sku="SKU-2",
            images=[Image(source="hero.png", position=1, label="Hero", roles=["image"])],
        )
    ]
    current = {
        "SKU-2": [
            {
                "id": 10,
                "media_type": "image",
                "label": "Hero",
                "position": 1,
                "disabled": False,
                "types": ["image"],
                "file": "/h/e/hero.png",
            }
        ]
    }

    def loader_must_not_be_called(source, base_dir=None, timeout_s=60):
        raise AssertionError("loader must not be called for a skipped image")

    result = plan_media(rows, current, loader=loader_must_not_be_called)

    assert result.operations == []
    assert result.failed == []


def test_none_and_empty_string_label_are_treated_equal():
    """label=None on the existing entry and label="" on the row image
    both mean "no label" and must compare equal (no op)."""
    rows = [
        ProductRow(
            sku="SKU-3",
            images=[Image(source="hero.png", position=1, label="", roles=["image"])],
        )
    ]
    current = {
        "SKU-3": [
            {
                "id": 11,
                "media_type": "image",
                "label": None,
                "position": 1,
                "disabled": False,
                "types": ["image"],
                "file": "/h/hero.png",
            }
        ]
    }

    def loader_must_not_be_called(source, base_dir=None, timeout_s=60):
        raise AssertionError("loader must not be called when labels are equal")

    result = plan_media(rows, current, loader=loader_must_not_be_called)

    assert result.operations == []
    assert result.failed == []


def test_new_position_adds_without_delete():
    """A position with no existing entry only adds - no DELETE op - and
    the content name is the basename of the source with any query string
    stripped."""
    rows = [
        ProductRow(
            sku="SKU-4",
            images=[
                Image(
                    source="https://cdn.example.com/img/new.png?v=2",
                    position=5,
                    label="New",
                    roles=["image", "small_image"],
                    disabled=True,
                )
            ],
        )
    ]
    loader = fake_loader({"https://cdn.example.com/img/new.png?v=2": (PNG_BYTES, "image/png")})

    result = plan_media(rows, current={}, loader=loader)

    assert result.failed == []
    assert len(result.operations) == 1
    op = result.operations[0]
    assert op.method == "POST"
    assert op.endpoint == "products/SKU-4/media"
    assert op.row_refs == ("SKU-4",)
    assert op.payload == {
        "entry": {
            "media_type": "image",
            "label": "New",
            "position": 5,
            "disabled": True,
            "types": ["image", "small_image"],
            "content": {
                "base64_encoded_data": base64.b64encode(PNG_BYTES).decode("ascii"),
                "type": "image/png",
                "name": "new.png",
            },
        }
    }
    assert op.bulk.endpoint == "products/bySku/media"
    assert op.bulk.payload == {"sku": "SKU-4", "entry": op.payload["entry"]}


def test_changed_image_deletes_then_adds():
    """A position whose existing label differs from the row's is replaced
    by a DELETE of the old entry id followed by the POST add. Deletes for
    a row come before its adds, even across multiple images."""
    rows = [
        ProductRow(
            sku="SKU-5",
            images=[
                Image(source="a.png", position=1, label="Changed", roles=["image"]),
                Image(source="b.png", position=2, label="Second", roles=["thumbnail"]),
            ],
        )
    ]
    current = {
        "SKU-5": [
            {
                "id": 20,
                "media_type": "image",
                "label": "Old",
                "position": 1,
                "disabled": False,
                "types": ["image"],
                "file": "/a/a.png",
            }
        ]
    }
    loader = fake_loader(
        {
            "a.png": (PNG_BYTES, "image/png"),
            "b.png": (PNG_BYTES, "image/png"),
        }
    )

    result = plan_media(rows, current, loader=loader)

    assert result.failed == []
    assert len(result.operations) == 3
    delete_op, add_op_1, add_op_2 = result.operations

    assert delete_op.method == "DELETE"
    assert delete_op.endpoint == "products/SKU-5/media/20"
    assert delete_op.payload is None
    assert delete_op.bulk is None
    assert delete_op.row_refs == ("SKU-5",)

    assert add_op_1.method == "POST"
    assert add_op_1.endpoint == "products/SKU-5/media"
    assert add_op_1.payload["entry"]["position"] == 1
    assert add_op_1.payload["entry"]["label"] == "Changed"

    assert add_op_2.method == "POST"
    assert add_op_2.payload["entry"]["position"] == 2
    assert add_op_2.payload["entry"]["label"] == "Second"


def test_delete_and_add_operations_never_share_a_payload_dict():
    """A row that mutated a returned payload dict must not corrupt a
    later plan_media call - each Operation/BulkSpec gets its own dict."""
    rows = [
        ProductRow(
            sku="SKU-6",
            images=[Image(source="a.png", position=1, label="A", roles=["image"])],
        )
    ]
    loader = fake_loader({"a.png": (PNG_BYTES, "image/png")})

    result = plan_media(rows, current={}, loader=loader)
    op = result.operations[0]
    op.payload["entry"]["label"] = "mutated"

    assert op.bulk.payload["entry"]["label"] == "A"
