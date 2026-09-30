import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent / "live"))

import fixture_capture  # noqa: E402

BASE = "https://shop.test"


def test_scrub_removes_host_token_and_trace():
    body = {
        "message": f"Failed at {BASE}/rest/V1/products for shop.test",
        "auth": "Bearer abc123def456",
        "token": "a" * 32,
        "trace": "#0 /var/www/vendor/x.php(12): boom()",
        "nested": [{"trace": "secret", "url": f"{BASE}/x", "ok": 5}],
    }

    scrubbed = fixture_capture.scrub(body, base_url=BASE)
    text = json.dumps(scrubbed)

    assert "shop.test" not in text
    assert "abc123def456" not in text
    assert "a" * 32 not in text
    assert "trace" not in text and "vendor" not in text
    assert scrubbed["nested"] == [{"url": "https://magento.example/x", "ok": 5}]
    assert scrubbed["auth"] == "Bearer <token>"


def test_save_fixture_writes_only_when_capturing(tmp_path, monkeypatch):
    monkeypatch.setattr(fixture_capture, "FIXTURE_DIR", tmp_path)
    monkeypatch.delenv("DAGSTER_CAPTURE_FIXTURES", raising=False)

    path = fixture_capture.save_fixture("probe", 400, {"message": "x"}, base_url=BASE)

    assert path == tmp_path / "probe.json"
    assert not path.exists()

    monkeypatch.setenv("DAGSTER_CAPTURE_FIXTURES", "1")
    fixture_capture.save_fixture("probe", 400, {"message": f"{BASE}/y"}, base_url=BASE)

    assert json.loads(path.read_text()) == {"status": 400, "body": {"message": "https://magento.example/y"}}
    assert fixture_capture.load_fixture("probe") == (400, {"message": "https://magento.example/y"})


def test_load_fixture_reads_from_the_fixture_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(fixture_capture, "FIXTURE_DIR", tmp_path)
    (tmp_path / "known.json").write_text(json.dumps({"status": 200, "body": [1]}))

    assert fixture_capture.load_fixture("known") == (200, [1])
    with pytest.raises(FileNotFoundError):
        fixture_capture.load_fixture("missing")
