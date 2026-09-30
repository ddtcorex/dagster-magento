"""Capture and load scrubbed Magento response fixtures.

A fixture is a real response recorded by a live probe: it is written only
when DAGSTER_CAPTURE_FIXTURES=1, so a normal live run never rewrites what
hermetic tests read. Host names, tokens and stack traces are removed before
anything reaches disk.
"""

import json
import os
import re
from pathlib import Path
from urllib.parse import urlparse

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "magento"
PLACEHOLDER_HOST = "magento.example"

_BEARER = re.compile(r"Bearer\s+\S+")
_TOKEN = re.compile(r"\b[A-Za-z0-9]{32}\b")


def scrub(value: object, *, base_url: str) -> object:
    """Recursively replace the store's host with a placeholder and drop
    bearer tokens, 32 character tokens and `trace` keys."""
    host = urlparse(base_url).netloc or base_url
    if isinstance(value, dict):
        return {key: scrub(item, base_url=base_url) for key, item in value.items() if key != "trace"}
    if isinstance(value, list):
        return [scrub(item, base_url=base_url) for item in value]
    if isinstance(value, str):
        text = value.replace(base_url.rstrip("/"), f"https://{PLACEHOLDER_HOST}").replace(host, PLACEHOLDER_HOST)
        text = _BEARER.sub("Bearer <token>", text)
        return _TOKEN.sub("<token>", text)
    return value


def save_fixture(name: str, status: int, body: object, *, base_url: str) -> Path:
    path = FIXTURE_DIR / f"{name}.json"
    if os.environ.get("DAGSTER_CAPTURE_FIXTURES") != "1":
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    document = {"status": status, "body": scrub(body, base_url=base_url)}
    path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
    return path


def load_fixture(name: str) -> tuple[int, object]:
    document = json.loads((FIXTURE_DIR / f"{name}.json").read_text())
    return document["status"], document["body"]
