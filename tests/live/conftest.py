"""Shared fixtures for `live` tests against a real Magento sandbox.

Every test here is marked `live` and excluded from the default run
(pyproject.toml's `addopts`). Run them against the local sandbox with:

    eval "$(scripts/sandbox.sh env)"
    MAGENTO_CA_BUNDLE=<path to the local proxy root CA> .venv/bin/pytest -m live -q

Credentials come only from the environment (MAGENTO_BASE_URL,
MAGENTO_ADMIN_USERNAME, MAGENTO_ADMIN_PASSWORD, MAGENTO_STORE_VIEW); the
whole directory skips when MAGENTO_BASE_URL is unset.

TLS: the sandbox is served behind a local development CA that the
certifi bundle used by requests does not know. MAGENTO_CA_BUNDLE is a
test-only knob: when set, the session builds one bundle from certifi plus
that CA and points requests at it through its documented
REQUESTS_CA_BUNDLE variable. Verification stays on, library defaults are
untouched, and public hosts (sample image downloads) still verify against
certifi.
"""

import os
from pathlib import Path

import certifi
import pytest


def pytest_collection_modifyitems(config, items):
    # Each test module also sets pytestmark = pytest.mark.live, so the
    # default addopts deselects them; this only covers `-m live` runs
    # with no sandbox configured.
    if os.environ.get("MAGENTO_BASE_URL"):
        return
    here = Path(__file__).parent
    skip = pytest.mark.skip(reason="MAGENTO_BASE_URL is not set; live tests need a Magento sandbox")
    for item in items:
        if here in Path(item.fspath).parents:
            item.add_marker(skip)


@pytest.fixture(scope="session", autouse=True)
def _ca_bundle(tmp_path_factory):
    extra = os.environ.get("MAGENTO_CA_BUNDLE")
    if not extra or not os.environ.get("MAGENTO_BASE_URL"):
        yield
        return
    bundle = tmp_path_factory.mktemp("tls") / "ca-bundle.pem"
    bundle.write_text(Path(certifi.where()).read_text() + "\n" + Path(extra).read_text())
    previous = os.environ.get("REQUESTS_CA_BUNDLE")
    os.environ["REQUESTS_CA_BUNDLE"] = str(bundle)
    yield
    if previous is None:
        os.environ.pop("REQUESTS_CA_BUNDLE", None)
    else:
        os.environ["REQUESTS_CA_BUNDLE"] = previous
