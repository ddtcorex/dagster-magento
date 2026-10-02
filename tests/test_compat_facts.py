import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "live"))

import live_support  # noqa: E402


def test_normalize_database_names_the_engine_and_drops_the_build_suffix():
    assert live_support.normalize_database("11.8.2-MariaDB-ubu2404") == "MariaDB 11.8.2"
    assert live_support.normalize_database("8.0.36") == "MySQL 8.0.36"
    assert live_support.normalize_database("8.4.1-google") == "MySQL 8.4.1"


def test_govard_setting_reads_a_stack_value():
    text = 'stack:\n    php_version: "8.5"\n    search_version: "3.0"\n    services:\n        search: opensearch\n'

    assert live_support.govard_setting(text, "search_version") == "3.0"
    assert live_support.govard_setting(text, "search") == "opensearch"
    assert live_support.govard_setting(text, "missing") is None


def _facts_with_bridge(monkeypatch, tmp_path, get):
    (tmp_path / ".govard.yml").write_text('stack:\n    search_version: "3.0"\n')
    monkeypatch.setattr(live_support, "SANDBOX_PROJECT", tmp_path)
    monkeypatch.setattr(
        live_support,
        "govard_php",
        lambda body: 'deprecated notice\n{"magento":"2.4.9","php":"8.5.9","database":"11.8.8-MariaDB","engine":"opensearch"}\n',
    )

    class Resource:
        def get(self, endpoint):
            return get(endpoint)

    monkeypatch.setattr(live_support, "make_resource", lambda: Resource())
    return live_support.sandbox_facts()


def _http_error(status):
    import requests

    response = requests.Response()
    response.status_code = status
    return requests.exceptions.HTTPError(f"{status}", response=response)


def test_sandbox_facts_reads_the_bridge_version(monkeypatch, tmp_path):
    facts = _facts_with_bridge(monkeypatch, tmp_path, lambda endpoint: {"version": "1.0.0", "capabilities": []})

    assert facts == {
        "magento": "2.4.9",
        "php": "8.5.9",
        "database": "MariaDB 11.8.8",
        "search": "opensearch 3.0",
        "bridge": "1.0.0",
    }


def test_only_a_404_means_the_bridge_is_not_installed(monkeypatch, tmp_path):
    def get(endpoint):
        raise _http_error(404)

    assert _facts_with_bridge(monkeypatch, tmp_path, get)["bridge"] == "not installed"


def test_any_other_failure_reading_the_bridge_is_unknown_not_not_installed(monkeypatch, tmp_path):
    def get(endpoint):
        raise _http_error(401)

    assert _facts_with_bridge(monkeypatch, tmp_path, get)["bridge"] == "unknown (HTTP 401)"


def test_a_rejected_credential_is_an_unknown_bridge_not_a_crash(monkeypatch, tmp_path):
    """A stale admin password fails in the token fetch with MagentoAuthError, not
    HTTPError: the record must keep every other fact and say the bridge is
    unknown instead of losing the whole fact set."""
    from dagster_magento.resource import MagentoAuthError

    def get(endpoint):
        raise MagentoAuthError("bad credentials")

    facts = _facts_with_bridge(monkeypatch, tmp_path, get)

    assert facts["bridge"] == "unknown (authentication failed)"
    assert facts["magento"] == "2.4.9"


def test_an_unreachable_store_is_an_unknown_bridge(monkeypatch, tmp_path):
    import requests

    def get(endpoint):
        raise requests.exceptions.ConnectionError("no route")

    assert _facts_with_bridge(monkeypatch, tmp_path, get)["bridge"] == "unknown (connection failed)"
