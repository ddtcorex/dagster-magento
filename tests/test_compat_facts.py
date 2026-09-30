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
