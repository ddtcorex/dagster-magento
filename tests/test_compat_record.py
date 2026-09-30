import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import compat_record  # noqa: E402

FACTS = {
    "magento": "2.4.9",
    "php": "8.4.11",
    "database": "MariaDB 11.8.2",
    "search": "OpenSearch 2.19",
    "bridge": "1.0.0",
}

JUNIT = """<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" tests="3">
<testcase classname="tests.live.test_a" name="test_ok" time="1.5"/>
<testcase classname="tests.live.test_a" name="test_bad" time="2.0"><failure message="boom">trace</failure></testcase>
<testcase classname="tests.live.test_a" name="test_skip" time="0.0"><skipped message="s"/></testcase>
</testsuite></testsuites>
"""


def test_parse_junit_maps_outcomes(tmp_path):
    path = tmp_path / "junit.xml"
    path.write_text(JUNIT)

    assert compat_record.parse_junit(str(path)) == [
        {"name": "tests.live.test_a::test_ok", "outcome": "passed", "seconds": 1.5},
        {"name": "tests.live.test_a::test_bad", "outcome": "failed", "seconds": 2.0},
        {"name": "tests.live.test_a::test_skip", "outcome": "skipped", "seconds": 0.0},
    ]


def test_build_record_not_provisioned_keeps_reason():
    record = compat_record.build_record("2.4.7", {}, [], "2026-09-30", "not provisioned", reason="image pull failed")

    assert record["status"] == "not provisioned"
    assert record["reason"] == "image pull failed"
    assert record["version"] == "2.4.7"


def test_build_record_rejects_an_unknown_status():
    with pytest.raises(ValueError):
        compat_record.build_record("2.4.9", FACTS, [], "2026-09-30", "great")


def test_render_table_orders_versions_and_marks_failed():
    passed = [{"name": "a", "outcome": "passed", "seconds": 1.0}]
    failed = [{"name": "a", "outcome": "passed", "seconds": 1.0}, {"name": "b", "outcome": "failed", "seconds": 1.0}]
    records = [
        compat_record.build_record("2.4.9", FACTS, passed, "2026-09-30", "verified"),
        compat_record.build_record("2.4.10", FACTS, passed, "2026-09-30", "verified"),
        compat_record.build_record("2.4.6", FACTS, failed, "2026-09-30", "failed"),
        compat_record.build_record("2.4.7", {}, [], "2026-09-30", "not provisioned", reason="no image"),
    ]

    lines = compat_record.render_table(records).splitlines()

    assert lines[0].startswith("| Version | Magento patch | PHP | Database | Search | Bridge | Date | Result |")
    assert [line.split("|")[1].strip() for line in lines[2:]] == ["2.4.6", "2.4.7", "2.4.9", "2.4.10"]
    assert "failed (1 of 2 failed)" in lines[2]
    assert "not provisioned: no image" in lines[3]
    assert "verified (1 of 1 passed)" in lines[4]


def test_write_table_replaces_between_markers_and_errors_without_them(tmp_path):
    readme = tmp_path / "README.md"
    readme.write_text("intro\n<!-- compat:start -->\nold\n<!-- compat:end -->\noutro\n")

    compat_record.write_table(str(readme), "| new |")

    assert readme.read_text() == "intro\n<!-- compat:start -->\n| new |\n<!-- compat:end -->\noutro\n"

    bare = tmp_path / "BARE.md"
    bare.write_text("no markers here\n")
    with pytest.raises(ValueError):
        compat_record.write_table(str(bare), "| new |")


def test_cli_record_then_table_round_trip(tmp_path, capsys):
    junit = tmp_path / "junit.xml"
    junit.write_text(JUNIT)
    facts = tmp_path / "facts.json"
    facts.write_text('{"magento": "2.4.9", "php": "8.4.1", "database": "MariaDB 11.8.2", "search": "opensearch 3.0", "bridge": "1.0.0"}')
    results = tmp_path / "results"
    results.mkdir()
    readme = tmp_path / "README.md"
    readme.write_text("<!-- compat:start -->\n<!-- compat:end -->\n")

    assert compat_record.main([
        "record", "--version", "2.4.9", "--junit", str(junit), "--facts", str(facts),
        "--date", "2026-09-30", "--status", "failed", "--out", str(results / "2.4.9-2026-09-30.json"),
    ]) == 0
    assert compat_record.main([
        "record", "--version", "2.4.7", "--date", "2026-09-30", "--status", "not provisioned",
        "--reason", "no image", "--out", str(results / "2.4.7-2026-09-30.json"),
    ]) == 0
    # A newer record for the same version replaces an older one in the table.
    compat_record.main([
        "record", "--version", "2.4.9", "--junit", str(junit), "--facts", str(facts),
        "--date", "2026-10-15", "--status", "verified", "--out", str(results / "2.4.9-2026-10-15.json"),
    ])

    assert compat_record.main(["table", "--results", str(results), "--readme", str(readme)]) == 0

    text = readme.read_text()
    assert "2026-10-15" in text and "2026-09-30 | failed" not in text
    assert "not provisioned: no image" in text
    assert text.count("| 2.4.9 |") == 1


def test_cli_table_reports_a_readme_without_markers(tmp_path, capsys):
    results = tmp_path / "results"
    results.mkdir()
    readme = tmp_path / "README.md"
    readme.write_text("no markers\n")

    assert compat_record.main(["table", "--results", str(results), "--readme", str(readme)]) == 1
    assert "compat:start" in capsys.readouterr().err


def test_render_table_shows_the_reason_of_a_failed_row():
    tests = [{"name": "a", "outcome": "failed", "seconds": 1.0}]
    record = compat_record.build_record(
        "2.4.9", FACTS, tests, "2026-09-30", "failed", reason="sandbox domain stopped resolving"
    )

    row = compat_record.render_table([record]).splitlines()[2]

    assert "failed (1 of 1 failed): sandbox domain stopped resolving" in row
