"""Build and render the per-version compatibility records.

Used by scripts/compat-matrix.sh: a pytest JUnit file plus the facts read
from the sandbox become one JSON record per Magento version, and the records
render into the README compatibility table.
"""

import xml.etree.ElementTree as ET

STATUSES = ("verified", "failed", "not provisioned")
START_MARKER = "<!-- compat:start -->"
END_MARKER = "<!-- compat:end -->"
COLUMNS = ("Version", "Magento patch", "PHP", "Database", "Search", "Bridge", "Date", "Result")


def parse_junit(path: str) -> list[dict]:
    tests = []
    for case in ET.parse(path).getroot().iter("testcase"):
        if case.find("failure") is not None or case.find("error") is not None:
            outcome = "failed"
        elif case.find("skipped") is not None:
            outcome = "skipped"
        else:
            outcome = "passed"
        tests.append(
            {
                "name": f"{case.get('classname')}::{case.get('name')}",
                "outcome": outcome,
                "seconds": float(case.get("time", 0)),
            }
        )
    return tests


def build_record(
    version: str, facts: dict, tests: list[dict], date: str, status: str, reason: str | None = None
) -> dict:
    if status not in STATUSES:
        raise ValueError(f"status must be one of {STATUSES}, got {status!r}")
    record = {"version": version, "facts": facts, "tests": tests, "date": date, "status": status}
    if reason is not None:
        record["reason"] = reason
    return record


def _version_key(version: str) -> tuple:
    return tuple(int(part) if part.isdigit() else part for part in version.split("."))


def _result(record: dict) -> str:
    tests = record["tests"]
    if record["status"] == "not provisioned":
        return f"not provisioned: {record.get('reason', 'unknown')}"
    ran = [test for test in tests if test["outcome"] != "skipped"]
    passed = sum(1 for test in ran if test["outcome"] == "passed")
    if record["status"] == "failed":
        return f"failed ({len(ran) - passed} of {len(ran)} failed)"
    return f"verified ({passed} of {len(ran)} passed)"


def render_table(records: list[dict]) -> str:
    lines = ["| " + " | ".join(COLUMNS) + " |", "|" + "|".join(" --- " for _ in COLUMNS) + "|"]
    for record in sorted(records, key=lambda item: _version_key(item["version"])):
        facts = record["facts"]
        cells = (
            record["version"],
            facts.get("magento", "-"),
            facts.get("php", "-"),
            facts.get("database", "-"),
            facts.get("search", "-"),
            facts.get("bridge", "-"),
            record["date"],
            _result(record),
        )
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def write_table(readme_path: str, table: str) -> None:
    with open(readme_path) as handle:
        text = handle.read()
    start, end = text.find(START_MARKER), text.find(END_MARKER)
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"{readme_path} needs both {START_MARKER} and {END_MARKER}")
    head = text[: start + len(START_MARKER)]
    with open(readme_path, "w") as handle:
        handle.write(f"{head}\n{table}\n{text[end:]}")
