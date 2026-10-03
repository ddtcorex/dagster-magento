"""Build and render the per-version compatibility records.

Used by scripts/compat-matrix.sh: a pytest JUnit file plus the facts read
from the sandbox become one JSON record per Magento version, and the records
render into the results table of docs/Compatibility.md.
"""

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

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
    """'2.4.7-p10' sorts as (2, 4, 7, 10) and a base release '2.4.9' as
    (2, 4, 9, 0), so patches order by number, never as text."""
    numbers = [int(part) for part in re.findall(r"\d+", version)]
    return tuple(numbers) if "-p" in version else tuple(numbers + [0])


def _result(record: dict) -> str:
    tests = record["tests"]
    if record["status"] == "not provisioned":
        return f"not provisioned: {record.get('reason', 'unknown')}"
    ran = [test for test in tests if test["outcome"] != "skipped"]
    passed = sum(1 for test in ran if test["outcome"] == "passed")
    if record["status"] == "failed":
        detail = f": {record['reason']}" if record.get("reason") else ""
        return f"failed ({len(ran) - passed} of {len(ran)} failed){detail}"
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
    """Replace what sits between the two markers. A marker only counts when it
    stands alone on its line, so a page can quote the marker text in a sentence."""
    with open(readme_path) as handle:
        text = handle.read()
    start = re.search(rf"^{re.escape(START_MARKER)}[ \t]*$", text, re.MULTILINE)
    end = re.search(rf"^{re.escape(END_MARKER)}[ \t]*$", text, re.MULTILINE)
    if start is None or end is None or end.start() < start.end():
        raise ValueError(f"{readme_path} needs {START_MARKER} and {END_MARKER} each on a line of its own")
    with open(readme_path, "w") as handle:
        handle.write(f"{text[: start.end()]}\n{table}\n{text[end.start():]}")


def _latest_per_version(results_dir: str) -> list[dict]:
    """One record per Magento version: the one with the newest date (file
    name breaks a tie), so a rerun replaces an older row in the table."""
    latest: dict[str, tuple[tuple[str, str], dict]] = {}
    for path in sorted(Path(results_dir).glob("*.json")):
        record = json.loads(path.read_text())
        rank = (record["date"], path.name)
        if record["version"] not in latest or rank > latest[record["version"]][0]:
            latest[record["version"]] = (rank, record)
    return [record for _, record in latest.values()]


def _cmd_record(args) -> int:
    facts = json.loads(Path(args.facts).read_text()) if args.facts else {}
    tests = parse_junit(args.junit) if args.junit else []
    record = build_record(args.version, facts, tests, args.date, args.status, reason=args.reason)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    return 0


def _cmd_table(args) -> int:
    table = render_table(_latest_per_version(args.results))
    try:
        write_table(args.readme, table)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    record = commands.add_parser("record", help="write one per-version JSON record")
    record.add_argument("--version", required=True)
    record.add_argument("--junit")
    record.add_argument("--facts", help="JSON file with the sandbox facts")
    record.add_argument("--date", required=True)
    record.add_argument("--status", required=True, choices=STATUSES)
    record.add_argument("--reason")
    record.add_argument("--out", required=True)
    record.set_defaults(func=_cmd_record)

    table = commands.add_parser("table", help="render the newest record per version into a page between its markers")
    table.add_argument("--results", required=True)
    table.add_argument("--readme", required=True)
    table.set_defaults(func=_cmd_table)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
