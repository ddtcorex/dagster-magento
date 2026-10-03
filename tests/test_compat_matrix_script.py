import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "compat-matrix.sh"


def _run(*args):
    return subprocess.run([str(SCRIPT), *args], capture_output=True, text=True, timeout=30)


def test_dry_run_lists_the_default_versions_in_order_and_touches_nothing():
    completed = _run("--dry-run")

    assert completed.returncode == 0
    planned = [line.split()[-1] for line in completed.stdout.splitlines() if line.startswith("would run")]
    assert planned == ["2.4.6-p15", "2.4.7-p10", "2.4.8-p5", "2.4.9"]


def test_dry_run_honours_an_explicit_version_list():
    completed = _run("--dry-run", "--versions", "2.4.9,2.4.6")

    planned = [line.split()[-1] for line in completed.stdout.splitlines() if line.startswith("would run")]
    assert planned == ["2.4.9", "2.4.6"]


def test_an_unknown_flag_is_refused():
    completed = _run("--bogus")

    assert completed.returncode == 2
    assert "usage" in completed.stderr.lower()
