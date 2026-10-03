"""The wiki sources under docs/ and the README that points at them stay consistent:
no broken links, nothing orphaned, the generated table has its markers."""

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
PAGES = sorted(path for path in DOCS.glob("*.md") if not path.name.startswith("_"))


def _links(text: str) -> set[str]:
    return {target.split("#")[0] for target in re.findall(r"\]\(([A-Za-z0-9_-]+(?:#[A-Za-z0-9_-]+)?)\)", text)}


def test_the_docs_check_script_passes_on_the_real_pages():
    completed = subprocess.run([str(ROOT / "scripts" / "check-docs.sh"), str(DOCS)], capture_output=True, text=True)

    assert completed.returncode == 0, completed.stderr


def test_the_docs_check_script_refuses_a_broken_link_and_a_missing_home(tmp_path):
    script = str(ROOT / "scripts" / "check-docs.sh")
    (tmp_path / "Page.md").write_text("see [gone](Nowhere)\n")

    no_home = subprocess.run([script, str(tmp_path)], capture_output=True, text=True)
    (tmp_path / "Home.md").write_text("[page](Page)\n")
    broken = subprocess.run([script, str(tmp_path)], capture_output=True, text=True)

    assert no_home.returncode == 1 and "Home.md is missing" in no_home.stderr
    assert broken.returncode == 1 and "missing page 'Nowhere'" in broken.stderr


def test_every_page_is_in_the_sidebar_and_on_the_home_page():
    sidebar = _links((DOCS / "_Sidebar.md").read_text())
    home = _links((DOCS / "Home.md").read_text())
    names = {page.stem for page in PAGES}

    assert names - sidebar == set(), f"not in the sidebar: {sorted(names - sidebar)}"
    assert names - home - {"Home"} == set(), f"not linked from Home: {sorted(names - home - {'Home'})}"


def test_the_readme_only_links_to_wiki_pages_that_exist():
    readme = (ROOT / "README.md").read_text()
    targets = set(re.findall(r"github\.com/ddtcorex/dagster-magento/wiki/([A-Za-z0-9-]+)", readme))

    assert targets, "the README should point at the wiki"
    assert targets <= {page.stem for page in PAGES}, sorted(targets - {page.stem for page in PAGES})


def test_the_generated_compatibility_table_has_its_markers():
    text = (DOCS / "Compatibility.md").read_text()

    assert text.count("<!-- compat:start -->") == 1 and text.count("<!-- compat:end -->") == 1
    assert text.index("<!-- compat:start -->") < text.index("<!-- compat:end -->")
    assert "compat:start" not in (ROOT / "README.md").read_text()


def test_no_em_or_en_dashes_in_the_docs_or_the_readme():
    offenders = [
        path.name
        for path in [*DOCS.glob("*.md"), ROOT / "README.md"]
        if re.search("[–—]", path.read_text())
    ]

    assert offenders == []
