import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "sandbox.sh"


def _sh(sandbox_root: Path, body: str) -> subprocess.CompletedProcess:
    project = sandbox_root / "dagster-magento-sandbox"
    script = f'''
        source "{SCRIPT}"
        SANDBOX_ROOT="{sandbox_root}"
        PROJECT_DIR="{project}"
        BRIDGE_STASH_DIR="{sandbox_root}/.bridge-stash"
        {body}
    '''
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, timeout=30)


def _module(project: Path) -> Path:
    module = project / "app" / "code" / "DDTCoreX" / "DagsterBridge"
    (module / "vendor" / "pkg").mkdir(parents=True)
    (module / ".phpstan.cache").mkdir()
    (module / "Model").mkdir()
    (module / "Model" / "X.php").write_text("<?php")
    (module / "registration.php").write_text("<?php")
    return module


def test_stash_keeps_the_module_but_not_its_dev_leftovers(tmp_path):
    module = _module(tmp_path / "dagster-magento-sandbox")

    completed = _sh(tmp_path, "stash_bridge_checkout")

    stashed = tmp_path / ".bridge-stash" / "DDTCoreX" / "DagsterBridge"
    assert completed.returncode == 0, completed.stderr
    assert (stashed / "Model" / "X.php").exists() and (stashed / "registration.php").exists()
    assert not (stashed / "vendor").exists() and not (stashed / ".phpstan.cache").exists()
    assert (module / "vendor").exists()  # the live tree is untouched by a stash


def test_a_failed_reset_does_not_lose_the_stash(tmp_path):
    _module(tmp_path / "dagster-magento-sandbox")
    _sh(tmp_path, "stash_bridge_checkout")
    # A reset that died in cmd_up leaves a project without the module: the
    # next reset must keep the earlier stash instead of replacing it.
    project = tmp_path / "dagster-magento-sandbox"
    subprocess.run(["rm", "-rf", str(project)], check=True)
    project.mkdir()

    completed = _sh(tmp_path, "stash_bridge_checkout")

    assert completed.returncode == 0, completed.stderr
    assert (tmp_path / ".bridge-stash" / "DDTCoreX" / "DagsterBridge" / "registration.php").exists()


def test_restore_puts_the_module_back_and_clears_the_stash(tmp_path):
    project = tmp_path / "dagster-magento-sandbox"
    _module(project)
    _sh(tmp_path, "stash_bridge_checkout")
    subprocess.run(["rm", "-rf", str(project)], check=True)
    project.mkdir()

    completed = _sh(tmp_path, "restore_bridge_checkout")

    assert completed.returncode == 0, completed.stderr
    assert (project / "app" / "code" / "DDTCoreX" / "DagsterBridge" / "registration.php").exists()
    assert not (tmp_path / ".bridge-stash").exists()


def test_restore_without_a_stash_is_a_no_op(tmp_path):
    (tmp_path / "dagster-magento-sandbox").mkdir()

    completed = _sh(tmp_path, "restore_bridge_checkout || echo rc=$?")

    assert "rc=1" in completed.stdout
