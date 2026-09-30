"""Helpers the live tests share: building the resource from the
environment and driving the local sandbox through scripts/sandbox.sh and
govard. Imported directly by test modules (tests/live is on sys.path);
conftest.py holds only hooks and fixtures.
"""

import json
import os
import re
import subprocess
import textwrap
import time
from pathlib import Path

import pytest
import requests

from dagster_magento import MagentoResource

REPO_ROOT = Path(__file__).resolve().parents[2]
SANDBOX_SCRIPT = REPO_ROOT / "scripts" / "sandbox.sh"
SANDBOX_PROJECT = REPO_ROOT / "sandbox" / "dagster-magento-sandbox"

_ENV_VARS = ("MAGENTO_BASE_URL", "MAGENTO_ADMIN_USERNAME", "MAGENTO_ADMIN_PASSWORD", "MAGENTO_STORE_VIEW")


def make_resource() -> MagentoResource:
    """Build a resource from the current environment. Read on every call,
    because a sandbox reset rotates the admin password (see reload_env)."""
    return MagentoResource(
        base_url=os.environ["MAGENTO_BASE_URL"],
        username=os.environ["MAGENTO_ADMIN_USERNAME"],
        password=os.environ["MAGENTO_ADMIN_PASSWORD"],
        store_view=os.environ.get("MAGENTO_STORE_VIEW", "all"),
    )


def require_sandbox() -> None:
    """Tests that drive the sandbox itself (cron, reset, bin/magento) need
    the local checkout of it, not just a reachable URL."""
    if not SANDBOX_PROJECT.is_dir():
        pytest.skip(f"no local sandbox project at {SANDBOX_PROJECT.relative_to(REPO_ROOT)}")


def sandbox(*args: str, timeout: float = 600) -> str:
    """Run a scripts/sandbox.sh subcommand and return its stdout."""
    require_sandbox()
    completed = subprocess.run(
        [str(SANDBOX_SCRIPT), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
    )
    return completed.stdout


def magento_cli(*args: str, timeout: float = 600) -> str:
    """Run bin/magento inside the sandbox through govard, as sandbox.sh does."""
    require_sandbox()
    completed = subprocess.run(
        ["govard", "tool", "magento", *args],
        cwd=SANDBOX_PROJECT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=True,
    )
    return completed.stdout


def reload_env() -> None:
    """Re-read MAGENTO_* from `sandbox.sh env` into os.environ, after a
    reset generated a new admin password."""
    for line in sandbox("env").splitlines():
        name, _, value = line.removeprefix("export ").partition("=")
        if name in _ENV_VARS:
            os.environ[name] = subprocess.run(
                ["bash", "-c", f"printf %s {value}"], capture_output=True, text=True, check=True
            ).stdout


def prepare_sandbox() -> None:
    """Idempotent sandbox setup the live tests need:
    - the sample downloadable links point at firebearstudio.com, which
      Magento rejects unless the domain is whitelisted;
    - advanced_pricing.csv sets per-website tier prices ("base"), which
      price storage rejects ("Invalid attribute Website ID") while the
      price scope is global, so the scope is set to website;
    - a second store view "fr" for the store-scope test;
    - bulk mode needs async.operations.all consumers running. A consumer
      caches the store list when it starts and answers "The store that was
      requested wasn't found" for a store created later, so consumers are
      restarted when this call had to create the store view;
    - the search indices must exist before the first product save. Every
      product save runs the stock→MSI→fulltext chain inline, and concurrent
      first saves race index creation against each other: the losers get a
      400 resource_already_exists_exception that rolls the whole save back
      ("The stock item was unable to be saved", seen live on 2.4.6). A
      serial fulltext reindex up front creates the indices once, so the
      saves that follow only write documents."""
    magento_cli("downloadable:domains:add", "firebearstudio.com")
    magento_cli("config:set", "catalog/price/scope", "1")
    created = ensure_store_view("fr", "French")
    magento_cli("cache:flush")
    magento_cli("indexer:reindex", "catalogsearch_fulltext")
    running = consumer_count()
    if created and running:
        stop_consumers()
        running = 0
    if running == 0:
        sandbox("consumers")
    wait_until_ready()


def wait_until_ready(timeout_s: float = 180) -> None:
    """Wait for the admin token endpoint to answer 200. Right after a reset
    or a cache flush the sandbox briefly answers 503 (seen live).

    A ConnectionError means the domain itself stopped resolving: the proxy
    registration of the domain is lost with the torn-down environment and is
    not always re-registered by the time the next `govard up` returns. The
    wait re-registers it once through `govard up` (idempotent) instead of
    polling a name that resolves to nothing until the deadline."""
    url = f"{os.environ['MAGENTO_BASE_URL']}/rest/V1/integration/admin/token"
    credentials = {"username": os.environ["MAGENTO_ADMIN_USERNAME"], "password": os.environ["MAGENTO_ADMIN_PASSWORD"]}
    deadline = time.monotonic() + timeout_s
    re_registered = False
    while True:
        try:
            if requests.post(url, json=credentials, timeout=30).status_code == 200:
                return
        except requests.exceptions.ConnectionError:
            if not re_registered:
                re_registered = True
                re_register_domain()
        if time.monotonic() > deadline:
            raise AssertionError(f"sandbox not ready after {timeout_s}s")
        time.sleep(5)


def re_register_domain() -> None:
    """Re-register the sandbox domain with the govard proxy (idempotent)."""
    completed = subprocess.run(
        ["govard", "up"],
        cwd=SANDBOX_PROJECT,
        capture_output=True,
        text=True,
        timeout=600,
    )
    if completed.returncode != 0:
        print(completed.stdout[-500:], completed.stderr[-500:])


def ensure_store_view(code: str, name: str) -> bool:
    """Create store view `code` in the default website and store group if
    missing; True when it was created. Native Magento has no CLI or REST
    call that creates a store view, so this runs a short PHP script."""
    output = govard_php(
        textwrap.dedent(
            f"""
            $om = \\Magento\\Framework\\App\\Bootstrap::create(BP, $_SERVER)->getObjectManager();
            $om->get(\\Magento\\Framework\\App\\State::class)->setAreaCode('adminhtml');
            $store = $om->create(\\Magento\\Store\\Model\\Store::class)->load('{code}', 'code');
            if (!$store->getId()) {{
                $store->setCode('{code}')->setName('{name}')->setWebsiteId(1)->setGroupId(1)
                    ->setIsActive(1)->setSortOrder(10);
                $om->get(\\Magento\\Store\\Model\\ResourceModel\\Store::class)->save($store);
                echo "created\\n";
            }}
            """
        )
    )
    return "created" in output


def stop_consumers() -> None:
    """Stop the sandbox's async.operations.all consumers. Runs inside the
    sandbox php container only and matches the exact consumer command; the
    [b] keeps the pattern from matching this shell's own command line."""
    require_sandbox()
    subprocess.run(
        ["govard", "shell", "-c", "pkill -f '[b]in/magento queue:consumers:start async.operations.all' || true"],
        cwd=SANDBOX_PROJECT,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )


def consumer_count() -> int:
    """How many async.operations.all consumers run in the sandbox."""
    require_sandbox()
    completed = subprocess.run(
        ["govard", "shell", "-c", "ps aux | grep -c '[q]ueue:consumers:start async.operations.all' || true"],
        cwd=SANDBOX_PROJECT,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    return int(completed.stdout.strip().splitlines()[-1])


def govard_php(body: str, timeout: float = 300) -> str:
    """Run a PHP snippet inside the sandbox with Magento's bootstrap
    loaded, for setup native Magento has no CLI or REST call for. The file
    goes under the host-mounted var/ (as sandbox.sh does) and is removed."""
    require_sandbox()
    script = SANDBOX_PROJECT / "var" / f"dagster-live-{os.getpid()}.php"
    script.write_text("<?php\nrequire __DIR__ . '/../app/bootstrap.php';\n" + body)
    try:
        completed = subprocess.run(
            ["govard", "shell", "-c", f"php var/{script.name}"],
            cwd=SANDBOX_PROJECT,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        )
    finally:
        script.unlink(missing_ok=True)
    return completed.stdout


def normalize_database(version: str) -> str:
    """'11.8.2-MariaDB-ubu2404' -> 'MariaDB 11.8.2'; a plain number is MySQL."""
    number = re.match(r"[\d.]+", version).group(0)
    return f"{'MariaDB' if 'mariadb' in version.lower() else 'MySQL'} {number}"


def govard_setting(text: str, key: str) -> str | None:
    """A `key: value` line from a .govard.yml, quotes removed."""
    match = re.search(rf"^\s*{re.escape(key)}:\s*(.+?)\s*$", text, re.MULTILINE)
    return match.group(1).strip("\"'") if match else None


def sandbox_facts() -> dict:
    """What the compatibility record states about the running sandbox: the
    exact Magento version (with patch), PHP, database, search engine and the
    bridge module version the capabilities endpoint reports."""
    output = govard_php(
        textwrap.dedent(
            """
            $om = \\Magento\\Framework\\App\\Bootstrap::create(BP, $_SERVER)->getObjectManager();
            $config = $om->get(\\Magento\\Framework\\App\\Config\\ScopeConfigInterface::class);
            $connection = $om->get(\\Magento\\Framework\\App\\ResourceConnection::class)->getConnection();
            echo "\\n" . json_encode([
                'magento' => $om->get(\\Magento\\Framework\\App\\ProductMetadataInterface::class)->getVersion(),
                'php' => PHP_VERSION,
                'database' => $connection->fetchOne('SELECT VERSION()'),
                'engine' => $config->getValue('catalog/search/engine'),
            ]) . "\\n";
            """
        )
    )
    raw = json.loads(next(line for line in reversed(output.splitlines()) if line.startswith("{")))
    govard_yml = (SANDBOX_PROJECT / ".govard.yml").read_text()
    search_version = govard_setting(govard_yml, "search_version")
    try:
        bridge = str(make_resource().get("dagster-bridge/capabilities").get("version", "unknown"))
    except Exception:  # noqa: BLE001 - a store without the module answers 404
        bridge = "not installed"
    return {
        "magento": raw["magento"],
        "php": raw["php"],
        "database": normalize_database(raw["database"]),
        "search": f"{raw['engine']} {search_version}" if search_version else raw["engine"],
        "bridge": bridge,
    }
