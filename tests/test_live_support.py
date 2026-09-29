"""Live-support helpers that can be proven without a sandbox."""

import subprocess

import pytest
import requests

from tests.live import live_support


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("MAGENTO_BASE_URL", "https://sandbox.test")
    monkeypatch.setenv("MAGENTO_ADMIN_USERNAME", "admin")
    monkeypatch.setenv("MAGENTO_ADMIN_PASSWORD", "secret")


def test_wait_until_ready_re_registers_the_domain_once(monkeypatch):
    """A ConnectionError means the domain stopped resolving, not that Magento
    is slow: the wait re-registers it once instead of polling a name that
    resolves to nothing until the deadline."""
    outcomes = [requests.exceptions.ConnectionError, True]
    calls = {"post": 0, "sleep": 0}
    proxy_runs = []

    def fake_post(url, json=None, timeout=None):
        calls["post"] += 1
        outcome = outcomes.pop(0)
        if outcome is True:
            return type("Response", (), {"status_code": 200})()
        raise outcome

    def fake_sleep(seconds):
        calls["sleep"] += 1

    def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None):
        proxy_runs.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    monkeypatch.setattr(live_support.requests, "post", fake_post)
    monkeypatch.setattr(live_support.time, "sleep", fake_sleep)
    monkeypatch.setattr(live_support.subprocess, "run", fake_run)

    live_support.wait_until_ready(timeout_s=60)

    assert calls["post"] == 2
    assert proxy_runs == [["govard", "up"]]


def test_wait_until_ready_does_not_re_register_twice(monkeypatch):
    """One re-registration attempt: a still-dead domain must reach the
    deadline instead of restarting the environment over and over."""
    def fake_post(url, json=None, timeout=None):
        raise requests.exceptions.ConnectionError("name does not resolve")

    proxy_runs = []

    def fake_run(cmd, cwd=None, capture_output=None, text=None, timeout=None):
        proxy_runs.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, "", "")

    clock = {"now": 0.0}
    monkeypatch.setattr(live_support.requests, "post", fake_post)
    monkeypatch.setattr(live_support.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(live_support.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + seconds))
    monkeypatch.setattr(live_support.subprocess, "run", fake_run)

    with pytest.raises(AssertionError):
        live_support.wait_until_ready(timeout_s=30)

    assert proxy_runs == [["govard", "up"]]
