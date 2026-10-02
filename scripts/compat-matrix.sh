#!/usr/bin/env bash
# Run the live suite against a fresh sandbox of each supported Magento line
# and record one JSON result per version under compat/results/.
#
# Usage: scripts/compat-matrix.sh [--versions 2.4.6-p15,2.4.7-p10,2.4.8-p5,2.4.9] [--write] [--dry-run]
#   --versions  comma separated list, run in the order given
#   --write     regenerate the README compatibility table from compat/results/
#   --dry-run   print the plan and touch nothing
#
# A version that cannot be provisioned is recorded as "not provisioned" with
# the reason; a failing version is recorded as "failed". Neither stops the
# loop, and the exit code is non-zero when any version was not verified. The
# sandbox is reset to the default version at the end.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$REPO_ROOT/compat/results"
DEFAULT_VERSION="2.4.9"
# The newest patch of each supported line. A base release such as 2.4.7 is
# refused by Composer's security blocking once advisories exist for it, so the
# latest patch is also the one that installs. Update these when a newer patch
# ships; the record states the exact version each run used.
VERSIONS="2.4.6-p15,2.4.7-p10,2.4.8-p5,2.4.9"
WRITE=0
DRY_RUN=0

usage() {
  echo "usage: $(basename "$0") [--versions 2.4.6-p15,2.4.7-p10,2.4.8-p5,2.4.9] [--write] [--dry-run]" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --versions) [[ $# -ge 2 ]] || usage; VERSIONS="$2"; shift 2 ;;
    --write) WRITE=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) usage ;;
  esac
done

IFS=',' read -r -a VERSION_LIST <<< "$VERSIONS"

if [[ "$DRY_RUN" == 1 ]]; then
  for version in "${VERSION_LIST[@]}"; do
    echo "would run $version"
  done
  [[ "$WRITE" == 1 ]] && echo "would write the README table"
  exit 0
fi

log() { printf '[compat] %s\n' "$*" >&2; }

SANDBOX_DOMAIN="dagster-magento-sandbox.test"

# The sandbox domain is served by govard's shared proxy, which other govard
# sessions on the same machine can remove or recreate. Bring it back once if
# it is gone, so a lost proxy is not mistaken for a failing suite.
domain_resolves() { getent hosts "$SANDBOX_DOMAIN" > /dev/null 2>&1; }

ensure_domain() {
  domain_resolves && return 0
  log "$SANDBOX_DOMAIN does not resolve; restoring govard's global services"
  govard svc up --no-trust > /dev/null 2>&1 || true
  sleep 3
  domain_resolves
}

PYTHON="$REPO_ROOT/.venv/bin/python"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
TODAY="$(date +%F)"
FAILURES=0

record() {  # record <version> <status> [reason] [junit] [facts]
  local args=(record --version "$1" --date "$TODAY" --status "$2"
    --out "$RESULTS_DIR/$1-$TODAY.json")
  [[ -n "${3:-}" ]] && args+=(--reason "$3")
  [[ -n "${4:-}" ]] && args+=(--junit "$4")
  [[ -n "${5:-}" ]] && args+=(--facts "$5")
  "$PYTHON" "$SCRIPT_DIR/compat_record.py" "${args[@]}"
}

# One CA bundle (certifi plus the local development CA) for the facts call;
# pytest builds its own from MAGENTO_CA_BUNDLE.
export MAGENTO_CA_BUNDLE="${MAGENTO_CA_BUNDLE:-$HOME/.govard/ssl/root.crt}"
"$PYTHON" - "$WORK/ca.pem" <<'PY'
import sys, certifi, pathlib, os
extra = pathlib.Path(os.environ["MAGENTO_CA_BUNDLE"])
pathlib.Path(sys.argv[1]).write_text(pathlib.Path(certifi.where()).read_text() + "\n" + extra.read_text())
PY

# Full pytest output survives the run (WORK is removed on exit): a failing
# version is diagnosed from its log, not from the JSON record alone.
LOG_DIR="$REPO_ROOT/sandbox/compat-logs"
mkdir -p "$LOG_DIR"

# Restores govard's shared proxy while the suite runs: other govard sessions on
# the same machine can remove it mid-run. Leaves a marker when it had to act.
watch_domain() {
  local lost_marker="$1"
  while sleep 15; do
    if ! domain_resolves; then
      : > "$lost_marker"
      ensure_domain || true
    fi
  done
}

# Reset the sandbox to $1, run the suite and set STATUS, REASON, PROVISIONED.
run_version() {
  local version="$1" lost="$WORK/lost-$1"
  rm -f "$lost"
  PROVISIONED=1 STATUS=verified REASON=""
  log "=== $version: reset"
  if ! "$SCRIPT_DIR/sandbox.sh" reset --version "$version" > "$WORK/reset-$version.log" 2>&1; then
    PROVISIONED=0 STATUS="not provisioned"
    if grep -q "affected by security advisories" "$WORK/reset-$version.log"; then
      # govard's Magento bootstrap does not pass --no-security-blocking (its
      # Laravel one does), so a release whose dependencies carry advisories
      # cannot be installed at all.
      REASON="Composer security blocking refused a dependency of $version and govard bootstrap cannot disable it"
    else
      REASON="reset failed: $(tail -n 3 "$WORK/reset-$version.log" | tr '\n' ' ' | cut -c1-300)"
    fi
    return 0
  fi

  eval "$("$SCRIPT_DIR/sandbox.sh" env)"
  log "=== $version: live suite"
  ensure_domain || log "$SANDBOX_DOMAIN still does not resolve; the suite will fail"
  watch_domain "$lost" &
  local watcher=$!
  REQUESTS_CA_BUNDLE="$WORK/ca.pem" "$PYTHON" -m pytest -m live -q -p no:cacheprovider \
    --junitxml="$WORK/junit-$version.xml" "$REPO_ROOT/tests" > "$WORK/pytest-$version.log" 2>&1 || STATUS=failed
  kill "$watcher" 2> /dev/null || true
  wait "$watcher" 2> /dev/null || true
  cp "$WORK/pytest-$version.log" "$LOG_DIR/$version-$TODAY.log"
  tail -n 3 "$WORK/pytest-$version.log" >&2
  if [[ "$STATUS" == failed && -e "$lost" ]]; then
    REASON="govard's shared proxy was removed during the run and had to be restored (full log: $LOG_DIR/$version-$TODAY.log)"
  fi
  # A test in the suite resets the sandbox, which rotates the admin password:
  # read the environment again before asking the store anything.
  eval "$("$SCRIPT_DIR/sandbox.sh" env)"
}

for version in "${VERSION_LIST[@]}"; do
  run_version "$version"
  # A failure with the proxy lost mid-run says nothing about the code: one
  # more attempt on a fresh sandbox before it is recorded as a failure.
  if [[ "$STATUS" == failed && -e "$WORK/lost-$version" ]]; then
    log "$version: the proxy was lost during the run; trying once more"
    run_version "$version"
  fi

  if [[ "$PROVISIONED" == 0 ]]; then
    log "$version not provisioned ($REASON)"
    record "$version" "$STATUS" "$REASON"
    FAILURES=$((FAILURES + 1))
    continue
  fi

  REQUESTS_CA_BUNDLE="$WORK/ca.pem" "$PYTHON" - > "$WORK/facts-$version.json" <<PY || echo '{}' > "$WORK/facts-$version.json"
import json, sys
sys.path.insert(0, "$REPO_ROOT/tests/live")
import live_support
print(json.dumps(live_support.sandbox_facts()))
PY
  record "$version" "$STATUS" "$REASON" "$WORK/junit-$version.xml" "$WORK/facts-$version.json"
  [[ "$STATUS" == verified ]] || FAILURES=$((FAILURES + 1))
done

if [[ "${VERSION_LIST[-1]}" != "$DEFAULT_VERSION" ]]; then
  log "=== restoring the sandbox to $DEFAULT_VERSION"
  "$SCRIPT_DIR/sandbox.sh" reset --version "$DEFAULT_VERSION" > "$WORK/restore.log" 2>&1 \
    || log "could not restore the sandbox to $DEFAULT_VERSION (see: scripts/sandbox.sh reset)"
fi

if [[ "$WRITE" == 1 ]]; then
  "$PYTHON" "$SCRIPT_DIR/compat_record.py" table --results "$RESULTS_DIR" --readme "$REPO_ROOT/README.md" \
    || FAILURES=$((FAILURES + 1))
fi

log "done: $FAILURES version(s) not verified"
[[ "$FAILURES" -eq 0 ]]
