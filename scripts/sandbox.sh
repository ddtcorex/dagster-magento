#!/usr/bin/env bash
# sandbox.sh -- local Magento 2 sandbox for dagster-magento live verification.
#
# Wraps govard to bring up a fresh, disposable Magento install used by the
# live tasks in the catalog import work. Every container command goes
# through govard (`govard tool magento ...` or `govard shell -c '...'`),
# never a raw docker exec.
#
# Usage:
#   scripts/sandbox.sh up [--version 2.4.9]   # bootstrap a fresh install
#   scripts/sandbox.sh down                   # stop containers, keep volumes
#   scripts/sandbox.sh reset [--version 2.4.9]  # down -v, then up again
#   scripts/sandbox.sh consumers               # start 4 async.operations.all consumers
#   scripts/sandbox.sh env                     # print MAGENTO_* vars for later tasks
#   scripts/sandbox.sh cron-run                 # run bin/magento cron:run twice
#   scripts/sandbox.sh bridge                   # enable the optional bridge checkout again
#   scripts/sandbox.sh bridge-off               # disable it, to prove the native fallback
#
# `up` also writes the two settings the async bulk path needs, both into
# app/etc/env.php: cron_consumers_runner (no cron-managed consumers) and a
# READ COMMITTED session transaction isolation level, without which MariaDB
# rejects and drops a bulk operation whose message outruns its own row. See
# write_db_isolation_config below for the measurement.
set -euo pipefail

DEFAULT_VERSION="2.4.9"
DOMAIN="dagster-magento-sandbox.test"
ADMIN_USER="dagster"
ADMIN_EMAIL="dagster@example.test"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SANDBOX_ROOT="$REPO_ROOT/sandbox"
PROJECT_DIR="$SANDBOX_ROOT/dagster-magento-sandbox"
PASSWORD_FILE="$SANDBOX_ROOT/.admin-password"

# Development leftovers a bridge checkout accumulates (composer dev
# dependencies, static analysis caches). Magento scans every PHP file under
# app/code, so setup:di:compile loads them and fails ("Phar wrapper is not
# registered" out of PHPStan's cache), and a reset would copy hundreds of MB.
BRIDGE_DEV_ARTIFACTS=(vendor .phpstan.cache .phpunit.cache .phpcs-cache)
PARKED_DIR=""

# Where a reset keeps the bridge checkout while the project is rebuilt. A fixed
# path, so a reset that died before restoring it leaves it findable, and the next
# reset does not replace it with nothing.
BRIDGE_STASH_DIR="$SANDBOX_ROOT/.bridge-stash"

stash_bridge_checkout() {
  local source="$PROJECT_DIR/app/code/DDTCoreX"
  # No module in the project: an earlier reset died before restoring it, so
  # the stash already holds the only copy. Keep it.
  [[ -d "$source" ]] || return 0
  rm -rf "$BRIDGE_STASH_DIR"
  mkdir -p "$BRIDGE_STASH_DIR"
  # Dev leftovers (vendor, analysis caches) stay behind: hundreds of MB the
  # fresh install never needs and that production compile cannot scan.
  rsync -a --exclude='DagsterBridge/vendor' --exclude=.phpstan.cache --exclude=.phpunit.cache \
    --exclude=.phpcs-cache "$source" "$BRIDGE_STASH_DIR/"
  log "keeping app/code/DDTCoreX across the reset at $BRIDGE_STASH_DIR"
}

restore_bridge_checkout() {
  [[ -d "$BRIDGE_STASH_DIR/DDTCoreX" ]] || return 1
  # This runs as an `if` condition, where bash ignores `set -e`: every step
  # that can fail returns explicitly, so the stash (the only copy once the
  # project was removed) is never deleted after a failed copy.
  mkdir -p "$PROJECT_DIR/app/code" || return 1
  cp -a "$BRIDGE_STASH_DIR/DDTCoreX" "$PROJECT_DIR/app/code/" || return 1
  rm -rf "$BRIDGE_STASH_DIR"
}

park_bridge_dev_artifacts() {
  local module="$PROJECT_DIR/app/code/DDTCoreX/DagsterBridge" name
  [[ -d "$module" ]] || return 0
  PARKED_DIR="$(mktemp -d "$SANDBOX_ROOT/.parked.XXXXXX")"
  for name in "${BRIDGE_DEV_ARTIFACTS[@]}"; do
    [[ -e "$module/$name" ]] && mv "$module/$name" "$PARKED_DIR/"
  done
  trap restore_bridge_dev_artifacts EXIT
}

restore_bridge_dev_artifacts() {
  local module="$PROJECT_DIR/app/code/DDTCoreX/DagsterBridge" name
  [[ -n "$PARKED_DIR" && -d "$PARKED_DIR" ]] || return 0
  for name in "${BRIDGE_DEV_ARTIFACTS[@]}"; do
    [[ -e "$PARKED_DIR/$name" ]] && mv "$PARKED_DIR/$name" "$module/"
  done
  rmdir "$PARKED_DIR" 2>/dev/null || true
  PARKED_DIR=""
}

log() { printf '[sandbox] %s\n' "$*" >&2; }
die() { printf '[sandbox] error: %s\n' "$*" >&2; exit 1; }

parse_version() {
  VERSION="$DEFAULT_VERSION"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --version)
        VERSION="${2:?--version needs a value}"
        shift 2
        ;;
      --version=*)
        VERSION="${1#*=}"
        shift
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
  done
}

require_project_dir() {
  [[ -d "$PROJECT_DIR" ]] || die "sandbox not found at $PROJECT_DIR; run 'up' first"
}

write_cron_consumers_config() {
  # Merge cron_consumers_runner into app/etc/env.php from inside the project
  # directory (host-mounted into the container), then run it via govard
  # shell so no raw docker exec is used.
  local helper="$PROJECT_DIR/var/dagster-cron-consumers.php"
  mkdir -p "$PROJECT_DIR/var"
  cat > "$helper" <<'PHP'
<?php
$path = __DIR__ . '/../app/etc/env.php';
$config = include $path;
$config['cron_consumers_runner'] = [
    'cron_run' => false,
    'max_messages' => 0,
    'consumers' => ['async.operations.all'],
    'multiple_processes' => ['async.operations.all' => 4],
];
file_put_contents($path, "<?php\nreturn " . var_export($config, true) . ";\n");
echo "cron_consumers_runner updated\n";
PHP
  ( cd "$PROJECT_DIR" && govard shell -c 'php var/dagster-cron-consumers.php' )
  rm -f "$helper"
}

write_db_isolation_config() {
  # Magento publishes an async bulk on the broker before it commits the rows
  # that bulk belongs to: MassSchedule::publishMass calls
  # BulkManagement::scheduleBulk (which publishes and commits the bulk
  # summary) and only then SaveMultipleOperations::execute inserts
  # magento_operation. A consumer can therefore reach its row while that
  # insert is still uncommitted, and on MariaDB under its default
  # REPEATABLE READ the consumer's
  # `UPDATE magento_operation SET started_at = ...` fails with SQLSTATE 1020
  # "Record has changed since last read in table 'magento_operation'".
  # MassConsumerEnvelopeCallback::execute catches that exception and calls
  # reject($message, false), which drops the message without requeue: the row
  # keeps status 4 (open) forever and the library can only report it pending.
  #
  # READ COMMITTED removes the race. Measured on this sandbox (Magento 2.4.9,
  # MariaDB 11.8) with the operation insert held open for a forced 400 ms:
  # 4 of 20 operations dropped and 4 rejection lines per run before, 0 of 20
  # over three runs after.
  #
  # 1002 is PDO::MYSQL_ATTR_INIT_COMMAND, taken verbatim from
  # app/etc/env.php's driver_options, which Magento passes straight to the
  # PDO constructor. The literal number keeps the generated env.php
  # independent of the PDO class being loaded by whatever reads it.
  local helper="$PROJECT_DIR/var/dagster-db-isolation.php"
  mkdir -p "$PROJECT_DIR/var"
  cat > "$helper" <<'PHP'
<?php
$path = __DIR__ . '/../app/etc/env.php';
$config = include $path;
$config['db']['connection']['default']['driver_options'][1002] =
    'SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED';
file_put_contents($path, "<?php\nreturn " . var_export($config, true) . ";\n");
echo "db transaction isolation updated\n";
PHP
  ( cd "$PROJECT_DIR" && govard shell -c 'php var/dagster-db-isolation.php' )
  rm -f "$helper"
}

enable_bridge_module() {
  # The DagsterBridge module is an optional companion checkout that lives inside
  # this gitignored sandbox tree. When it is present the sandbox enables it, so
  # the live matrix can exercise the bridge; without the checkout the sandbox
  # simply runs the library's native paths.
  local module_dir="$PROJECT_DIR/app/code/DDTCoreX/DagsterBridge"
  if [[ ! -f "$module_dir/registration.php" ]]; then
    log "no bridge module checkout at $module_dir, running without it"
    return 0
  fi
  log "enabling the bridge module"
  ( cd "$PROJECT_DIR" && govard tool magento module:enable DDTCoreX_DagsterBridge )
  ( cd "$PROJECT_DIR" && govard tool magento setup:upgrade )
  ( cd "$PROJECT_DIR" && govard tool magento cache:flush )
}

use_supported_search_backend() {
  # Magento 2.4.6 ships no OpenSearch adapter: its elasticsearch7 engine
  # speaks typed URLs (…/document/_bulk) that OpenSearch 2.x rejects with "no
  # handler found for uri", so every product save fails inline through the
  # stock→MSI→fulltext chain ("The stock item was unable to be saved").
  # govard pins OpenSearch 2.5 for 2.4.6, which can never work there, so the
  # sandbox uses the locally available Elasticsearch 7.17 instead. Newer
  # Magento (with module-opensearch) keeps the profile default.
  case "$VERSION" in
    2.4.6*) ;;
    *) return 0 ;;
  esac
  command -v docker >/dev/null || die "docker is required to switch the search backend for $VERSION"
  log "switching the search backend to Elasticsearch 7.17 for $VERSION"
  ( cd "$PROJECT_DIR" && sed -i 's/^\(\s*search:\) opensearch$/\1 elasticsearch/' .govard.yml )
  ( cd "$PROJECT_DIR" && sed -i 's/^\(\s*search_version:\) .*/\1 "7.17.28"/' .govard.yml )
  local project
  project="$(basename "$PROJECT_DIR")"
  docker rm -f "${project}-elasticsearch-1" >/dev/null 2>&1 || true
  docker volume rm "${project}_search-data" >/dev/null 2>&1 || true
  ( cd "$PROJECT_DIR" && govard up )
  # A fresh Elasticsearch needs up to a couple of minutes for first boot
  # (JVM + cluster formation); setup:upgrade validates the connection and
  # fails with "No alive nodes" if it runs too early, so wait for it here.
  log "waiting for Elasticsearch to answer"
  local attempt
  for attempt in $(seq 1 36); do
    if ( cd "$PROJECT_DIR" && govard elasticsearch / >/dev/null 2>&1 ); then
      log "Elasticsearch is up"
      return 0
    fi
    sleep 5
  done
  die "Elasticsearch did not answer within 3 minutes"
}

cmd_up() {
  parse_version "$@"

  if [[ -d "$PROJECT_DIR" ]] && [[ -n "$(ls -A "$PROJECT_DIR" 2>/dev/null)" ]]; then
    die "sandbox already exists at $PROJECT_DIR; run 'down' or 'reset' first"
  fi

  mkdir -p "$PROJECT_DIR"
  log "bootstrapping Magento $VERSION into $PROJECT_DIR"
  ( cd "$PROJECT_DIR" && govard bootstrap --framework magento2 --fresh --framework-version "$VERSION" --yes )

  use_supported_search_backend

  # A fresh install's `queue_topology.xml` declarations (async.operations.all
  # among them) are not applied until setup:upgrade runs at least once --
  # without this, `queue:consumers:start` fails with "no queue ... in vhost"
  # and the consumers subcommand has nothing to attach to.
  ( cd "$PROJECT_DIR" && govard tool magento setup:upgrade )

  ( cd "$PROJECT_DIR" && govard tool magento indexer:set-mode schedule )
  ( cd "$PROJECT_DIR" && govard tool magento cache:enable full_page )
  ( cd "$PROJECT_DIR" && govard tool magento config:set dev/grid/async_indexing 1 )

  # Magento 2.4.9 enables Magento_TwoFactorAuth by default, which blocks the
  # REST admin token endpoint until a 2FA provider is configured. Disabling
  # it is the standard dev-sandbox workaround; govard's own auto-config
  # tries to flip the "Disable 2FA" system setting but skips it here because
  # the setting path is only reachable once the module is enabled once, so
  # this is done explicitly instead of relying on that step.
  ( cd "$PROJECT_DIR" && govard tool magento module:disable Magento_TwoFactorAuth Magento_AdminAdobeImsTwoFactorAuth )

  mkdir -p "$SANDBOX_ROOT"
  ADMIN_PASSWORD="Dg$(openssl rand -hex 16)!"
  printf '%s' "$ADMIN_PASSWORD" > "$PASSWORD_FILE"
  chmod 600 "$PASSWORD_FILE"

  ( cd "$PROJECT_DIR" && govard tool magento admin:user:create \
      --admin-user="$ADMIN_USER" \
      --admin-password="$ADMIN_PASSWORD" \
      --admin-email="$ADMIN_EMAIL" \
      --admin-firstname=Dagster \
      --admin-lastname=Sandbox )

  write_cron_consumers_config
  write_db_isolation_config
  enable_bridge_module

  log "sandbox ready at https://$DOMAIN"
}

cmd_down() {
  require_project_dir
  ( cd "$PROJECT_DIR" && govard down )
}

cmd_reset() {
  # Default to the version the existing sandbox was built with, so a plain
  # `reset` (no --version) reproduces the same install; an explicit
  # --version still overrides this.
  local existing_version=""
  if [[ -f "$PROJECT_DIR/.govard.yml" ]]; then
    existing_version="$(sed -n 's/^framework_version:[[:space:]]*//p' "$PROJECT_DIR/.govard.yml" | head -n1)"
  fi
  DEFAULT_VERSION="${existing_version:-$DEFAULT_VERSION}"

  parse_version "$@"

  # A reset wipes the project directory, and the optional bridge module is a
  # git checkout that lives inside it: keep it aside so a reset does not throw
  # away unpushed work, then put it back and enable it on the fresh install.
  # The stash survives a reset that dies half way (a Composer failure while
  # provisioning, say) and is only removed once it has been restored.
  stash_bridge_checkout

  if [[ -d "$PROJECT_DIR" ]]; then
    ( cd "$PROJECT_DIR" && govard down -v )
    rm -rf "$PROJECT_DIR"
  fi
  rm -f "$PASSWORD_FILE"
  cmd_up --version "$VERSION"

  if restore_bridge_checkout; then
    enable_bridge_module
  fi
}

cmd_consumers() {
  require_project_dir
  ( cd "$PROJECT_DIR" && govard shell -c '
mkdir -p var/log
for i in 1 2 3 4; do
  nohup bin/magento queue:consumers:start async.operations.all > "var/log/dagster-consumer-$i.log" 2>&1 &
done
sleep 1
disown -a || true
' )
  log "started 4 async.operations.all consumers"
}

cmd_cron_run() {
  require_project_dir
  ( cd "$PROJECT_DIR" && govard tool magento cron:run )
  ( cd "$PROJECT_DIR" && govard tool magento cron:run )
}

cmd_bridge_off() {
  require_project_dir
  ( cd "$PROJECT_DIR" && govard tool magento module:disable DDTCoreX_DagsterBridge )
  ( cd "$PROJECT_DIR" && govard tool magento setup:upgrade )
  ( cd "$PROJECT_DIR" && govard tool magento cache:flush )
  log "bridge module disabled"
}

cmd_deploy_mode() {
  local mode="${1:-}"
  case "$mode" in
    developer|production) ;;
    *) die "deploy-mode needs developer or production" ;;
  esac
  require_project_dir
  # Switching to production compiles and deploys static content, which takes
  # several minutes; back to developer is quick. The compile must not see the
  # bridge checkout's dev leftovers, so they are parked and put back on exit.
  [[ "$mode" == production ]] && park_bridge_dev_artifacts
  ( cd "$PROJECT_DIR" && govard tool magento deploy:mode:set "$mode" ) >&2
  ( cd "$PROJECT_DIR" && govard tool magento cache:flush ) >&2
  local shown
  shown="$( cd "$PROJECT_DIR" && govard tool magento deploy:mode:show )"
  log "$shown"
  printf 'deploy mode: %s\n' "$mode"
}

cmd_env() {
  [[ -f "$PASSWORD_FILE" ]] || die "no admin password found at $PASSWORD_FILE; run 'up' first"
  local password
  password="$(cat "$PASSWORD_FILE")"
  printf 'export MAGENTO_BASE_URL=%q\n' "https://$DOMAIN"
  printf 'export MAGENTO_ADMIN_USERNAME=%q\n' "$ADMIN_USER"
  printf 'export MAGENTO_ADMIN_PASSWORD=%q\n' "$password"
  printf 'export MAGENTO_STORE_VIEW=%q\n' "all"
}

main() {
  local sub="${1:-}"
  [[ $# -gt 0 ]] && shift || true

  case "$sub" in
    up) cmd_up "$@" ;;
    down) cmd_down "$@" ;;
    reset) cmd_reset "$@" ;;
    consumers) cmd_consumers "$@" ;;
    cron-run) cmd_cron_run "$@" ;;
    bridge) enable_bridge_module ;;
    bridge-off) cmd_bridge_off "$@" ;;
    deploy-mode) cmd_deploy_mode "$@" ;;
    env) cmd_env "$@" ;;
    *)
      cat >&2 <<USAGE
Usage: $(basename "$0") <up|down|reset|consumers|env|cron-run|deploy-mode> [options]
  up [--version V]     bootstrap a fresh Magento sandbox (default version $DEFAULT_VERSION)
  down                 stop containers, keep volumes
  reset [--version V]  down -v, then up again with a fresh database
  consumers            start 4 async.operations.all consumer processes
  cron-run             run bin/magento cron:run twice
  bridge               enable the optional bridge module checkout
  bridge-off           disable the bridge module, to prove the native fallback
  deploy-mode <mode>   switch the sandbox to developer or production mode
  env                  print MAGENTO_BASE_URL / MAGENTO_ADMIN_USERNAME / MAGENTO_ADMIN_PASSWORD / MAGENTO_STORE_VIEW
USAGE
      exit 1
      ;;
  esac
}

# Sourced by the tests to reach the functions above without running a command.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
