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

cmd_up() {
  parse_version "$@"

  if [[ -d "$PROJECT_DIR" ]] && [[ -n "$(ls -A "$PROJECT_DIR" 2>/dev/null)" ]]; then
    die "sandbox already exists at $PROJECT_DIR; run 'down' or 'reset' first"
  fi

  mkdir -p "$PROJECT_DIR"
  log "bootstrapping Magento $VERSION into $PROJECT_DIR"
  ( cd "$PROJECT_DIR" && govard bootstrap --framework magento2 --fresh --framework-version "$VERSION" --yes )

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

  log "sandbox ready at https://$DOMAIN"
}

cmd_down() {
  require_project_dir
  ( cd "$PROJECT_DIR" && govard down )
}

cmd_reset() {
  parse_version "$@"
  if [[ -d "$PROJECT_DIR" ]]; then
    ( cd "$PROJECT_DIR" && govard down -v )
    rm -rf "$PROJECT_DIR"
  fi
  rm -f "$PASSWORD_FILE"
  cmd_up --version "$VERSION"
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
    env) cmd_env "$@" ;;
    *)
      cat >&2 <<USAGE
Usage: $(basename "$0") <up|down|reset|consumers|env|cron-run> [options]
  up [--version V]     bootstrap a fresh Magento sandbox (default version $DEFAULT_VERSION)
  down                 stop containers, keep volumes
  reset [--version V]  down -v, then up again with a fresh database
  consumers            start 4 async.operations.all consumer processes
  cron-run             run bin/magento cron:run twice
  env                  print MAGENTO_BASE_URL / MAGENTO_ADMIN_USERNAME / MAGENTO_ADMIN_PASSWORD / MAGENTO_STORE_VIEW
USAGE
      exit 1
      ;;
  esac
}

main "$@"
