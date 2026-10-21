#!/usr/bin/env bash
# Nightly backup of the backlog-server data directory.
#
# Asks the server for a hard-link snapshot of a consistent checkpoint,
# copies it to $BACKLOG_BACKUP_TARGET/<YYYY-MM-DD>/ with rsync
# (hard-linking unchanged files against the previous backup), deletes
# the local snapshot and prunes backups older than the keep window.
#
# Settings (from the environment or the .env file next to the
# docker-compose files; see docs/operator/ for the runbook):
#   BACKLOG_PASSWORD          RESP password of the server
#   BACKLOG_PORT              RESP port (default 6390)
#   BACKLOG_QUEUE             queue to back up (default youtube:video)
#   BACKLOG_DATA_DIR          data dir as the server sees it (default /data)
#   BACKLOG_DATA_DIR_HOST     the same directory on this host
#   BACKLOG_BACKUP_TARGET     rsync destination, local path or host:path
#   BACKLOG_BACKUP_KEEP_DAYS  days of backups to keep (default 7)
#   REDIS_CLI                 command to run redis-cli (default
#                             "docker exec backlog-server redis-cli")
#
# Restore: copy one backup directory to an empty
# <data dir>/<queue dir>/ and start the server; it recovers from the
# checkpoint like after a crash.
set -euo pipefail

ENV_FILE="${ENV_FILE:-$(dirname "$0")/../.env}"
if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
fi

: "${BACKLOG_PASSWORD:?BACKLOG_PASSWORD is not set}"
: "${BACKLOG_DATA_DIR_HOST:?BACKLOG_DATA_DIR_HOST is not set}"
: "${BACKLOG_BACKUP_TARGET:?BACKLOG_BACKUP_TARGET is not set}"
PORT="${BACKLOG_PORT:-6390}"
QUEUE="${BACKLOG_QUEUE:-youtube:video}"
SERVER_DATA_DIR="${BACKLOG_DATA_DIR:-/data}"
KEEP_DAYS="${BACKLOG_BACKUP_KEEP_DAYS:-7}"
read -r -a CLI <<< "${REDIS_CLI:-docker exec backlog-server redis-cli}"

log() {
    printf '%s %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

snapshot="$(REDISCLI_AUTH="$BACKLOG_PASSWORD" "${CLI[@]}" \
    -p "$PORT" --no-raw BACKLOG.SNAPSHOT "$QUEUE" | tr -d '"')"
case "$snapshot" in
    "$SERVER_DATA_DIR"/snapshots/*) ;;
    *)
        log "snapshot failed: $snapshot"
        exit 1
        ;;
esac
local_snapshot="$BACKLOG_DATA_DIR_HOST/${snapshot#"$SERVER_DATA_DIR"/}"
trap 'rm -rf -- "$local_snapshot"' EXIT
log "snapshot $local_snapshot"

today="$(date -u +%Y-%m-%d)"
cutoff="$(date -u -d "-$KEEP_DAYS days" +%Y-%m-%d)"

if [[ "$BACKLOG_BACKUP_TARGET" == *:* ]]; then
    host="${BACKLOG_BACKUP_TARGET%%:*}"
    dir="${BACKLOG_BACKUP_TARGET#*:}"
    # Arguments are built locally on purpose.
    # shellcheck disable=SC2029
    run_target() { ssh "$host" "$@"; }
else
    dir="$BACKLOG_BACKUP_TARGET"
    run_target() { bash -c "$*"; }
fi

run_target "mkdir -p '$dir'"
previous="$(run_target "ls -1 '$dir'" | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' \
    | grep -v "^$today\$" | sort | tail -n 1 || true)"
link_dest=()
if [[ -n "$previous" ]]; then
    link_dest=(--link-dest="../$previous")
fi

rsync -a --delete "${link_dest[@]}" "$local_snapshot/" \
    "$BACKLOG_BACKUP_TARGET/$today/"
log "copied to $BACKLOG_BACKUP_TARGET/$today"

for old in $(run_target "ls -1 '$dir'" \
    | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' | sort); do
    if [[ "$old" < "$cutoff" ]]; then
        run_target "rm -rf -- '$dir/$old'"
        log "pruned $old"
    fi
done
