#!/bin/bash
# Daily cron entry point: stop discovery, rotate, restart, then import.
set -euo pipefail

BASEDIR="${HOME}/src/scrape-python"
FILEPATH="${BASEDIR}/data/searched_channels.jsonl"
SERVICE="yt-discover-search"
ARCHIVE=""

# Prevent overlapping cron/manual runs from rotating an active import.
exec 9>"${BASEDIR}/data/.yt-discover-search-rotate.lock"
flock -n 9 || exit 0
exec >>/tmp/cron-discover.log 2>&1
echo "Discovery rotation started: $(date -Is)"

cd "${BASEDIR}"
test -d data
docker compose config --quiet
docker compose stop "${SERVICE}"

# Restart discovery even if rotation fails after stopping the service.
trap 'docker compose up -d --no-deps "${SERVICE}"' EXIT
if [[ -f "${FILEPATH}" ]]; then
    ARCHIVE=$(mktemp "${FILEPATH}-$(date -I).XXXXXX")
    mv -- "${FILEPATH}" "${ARCHIVE}"
    echo "Rotated output to ${ARCHIVE}"
else
    echo "No existing output file ${FILEPATH}"
fi

docker compose up -d --no-deps "${SERVICE}"
trap - EXIT

if [[ -n "${ARCHIVE}" ]]; then
    echo "Importing ${ARCHIVE}"
    "${HOME}/.local/bin/uv" run --no-sync python \
        tools/yt_channel_queue.py import "${ARCHIVE}"
fi
