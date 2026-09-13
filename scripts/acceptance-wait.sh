#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "Usage: $0 <business-date YYYY-MM-DD> <expected-count> [timeout-seconds] [poll-seconds]" >&2
  exit 1
fi

BUSINESS_DATE="$1"
EXPECTED="$2"
TIMEOUT_SECONDS="${3:-600}"
POLL_SECONDS="${4:-5}"

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

DB_USER=$(grep -m1 '^POSTGRES_USER=' .env 2>/dev/null | cut -d= -f2 | tr -d '\r')
DB_USER=${DB_USER:-bertsonal}

if [ -z "${COMPOSE_BIN:-}" ]; then
  if command -v podman >/dev/null 2>&1; then
    COMPOSE_BIN="podman compose"
  elif command -v podman.exe >/dev/null 2>&1; then
    COMPOSE_BIN="podman.exe compose"
  else
    echo "[acceptance-wait] podman is not available in PATH" >&2
    exit 1
  fi
fi

start=$(date +%s)
while true; do
  now=$(date +%s)
  elapsed=$((now - start))
  if [ "$elapsed" -ge "$TIMEOUT_SECONDS" ]; then
    echo "[acceptance-wait] timeout after ${TIMEOUT_SECONDS}s waiting for provisional_scores count=${EXPECTED} for ${BUSINESS_DATE}" >&2
    exit 2
  fi

  count=$($COMPOSE_BIN exec -T postgres psql -U "$DB_USER" -d bertsonal -t -A -c "SELECT COUNT(*) FROM serving.provisional_scores WHERE business_date = DATE '${BUSINESS_DATE}';" | tr -d '\r')
  count=${count:-0}
  echo "[acceptance-wait] provisional_scores for ${BUSINESS_DATE}: ${count}/${EXPECTED}" >&2

  if [ "$count" -ge "$EXPECTED" ]; then
    break
  fi
  sleep "$POLL_SECONDS"
done

echo "[acceptance-wait] reached ${count}/${EXPECTED} for ${BUSINESS_DATE}" >&2
