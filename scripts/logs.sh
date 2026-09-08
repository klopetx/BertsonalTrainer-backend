#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 <service-name>" >&2
  exit 1
fi

SERVICE="$1"
ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

if [ -z "${COMPOSE_BIN:-}" ]; then
  if command -v podman >/dev/null 2>&1; then
    COMPOSE_BIN="podman compose"
  elif command -v podman.exe >/dev/null 2>&1; then
    COMPOSE_BIN="podman.exe compose"
  else
    echo "[logs] podman is not available in PATH" >&2
    exit 1
  fi
fi

cd "$ROOT_DIR"

echo "[logs] tailing $SERVICE" >&2
$COMPOSE_BIN logs -f "$SERVICE"
