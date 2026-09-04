#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

if [ -z "${COMPOSE_BIN:-}" ]; then
  if command -v podman >/dev/null 2>&1; then
    COMPOSE_BIN="podman compose"
  elif command -v podman.exe >/dev/null 2>&1; then
    COMPOSE_BIN="podman.exe compose"
  else
    echo "[down] podman is not available in PATH" >&2
    exit 1
  fi
fi

cd "$ROOT_DIR"

echo "[down] stopping services" >&2
$COMPOSE_BIN down "$@"
