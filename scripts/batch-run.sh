#!/usr/bin/env bash
set -euo pipefail

if [ $# -eq 0 ]; then
  echo "Usage: $0 --business-date YYYY-MM-DD [--force]" >&2
  exit 1
fi

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

if [ -z "${COMPOSE_BIN:-}" ]; then
  if command -v podman >/dev/null 2>&1; then
    COMPOSE_BIN="podman compose"
  elif command -v podman.exe >/dev/null 2>&1; then
    COMPOSE_BIN="podman.exe compose"
  else
    echo "[batch-run] podman is not available in PATH" >&2
    exit 1
  fi
fi

"$ROOT_DIR"/scripts/infra-up.sh >/dev/null

cd "$ROOT_DIR"

echo "[batch-run] running spark-batch with args: $*" >&2
$COMPOSE_BIN run --rm spark-batch "$@"
