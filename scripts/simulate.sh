#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
COMPOSE_BIN=${COMPOSE_BIN:-podman compose}

cd "$ROOT_DIR"

echo "[simulate] running simulator container via $COMPOSE_BIN" >&2
$COMPOSE_BIN run --rm simulator "$@"
