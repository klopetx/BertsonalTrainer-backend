#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)

if [ -z "${COMPOSE_BIN:-}" ]; then
  if command -v podman >/dev/null 2>&1; then
    COMPOSE_BIN="podman compose"
  elif command -v podman.exe >/dev/null 2>&1; then
    COMPOSE_BIN="podman.exe compose"
  else
    echo "[infra-up] podman is not available in PATH" >&2
    exit 1
  fi
fi

cd "$ROOT_DIR"

if [ "$#" -gt 0 ];
then
  SERVICES=("$@")
else
  SERVICES=(kafka kafka-init minio minio-init postgres)
fi

echo "[infra-up] starting services: ${SERVICES[*]}" >&2
$COMPOSE_BIN up -d "${SERVICES[@]}"
