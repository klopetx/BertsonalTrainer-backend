#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
COMPOSE_BIN=${COMPOSE_BIN:-podman compose}

cd "$ROOT_DIR"

if [ "$#" -gt 0 ];
then
  SERVICES=("$@")
else
  SERVICES=(kafka kafka-init)
fi

echo "[infra-up] starting services: ${SERVICES[*]}" >&2
$COMPOSE_BIN up -d "${SERVICES[@]}"
