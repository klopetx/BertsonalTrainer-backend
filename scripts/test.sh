#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
export PYTHONPATH="$ROOT_DIR/services/simulator:${PYTHONPATH:-}"

cd "$ROOT_DIR"

python -m pytest "$@"
