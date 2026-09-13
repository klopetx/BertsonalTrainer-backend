#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
export PYTHONPATH="$ROOT_DIR/services/simulator:$ROOT_DIR/services/spark:${PYTHONPATH:-}"

cd "$ROOT_DIR"

if command -v py >/dev/null 2>&1; then
  PY_CMD="py -3.11"
elif [ -x /c/Windows/py.exe ]; then
  PY_CMD="/c/Windows/py.exe -3.11"
elif command -v python >/dev/null 2>&1; then
  PY_CMD=python
elif command -v python3 >/dev/null 2>&1; then
  PY_CMD=python3
else
  echo "[test.sh] python executable not found" >&2
  exit 1
fi

${PY_CMD} -m pytest "$@"
