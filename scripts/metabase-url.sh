#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT_DIR"

if command -v podman >/dev/null 2>&1; then
  PODMAN="podman"
elif command -v podman.exe >/dev/null 2>&1; then
  PODMAN="podman.exe"
else
  echo "[metabase-url] podman is not available in PATH" >&2
  exit 1
fi

CANDIDATES=$("$PODMAN" machine ssh -- grep -B1 /32 /proc/net/fib_trie 2>/dev/null \
  | grep -oE '([0-9]+\.){3}[0-9]+' \
  | sort -u \
  | grep -vE '^127\.|\.0$|\.255$|^10\.89\.' || true)

MACHINE_IP=$(printf '%s\n' "$CANDIDATES" | head -n 1)

if [ -z "$MACHINE_IP" ]; then
  echo "[metabase-url] could not detect the podman machine IP" >&2
  exit 1
fi

echo "http://${MACHINE_IP}:3000"
