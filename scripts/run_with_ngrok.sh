#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NGROK_BIN="$ROOT_DIR/.tools/ngrok/ngrok"

if [[ ! -x "$NGROK_BIN" ]]; then
  echo "ngrok not found at $NGROK_BIN"
  echo "Run: ./scripts/install_ngrok.sh"
  exit 1
fi

cd "$ROOT_DIR"

echo "Starting FastAPI on http://127.0.0.1:8000"
uvicorn app:app --reload --port 8000 &
API_PID=$!

cleanup() {
  echo "Stopping processes..."
  kill "$API_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

sleep 2

echo "Starting ngrok tunnel on port 8000"
"$NGROK_BIN" http 8000
