#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if command -v python3.11 >/dev/null 2>&1; then
  PY_BIN="python3.11"
else
  PY_BIN="python3"
fi

echo "[1/6] Python: $($PY_BIN --version)"

if [ ! -d "venv" ]; then
  echo "[2/6] Creating virtual environment (venv)..."
  "$PY_BIN" -m venv venv
else
  echo "[2/6] Reusing existing virtual environment (venv)."
fi

echo "[3/6] Activating venv..."
# shellcheck disable=SC1091
source venv/bin/activate

echo "[4/6] Upgrading pip..."
python -m pip install --upgrade pip

echo "[5/6] Installing runtime dependencies..."
pip install -r requirements.txt
pip install fastapi uvicorn jinja2 aiohttp cryptography

echo "[6/6] Starting FastAPI (reload mode)..."
echo "Open in browser: http://127.0.0.1:8000"
echo "Stop server: Ctrl+C"

exec uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
