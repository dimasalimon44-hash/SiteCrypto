#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TOOLS_DIR="$ROOT_DIR/.tools"
NGROK_DIR="$TOOLS_DIR/ngrok"
BIN_PATH="$NGROK_DIR/ngrok"

mkdir -p "$NGROK_DIR"

if [[ -x "$BIN_PATH" ]]; then
  echo "ngrok already installed: $BIN_PATH"
  "$BIN_PATH" version || true
  exit 0
fi

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$OS" in
  linux) os_slug="linux" ;;
  darwin) os_slug="darwin" ;;
  *)
    echo "Unsupported OS: $OS"
    echo "Install ngrok manually from https://ngrok.com/download and put binary to $BIN_PATH"
    exit 1
    ;;
esac

case "$ARCH" in
  x86_64|amd64) arch_slug="amd64" ;;
  aarch64|arm64) arch_slug="arm64" ;;
  *)
    echo "Unsupported arch: $ARCH"
    echo "Install ngrok manually from https://ngrok.com/download and put binary to $BIN_PATH"
    exit 1
    ;;
esac

ZIP_URL="https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-${os_slug}-${arch_slug}.zip"
TMP_ZIP="$NGROK_DIR/ngrok.zip"

echo "Downloading ngrok from: $ZIP_URL"
curl -fsSL "$ZIP_URL" -o "$TMP_ZIP"
unzip -o "$TMP_ZIP" -d "$NGROK_DIR" >/dev/null
rm -f "$TMP_ZIP"
chmod +x "$BIN_PATH"

echo "✅ ngrok installed at: $BIN_PATH"
"$BIN_PATH" version || true

echo
echo "Next steps:"
echo "1) Configure auth token once: $BIN_PATH config add-authtoken <YOUR_TOKEN>"
echo "2) Start API: uvicorn app:app --reload --port 8000"
echo "3) Start tunnel: $BIN_PATH http 8000"
