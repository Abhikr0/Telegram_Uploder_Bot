#!/data/data/com.termux/files/usr/bin/bash
set -euo pipefail

echo "============================================================"
echo "  Telegram Uploader Bot - Termux (Android) Setup"
echo "============================================================"

if [ -z "${TERMUX_VERSION:-}" ] && [[ "${PREFIX:-}" != *"com.termux"* ]]; then
    echo "[!] Warning: This script is intended for Termux on Android."
fi

case "$PWD" in
    /sdcard*|/storage/emulated/*)
        echo "[!] Warning: Running inside shared storage ($PWD)."
        echo "    Android FUSE storage is slow and can lock Telethon's SQLite session."
        echo "    Recommended: move/clone this repo into \$HOME (e.g. ~/Telegram_Uploder_Bot)."
        ;;
esac

echo "[1/4] Updating Termux packages and installing Python + FFmpeg..."
pkg update -y
pkg install -y python ffmpeg git termux-api

echo "[2/4] Creating Python virtual environment (.venv)..."
if [ ! -d ".venv" ]; then
    python -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate

echo "[3/4] Installing pure-Python dependencies (no pydantic / Rust)..."
pip install --upgrade pip
pip install httpx telethon python-dotenv beautifulsoup4 tqdm hachoir requests
pip install --no-deps --upgrade yt-dlp

echo "[4/4] Checking .env configuration..."
if [ ! -f ".env" ]; then
    cp .env.termux.example .env
    echo "    Created .env from .env.termux.example. Please edit .env with your credentials."
else
    echo "    Existing .env found; leaving it untouched."
fi

echo ""
echo "Setup complete! Next steps:"
echo "  1. Edit your credentials:  nano .env"
echo "  2. Acquire wake-lock:      termux-wake-lock"
echo "  3. Start the bot:          source .venv/bin/activate && python bot.py"
echo "============================================================"
