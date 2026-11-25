# Running Telegram Uploader Bot on Termux (Android)

This codebase has been adapted to run natively on **Termux (Android)** without `pydantic`, `pydantic-core`, `cryptography`, `Rust`, or desktop `imageio-ffmpeg` binaries:

- **Supabase indexing** uses a lightweight `httpx` PostgREST client in `bot.py` (`SupabaseRestClient`) instead of the heavy `supabase` SDK.
- **FFmpeg / FFprobe** uses Termux's native `ffmpeg` package (`pkg install ffmpeg`).
- **Low-RAM Termux defaults** are applied automatically when `TERMUX_VERSION` or `com.termux` is detected (`ENABLE_STREAM_UPLOAD=false`, `STREAM_CONCURRENCY=1`, `DOWNLOAD_CONCURRENCY=2`, `MTPROTO_CONNECTIONS=2`, `STREAM_PROBE_BYTES=6291456`).

## Quick Setup in Termux

> Keep the repository inside Termux's internal `$HOME` directory (e.g., `~/Telegram_Uploder_Bot`), **not** `/sdcard`, so Telethon's SQLite session (`uploader_bot_session.session`) does not hit Android FUSE locking errors.

```bash
cd ~
# Clone or copy the repo into ~/Telegram_Uploder_Bot, then:
cd ~/Telegram_Uploder_Bot
chmod +x termux_setup.sh
./termux_setup.sh
```

Or install manually:

```bash
pkg update -y && pkg install -y python ffmpeg git termux-api
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements-termux.txt
cp .env.termux.example .env
nano .env
```

## Running Safely on Android

1. Disable Android battery optimization for **Termux** in Android Settings.
2. Acquire a wake-lock before starting the bot so uploads aren't suspended when the screen turns off:
   ```bash
   termux-wake-lock
   source .venv/bin/activate
   python bot.py
   ```
3. To release the wake-lock later:
   ```bash
   termux-wake-unlock
   ```

## Optional: Faster Telethon Uploads (`cryptg`)

Telethon works out of the box in pure Python (`pyaes`), which is already fast enough for normal usage. If you want native C/Rust encryption speed for large 1–2 GB uploads and your device has enough RAM to build Rust crates:

```bash
pkg install -y rust binutils clang
pip install cryptg
```
