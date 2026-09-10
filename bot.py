import os
import sys
import asyncio
import logging
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient, events, Button, functions, types, errors
from telethon.tl.types import DocumentAttributeVideo, BotCommand, BotCommandScopeDefault
from tqdm import tqdm
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser

# Fix Windows console UTF-8 emoji printing
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Import fasttelethon from local file
from fasttelethon import upload_file

# Import AI caption & metadata generator
from ai_caption import generate_ai_caption

# Import scraper functions
from scraper import (
    parse_media_url,
    parse_profile_url, 
    parse_page_range, 
    fetch_all_posts, 
    extract_video_urls, 
    download_video
)

# ── Configuration ────────────────────────────────────────────────────────────

load_dotenv()

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID", 0))
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "bot_downloads")
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# Initialize optional Supabase client for auto-indexing
supabase_client = None
if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client
        supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logging.info("Connected to Supabase for automatic upload indexing.")
    except Exception as e:
        logging.warning(f"Could not connect to Supabase: {e}")

# Ensure download directory exists
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

MAX_UPLOAD_RETRIES = 3
RETRY_DELAY = 5

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize Telethon Client
client = TelegramClient("uploader_bot_session", API_ID, API_HASH)

# State management for interactive flow
USER_STATES = {}
RUNNING_TASKS = {}
LAST_TASK_INFO = {}

class BarPositionManager:
    def __init__(self, max_bars=6):
        self.max_bars = max_bars
        self.occupied = [False] * max_bars
        self.lock = asyncio.Lock()

    async def get_pos(self):
        async with self.lock:
            for i in range(self.max_bars):
                if not self.occupied[i]:
                    self.occupied[i] = True
                    return i + 1  # Offset by 1 for overall progress
            return 0

    async def release_pos(self, pos):
        if pos <= 0: return
        async with self.lock:
            if pos - 1 < len(self.occupied):
                self.occupied[pos - 1] = False

BAR_MANAGER = BarPositionManager()

# ── Helpers ──────────────────────────────────────────────────────────────────

def is_admin(user_id):
    return not ADMIN_IDS or user_id in ADMIN_IDS

async def check_admin_or_notify(event):
    if is_admin(event.sender_id):
        return True
    await event.respond(
        f"⛔ <b>Access Restricted</b>\n\n"
        f"Your Telegram User ID is: <code>{event.sender_id}</code>\n"
        f"To enable access, add this ID to <code>ADMIN_IDS</code> in <code>.env</code> or ask the bot administrator.",
        parse_mode='html'
    )
    return False

def format_bytes(size_bytes: int) -> str:
    if not size_bytes:
        return "0 MB"
    if size_bytes >= 1024 * 1024 * 1024:
        return f"{size_bytes / (1024**3):.2f} GB"
    return f"{size_bytes / (1024**2):.1f} MB"

def get_progress_bar(current, total):
    if not total or total <= 0:
        return f"<code>[{'🟦' * 5}{'⬜' * 5}] {format_bytes(current)}</code>"
    current = min(current, total)
    percentage = current / total
    done = int(percentage * 10)
    remain = 10 - done
    return f"<code>[{'🟦' * done}{'⬜' * remain}] {percentage:.1%} ({format_bytes(current)} / {format_bytes(total)})</code>"

def get_video_metadata(filepath):
    """Extract duration, width, and height from video file."""
    metadata = {'duration': 0, 'width': 0, 'height': 0}
    try:
        parser = createParser(str(filepath))
        if not parser:
            return metadata
        with parser:
            data = extractMetadata(parser)
            if data:
                if data.has('duration'):
                    metadata['duration'] = int(data.get('duration').seconds)
                if data.has('width'):
                    metadata['width'] = int(data.get('width'))
                if data.has('height'):
                    metadata['height'] = int(data.get('height'))
    except Exception as e:
        logger.warning(f"Metadata extraction failed for {filepath}: {e}")
    return metadata

async def generate_thumbnail(filepath, thumb_path):
    """Generate a thumbnail for the video using ffmpeg."""
    try:
        ffmpeg_cmd = "ffmpeg"
        try:
            import imageio_ffmpeg
            ffmpeg_cmd = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            pass
        process = await asyncio.create_subprocess_exec(
            ffmpeg_cmd, '-y', '-i', str(filepath),
            '-ss', '00:00:01.000', '-vframes', '1',
            str(thumb_path),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await process.communicate()
        return thumb_path.exists()
    except Exception as e:
        logger.warning(f"Thumbnail generation failed for {filepath}: {e}")
        return False

def render_album_browser(user_id: int):
    """Render the paginated album files selector with checkbox toggles and rich field previews."""
    info = USER_STATES.get(user_id, {})
    url = info.get('url', '')
    videos = info.get('videos', [])
    selected = info.get('selected', set())
    page = info.get('browser_page', 0)
    page_size = 5

    total_items = len(videos)
    total_pages = max(1, (total_items + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    info['browser_page'] = page

    start_idx = page * page_size
    end_idx = min(total_items, start_idx + page_size)

    file_lines = []
    for i in range(start_idx, end_idx):
        vid = videos[i]
        name = vid.get("name") or vid.get("title") or f"File_{i+1}"
        service = vid.get("service") or "media"
        checked = "✅" if i in selected else "◻️"
        file_lines.append(f"{checked} <b>#{i+1}</b> <code>{name[:38]}</code> ({service})")

    file_preview = "\n".join(file_lines) if file_lines else "<i>No files on this page.</i>"

    text = (
        f"📁 <b>Album File Browser (Page {page + 1}/{total_pages})</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"• <b>Source:</b> <code>{url[:55]}...</code>\n"
        f"• <b>Total Files:</b> {total_items} items\n"
        f"• <b>Selected:</b> <b>{len(selected)}</b> / {total_items} items\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{file_preview}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Tap an item below to toggle selection:</i>"
    )

    buttons = []
    # Video rows with checkboxes
    for i in range(start_idx, end_idx):
        vid = videos[i]
        name = vid.get("name") or vid.get("title") or f"File_{i+1}"
        display_name = name[:26]
        checked = "✅" if i in selected else "◻️"
        btn_text = f"{checked} #{i+1} {display_name}"
        buttons.append([Button.inline(btn_text, f"sel_tog:{i}".encode())])

    # Navigation row
    nav_row = []
    if page > 0:
        nav_row.append(Button.inline("◀️ Prev", f"sel_pg:{page - 1}".encode()))
    nav_row.append(Button.inline(f"📄 {page + 1}/{total_pages}", b"noop"))
    if page < total_pages - 1:
        nav_row.append(Button.inline("Next ▶️", f"sel_pg:{page + 1}".encode()))
    buttons.append(nav_row)

    # Page controls & Select All
    buttons.append([
        Button.inline(f"✅ Select All ({total_items})", b"sel_all_items"),
        Button.inline("🔘 Select Page", f"sel_all_pg:{page}".encode()),
        Button.inline("🔄 Clear", b"sel_clear")
    ])

    # Download actions
    buttons.append([
        Button.inline(f"📥 Download Selected ({len(selected)})", b"sel_confirm")
    ])
    buttons.append([
        Button.inline(f"🚀 Download All ({total_items})", b"sel_all_download"),
        Button.inline("🏠 Menu", b"cancel_flow")
    ])

    return text, buttons


async def download_and_upload(event, url, page_range_str, target_channel_id=None, selected_videos=None):
    user_id = event.sender_id
    LAST_TASK_INFO[user_id] = {
        'url': url,
        'range': page_range_str,
        'dest': target_channel_id,
        'selected_videos': selected_videos
    }
    RUNNING_TASKS[user_id] = asyncio.current_task()
    
    if target_channel_id is None:
        target_channel_id = STORAGE_CHANNEL_ID

    status_msg = None
    temp_dir = None
    status_task = None
    try:
        domain, service, cur_user_id, post_id = parse_media_url(url)
        if not domain: 
            await event.respond("❌ <b>Invalid URL.</b>\nSupported formats: Coomer creator/post URLs, Bunkr album/video URLs.", parse_mode='html')
            return
            
        page_range = parse_page_range(page_range_str) if page_range_str else None
        
        temp_dir = Path(DOWNLOAD_DIR) / f"{service}_{cur_user_id}_{event.id}"
        temp_dir.mkdir(parents=True, exist_ok=True)

        if selected_videos:
            videos = selected_videos
            status_msg = await event.respond(f"📦 <b>Starting download of {len(videos)} selected files...</b>", parse_mode='html')
        else:
            status_msg = await event.respond(f"🔍 <b>Scraping:</b> {service}/{cur_user_id}...", parse_mode='html')
            
            posts = await fetch_all_posts(domain, service, cur_user_id, page_range, post_id=post_id)
            if not posts:
                await status_msg.edit("❌ No posts found.")
                return
                
            videos = extract_video_urls(domain, posts)
            if not videos:
                await status_msg.edit("⚠ No videos found in posts.")
                return
                
            await status_msg.edit(f"📦 Found {len(videos)} videos. Starting parallel optimized download & upload...")
        
        # Shared progress state
        state = {
            "total": len(videos),
            "downloaded": 0,
            "uploaded": 0,
            "skipped": 0,
            "failed": 0,
            "active_downloads": {},  # video_name: progress_str
            "active_uploads": {},    # video_name: progress_str
            "running": True
        }

        cancel_button = [Button.inline("🛑 Cancel Task", b"cancel_task")]

        # Background status updater (throttled to avoid FloodWait)
        async def status_updater():
            last_text = ""
            while state["running"]:
                processed = state["uploaded"] + state["skipped"] + state["failed"]
                text = (
                    f"📊 <b>Pipeline Progress:</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📥 <b>Downloaded:</b> {state['downloaded']}/{state['total']} videos\n"
                    f"📤 <b>Uploaded:</b> {state['uploaded']}/{state['total']} videos\n"
                )
                if state["skipped"] > 0:
                    text += f"⏩ <b>Skipped:</b> {state['skipped']}\n"
                if state["failed"] > 0:
                    text += f"❌ <b>Failed:</b> {state['failed']}\n"
                text += "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

                active_dl = list(state["active_downloads"].items())
                if active_dl:
                    text += "<b>📥 Active Downloads:</b>\n"
                    for v_name, pbar in active_dl[:3]:
                        text += f"• <code>{v_name[:24]}...</code>\n  {pbar}\n"
                    text += "\n"

                active_up = list(state["active_uploads"].items())
                if active_up:
                    text += "<b>🚀 Active Uploads:</b>\n"
                    for v_name, pbar in active_up[:3]:
                        text += f"• <code>{v_name[:24]}...</code>\n  {pbar}\n"

                if text != last_text:
                    try:
                        await status_msg.edit(text, parse_mode='html', buttons=cancel_button)
                        last_text = text
                    except errors.FloodWaitError as fwe:
                        await asyncio.sleep(fwe.seconds)
                    except Exception:
                        pass
                await asyncio.sleep(3.0)

        status_task = asyncio.create_task(status_updater())

        # Bounded pipeline queue (at most 3 files on disk to prevent disk exhaustion)
        upload_queue = asyncio.Queue(maxsize=3)

        # Download worker function
        async def download_worker(vid):
            pos = await BAR_MANAGER.get_pos()
            vid_name = vid.get('name') or vid.get('title') or "video"
            state["active_downloads"][vid_name] = "<code>[Connecting...] ⏳</code>"

            last_dl_pbar_time = [0.0]
            async def dl_progress_callback(current, total):
                if time.time() - last_dl_pbar_time[0] >= 2.0:
                    state["active_downloads"][vid_name] = get_progress_bar(current, total)
                    last_dl_pbar_time[0] = time.time()

            try:
                download_res = await download_video(
                    domain, vid, temp_dir, pos=pos, progress_callback=dl_progress_callback
                )
                if download_res in ["skipped_size", "skipped_small", "error_html"]:
                    state["skipped"] += 1
                    return
                filepath = temp_dir / f"{vid['id']}_{vid['name']}"
                if download_res and filepath.exists():
                    state["downloaded"] += 1
                    await upload_queue.put((vid, filepath))
                else:
                    state["failed"] += 1
            finally:
                state["active_downloads"].pop(vid_name, None)
                await BAR_MANAGER.release_pos(pos)

        # Upload consumer worker
        async def upload_worker():
            while state["running"]:
                try:
                    item = await upload_queue.get()
                except asyncio.CancelledError:
                    break
                if item is None:
                    upload_queue.task_done()
                    break

                vid, filepath = item
                pos = await BAR_MANAGER.get_pos()
                try:
                    file_size = filepath.stat().st_size
                    state["active_uploads"][vid['name']] = "<code>[Processing Metadata...] ⚙️</code>"

                    last_pbar_time = [0.0]
                    async def progress_callback(current, total):
                        if time.time() - last_pbar_time[0] > 3:
                            state["active_uploads"][vid['name']] = get_progress_bar(current, total)
                            last_pbar_time[0] = time.time()

                    metadata = get_video_metadata(filepath)
                    thumb_path = filepath.with_suffix('.jpg')
                    has_thumb = await generate_thumbnail(filepath, thumb_path)
                    attributes = [DocumentAttributeVideo(duration=metadata['duration'], w=metadata['width'], h=metadata['height'], supports_streaming=True)]

                    # Prepare rich caption & metadata (via Mistral AI)
                    ai_meta = await generate_ai_caption(vid, service, cur_user_id)
                    rich_caption = ai_meta["rich_caption"]
                    db_title = ai_meta["db_title"]

                    sent_msg = None
                    for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
                        try:
                            with tqdm(total=file_size, unit="B", unit_scale=True, desc=f"   [UP] {vid['name'][:20]}", position=pos, leave=False) as t_pbar:
                                async def upload_progress(current, total):
                                    await progress_callback(current, total)
                                    t_pbar.n = current
                                    t_pbar.refresh()
                                with open(filepath, "rb") as f:
                                    uploaded_file = await upload_file(client, f, progress_callback=upload_progress)
                                sent_msg = await client.send_file(
                                    target_channel_id,
                                    uploaded_file,
                                    caption=rich_caption,
                                    parse_mode='html',
                                    supports_streaming=True,
                                    attributes=attributes,
                                    thumb=str(thumb_path) if has_thumb else None,
                                    video=True
                                )
                            break
                        except Exception as e:
                            if attempt == MAX_UPLOAD_RETRIES:
                                raise e
                            logger.warning(f"Upload attempt {attempt} failed for {vid['name']}: {e}. Retrying in {RETRY_DELAY}s...")
                            await asyncio.sleep(RETRY_DELAY)

                    # Automated indexing into Supabase if configured
                    if sent_msg and supabase_client:
                        try:
                            db_entry = {
                                "id": str(uuid.uuid4()),
                                "title": db_title[:150],
                                "file_id": "mtproto_uploaded",
                                "message_id": sent_msg.id,
                                "file_size": file_size,
                                "upload_date": datetime.now(timezone.utc).isoformat()
                            }
                            await asyncio.to_thread(supabase_client.table("media").insert(db_entry).execute)
                            logger.info(f"Indexed to Supabase: {db_title[:60]} (Msg ID: {sent_msg.id})")
                        except Exception as db_err:
                            logger.warning(f"Supabase auto-index error: {db_err}")

                    state["uploaded"] += 1
                except Exception as e:
                    logger.error(f"Error uploading {vid['name']}: {e}")
                    state["failed"] += 1
                finally:
                    state["active_uploads"].pop(vid['name'], None)
                    if has_thumb and thumb_path.exists():
                        try: os.remove(thumb_path)
                        except: pass
                    if filepath.exists():
                        try: os.remove(filepath)
                        except: pass
                    await BAR_MANAGER.release_pos(pos)
                    upload_queue.task_done()

        # Start dedicated sequential upload worker for maximum stability and speed
        upload_tasks = [asyncio.create_task(upload_worker()) for _ in range(1)]

        # Run downloads with concurrency 3
        dl_sem = asyncio.Semaphore(3)
        async def bounded_download(vid):
            async with dl_sem:
                await download_worker(vid)

        await asyncio.gather(*[bounded_download(v) for v in videos])

        # Signal upload workers to finish
        for _ in range(2):
            await upload_queue.put(None)
        await asyncio.gather(*upload_tasks)

        state["running"] = False
        if status_task:
            status_task.cancel()

        final_text = (
            f"🏁 <b>Task Completed!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📥 <b>Downloaded:</b> {state['downloaded']}/{len(videos)} videos\n"
            f"✅ <b>Uploaded:</b> {state['uploaded']}/{len(videos)} videos"
        )
        if state['skipped'] > 0:
            final_text += f"\n⏩ <b>Skipped:</b> {state['skipped']}"
        if state['failed'] > 0:
            final_text += f"\n❌ <b>Failed:</b> {state['failed']}"
        final_text += "\n━━━━━━━━━━━━━━━━━━━━━━━━"

        await status_msg.edit(final_text, parse_mode='html')

    except asyncio.CancelledError:
        logger.info(f"Task for {url} was cancelled.")
        if status_msg:
            try:
                await status_msg.edit(f"🛑 <b>Task Cancelled.</b>\n🔗 {url}", parse_mode='html')
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Global error in download_and_upload: {e}")
        try:
            await event.respond(f"❌ <b>Error:</b> {e}", parse_mode='html')
        except Exception:
            pass
    finally:
        RUNNING_TASKS.pop(user_id, None)
        if status_task and not status_task.done():
            status_task.cancel()
        if temp_dir and temp_dir.exists():
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

# ── Commands & Menu ──────────────────────────────────────────────────────────

@client.on(events.NewMessage(pattern=r"^/start$"))
async def start_handler(event):
    if not await check_admin_or_notify(event): return
    
    # Calculate quick stats
    active_count = len(RUNNING_TASKS)
    temp_size_mb = 0
    if os.path.exists(DOWNLOAD_DIR):
        try:
            total_b = sum(f.stat().st_size for f in Path(DOWNLOAD_DIR).glob('**/*') if f.is_file())
            temp_size_mb = round(total_b / (1024 * 1024), 1)
        except Exception:
            pass

    text = (
        "✨ <b>Ultimate Media Downloader & Archiver</b> ✨\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Welcome! I scrape, download, and archive media directly into Telegram storage with MTProto multi-chunk speeds.\n\n"
        "⚡ <b>Supported Sources:</b>\n"
        "• <b>Bunkr:</b> Albums (<code>/a/</code>) & Videos (<code>/v/</code>, <code>/f/</code>)\n"
        "• <b>Coomer / Kemono:</b> Creator profiles & single posts\n\n"
        "📊 <b>Current System Overview:</b>\n"
        f"• <b>Status:</b> 🟢 Online & MTProto Ready\n"
        f"• <b>Storage Channel:</b> <code>{STORAGE_CHANNEL_ID}</code>\n"
        f"• <b>Active Tasks:</b> <code>{active_count}</code>\n"
        f"• <b>Temp Cache:</b> <code>{temp_size_mb} MB</code>\n"
        f"• <b>Supabase Auto-Index:</b> {'🟢 Enabled' if supabase_client else '⚪ Disabled'}\n"
        f"• <b>Mistral AI Metadata:</b> ⚡ Active\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🚀 <b>Quick Start:</b>\n"
        "Simply <b>paste any URL directly into this chat</b>, or click <b>New Download</b> below."
    )
    
    buttons = [
        [Button.inline("🚀 New Download", b"new_download")],
    ]
    
    # Add Resume button if exists
    if event.sender_id in LAST_TASK_INFO:
        buttons.append([Button.inline("⏯ Resume Last Task", b"resume_last")])
    
    buttons.append([
        Button.inline("📊 Storage Stats", b"stats_info"),
        Button.inline("🧹 Clear Temp", b"clear_temp")
    ])
    buttons.append([Button.inline("❓ Help Guide", b"help")])
    
    # Safe storage channel button
    storage_link = f"https://t.me/c/{str(abs(STORAGE_CHANNEL_ID))[3:]}" if STORAGE_CHANNEL_ID and str(STORAGE_CHANNEL_ID).startswith("-100") else None
    if storage_link:
        buttons.append([Button.url("📂 View Storage Channel", storage_link)])
    
    await event.respond(text, parse_mode='html', buttons=buttons)

@client.on(events.CallbackQuery())
async def callback_handler(event):
    if not is_admin(event.sender_id):
        await event.answer("⛔ Unauthorized.", alert=True)
        return
    
    data = event.data.decode()
    user_id = event.sender_id
    
    if data == "new_download":
        if user_id in RUNNING_TASKS:
            await event.answer("⚠️ A task is already running! Cancel it first.", alert=True)
            return

        USER_STATES[user_id] = {'state': 'WAITING_URL'}
        await event.respond(
            "🔗 <b>Step 1: From Where?</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Please send the Coomer profile or Bunkr URL you want to scrape.\n\n"
            "💡 <i>Examples:</i>\n"
            "• <code>https://coomer.st/onlyfans/user/example</code>\n"
            "• <code>https://bunkr.cr/a/album_id</code>\n"
            "• <code>https://bunkr.cr/v/video_id</code>", 
            parse_mode='html',
            buttons=[Button.inline("❌ Cancel", b"cancel_flow")]
        )
        await event.answer()
        
    elif data == "cancel_flow":
        USER_STATES.pop(user_id, None)
        await event.edit("❌ <b>Process Cancelled.</b>", parse_mode='html')
        await event.answer("Cancelled", alert=False)
        
    elif data == "clear_temp":
        if os.path.exists(DOWNLOAD_DIR):
            shutil.rmtree(DOWNLOAD_DIR)
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            await event.answer("🧹 Temporary files cleared!", alert=True)
        else:
            await event.answer("Temp folder is already empty.")

    elif data == "stats_info":
        total_idx = "N/A"
        if supabase_client:
            try:
                res = await asyncio.to_thread(lambda: supabase_client.table("media").select("id", count="exact").execute())
                total_idx = str(res.count or 0)
            except Exception:
                pass
        stat_text = (
            "📊 <b>Uploader & Storage Overview</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Active Upload Tasks:</b> <code>{len(RUNNING_TASKS)}</code>\n"
            f"• <b>Storage Channel ID:</b> <code>{STORAGE_CHANNEL_ID}</code>\n"
            f"• <b>Indexed Supabase Videos:</b> <code>{total_idx}</code>\n"
            f"• <b>Temp Download Dir:</b> <code>{DOWNLOAD_DIR}</code>\n"
            f"• <b>Upload Pipeline:</b> Sequential MTProto Queue\n"
            f"• <b>AI Tagging:</b> Mistral AI\n"
            f"• <b>Service Health:</b> 🟢 100% Operational\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        await event.respond(stat_text, parse_mode='html', buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]])
        await event.answer()

    elif data == "main_menu":
        USER_STATES.pop(user_id, None)
        await start_handler(event)
        await event.answer()
            
    elif data == "help":
        help_text = (
            "📖 <b>Premium Help Guide</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "<b>Commands:</b>\n"
            "• /start - 🚀 Open main menu\n"
            "• /download [url] - 📥 Quick download\n"
            "• /bunkr [url] - ⚡ Quick Bunkr scraper\n"
            "• /help - ❓ Show this guide\n\n"
            "<b>Interactive Flow:</b>\n"
            "1. Click 'New Download' or paste link\n"
            "2. Select 'Browse & Select Files' to pick items\n"
            "3. Choose destination channel\n"
            "4. Monitor real-time upload progress bars!"
        )
        await event.respond(help_text, parse_mode='html')
        await event.answer()

    elif data == "cancel_task":
        if user_id in RUNNING_TASKS:
            RUNNING_TASKS[user_id].cancel()
            await event.answer("🛑 Cancelling task...", alert=False)
        else:
            await event.answer("ℹ️ No active task to cancel.")

    elif data == "resume_last":
        if user_id in RUNNING_TASKS:
            await event.answer("⚠️ A task is already running! Cancel it first.", alert=True)
            return
        
        info = LAST_TASK_INFO.get(user_id)
        if info:
            await event.answer("⏯ Resuming last task...")
            await event.respond(f"⏯ <b>Resuming Last Task...</b>\n🔗 Source: {info['url']}", parse_mode='html')
            asyncio.create_task(download_and_upload(event, info['url'], info['range'], info['dest'], selected_videos=info.get('selected_videos')))
        else:
            await event.answer("❌ No previous task found.")

    elif data == "sel_all_items":
        if user_id not in USER_STATES or 'videos' not in USER_STATES[user_id]:
            await event.answer("ℹ️ Session expired. Please resend the link.", alert=True)
            return
        videos = USER_STATES[user_id]['videos']
        USER_STATES[user_id]['selected'] = set(range(len(videos)))
        text, buttons = render_album_browser(user_id)
        await event.edit(text, parse_mode='html', buttons=buttons)
        await event.answer(f"✅ Selected all {len(videos)} files!", alert=False)


    elif data == "crawl_album":
        if user_id not in USER_STATES:
            await event.answer("ℹ️ Session expired. Please send the link again.", alert=True)
            return

        if user_id in RUNNING_TASKS:
            await event.answer("⚠️ A task is already running! Cancel it first.", alert=True)
            return

        url = USER_STATES[user_id].get('url', '')
        await event.edit("⏳ <b>Crawling Album Files...</b>\nScanning source and extracting file list, please wait...", parse_mode='html')
        
        try:
            domain, service, cur_id, post_id = parse_media_url(url)
            posts = await fetch_all_posts(domain, service, cur_id, page_range=None, post_id=post_id)
            if not posts:
                await event.edit("❌ <b>No posts or items found in this album.</b>", parse_mode='html', buttons=[[Button.inline("❌ Close", b"cancel_flow")]])
                return

            videos = extract_video_urls(domain, posts)
            if not videos:
                await event.edit("⚠️ <b>No downloadable videos found in this album.</b>", parse_mode='html', buttons=[[Button.inline("❌ Close", b"cancel_flow")]])
                return

            USER_STATES[user_id]['videos'] = videos
            USER_STATES[user_id]['selected'] = set()
            USER_STATES[user_id]['browser_page'] = 0

            text, buttons = render_album_browser(user_id)
            await event.edit(text, parse_mode='html', buttons=buttons)
            await event.answer()
        except Exception as e:
            logger.error(f"Error crawling album: {e}")
            await event.edit(f"❌ <b>Error crawling album:</b> {e}", parse_mode='html', buttons=[[Button.inline("❌ Close", b"cancel_flow")]])

    elif data.startswith("sel_tog:"):
        if user_id not in USER_STATES or 'videos' not in USER_STATES[user_id]:
            await event.answer("ℹ️ Session expired. Please resend the link.", alert=True)
            return
        idx = int(data.split(":")[1])
        selected = USER_STATES[user_id].setdefault('selected', set())
        if idx in selected:
            selected.remove(idx)
        else:
            selected.add(idx)
        text, buttons = render_album_browser(user_id)
        await event.edit(text, parse_mode='html', buttons=buttons)
        await event.answer()

    elif data.startswith("sel_pg:"):
        if user_id not in USER_STATES or 'videos' not in USER_STATES[user_id]:
            await event.answer("ℹ️ Session expired.", alert=True)
            return
        pg = int(data.split(":")[1])
        USER_STATES[user_id]['browser_page'] = pg
        text, buttons = render_album_browser(user_id)
        await event.edit(text, parse_mode='html', buttons=buttons)
        await event.answer()

    elif data.startswith("sel_all_pg:"):
        if user_id not in USER_STATES or 'videos' not in USER_STATES[user_id]:
            await event.answer("ℹ️ Session expired.", alert=True)
            return
        pg = int(data.split(":")[1])
        page_size = 5
        videos = USER_STATES[user_id]['videos']
        selected = USER_STATES[user_id].setdefault('selected', set())
        start = pg * page_size
        end = min(len(videos), start + page_size)
        for i in range(start, end):
            selected.add(i)
        text, buttons = render_album_browser(user_id)
        await event.edit(text, parse_mode='html', buttons=buttons)
        await event.answer(f"Selected {end - start} items.")

    elif data == "sel_clear":
        if user_id not in USER_STATES or 'videos' not in USER_STATES[user_id]:
            await event.answer("ℹ️ Session expired.", alert=True)
            return
        USER_STATES[user_id]['selected'] = set()
        text, buttons = render_album_browser(user_id)
        await event.edit(text, parse_mode='html', buttons=buttons)
        await event.answer("Selection cleared.")

    elif data == "sel_confirm":
        if user_id not in USER_STATES or 'videos' not in USER_STATES[user_id]:
            await event.answer("ℹ️ Session expired.", alert=True)
            return
        selected = USER_STATES[user_id].get('selected', set())
        if not selected:
            await event.answer("⚠️ Please select at least one file first.", alert=True)
            return
        videos = USER_STATES[user_id]['videos']
        chosen = [videos[i] for i in sorted(selected)]
        USER_STATES[user_id]['selected_videos'] = chosen
        USER_STATES[user_id]['range'] = None
        await ask_destination(event, user_id)
        await event.answer()

    elif data == "sel_all_download":
        if user_id not in USER_STATES or 'videos' not in USER_STATES[user_id]:
            await event.answer("ℹ️ Session expired.", alert=True)
            return
        USER_STATES[user_id]['selected_videos'] = USER_STATES[user_id]['videos']
        USER_STATES[user_id]['range'] = None
        await ask_destination(event, user_id)
        await event.answer()

    elif data == "noop":
        await event.answer()

    elif data == "quick_download_confirm":
        if user_id in USER_STATES:
            await start_confirmed_download(event, user_id)
        else:
            await event.answer("Session expired, please paste the link again.", alert=True)

    elif data in ("range_all", "range_custom", "dest_default", "dest_custom"):
        if user_id not in USER_STATES:
            await event.answer("ℹ️ Session expired. Start again.", alert=True)
            return

        if data == "range_all":
            USER_STATES[user_id]['range'] = None
            await ask_destination(event, user_id)
            await event.answer()
            
        elif data == "range_custom":
            USER_STATES[user_id]['state'] = 'WAITING_RANGE_INPUT'
            await event.respond("🔢 Please send the page range (e.g., <code>1-3</code> or <code>1</code>).", parse_mode='html', buttons=[Button.inline("❌ Cancel", b"cancel_flow")])
            await event.answer()
            
        elif data == "dest_default":
            USER_STATES[user_id]['dest'] = STORAGE_CHANNEL_ID
            await start_confirmed_download(event, user_id)
            await event.answer()
            
        elif data == "dest_custom":
            USER_STATES[user_id]['state'] = 'WAITING_DEST_INPUT'
            await event.respond("🆔 Please send the numeric Channel ID (e.g., <code>-100123456789</code>).", parse_mode='html', buttons=[Button.inline("❌ Cancel", b"cancel_flow")])
            await event.answer()

@client.on(events.NewMessage())
async def message_handler(event):
    text = event.text.strip()
    if text.startswith('/'): return # Ignore commands here
    if not await check_admin_or_notify(event): return

    user_id = event.sender_id
    
    if user_id in USER_STATES:
        state = USER_STATES[user_id].get('state')
        
        if state == 'WAITING_URL':
            if text.startswith('http'):
                USER_STATES[user_id]['url'] = text
                domain, service, cur_id, post_id = parse_media_url(text)
                if post_id:
                    # Single item detected, skip range prompt
                    USER_STATES[user_id]['range'] = None
                    await ask_destination(event, user_id)
                else:
                    USER_STATES[user_id]['state'] = 'WAITING_RANGE'
                    await event.respond(
                        "📑 <b>Step 2: Choose Download Mode</b>\n"
                        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        "Do you want to browse and pick files, download all, or specify a page range?", 
                        parse_mode='html', 
                        buttons=[
                            [Button.inline("📋 Browse & Select Files", b"crawl_album")],
                            [Button.inline("📄 All Pages / Videos", b"range_all")],
                            [Button.inline("🔢 Custom Range", b"range_custom")],
                            [Button.inline("❌ Cancel", b"cancel_flow")]
                        ]
                    )
            else:
                await event.respond("❌ <b>Invalid URL.</b>\nPlease send a valid link starting with <code>http</code>.", parse_mode='html')
                
        elif state == 'WAITING_RANGE_INPUT':
            USER_STATES[user_id]['range'] = text
            await ask_destination(event, user_id)
            
        elif state == 'WAITING_DEST_INPUT':
            try:
                channel_id = int(text)
                USER_STATES[user_id]['dest'] = channel_id
                await start_confirmed_download(event, user_id)
            except ValueError:
                await event.respond("❌ <b>Invalid ID.</b>\nPlease send a numeric Channel ID.", parse_mode='html')
    else:
        # Direct URL pasting support without pressing buttons first
        if text.startswith('http://') or text.startswith('https://'):
            domain, service, cur_id, post_id = parse_media_url(text)
            if domain:
                if post_id:
                    # Single video / file
                    USER_STATES[user_id] = {
                        'url': text,
                        'range': None,
                        'dest': STORAGE_CHANNEL_ID
                    }
                    buttons = [
                        [Button.inline("🚀 Download Now", b"quick_download_confirm")],
                        [Button.inline("📍 Custom Channel", b"dest_custom")],
                        [Button.inline("❌ Cancel", b"cancel_flow")]
                    ]
                    await event.respond(
                        f"🎬 <b>Single Media Detected!</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"• <b>Platform:</b> {service.capitalize()}\n"
                        f"• <b>ID:</b> <code>{cur_id}</code>\n"
                        f"• <b>Destination:</b> Storage Channel\n\n"
                        f"Start download & upload to Telegram?",
                        parse_mode='html',
                        buttons=buttons
                    )
                else:
                    # Album or creator profile
                    USER_STATES[user_id] = {'url': text, 'state': 'WAITING_RANGE'}
                    await event.respond(
                        f"📁 <b>{service.capitalize()} Album/Profile Detected!</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"• <b>Source:</b> <code>{text[:60]}...</code>\n\n"
                        f"Do you want to browse files, download all, or specify a page range?",
                        parse_mode='html',
                        buttons=[
                            [Button.inline("📋 Browse & Select Files", b"crawl_album")],
                            [Button.inline("📄 All Pages / Videos", b"range_all")],
                            [Button.inline("🔢 Custom Range", b"range_custom")],
                            [Button.inline("❌ Cancel", b"cancel_flow")]
                        ]
                    )
            else:
                await event.respond(
                    "❌ <b>Unsupported URL format.</b>\n"
                    "Supported formats:\n"
                    "• <code>https://bunkr.cr/a/album_id</code>\n"
                    "• <code>https://bunkr.cr/v/video_id</code>\n"
                    "• <code>https://coomer.st/onlyfans/user/...</code>",
                    parse_mode='html'
                )

async def ask_destination(event, user_id):
    USER_STATES[user_id]['state'] = 'WAITING_CHANNEL'
    buttons = [
        [Button.inline("📌 Default Channel", b"dest_default")],
        [Button.inline("🆔 Custom Channel ID", b"dest_custom")],
        [Button.inline("❌ Cancel", b"cancel_flow")]
    ]
    await event.respond(
        "📍 <b>Step 3: To Where?</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Where should I upload the videos?", 
        parse_mode='html', 
        buttons=buttons
    )

async def start_confirmed_download(event, user_id):
    info = USER_STATES.pop(user_id, None)
    if not info: return
    url = info['url']
    page_range = info.get('range')
    dest = info.get('dest', STORAGE_CHANNEL_ID)
    selected_videos = info.get('selected_videos')
    
    if selected_videos:
        await event.respond(
            f"✅ <b>Starting Download!</b>\n\n"
            f"🔗 <b>Source:</b> {url}\n"
            f"📦 <b>Selected Files:</b> {len(selected_videos)} items\n"
            f"📍 <b>Destination:</b> <code>{dest}</code>",
            parse_mode='html'
        )
        asyncio.create_task(download_and_upload(event, url, None, dest, selected_videos=selected_videos))
    else:
        await event.respond(
            f"✅ <b>Starting Download!</b>\n\n"
            f"🔗 <b>Source:</b> {url}\n"
            f"📄 <b>Range:</b> {page_range or 'All'}\n"
            f"📍 <b>Destination:</b> <code>{dest}</code>",
            parse_mode='html'
        )
        asyncio.create_task(download_and_upload(event, url, page_range, dest))

@client.on(events.NewMessage(pattern=r"^/download(?:\s+(https?://\S+)(?:\s+([\d-]+))?)?$"))
async def download_handler(event):
    if not await check_admin_or_notify(event): return
    url = event.pattern_match.group(1)
    page_range = event.pattern_match.group(2)
    if not url:
        await event.respond(
            "📥 <b>How to Download:</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Simply <b>paste any link</b> directly into this chat, or use:\n\n"
            "• <code>/download &lt;url&gt;</code> - Download all items\n"
            "• <code>/download &lt;url&gt; &lt;pages&gt;</code> - Download specific pages (e.g. <code>1-3</code>)\n\n"
            "<b>Supported Platforms:</b>\n"
            "• <b>Bunkr:</b> Albums (<code>/a/</code>) & Videos (<code>/v/</code>, <code>/f/</code>)\n"
            "• <b>Coomer / Kemono:</b> Creator profiles & single posts",
            parse_mode='html'
        )
        return
    asyncio.create_task(download_and_upload(event, url, page_range, STORAGE_CHANNEL_ID))

@client.on(events.NewMessage(pattern=r"^/bunkr(?:\s+(https?://\S+)(?:\s+([\d-]+))?)?$"))
async def bunkr_command_handler(event):
    if not await check_admin_or_notify(event): return
    url = event.pattern_match.group(1)
    page_range = event.pattern_match.group(2)
    if not url:
        await event.respond(
            "⚡ <b>Bunkr Downloader</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Usage:\n"
            "• <code>/bunkr https://bunkr.cr/a/album_id</code>\n"
            "• <code>/bunkr https://bunkr.cr/v/video_id</code>\n\n"
            "Or simply paste any Bunkr URL directly into this chat!",
            parse_mode='html'
        )
        return
    asyncio.create_task(download_and_upload(event, url, page_range, STORAGE_CHANNEL_ID))

# ── Main ─────────────────────────────────────────────────────────────────────

async def set_bot_commands():
    commands = [
        BotCommand(command="start", description="🚀 Open main menu & status"),
        BotCommand(command="download", description="📥 Download from any URL"),
        BotCommand(command="bunkr", description="⚡ Quick Bunkr downloader"),
        BotCommand(command="stats", description="📊 Show uploader & storage stats"),
        BotCommand(command="help", description="❓ Show help guide"),
    ]
    await client(functions.bots.SetBotCommandsRequest(
        scope=BotCommandScopeDefault(),
        lang_code='en',
        commands=commands
    ))

async def main():
    print("🚀 Starting Bot...")
    await client.start(bot_token=BOT_TOKEN)
    print("✅ Setting commands...")
    await set_bot_commands()
    print("✅ Online.")
    await client.run_until_disconnected()

if __name__ == "__main__":
    client.loop.run_until_complete(main())
