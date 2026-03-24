import os
import asyncio
import logging
import shutil
import time
import math
import httpx
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient, events, Button, functions, types
from telethon.tl.types import DocumentAttributeVideo, BotCommand, BotCommandScopeDefault
from tqdm import tqdm

# Import fasttelethon from local file
from fasttelethon import upload_file

# Import scraper functions
from scraper import (
    parse_profile_url, 
    parse_page_range, 
    fetch_all_posts, 
    extract_video_urls, 
    download_video,
    DEFAULT_DOMAIN
)

# ── Configuration ────────────────────────────────────────────────────────────

load_dotenv()

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
STORAGE_CHANNEL_ID = int(os.getenv("STORAGE_CHANNEL_ID", 0))
ADMIN_IDS = [int(id.strip()) for id in os.getenv("ADMIN_IDS", "").split(",") if id.strip()]
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "bot_downloads")

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
# States: 'WAITING_URL', 'WAITING_RANGE', 'WAITING_CHANNEL'

class BarPositionManager:
    def __init__(self, max_bars=10):
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

def get_progress_bar(current, total):
    if total == 0: return " [⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜] 0%"
    current = min(current, total)
    percentage = current / total
    done = int(percentage * 10)
    remain = 10 - done
    return f"<code>[{'🟦' * done}{'⬜' * remain}] {percentage:.1%}</code>"

async def process_video(domain, vid, temp_dir, service, user_id, download_semaphore, upload_semaphore, status_update_func, target_channel_id):
    filepath = temp_dir / f"{vid['id']}_{vid['name']}"
    
    pos = await BAR_MANAGER.get_pos()
    try:
        # 1. Download
        # 1. Download
        async with download_semaphore:
            # Create a localized client for this download if not provided
            download_success = await download_video(domain, vid, temp_dir, pos=pos)
            
        if download_success and filepath.exists():
            await status_update_func(active_video=vid['name'], progress_html="<code>[Verifying...] 🔍</code>")
            last_pbar_time = [0.0]
            
            async def progress_callback(current, total):
                if time.time() - last_pbar_time[0] > 3:
                    pbar = get_progress_bar(current, total)
                    await status_update_func(active_video=vid['name'], progress_html=pbar)
                    last_pbar_time[0] = time.time()

            # 2. Fast Upload with Retry
            for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
                try:
                    async with upload_semaphore:
                        # Terminal progress for upload
                        file_size = filepath.stat().st_size
                        with tqdm(total=file_size, unit="B", unit_scale=True, desc=f"   [UP] {vid['name'][:20]}", position=pos, leave=False) as t_pbar:
                            async def upload_progress(current, total):
                                await progress_callback(current, total)
                                t_pbar.n = current
                                t_pbar.refresh()

                            with open(filepath, "rb") as f:
                                uploaded_file = await upload_file(client, f, progress_callback=upload_progress)
                            
                            await client.send_file(
                                target_channel_id,
                                uploaded_file,
                                caption=f"🎥 {vid['name']}\n👤 {service}/{user_id}\n🆔 {vid['id']}",
                                supports_streaming=True,
                                video=True
                            )
                    break # Success!
                except Exception as e:
                    if attempt == MAX_UPLOAD_RETRIES:
                        raise e
                    logger.warning(f"Upload attempt {attempt} failed for {vid['name']}: {e}. Retrying in {RETRY_DELAY}s...")
                    await asyncio.sleep(RETRY_DELAY)
            
            if filepath.exists(): os.remove(filepath)
            await status_update_func(success=True, active_video=vid['name'])
        else:
            await status_update_func(success=False)
            
    except Exception as e:
        logger.error(f"Error processing {vid['name']}: {e}")
        await status_update_func(success=False, active_video=vid['name'])
    finally:
        await BAR_MANAGER.release_pos(pos)

async def download_and_upload(event, url, page_range_str, target_channel_id=None):
    user_id = event.sender_id
    # Store for resume
    LAST_TASK_INFO[user_id] = {
        'url': url,
        'range': page_range_str,
        'dest': target_channel_id
    }
    
    # Store current task for cancellation
    RUNNING_TASKS[user_id] = asyncio.current_task()
    
    if target_channel_id is None:
        target_channel_id = STORAGE_CHANNEL_ID

    status_msg = None
    temp_dir = None
    try:
        domain, service, cur_user_id = parse_profile_url(url)
        if not domain: 
            await event.respond("❌ Invalid URL.")
            return
            
        page_range = parse_page_range(page_range_str) if page_range_str else None
        
        temp_dir = Path(DOWNLOAD_DIR) / f"{service}_{cur_user_id}_{event.id}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        status_msg = await event.respond(f"🔍 <b>Scraping:</b> {service}/{cur_user_id}...", parse_mode='html')
        
        posts = await fetch_all_posts(domain, service, cur_user_id, page_range)
        if not posts:
            await status_msg.edit("❌ No posts found.")
            return
            
        videos = extract_video_urls(domain, posts)
        if not videos:
            await status_msg.edit("⚠ No videos found.")
            return
            
        await status_msg.edit(f"📦 Found {len(videos)} videos. Starting parallel optimized uploads...")
        
        download_semaphore = asyncio.Semaphore(3)
        upload_semaphore = asyncio.Semaphore(2)
        success_count = [0]
        total_processed = [0]
        active_uploads = {}
        
        async def update_status(success=None, active_video=None, progress_html=None):
            if success is not None:
                if success: success_count[0] += 1
                total_processed[0] += 1
            
            if active_video:
                if progress_html:
                    active_uploads[active_video] = progress_html
                elif success is not None:
                    active_uploads.pop(active_video, None)

            # Inclusion of Cancel button
            cancel_button = [Button.inline("🛑 Cancel Task", b"cancel_task")]

            text = f"📤 <b>Processing:</b> {total_processed[0]}/{len(videos)} videos\n"
            text += f"✅ <b>Uploaded:</b> {success_count[0]}\n\n"
            
            if active_uploads:
                text += "<b>🚀 active Uploads:</b>\n"
                items = list(active_uploads.items())
                for v_name, pbar in items[:3]:
                    text += f"• {v_name[:20]}...\n  {pbar}\n"
            
            try:
                await status_msg.edit(text, parse_mode='html', buttons=cancel_button)
            except: pass

        tasks = [
            process_video(domain, vid, temp_dir, service, cur_user_id, download_semaphore, upload_semaphore, update_status, target_channel_id)
            for vid in videos
        ]
        
        await asyncio.gather(*tasks)
        await status_msg.edit(
            f"🏁 <b>Done!</b>\n"
            f"✅ Uploaded: {success_count[0]}/{len(videos)}", 
            parse_mode='html'
        )
        
    except asyncio.CancelledError:
        logger.info(f"Task for {url} was cancelled.")
        if status_msg:
            try: await status_msg.edit(f"🛑 <b>Task Cancelled.</b>\n🔗 {url}", parse_mode='html')
            except: pass
    except Exception as e:
        logger.error(f"Global error: {e}")
        try: await event.respond(f"❌ <b>Error:</b> {e}", parse_mode='html')
        except: pass
    finally:
        # Cleanup
        RUNNING_TASKS.pop(user_id, None)
        if temp_dir and temp_dir.exists() and not any(temp_dir.iterdir()):
            try: shutil.rmtree(temp_dir)
            except: pass

# ── Commands & Menu ──────────────────────────────────────────────────────────

@client.on(events.NewMessage(pattern="/start"))
async def start_handler(event):
    if not is_admin(event.sender_id): return
    
    text = (
        "✨ <b>Premium Coomer Uploader</b> ✨\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Welcome! I am your advanced media assistant. I can scrape, download, "
        "and upload videos directly to your archive with high speed.\n\n"
        "🚀 <b>Quick Start:</b>\n"
        "Press the button below to start a new download process or use commands "
        "from the menu.\n\n"
        "📊 <b>Current Stats:</b>\n"
        "• Status: 🟢 Online\n"
        "• Speed: ⚡ Optimized\n"
    )
    
    buttons = [
        [Button.inline("🚀 New Download", b"new_download")],
    ]
    
    # Add Resume button if exists
    if event.sender_id in LAST_TASK_INFO:
        buttons.append([Button.inline("⏯ Resume Last Task", b"resume_last")])
    
    buttons.extend([
        [Button.inline("🧹 Clear Temp Files", b"clear_temp"), Button.inline("❓ Help Guide", b"help")],
        [Button.url("📂 View Storage Channel", f"https://t.me/c/{str(abs(STORAGE_CHANNEL_ID))[3:]}")]
    ])
    
    await event.respond(text, parse_mode='html', buttons=buttons)

@client.on(events.CallbackQuery())
async def callback_handler(event):
    if not is_admin(event.sender_id): return
    
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
            "Please send the Coomer/Fansly profile URL you want to scrape.\n\n"
            "💡 <i>Example: https://coomer.st/onlyfans/user/example</i>", 
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
    elif data == "help":
        help_text = (
            "📖 <b>Premium Help Guide</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "<b>Commands:</b>\n"
            "• /start - 🚀 Open main menu\n"
            "• /download [url] - 📥 Quick download\n"
            "• /help - ❓ Show this guide\n\n"
            "<b>Interactive Flow:</b>\n"
            "1. Click 'New Download'\n"
            "2. Provide the Source URL\n"
            "3. Choose page range (or 'all')\n"
            "4. Confirm destination channel\n"
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
            asyncio.create_task(download_and_upload(event, info['url'], info['range'], info['dest']))
        else:
            await event.answer("❌ No previous task found.")

@client.on(events.NewMessage())
async def message_handler(event):
    if not is_admin(event.sender_id): return
    user_id = event.sender_id
    text = event.text.strip()
    
    if text.startswith('/'): return # Ignore commands here
    
    if user_id in USER_STATES:
        state = USER_STATES[user_id].get('state')
        
        if state == 'WAITING_URL':
            if text.startswith('http'):
                USER_STATES[user_id]['url'] = text
                USER_STATES[user_id]['state'] = 'WAITING_RANGE'
                await event.respond(
                    "📑 <b>Step 2: Page Range?</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "Do you want to scrape all pages or a specific range?", 
                    parse_mode='html', 
                    buttons=[
                        [Button.inline("📄 All Pages", b"range_all")],
                        [Button.inline("🔢 Custom Range", b"range_custom")],
                        [Button.inline("❌ Cancel", b"cancel_flow")]
                    ]
                )
            else:
                await event.respond("❌ <b>Invalid URL.</b>\nPlease send a valid link starting with <code>http</code>.", parse_mode='html')
                
        elif state == 'WAITING_RANGE_INPUT':
            # This is reached if they chose 'range_custom' then sent text
            USER_STATES[user_id]['range'] = text
            await ask_destination(event, user_id)
            
        elif state == 'WAITING_DEST_INPUT':
            try:
                channel_id = int(text)
                USER_STATES[user_id]['dest'] = channel_id
                await start_confirmed_download(event, user_id)
            except ValueError:
                await event.respond("❌ <b>Invalid ID.</b>\nPlease send a numeric Channel ID.", parse_mode='html')

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

@client.on(events.CallbackQuery(pattern=r"range_.*|dest_.*"))
async def flow_callback_handler(event):
    if not is_admin(event.sender_id): return
    user_id = event.sender_id
    data = event.data.decode()
    
    if user_id not in USER_STATES: return

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

async def start_confirmed_download(event, user_id):
    info = USER_STATES.pop(user_id)
    url = info['url']
    page_range = info.get('range')
    dest = info.get('dest', STORAGE_CHANNEL_ID)
    
    await event.respond(f"✅ <b>Starting Download!</b>\n\n🔗 Source: {url}\n📄 Range: {page_range or 'All'}\n📍 Destination: {dest}", parse_mode='html')
    asyncio.create_task(download_and_upload(event, url, page_range, dest))

@client.on(events.NewMessage(pattern=r"/download\s+(https?://\S+)(?:\s+([\d-]+))?"))
async def download_handler(event):
    if not is_admin(event.sender_id): return
    url = event.pattern_match.group(1)
    page_range = event.pattern_match.group(2)
    asyncio.create_task(download_and_upload(event, url, page_range, STORAGE_CHANNEL_ID))

# ── Main ─────────────────────────────────────────────────────────────────────

async def set_bot_commands():
    commands = [
        BotCommand(command="start", description="🚀 Start the bot & menu"),
        BotCommand(command="download", description="📥 Download from URL"),
        BotCommand(command="help", description="❓ Show help guide"),
        BotCommand(command="settings", description="⚙️ Bot settings"),
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
