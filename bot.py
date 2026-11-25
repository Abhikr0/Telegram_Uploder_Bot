import os
import sys
import json
import asyncio
import logging
import shutil
import time
import uuid
import subprocess
import re
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from dotenv import load_dotenv
import httpx
from telethon import TelegramClient, events, Button, functions, errors
from telethon.tl.types import DocumentAttributeVideo, DocumentAttributeFilename, BotCommand, BotCommandScopeDefault
from telethon.tl.functions.messages import CreateForumTopicRequest, GetForumTopicsRequest
from telethon.tl import types
from tqdm import tqdm
from hachoir.metadata import extractMetadata
from hachoir.parser import createParser
import hachoir.core.config
from fasttelethon import upload_file, upload_http_stream
from ai_caption import generate_ai_caption, generate_ai_name
from scraper import (
    parse_media_url,
    parse_page_range,
    fetch_all_posts,
    extract_video_urls,
    download_video,
    resolve_media_stream_info
)
from pimpbunny_adapter import (
    parse_pimpbunny_url,
    pimpbunny_to_video_dict,
    fetch_pimpbunny_all_posts,
    DEFAULT_DOMAIN as PINKBunny_DOMAIN,
)

hachoir.core.config.quiet = True
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
for noisy in ("telethon", "asyncio", "httpx"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
logging.getLogger("hachoir").setLevel(logging.ERROR)
_IS_TTY: bool = sys.stderr.isatty()

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

load_dotenv()
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
raw_storage = os.getenv("STORAGE_CHANNEL_ID", "")
STORAGE_CHANNELS = []
for x in raw_storage.replace('"', '').replace("'", "").split(","):
    x = x.strip()
    if x and x != "0":
        try:
            STORAGE_CHANNELS.append(int(x))
        except ValueError:
            pass
STORAGE_CHANNEL_ID = STORAGE_CHANNELS[0] if STORAGE_CHANNELS else 0
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
IS_TERMUX = bool(os.getenv("TERMUX_VERSION")) or "com.termux" in os.getenv("PREFIX", "")

def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, default)))
    except (TypeError, ValueError):
        return default

_raw_download_dir = os.getenv("DOWNLOAD_DIR", "bot_downloads").strip() or "bot_downloads"
if sys.platform != "win32" and re.match(r"^[a-zA-Z]:(?:[/\\]|$)", _raw_download_dir):
    logger.warning(
        f"Ignoring Windows drive path DOWNLOAD_DIR={_raw_download_dir!r} on {sys.platform}; using 'bot_downloads'"
    )
    _raw_download_dir = "bot_downloads"
DOWNLOAD_DIR = str(Path(_raw_download_dir).resolve())
ENABLE_STREAM_UPLOAD = os.getenv(
    "ENABLE_STREAM_UPLOAD", "false" if IS_TERMUX else "true"
).lower() in ("true", "1", "yes")
STREAM_CONCURRENCY = _env_int("STREAM_CONCURRENCY", 1 if IS_TERMUX else 2)
DOWNLOAD_CONCURRENCY = _env_int("DOWNLOAD_CONCURRENCY", 2 if IS_TERMUX else 3)
MTPROTO_CONNECTIONS = _env_int("MTPROTO_CONNECTIONS", 2 if IS_TERMUX else 4)
STREAM_PROBE_BYTES = _env_int(
    "STREAM_PROBE_BYTES", 6 * 1024 * 1024 if IS_TERMUX else 16 * 1024 * 1024, minimum=1024 * 1024
)
SESSION_NAME = os.getenv("TELETHON_SESSION", "uploader_bot_session").strip() or "uploader_bot_session"

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()


class SupabaseRestClient:
    """Lightweight async PostgREST client using httpx (avoids pydantic/Rust deps on Termux)."""

    def __init__(self, url: str, key: str):
        self.base_url = url.rstrip("/")
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }

    async def insert(self, table: str, row: dict) -> None:
        headers = {**self.headers, "Prefer": "return=minimal"}
        async with httpx.AsyncClient(timeout=15.0) as http_client:
            resp = await http_client.post(
                f"{self.base_url}/rest/v1/{table}",
                headers=headers,
                json=row,
            )
            resp.raise_for_status()

    async def count(self, table: str) -> int:
        headers = {**self.headers, "Prefer": "count=exact", "Range": "0-0"}
        async with httpx.AsyncClient(timeout=15.0) as http_client:
            resp = await http_client.get(
                f"{self.base_url}/rest/v1/{table}?select=id",
                headers=headers,
            )
            if resp.status_code not in (200, 206, 416):
                resp.raise_for_status()
            content_range = resp.headers.get("Content-Range", "")
            if "/" in content_range:
                total_part = content_range.split("/")[-1].strip()
                if total_part.isdigit():
                    return int(total_part)
            return 0


supabase_client = None
if SUPABASE_URL and SUPABASE_KEY:
    supabase_client = SupabaseRestClient(SUPABASE_URL, SUPABASE_KEY)
    logging.info("Configured lightweight Supabase REST client for automatic upload indexing.")

async def _index_to_supabase(db_title: str, message_id: int, file_size: int) -> None:
    if not supabase_client:
        return
    try:
        db_entry = {
            "id": str(uuid.uuid4()),
            "title": db_title[:150],
            "file_id": "mtproto_uploaded",
            "message_id": message_id,
            "file_size": file_size,
            "upload_date": datetime.now(timezone.utc).isoformat(),
        }
        await supabase_client.insert("media", db_entry)
        logger.info(f"Indexed to Supabase: {db_title[:60]} (Msg ID: {message_id})")
    except Exception as db_err:
        logger.warning(f"Supabase auto-index error: {db_err}")

async def _get_supabase_media_count() -> str:
    if not supabase_client:
        return "N/A"
    try:
        return str(await supabase_client.count("media"))
    except Exception:
        return "N/A"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
MAX_UPLOAD_RETRIES = 3
RETRY_DELAY = 5
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

class BoundedDict(dict):
    def __init__(self, maxsize=500):
        super().__init__()
        self.maxsize = maxsize
    def __setitem__(self, k, v):
        if len(self) >= self.maxsize and k not in self:
            self.pop(next(iter(self)))
        super().__setitem__(k, v)

USER_STATES = BoundedDict(500)
RUNNING_TASKS = BoundedDict(500)
LAST_TASK_INFO = BoundedDict(500)

class BarPositionManager:
    def __init__(self, max_bars=6):
        self.max_bars = max_bars
        self.occupied = [False] * max_bars
        self._lock = None

    @property
    def lock(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def get_pos(self):
        for _ in range(50):
            async with self.lock:
                for i in range(self.max_bars):
                    if not self.occupied[i]:
                        self.occupied[i] = True
                        return i + 1
            await asyncio.sleep(0.1)
        return 0

    async def release_pos(self, pos):
        if pos <= 0: return
        async with self.lock:
            if pos - 1 < len(self.occupied):
                self.occupied[pos - 1] = False

BAR_MANAGER = BarPositionManager()

# ── Helpers ──────────────────────────────────────────────────────────────────

def is_admin(user_id):
    try:
        load_dotenv(override=True)
        raw_admins = os.getenv("ADMIN_IDS", "")
        admin_list = [int(x.strip()) for x in raw_admins.split(",") if x.strip()]
        return not admin_list or user_id in admin_list
    except Exception:
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

async def is_forum_chat(chat_id):
    if not chat_id:
        return False
    try:
        cid = int(chat_id)
        entity = await client.get_entity(cid)
        return bool(getattr(entity, 'forum', False))
    except Exception as e:
        logger.debug(f"is_forum_chat check failed for {chat_id}: {e}")
        return False

CHAT_TITLES_CACHE = {}

async def get_chat_title_cached(chat_id):
    if not chat_id:
        return ""
    if chat_id in CHAT_TITLES_CACHE:
        return CHAT_TITLES_CACHE[chat_id]
    try:
        cid = int(chat_id)
        entity = await client.get_entity(cid)
        title = getattr(entity, 'title', None) or getattr(entity, 'first_name', str(chat_id))
        CHAT_TITLES_CACHE[chat_id] = title
        return title
    except Exception:
        return str(chat_id)

FORUM_TOPICS_FILE = Path(__file__).parent / "forum_topics.json"

def load_forum_topics():
    if not FORUM_TOPICS_FILE.exists():
        return {}
    try:
        with open(FORUM_TOPICS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading forum_topics.json: {e}")
        return {}

def save_forum_topic(chat_id, topic_id, title):
    if not chat_id or not topic_id:
        return
    data = load_forum_topics()
    cid_str = str(chat_id)
    if cid_str not in data:
        data[cid_str] = {}
    data[cid_str][str(topic_id)] = {
        "id": int(topic_id),
        "title": str(title).strip() or f"Topic #{topic_id}",
        "updated_at": int(time.time())
    }
    try:
        with open(FORUM_TOPICS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Error saving forum_topics.json: {e}")

def delete_forum_topic(chat_id, topic_id):
    data = load_forum_topics()
    cid_str = str(chat_id)
    if cid_str in data and str(topic_id) in data[cid_str]:
        del data[cid_str][str(topic_id)]
        try:
            with open(FORUM_TOPICS_FILE, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception:
            pass

async def discover_topics_from_chat(chat_id):
    """
    Attempts to discover topic creation messages from recent message IDs in the group.
    In Telegram forum supergroups, topic creation emits a message with MessageActionTopicCreate.
    """
    if not chat_id:
        return []
    discovered = []
    try:
        cid = int(chat_id)
        ids_to_check = list(range(1, 60))
        msgs = await client.get_messages(cid, ids=ids_to_check)
        for m in msgs:
            if not m:
                continue
            action = getattr(m, 'action', None)
            if isinstance(action, types.MessageActionTopicCreate):
                save_forum_topic(cid, m.id, action.title)
                discovered.append({'id': m.id, 'title': action.title})
            elif getattr(m, 'reply_to', None) and getattr(m.reply_to, 'forum_topic', False):
                top_id = getattr(m.reply_to, 'reply_to_top_id', None) or getattr(m.reply_to, 'reply_to_msg_id', None)
                if top_id and str(top_id) not in load_forum_topics().get(str(cid), {}):
                    save_forum_topic(cid, top_id, f"Topic #{top_id}")
                    discovered.append({'id': top_id, 'title': f"Topic #{top_id}"})
    except Exception as e:
        logger.debug(f"Topic discovery scan error for {chat_id}: {e}")
    return discovered

async def get_forum_topics_list(chat_id, limit=40):
    if not chat_id:
        return []
    cid = int(chat_id)
    cid_str = str(cid)

    # 1. MTProto GetForumTopicsRequest (works on user accounts, raises BotMethodInvalidError on bots)
    try:
        entity = await client.get_entity(cid)
        res = await client(GetForumTopicsRequest(
            peer=entity,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=limit
        ))
        mtproto_topics = []
        for t in getattr(res, 'topics', []):
            if isinstance(t, types.ForumTopic):
                mtproto_topics.append({
                    'id': t.id,
                    'title': t.title,
                    'closed': bool(getattr(t, 'closed', False))
                })
                save_forum_topic(cid, t.id, t.title)
        if mtproto_topics:
            return mtproto_topics
    except Exception as e:
        # Standard Telegram restriction: Bots are not allowed to call GetForumTopicsRequest
        logger.debug(f"MTProto GetForumTopicsRequest unavailable for bot on {chat_id}: {e}")

    # 2. Check local persistent store
    all_stored = load_forum_topics().get(cid_str, {})
    if not all_stored:
        # Try active discovery from recent group messages
        await discover_topics_from_chat(cid)
        all_stored = load_forum_topics().get(cid_str, {})

    topics = []
    for tid_str, tinfo in all_stored.items():
        topics.append({
            'id': int(tid_str),
            'title': tinfo.get('title', f"Topic #{tid_str}"),
            'closed': bool(tinfo.get('closed', False))
        })

    # Sort with newest topic first
    topics.sort(key=lambda x: x['id'], reverse=True)
    return topics[:limit]

async def create_forum_topic_helper(chat_id, title):
    if not chat_id or not title:
        return None
    cid = int(chat_id)
    clean_title = title.strip()[:128]
    # First: use official Telegram Bot API HTTP endpoint (recommended for bots)
    try:
        async with httpx.AsyncClient(timeout=15.0) as http_client:
            resp = await http_client.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/createForumTopic",
                json={"chat_id": cid, "name": clean_title}
            )
            data = resp.json()
            if data.get("ok"):
                topic_id = data["result"]["message_thread_id"]
                logger.info(f"Created forum topic '{clean_title}' via Bot API (ID: {topic_id})")
                save_forum_topic(cid, topic_id, clean_title)
                return topic_id
            else:
                desc = data.get("description", "")
                if "not enough rights" in desc.lower():
                    raise Exception(
                        "Bot lacks 'Manage Topics' admin permission in this group. "
                        "Please go to Group Settings > Administrators > Infinty > toggle 'Manage Topics' ON and tap Save."
                    )
                logger.warning(f"Bot API createForumTopic returned error: {desc}")
    except Exception as e:
        if "lacks 'Manage Topics'" in str(e):
            raise e
        logger.debug(f"HTTP createForumTopic failed, trying MTProto: {e}")

    # Fallback: MTProto
    try:
        import random
        entity = await client.get_entity(cid)
        rnd_id = random.randint(0, 0x7fffffffffffffff)
        result = await client(CreateForumTopicRequest(
            peer=entity,
            title=clean_title,
            random_id=rnd_id
        ))
        topic_id = None
        if hasattr(result, 'updates'):
            for u in result.updates:
                msg = getattr(u, 'message', None)
                if msg:
                    action = getattr(msg, 'action', None)
                    if isinstance(action, types.MessageActionTopicCreate):
                        topic_id = msg.id
                        break
            if not topic_id:
                for u in result.updates:
                    msg = getattr(u, 'message', None)
                    if msg and hasattr(msg, 'id'):
                        topic_id = msg.id
                        break
        elif hasattr(result, 'id'):
            topic_id = result.id

        if topic_id:
            save_forum_topic(cid, topic_id, clean_title)
        return topic_id
    except Exception as e:
        logger.error(f"Error creating topic '{title}' in {chat_id}: {e}")
        if "can't write" in str(e).lower() or "rights" in str(e).lower():
            raise Exception(
                "Bot lacks 'Manage Topics' admin permission in this group. "
                "Please go to Group Settings > Administrators > Infinty > toggle 'Manage Topics' ON and tap Save."
            )
        raise e

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

_FFMPEG: str | None = None
_FFPROBE: str | None = None

def _get_ffmpeg() -> str:
    global _FFMPEG
    if _FFMPEG is None:
        try:
            import imageio_ffmpeg
            _FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            _FFMPEG = shutil.which("ffmpeg") or "ffmpeg"
    return _FFMPEG

def _get_ffprobe() -> str | None:
    global _FFPROBE
    if _FFPROBE is None:
        _FFPROBE = shutil.which("ffprobe")
    return _FFPROBE

def _resolve_ffmpeg_input(src: str | Path) -> str:
    """Resolve local filesystem paths so ffmpeg never treats prefixes like 'E:' as a URL protocol."""
    s = str(src)
    if s.startswith(("http://", "https://")):
        return s
    return str(Path(s).resolve())

def _format_ffmpeg_seek(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}.000"

def _parse_ffmpeg_metadata_output(out: str, meta: dict) -> None:
    if not meta.get('duration'):
        m = re.search(r'Duration:\s*(\d+):(\d+):(\d+\.?\d*)', out)
        if m:
            meta['duration'] = int(int(m[1]) * 3600 + int(m[2]) * 60 + float(m[3]))
    if not meta.get('width') or not meta.get('height'):
        vm = re.search(r'Stream.*Video:.*?(\d{2,5})x(\d{2,5})', out)
        if vm:
            meta['width'], meta['height'] = int(vm[1]), int(vm[2])

def get_video_metadata(filepath):
    filepath = Path(filepath).resolve()
    meta = {'duration': 0, 'width': 0, 'height': 0}
    ffprobe = _get_ffprobe()
    if ffprobe:
        try:
            res = subprocess.run(
                [ffprobe, "-v", "error", "-select_streams", "v:0",
                 "-show_entries", "stream=width,height,duration:format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", str(filepath)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10
            )
            lines = [x.strip() for x in res.stdout.strip().splitlines() if x.strip()]
            for val in lines:
                try:
                    fval = float(val)
                    if '.' in val or meta['duration'] == 0:
                        meta['duration'] = int(fval)
                except ValueError:
                    pass
            if len(lines) >= 2:
                try:
                    meta['width'], meta['height'] = int(lines[0]), int(lines[1])
                except ValueError:
                    pass
        except Exception:
            pass
    if not meta['duration'] or not meta['width']:
        try:
            res = subprocess.run(
                [_get_ffmpeg(), "-hide_banner", "-i", str(filepath)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=8, errors="replace"
            )
            out = (res.stderr or "") + " " + (res.stdout or "")
            _parse_ffmpeg_metadata_output(out, meta)
        except Exception:
            pass
    if not meta['duration'] or not meta['width']:
        try:
            parser = createParser(str(filepath))
            if parser:
                with parser:
                    data = extractMetadata(parser)
                    if data:
                        if not meta['duration'] and data.has('duration'):
                            meta['duration'] = int(data.get('duration').seconds)
                        if not meta['width'] and data.has('width'):
                            meta['width'] = int(data.get('width'))
                        if not meta['height'] and data.has('height'):
                            meta['height'] = int(data.get('height'))
        except Exception:
            pass
    return meta

async def _run_ffmpeg_thumbnail(
    input_src: str | Path,
    thumb_path: Path,
    seek: str | None = None,
    headers: dict | None = None,
    timeout: float = 15.0,
) -> tuple[bool, str]:
    """Extract or convert a single JPEG thumbnail (<=320x320) using resolved paths."""
    resolved_thumb = Path(thumb_path).resolve()
    cmd = [_get_ffmpeg(), "-y"]
    if headers:
        clean_headers = {
            k: v for k, v in headers.items()
            if k.lower() not in ("connection", "accept-encoding", "range")
        }
        if clean_headers:
            cmd.extend(["-headers", "".join(f"{k}: {v}\r\n" for k, v in clean_headers.items())])
    if seek:
        cmd.extend(["-ss", seek])
    cmd.extend([
        "-i", _resolve_ffmpeg_input(input_src),
        "-vframes", "1",
        "-vf", "scale=320:320:force_original_aspect_ratio=decrease",
        "-q:v", "2",
        str(resolved_thumb),
    ])
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            try:
                proc.kill()
                await proc.communicate()
            except Exception:
                pass
            logger.debug(f"ffmpeg thumbnail timed out after {timeout}s for {str(input_src)[:80]}")
            return False, ""
        out = (stderr.decode(errors="replace") or "") + " " + (stdout.decode(errors="replace") or "")
        ok = proc.returncode == 0 and resolved_thumb.exists() and resolved_thumb.stat().st_size > 0
        if not ok:
            tail = " | ".join(out.strip().splitlines()[-3:])
            logger.debug(f"ffmpeg thumbnail failed (rc={proc.returncode}): {tail}")
        return ok, out
    except Exception as e:
        logger.debug(f"ffmpeg thumbnail execution error: {e}")
        return False, ""

async def generate_thumbnail(filepath, thumb_path, duration=0):
    resolved_thumb = Path(thumb_path).resolve()
    seeks = (
        [_format_ffmpeg_seek(max(1, duration // 2)), "00:00:05.000", "00:00:01.000", "00:00:00.000"]
        if duration > 2
        else ["00:00:05.000", "00:00:01.000", "00:00:00.000"]
    )
    best_bytes = b""
    for ss in seeks:
        ok, _ = await _run_ffmpeg_thumbnail(filepath, resolved_thumb, seek=ss, timeout=15.0)
        if not ok:
            continue
        size = resolved_thumb.stat().st_size
        if size >= 3000:
            return True
        if size > len(best_bytes):
            best_bytes = resolved_thumb.read_bytes()
    if best_bytes:
        resolved_thumb.write_bytes(best_bytes)
        return True
    return False

async def _fetch_fallback_thumbnail(vid: dict, thumb_path: Path) -> bool:
    """Download external thumbnail/poster URL and transcode to Telegram-compatible <=320x320 JPEG."""
    thumb_url = vid.get("thumbnail") or vid.get("poster")
    if not thumb_url:
        return False
    resolved_thumb = Path(thumb_path).resolve()
    raw_thumb = resolved_thumb.with_suffix(".img")
    try:
        parsed = urlparse(thumb_url)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Referer": f"{parsed.scheme}://{parsed.netloc}/" if parsed.netloc else "",
        }
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, headers=headers) as thumb_client:
            t_resp = await thumb_client.get(thumb_url)
            if t_resp.status_code == 200 and len(t_resp.content) > 500:
                raw_thumb.write_bytes(t_resp.content)
                ok, _ = await _run_ffmpeg_thumbnail(raw_thumb, resolved_thumb, seek=None, timeout=15.0)
                if ok and resolved_thumb.stat().st_size <= 200 * 1024:
                    return True
    except Exception as e:
        logger.debug(f"Fallback thumbnail download/convert failed for {thumb_url}: {e}")
    finally:
        raw_thumb.unlink(missing_ok=True)
    return False

async def generate_stream_thumbnail_and_metadata(url: str, headers: dict, thumb_path: Path, file_size: int = 0) -> tuple[dict, bool]:
    meta = {'duration': 0, 'width': 0, 'height': 0}
    resolved_thumb = Path(thumb_path).resolve()
    ok, out = await _run_ffmpeg_thumbnail(url, resolved_thumb, seek="00:00:05.000", headers=headers, timeout=20.0)
    if out:
        _parse_ffmpeg_metadata_output(out, meta)

    if ok and resolved_thumb.stat().st_size < 3000 and meta['duration'] > 20:
        alt_thumb = resolved_thumb.with_name(f"{resolved_thumb.stem}_alt.jpg")
        try:
            alt_seek = _format_ffmpeg_seek(min(30, max(15, meta['duration'] // 10)))
            alt_ok, _ = await _run_ffmpeg_thumbnail(url, alt_thumb, seek=alt_seek, headers=headers, timeout=15.0)
            if alt_ok and alt_thumb.stat().st_size > resolved_thumb.stat().st_size:
                alt_thumb.replace(resolved_thumb)
        finally:
            alt_thumb.unlink(missing_ok=True)
    elif not ok and meta['duration'] > 0:
        fallback_seek = _format_ffmpeg_seek(max(0, meta['duration'] // 2))
        ok, _ = await _run_ffmpeg_thumbnail(url, resolved_thumb, seek=fallback_seek, headers=headers, timeout=15.0)

    has_thumb = ok and resolved_thumb.exists() and resolved_thumb.stat().st_size > 0

    if not has_thumb or meta['duration'] == 0:
        chunk_path = resolved_thumb.with_suffix('.part.mp4')
        try:
            chunk_headers = dict(headers or {})
            chunk_headers["Range"] = f"bytes=0-{STREAM_PROBE_BYTES - 1}"
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as hclient:
                written = 0
                async with hclient.stream("GET", url, headers=chunk_headers) as r:
                    if r.status_code in (200, 206):
                        with open(chunk_path, "wb") as cf:
                            async for chunk in r.aiter_bytes(65536):
                                cf.write(chunk)
                                written += len(chunk)
                                if written >= STREAM_PROBE_BYTES:
                                    break
                if written > 1024:
                    if not has_thumb:
                        has_thumb = await generate_thumbnail(chunk_path, resolved_thumb, duration=meta['duration'])
                    if meta['duration'] == 0 or not meta['width']:
                        part_meta = get_video_metadata(chunk_path)
                        if meta['duration'] == 0 and part_meta['duration'] > 0:
                            meta['duration'] = part_meta['duration']
                        if not meta['width']:
                            meta['width'], meta['height'] = part_meta['width'], part_meta['height']

                    if meta['duration'] == 0 and file_size > STREAM_PROBE_BYTES + 2097152:
                        tail_headers = dict(headers or {})
                        tail_headers["Range"] = f"bytes={file_size - 2097152}-{file_size - 1}"
                        tail_path = resolved_thumb.with_suffix('.tail.mp4')
                        try:
                            tail_written = 0
                            async with hclient.stream("GET", url, headers=tail_headers) as tr:
                                if tr.status_code in (200, 206):
                                    with open(tail_path, "wb") as tf:
                                        async for chunk in tr.aiter_bytes(65536):
                                            tf.write(chunk)
                                            tail_written += len(chunk)
                            if tail_written > 1024:
                                tail_meta = get_video_metadata(tail_path)
                                if tail_meta['duration'] > 0:
                                    meta['duration'] = tail_meta['duration']
                        finally:
                            if tail_path.exists(): tail_path.unlink(missing_ok=True)
        except Exception as e:
            logger.debug(f"Stream chunk probe fallback failed: {e}")
        finally:
            if chunk_path.exists(): chunk_path.unlink(missing_ok=True)

    return meta, has_thumb

async def _send_video_with_thumb(
    target_channel_id: int,
    uploaded_file,
    clean_name: str,
    rich_caption: str,
    metadata: dict,
    thumb_path: Path | None,
    has_thumb: bool,
    topic_id=None,
):
    """Send an uploaded video with validated thumbnail, video attributes, and video MIME type."""
    valid_thumb = None
    if has_thumb and thumb_path:
        resolved_thumb = Path(thumb_path).resolve()
        if resolved_thumb.exists() and resolved_thumb.stat().st_size > 0:
            valid_thumb = str(resolved_thumb)

    mime_type = mimetypes.guess_type(clean_name)[0]
    if not mime_type or not mime_type.startswith("video/"):
        mime_type = "video/mp4"

    attributes = [
        DocumentAttributeVideo(
            duration=int(metadata.get('duration') or 0),
            w=int(metadata.get('width') or (320 if valid_thumb else 0)),
            h=int(metadata.get('height') or (320 if valid_thumb else 0)),
            supports_streaming=True,
        ),
        DocumentAttributeFilename(file_name=clean_name),
    ]

    return await client.send_file(
        target_channel_id,
        uploaded_file,
        caption=rich_caption,
        parse_mode='html',
        supports_streaming=True,
        mime_type=mime_type,
        attributes=attributes,
        thumb=valid_thumb,
        reply_to=int(topic_id) if topic_id else None,
    )


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


async def download_and_upload(event, url, page_range_str, target_channel_id=None, selected_videos=None, topic_id=None):
    user_id = event.sender_id
    LAST_TASK_INFO[user_id] = {
        'url': url,
        'range': page_range_str,
        'dest': target_channel_id,
        'selected_videos': selected_videos,
        'topic_id': topic_id
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
            await event.respond("❌ <b>Invalid URL.</b>\nSupported formats: Viralchut, Coomer creator/post URLs, Bunkr album/video URLs.", parse_mode='html')
            return
            
        page_range = parse_page_range(page_range_str) if page_range_str else None
        
        safe_folder = re.sub(r'[^a-zA-Z0-9_-]', '_', f"{service}_{cur_user_id}_{event.id}")[:64]
        temp_dir = Path(DOWNLOAD_DIR) / safe_folder
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
                    f"📊 <b>Pipeline Progress ({'⚡ RAM Stream' if ENABLE_STREAM_UPLOAD else '💾 Disk Pipeline'}):</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📥 <b>Processed:</b> {state['downloaded']}/{state['total']} videos\n"
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

        # Pipeline worker definitions
        async def disk_pipeline_worker(vid, pos=None):
            vid = dict(vid)
            if 'name' not in vid or not vid['name'].lower().endswith(('.mp4', '.mkv', '.webm', '.avi', '.mov', '.m4v')):
                vid['name'] = (vid.get('name') or 'video') + '.mp4'
            need_release = False
            if pos is None:
                pos = await BAR_MANAGER.get_pos()
                need_release = True
            vid_name = vid.get('name') or vid.get('title') or "video"
            state["active_downloads"][vid_name] = "<code>[Connecting...] ⏳</code>"

            last_dl_pbar_time = [0.0]
            def dl_progress_callback(current, total):
                if time.time() - last_dl_pbar_time[0] >= 2.0:
                    state["active_downloads"][vid_name] = get_progress_bar(current, total)
                    last_dl_pbar_time[0] = time.time()

            filepath = None
            thumb_path = None
            has_thumb = False
            try:
                download_res = await download_video(
                    domain, vid, temp_dir, pos=pos, progress_callback=dl_progress_callback
                )
                if download_res in ["skipped_size", "skipped_small", "error_html"]:
                    state["skipped"] += 1
                    return
                filepath = temp_dir / f"{vid['id']}_{vid['name']}"
                if not (download_res and filepath.exists()):
                    state["failed"] += 1
                    return

                state["downloaded"] += 1
                state["active_downloads"].pop(vid_name, None)

                # Upload phase
                file_size = filepath.stat().st_size
                state["active_uploads"][vid['name']] = "<code>[Processing Metadata...] ⚙️</code>"

                last_pbar_time = [0.0]
                async def progress_callback(current, total):
                    if time.time() - last_pbar_time[0] > 3:
                        state["active_uploads"][vid['name']] = get_progress_bar(current, total)
                        last_pbar_time[0] = time.time()

                metadata = get_video_metadata(filepath)
                thumb_path = filepath.with_name(f"{filepath.stem}_{uuid.uuid4().hex[:8]}.jpg")
                has_thumb = await generate_thumbnail(filepath, thumb_path, duration=metadata.get('duration', 0))
                if not has_thumb:
                    has_thumb = await _fetch_fallback_thumbnail(vid, thumb_path)

                ai_meta = await generate_ai_caption(vid, service, cur_user_id)
                rich_caption = ai_meta["rich_caption"]
                db_title = ai_meta["db_title"]
                clean_name = ai_meta.get("file_name") or vid['name']

                sent_msg = None
                for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
                    try:
                        with tqdm(total=file_size, unit="B", unit_scale=True, desc=f"   [UP] {vid['name'][:20]}", position=pos, leave=False, disable=not _IS_TTY) as t_pbar:
                            async def upload_progress(current, total):
                                await progress_callback(current, total)
                                t_pbar.n = current
                                t_pbar.refresh()
                            with open(filepath, "rb") as f:
                                uploaded_file = await upload_file(
                                    client, f, progress_callback=upload_progress,
                                    file_name=clean_name, connection_count=MTPROTO_CONNECTIONS
                                )
                            sent_msg = await _send_video_with_thumb(
                                target_channel_id, uploaded_file, clean_name,
                                rich_caption, metadata, thumb_path, has_thumb, topic_id=topic_id
                            )
                        break
                    except Exception as e:
                        if attempt == MAX_UPLOAD_RETRIES:
                            raise e
                        logger.warning(f"Upload attempt {attempt} failed for {vid['name']}: {e}. Retrying in {RETRY_DELAY}s...")
                        await asyncio.sleep(RETRY_DELAY)

                # Automated indexing into Supabase if configured
                if sent_msg:
                    await _index_to_supabase(db_title, sent_msg.id, file_size)

                state["uploaded"] += 1
            except Exception as e:
                logger.error(f"Error processing {vid_name}: {e}")
                state["failed"] += 1
            finally:
                state["active_downloads"].pop(vid_name, None)
                state["active_uploads"].pop(vid['name'], None)
                if has_thumb and thumb_path and thumb_path.exists():
                    try: os.remove(thumb_path)
                    except Exception: pass
                if filepath and filepath.exists():
                    try: os.remove(filepath)
                    except Exception: pass
                if need_release:
                    await BAR_MANAGER.release_pos(pos)

        async def stream_pipeline_worker(vid):
            vid = dict(vid)
            if 'name' not in vid or not vid['name'].lower().endswith(('.mp4', '.mkv', '.webm', '.avi', '.mov', '.m4v')):
                vid['name'] = (vid.get('name') or 'video') + '.mp4'
            pos = await BAR_MANAGER.get_pos()
            vid_name = vid.get('name') or vid.get('title') or "video"
            state["active_uploads"][vid_name] = "<code>[Connecting Stream...] ⏳</code>"

            last_pbar_time = [0.0]
            async def progress_callback(current, total):
                if time.time() - last_pbar_time[0] >= 2.0:
                    state["active_uploads"][vid_name] = get_progress_bar(current, total)
                    last_pbar_time[0] = time.time()

            thumb_path = None
            has_thumb = False
            try:
                info = await resolve_media_stream_info(domain, vid)
                status = info.get("status")

                if status in ["skipped_size", "skipped_small"]:
                    state["skipped"] += 1
                    return
                elif status == "error_html" or not info.get("url") or not info.get("file_size"):
                    logger.warning(f"Stream resolution unavailable for {vid_name} ({info.get('error', 'no length')}). Falling back to disk pipeline...")
                    await disk_pipeline_worker(vid, pos)
                    return

                stream_url = info["url"]
                stream_headers = {k: v for k, v in info.get("headers", {}).items() if k.lower() != "range"}
                file_size = info["file_size"]
                stream_filename = info.get("name") or f"{vid['id']}_{vid_name}"

                state["active_uploads"][vid_name] = "<code>[Probing Stream...] ⚙️</code>"
                thumb_path = temp_dir / f"{vid['id']}_{uuid.uuid4().hex[:8]}_thumb.jpg"
                meta, has_thumb = await generate_stream_thumbnail_and_metadata(stream_url, stream_headers, thumb_path, file_size=file_size)

                if not has_thumb:
                    has_thumb = await _fetch_fallback_thumbnail(vid, thumb_path)

                ai_meta = await generate_ai_caption(vid, service, cur_user_id)
                rich_caption = ai_meta["rich_caption"]
                db_title = ai_meta["db_title"]
                clean_name = ai_meta.get("file_name") or stream_filename

                state["active_uploads"][vid_name] = "<code>[Streaming to Telegram...] 🚀</code>"

                sent_msg = None
                for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
                    try:
                        with tqdm(total=file_size, unit="B", unit_scale=True, desc=f"   [STREAM] {vid_name[:20]}", position=pos, leave=False, disable=not _IS_TTY) as t_pbar:
                            async def upload_progress(current, total):
                                await progress_callback(current, total)
                                t_pbar.n = current
                                t_pbar.refresh()

                            uploaded_file, _ = await upload_http_stream(
                                client=client,
                                url=stream_url,
                                file_name=clean_name,
                                headers=stream_headers,
                                progress_callback=upload_progress,
                                connection_count=MTPROTO_CONNECTIONS
                            )
                            sent_msg = await _send_video_with_thumb(
                                target_channel_id, uploaded_file, clean_name,
                                rich_caption, meta, thumb_path, has_thumb, topic_id=topic_id
                            )
                        break
                    except Exception as e:
                        if attempt == MAX_UPLOAD_RETRIES:
                            logger.warning(f"Stream upload failed for {vid_name} after {MAX_UPLOAD_RETRIES} attempts: {e}. Falling back to disk pipeline...")
                            await disk_pipeline_worker(vid, pos)
                            return
                        logger.warning(f"Stream attempt {attempt} failed for {vid_name}: {e}. Retrying in {RETRY_DELAY}s...")
                        await asyncio.sleep(RETRY_DELAY)

                if sent_msg:
                    await _index_to_supabase(db_title, sent_msg.id, file_size)

                state["uploaded"] += 1
                state["downloaded"] += 1
            except Exception as e:
                logger.error(f"Error in stream pipeline for {vid_name}: {e}")
                state["failed"] += 1
            finally:
                state["active_uploads"].pop(vid_name, None)
                if has_thumb and thumb_path and thumb_path.exists():
                    try: os.remove(thumb_path)
                    except Exception: pass
                await BAR_MANAGER.release_pos(pos)

        if ENABLE_STREAM_UPLOAD:
            # Direct RAM-buffered streaming pipeline (zero disk usage for media files)
            stream_sem = asyncio.Semaphore(STREAM_CONCURRENCY)
            async def bounded_stream(vid):
                async with stream_sem:
                    await stream_pipeline_worker(vid)

            await asyncio.gather(*[bounded_stream(v) for v in videos])
        else:
            # Traditional disk-buffered pipeline
            upload_queue = asyncio.Queue(maxsize=3)

            async def download_worker(vid):
                pos = await BAR_MANAGER.get_pos()
                vid_name = vid.get('name') or vid.get('title') or "video"
                state["active_downloads"][vid_name] = "<code>[Connecting...] ⏳</code>"

                last_dl_pbar_time = [0.0]
                def dl_progress_callback(current, total):
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
                    vid = dict(vid)
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
                        thumb_path = filepath.with_name(f"{filepath.stem}_{uuid.uuid4().hex[:8]}.jpg")
                        has_thumb = await generate_thumbnail(filepath, thumb_path, duration=metadata.get('duration', 0))
                        if not has_thumb:
                            has_thumb = await _fetch_fallback_thumbnail(vid, thumb_path)
                        ai_meta = await generate_ai_caption(vid, service, cur_user_id)
                        rich_caption = ai_meta["rich_caption"]
                        db_title = ai_meta["db_title"]
                        clean_name = ai_meta.get("file_name") or vid['name']

                        sent_msg = None
                        for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
                            try:
                                with tqdm(total=file_size, unit="B", unit_scale=True, desc=f"   [UP] {vid['name'][:20]}", position=pos, leave=False, disable=not _IS_TTY) as t_pbar:
                                    async def upload_progress(current, total):
                                        await progress_callback(current, total)
                                        t_pbar.n = current
                                        t_pbar.refresh()
                                    with open(filepath, "rb") as f:
                                        uploaded_file = await upload_file(
                                            client, f, progress_callback=upload_progress,
                                            file_name=clean_name, connection_count=MTPROTO_CONNECTIONS
                                        )
                                    sent_msg = await _send_video_with_thumb(
                                        target_channel_id, uploaded_file, clean_name,
                                        rich_caption, metadata, thumb_path, has_thumb, topic_id=topic_id
                                    )
                                break
                            except Exception as e:
                                if attempt == MAX_UPLOAD_RETRIES:
                                    raise e
                                logger.warning(f"Upload attempt {attempt} failed for {vid['name']}: {e}. Retrying in {RETRY_DELAY}s...")
                                await asyncio.sleep(RETRY_DELAY)

                        if sent_msg:
                            await _index_to_supabase(db_title, sent_msg.id, file_size)

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

            upload_tasks = [asyncio.create_task(upload_worker()) for _ in range(1)]
            dl_sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)
            async def bounded_download(vid):
                async with dl_sem:
                    await download_worker(vid)

            await asyncio.gather(*[bounded_download(v) for v in videos])

            for _ in range(2):
                await upload_queue.put(None)
            await asyncio.gather(*upload_tasks)

        state["running"] = False
        if status_task:
            status_task.cancel()
            try:
                await status_task
            except (asyncio.CancelledError, Exception):
                pass

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
        "• <b>XMoviesForYou:</b> HD Videos, Studios, Pornstars, Categories, Playlists (<code>https://xmoviesforyou.com/</code>)\n"
        "• <b>UXSeries:</b> Actresses, Series, Episodes, Platforms (<code>https://uxseries.co/</code>)\n"
        "• <b>HDMaal:</b> Models, OTT, Web Series (<code>https://hdmaal.movie/</code>)\n"
        "• <b>Universal Web Scraper & Crawler:</b> Any website, tube, blog, gallery, or direct video\n"
        "• <b>Viralchut:</b> Single videos, Categories, Tags, Search, Latest\n"
        "• <b>Bunkr:</b> Albums (<code>/a/</code>) & Videos (<code>/v/</code>, <code>/f/</code>)\n"
        "• <b>PimpBunny:</b> Videos, Categories, Models, Tags (<code>https://pimpbunny.com/</code>)• <b>Coomer / Kemono:</b> Creator profiles & single posts\n\n"
        "📊 <b>Current System Overview:</b>\n"
        f"• <b>Status:</b> 🟢 Online & MTProto Ready\n"
        f"• <b>Storage Channels:</b> <code>{', '.join(str(c) for c in STORAGE_CHANNELS)}</code>\n"
        f"• <b>Active Tasks:</b> <code>{active_count}</code>\n"
        f"• <b>Temp Cache:</b> <code>{temp_size_mb} MB</code>\n"
        f"• <b>Supabase Auto-Index:</b> {'🟢 Enabled' if supabase_client else '⚪ Disabled'}\n"
        f"• <b>Mistral AI:</b> {'⚡ Active' if MISTRAL_API_KEY else '⚪ Disabled'}\n"
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
    
    # Safe storage channel buttons
    for idx, cid in enumerate(STORAGE_CHANNELS):
        if cid and str(cid).startswith("-100"):
            clean_id = str(abs(cid))[3:]
            label = f"📂 Storage Channel {idx+1}" if len(STORAGE_CHANNELS) > 1 else "📂 View Storage Channel"
            buttons.append([Button.url(label, f"https://t.me/c/{clean_id}")])
    
    await event.respond(text, parse_mode='html', buttons=buttons)

async def show_topic_options(event, user_id, dest_id):
    USER_STATES[user_id]['dest'] = dest_id
    text = (
        "🧵 <b>Forum Topic Selection</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 <b>Destination Group:</b> <code>{dest_id}</code>\n\n"
        "Where should the files be uploaded?\n"
        "• <b>➕ Create New Topic:</b> Start a new topic (bot creates it in group)\n"
        "• <b>📑 Choose Existing Topic:</b> Pick from active topics in this group\n"
        "• <b>💬 General Chat:</b> Upload without a specific topic"
    )
    buttons = [
        [Button.inline("➕ Create New Topic", b"topic_create")],
        [Button.inline("📑 Choose Existing Topic", b"topic_existing_0")],
        [Button.inline("💬 General / Main Chat", b"topic_general")],
        [Button.inline("❌ Cancel", b"cancel_flow")]
    ]
    try:
        await event.edit(text, parse_mode='html', buttons=buttons)
    except Exception:
        await event.respond(text, parse_mode='html', buttons=buttons)

async def handle_dest_selected(event, user_id, dest_id):
    USER_STATES[user_id]['dest'] = dest_id
    if await is_forum_chat(dest_id):
        await show_topic_options(event, user_id, dest_id)
    else:
        USER_STATES[user_id]['topic_id'] = None
        USER_STATES[user_id]['topic_title'] = None
        await start_confirmed_download(event, user_id)

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
            "Please send the XMoviesForYou, UXSeries, HDMaal, Viralchut, Coomer, or Bunkr URL you want to scrape.\n\n"
            "💡 <i>Examples:</i>\n"
            "• <code>https://xmoviesforyou.com/page/7</code>\n"
            "• <code>https://xmoviesforyou.com/devilsfilm-serena-sterling-deepthroat-diagnosis</code>\n"
            "• <code>https://uxseries.co/actress/bharti-jha/</code>\n"
            "• <code>https://hdmaal.movie/model/bharti-jha/</code>\n"
            "• <code>https://hdmaal.movie/jaan-bujh-kar-s2-episode-5/</code>\n"
            "• <code>https://uxseries.co/series/shatir/</code>\n"
            "• <code>https://viralchut.com/video-slug/</code>\n"
            "• <code>https://bunkr.cr/a/album_id</code>\n"
            "• <code>https://coomer.st/onlyfans/user/example</code>", 
            parse_mode='html',
            buttons=[Button.inline("❌ Cancel", b"cancel_flow")]
        )
        await event.answer()
        
    elif data == "cancel_flow":
        USER_STATES.pop(user_id, None)
        await event.edit("❌ <b>Process Cancelled.</b>", parse_mode='html')
        await event.answer("Cancelled", alert=False)
        
    elif data == "clear_temp":
        if RUNNING_TASKS:
            await event.answer("⚠️ Active tasks are running. Cancel them first.", alert=True)
            return
        if os.path.exists(DOWNLOAD_DIR):
            shutil.rmtree(DOWNLOAD_DIR)
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            await event.answer("🧹 Temporary files cleared!", alert=True)
        else:
            await event.answer("Temp folder is already empty.")

    elif data == "stats_info":
        total_idx = await _get_supabase_media_count()
        stat_text = (
            "📊 <b>Uploader & Storage Overview</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"• <b>Active Upload Tasks:</b> <code>{len(RUNNING_TASKS)}</code>\n"
            f"• <b>Storage Channel ID:</b> <code>{STORAGE_CHANNEL_ID}</code>\n"
            f"• <b>Indexed Supabase Videos:</b> <code>{total_idx}</code>\n"
            f"• <b>Temp Download Dir:</b> <code>{DOWNLOAD_DIR}</code>\n"
            f"• <b>Upload Pipeline:</b> Sequential MTProto Queue\n"
            f"• <b>Mistral AI:</b> {'⚡ Active' if MISTRAL_API_KEY else '⚪ Disabled'}\n"
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
            "• /viralchut [url] - 🔥 Quick Viralchut scraper\n"
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
            asyncio.create_task(download_and_upload(
                event,
                info['url'],
                info['range'],
                info['dest'],
                selected_videos=info.get('selected_videos'),
                topic_id=info.get('topic_id')
            ))
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
            await handle_dest_selected(event, user_id, STORAGE_CHANNEL_ID)
        else:
            await event.answer("Session expired, please paste the link again.", alert=True)

    elif data == "quick_choose_dest":
        if user_id in USER_STATES:
            await ask_destination(event, user_id)
            await event.answer()
        else:
            await event.answer("Session expired, please paste the link again.", alert=True)

    elif data == "quick_dest_topic":
        if user_id in USER_STATES:
            await show_topic_options(event, user_id, STORAGE_CHANNEL_ID)
        else:
            await event.answer("Session expired, please paste the link again.", alert=True)

    elif data.startswith("dest_pick:"):
        if user_id not in USER_STATES:
            await event.answer("Session expired.", alert=True)
            return
        cid = int(data.split(":")[1])
        await handle_dest_selected(event, user_id, cid)
        await event.answer()

    elif data.startswith("dest_topic_pick:"):
        if user_id not in USER_STATES:
            await event.answer("Session expired.", alert=True)
            return
        cid = int(data.split(":")[1])
        await show_topic_options(event, user_id, cid)
        await event.answer()

    elif data in ("range_all", "range_custom", "dest_default", "dest_default_topic", "dest_custom"):
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
            await handle_dest_selected(event, user_id, STORAGE_CHANNEL_ID)
            await event.answer()

        elif data == "dest_default_topic":
            await show_topic_options(event, user_id, STORAGE_CHANNEL_ID)
            await event.answer()
            
        elif data == "dest_custom":
            USER_STATES[user_id]['state'] = 'WAITING_DEST_INPUT'
            await event.respond("🆔 Please send the numeric Channel ID (e.g., <code>-100123456789</code>).", parse_mode='html', buttons=[Button.inline("❌ Cancel", b"cancel_flow")])
            await event.answer()

    elif data == "topic_create":
        if user_id not in USER_STATES:
            await event.answer("Session expired. Please start again.", alert=True)
            return
        USER_STATES[user_id]['state'] = 'WAITING_TOPIC_TITLE'
        prompt_text = (
            "✏️ <b>Enter New Topic Title:</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Please send the title/name for the new forum topic in your group.\n\n"
            "💡 <i>Example:</i> <code>Serena Sterling Collection</code> or <code>Bunkr - Vacation</code>\n"
            "The bot will create the topic in your group and upload into it."
        )
        await event.respond(prompt_text, parse_mode='html', buttons=[[Button.inline("❌ Cancel", b"cancel_flow")]])
        await event.answer()

    elif data.startswith("topic_existing"):
        if user_id not in USER_STATES:
            await event.answer("Session expired. Please start again.", alert=True)
            return
        dest_id = USER_STATES[user_id].get('dest', STORAGE_CHANNEL_ID)
        page_num = 0
        try:
            parts = data.split("_")
            if len(parts) >= 3:
                page_num = int(parts[2])
        except Exception:
            page_num = 0

        topics = await get_forum_topics_list(dest_id, limit=50)
        if not topics:
            await event.edit(
                "⚠️ <b>No Topics Found in this Group</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"No saved topics yet for group <code>{dest_id}</code>.\n\n"
                "• Tap <b>➕ Create New Topic</b> to have the bot create one.\n"
                "• Tap <b>🔢 Enter Topic ID</b> to enter an existing topic ID.\n"
                "• Or run <code>/addtopic &lt;id&gt; &lt;name&gt;</code>.",
                parse_mode='html',
                buttons=[
                    [Button.inline("➕ Create New Topic", b"topic_create")],
                    [Button.inline("🔢 Enter Topic ID Manually", b"topic_manual")],
                    [Button.inline("💬 General / Main Chat", b"topic_general")],
                    [Button.inline("🔙 Back", f"dest_topic_pick:{dest_id}".encode())]
                ]
            )
            await event.answer()
            return

        page_size = 5
        start_idx = page_num * page_size
        end_idx = min(len(topics), start_idx + page_size)
        cur_topics = topics[start_idx:end_idx]

        buttons = []
        for t in cur_topics:
            title_disp = (t['title'][:22] + "...") if len(t['title']) > 22 else t['title']
            cb = f"top_pick:{t['id']}:{t['title'][:16]}".encode()
            buttons.append([Button.inline(f"📑 {title_disp}", cb)])

        nav_row = []
        if page_num > 0:
            nav_row.append(Button.inline("⬅️ Prev", f"topic_existing_{page_num - 1}".encode()))
        if end_idx < len(topics):
            nav_row.append(Button.inline("Next ➡️", f"topic_existing_{page_num + 1}".encode()))
        if nav_row:
            buttons.append(nav_row)

        buttons.append([
            Button.inline("➕ Create New", b"topic_create"),
            Button.inline("🔢 Manual ID", b"topic_manual")
        ])
        buttons.append([
            Button.inline("💬 General", b"topic_general"),
            Button.inline("❌ Cancel", b"cancel_flow")
        ])

        text = (
            f"📑 <b>Select Topic ({start_idx + 1}-{end_idx} of {len(topics)}):</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Click a topic to upload into it:"
        )
        await event.edit(text, parse_mode='html', buttons=buttons)
        await event.answer()

    elif data.startswith("top_pick:"):
        if user_id not in USER_STATES:
            await event.answer("Session expired. Please start again.", alert=True)
            return
        parts = data.split(":", 2)
        topic_id = int(parts[1])
        dest_id = USER_STATES[user_id].get('dest', STORAGE_CHANNEL_ID)
        stored_title = load_forum_topics().get(str(dest_id), {}).get(str(topic_id), {}).get('title')
        topic_title = stored_title or (parts[2] if len(parts) > 2 else f"ID {topic_id}")
        USER_STATES[user_id]['topic_id'] = topic_id
        USER_STATES[user_id]['topic_title'] = topic_title
        await event.edit(f"✅ Selected Topic: <b>{topic_title}</b> (ID: <code>{topic_id}</code>).\nStarting upload...", parse_mode='html')
        await start_confirmed_download(event, user_id)
        await event.answer()

    elif data == "topic_manual":
        if user_id not in USER_STATES:
            await event.answer("Session expired. Please start again.", alert=True)
            return
        USER_STATES[user_id]['state'] = 'WAITING_TOPIC_ID'
        await event.respond(
            "🔢 <b>Enter Numeric Topic ID:</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Send the numeric Topic ID (Thread ID) to upload into (e.g. <code>2</code> or <code>12345</code>).\n\n"
            "💡 <i>Tip: Copy link to any message inside that topic. The number after the group ID is the topic ID.</i>",
            parse_mode='html',
            buttons=[[Button.inline("❌ Cancel", b"cancel_flow")]]
        )
        await event.answer()

    elif data == "topic_general":
        if user_id not in USER_STATES:
            await event.answer("Session expired. Please start again.", alert=True)
            return
        USER_STATES[user_id]['topic_id'] = None
        USER_STATES[user_id]['topic_title'] = "General / Main"
        await event.edit("💬 Selected <b>General / Main Chat</b>. Starting...", parse_mode='html')
        await start_confirmed_download(event, user_id)
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
                await handle_dest_selected(event, user_id, channel_id)
            except ValueError:
                await event.respond("❌ <b>Invalid ID.</b>\nPlease send a numeric Channel ID.", parse_mode='html')

        elif state == 'WAITING_TOPIC_TITLE':
            topic_title = text.strip()
            dest_id = USER_STATES[user_id].get('dest', STORAGE_CHANNEL_ID)
            status_m = await event.respond(f"⏳ Creating topic <b>{topic_title}</b> in group...", parse_mode='html')
            try:
                topic_id = await create_forum_topic_helper(dest_id, topic_title)
                if topic_id:
                    await status_m.edit(f"✅ Created topic <b>{topic_title}</b> (ID: <code>{topic_id}</code>)!")
                    USER_STATES[user_id]['topic_id'] = topic_id
                    USER_STATES[user_id]['topic_title'] = topic_title
                    await start_confirmed_download(event, user_id)
                else:
                    await status_m.edit(
                        "⚠️ Failed to obtain topic ID.\n"
                        "Please verify bot has 'Manage Topics' admin permission in the group.",
                        buttons=[
                            [Button.inline("➕ Try Again", b"topic_create")],
                            [Button.inline("📑 Choose Existing Topic", b"topic_existing_0")],
                            [Button.inline("💬 General / Main Chat", b"topic_general")],
                            [Button.inline("❌ Cancel", b"cancel_flow")]
                        ]
                    )
            except Exception as e:
                logger.error(f"Error creating topic: {e}")
                await status_m.edit(
                    f"❌ <b>Error creating topic:</b> <code>{e}</code>\n\n"
                    "<i>Make sure the bot has Admin rights with 'Manage Topics' enabled in the group.</i>",
                    parse_mode='html',
                    buttons=[
                        [Button.inline("➕ Try Again", b"topic_create")],
                        [Button.inline("📑 Choose Existing Topic", b"topic_existing_0")],
                        [Button.inline("💬 General / Main Chat", b"topic_general")],
                        [Button.inline("❌ Cancel", b"cancel_flow")]
                    ]
                )

        elif state == 'WAITING_TOPIC_ID':
            try:
                raw_parts = text.strip().split(maxsplit=1)
                topic_id = int(raw_parts[0])
                dest_id = USER_STATES[user_id].get('dest', STORAGE_CHANNEL_ID)
                if len(raw_parts) > 1 and raw_parts[1].strip():
                    topic_title = raw_parts[1].strip()
                else:
                    stored = load_forum_topics().get(str(dest_id), {}).get(str(topic_id))
                    if stored and stored.get('title'):
                        topic_title = stored['title']
                    else:
                        topic_title = f"Topic #{topic_id}"
                save_forum_topic(dest_id, topic_id, topic_title)
                USER_STATES[user_id]['topic_id'] = topic_id
                USER_STATES[user_id]['topic_title'] = topic_title
                await event.respond(f"✅ Selected Topic: <b>{topic_title}</b> (ID: <code>{topic_id}</code>).\nStarting upload...", parse_mode='html')
                await start_confirmed_download(event, user_id)
            except ValueError:
                await event.respond("❌ <b>Invalid ID.</b>\nPlease send a valid numeric Topic ID (e.g. <code>8</code> or <code>8 My Topic</code>).", parse_mode='html')
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
                    if len(STORAGE_CHANNELS) > 1:
                        buttons = [
                            [Button.inline("🚀 Quick Download (Default)", b"quick_download_confirm")],
                            [Button.inline("📍 Select Destination / Topic", b"quick_choose_dest")],
                            [Button.inline("❌ Cancel", b"cancel_flow")]
                        ]
                    else:
                        buttons = [
                            [Button.inline("🚀 Download Now", b"quick_download_confirm")],
                            [Button.inline("🧵 Topic Options", b"quick_dest_topic")],
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
                    "• <code>https://xmoviesforyou.com/page/7</code>\n"
                    "• <code>https://xmoviesforyou.com/devilsfilm-serena-sterling-deepthroat-diagnosis</code>\n"
                    "• <code>https://uxseries.co/actress/bharti-jha/</code>\n"
                    "• <code>https://hdmaal.movie/model/bharti-jha/</code>\n"
                    "• <code>https://hdmaal.movie/jaan-bujh-kar-s2-episode-5/</code>\n"
                    "• <code>https://uxseries.co/series/shatir/</code>\n"
                    "• <code>https://uxseries.co/episode/shatir-episode-1/</code>\n"
                    "• <code>https://viralchut.com/video-slug/</code>\n"
                    "• <code>https://viralchut.com/category/name/</code>\n"
                    "• <code>https://bunkr.cr/a/album_id</code>\n"
                    "• <code>https://bunkr.cr/v/video_id</code>\n"
                    "• <code>https://coomer.st/onlyfans/user/...</code>",
                    parse_mode='html'
                )

async def ask_destination(event, user_id):
    USER_STATES[user_id]['state'] = 'WAITING_CHANNEL'
    buttons = []
    if len(STORAGE_CHANNELS) <= 1:
        buttons.append([Button.inline("📌 Default Group/Channel", b"dest_default")])
        buttons.append([Button.inline("🧵 Topic in Default Group", b"dest_default_topic")])
    else:
        for idx, cid in enumerate(STORAGE_CHANNELS):
            name = await get_chat_title_cached(cid)
            is_forum = await is_forum_chat(cid)
            label = f"📌 {name[:20]}" if name else f"📌 Channel {idx+1}"
            row = [Button.inline(label, f"dest_pick:{cid}".encode())]
            if is_forum:
                row.append(Button.inline("🧵 Topics", f"dest_topic_pick:{cid}".encode()))
            buttons.append(row)

    buttons.append([Button.inline("🆔 Custom Channel ID", b"dest_custom")])
    buttons.append([Button.inline("❌ Cancel", b"cancel_flow")])
    
    desc_text = "Where should I upload the videos?\n"
    if len(STORAGE_CHANNELS) > 1:
        desc_text += "• <b>Configured Channels:</b> Select one from the buttons below\n"
    else:
        desc_text += "• <b>Default:</b> Auto-detects if group has topics\n• <b>Topic:</b> Create or choose topic in default group\n"
    desc_text += "• <b>Custom:</b> Enter another channel or group ID"

    await event.respond(
        "📍 <b>Step 3: Choose Destination</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n" + desc_text, 
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
    topic_id = info.get('topic_id')
    topic_title = info.get('topic_title')
    
    topic_info = f"\n🧵 <b>Topic:</b> {topic_title} (<code>{topic_id}</code>)" if topic_id else ""
    if selected_videos:
        await event.respond(
            f"✅ <b>Starting Download!</b>\n\n"
            f"🔗 <b>Source:</b> {url}\n"
            f"📦 <b>Selected Files:</b> {len(selected_videos)} items\n"
            f"📍 <b>Destination:</b> <code>{dest}</code>{topic_info}",
            parse_mode='html'
        )
        asyncio.create_task(download_and_upload(event, url, None, dest, selected_videos=selected_videos, topic_id=topic_id))
    else:
        await event.respond(
            f"✅ <b>Starting Download!</b>\n\n"
            f"🔗 <b>Source:</b> {url}\n"
            f"📄 <b>Range:</b> {page_range or 'All'}\n"
            f"📍 <b>Destination:</b> <code>{dest}</code>{topic_info}",
            parse_mode='html'
        )
        asyncio.create_task(download_and_upload(event, url, page_range, dest, topic_id=topic_id))

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
            "• <code>/download &lt;url&gt; &lt;pages&gt;</code> - Download specific pages (e.g. <code>1-3</code>)\n"
            "• <code>/crawl &lt;url&gt;</code> - Crawl & select videos from any webpage\n\n"
            "<b>Supported Platforms:</b>\n"
            "• <b>Universal Web:</b> Any website, tube, blog, gallery, or direct media\n"
            "• <b>Viralchut:</b> Single videos, Categories, Tags, Search\n"
            "• <b>Bunkr:</b> Albums (<code>/a/</code>) & Videos (<code>/v/</code>, <code>/f/</code>)\n"
            "• <b>Coomer / Kemono:</b> Creator profiles & single posts",
            parse_mode='html'
        )
        return
    dest_id = STORAGE_CHANNEL_ID
    topic_id = None
    if event.is_group:
        dest_id = event.chat_id
        if getattr(event.message, 'reply_to', None):
            topic_id = getattr(event.message.reply_to, 'reply_to_top_id', None) or getattr(event.message.reply_to, 'reply_to_msg_id', None)
    asyncio.create_task(download_and_upload(event, url, page_range, dest_id, topic_id=topic_id))

@client.on(events.NewMessage(pattern=r"^/pimpbunny(?:\s+(https?://\S+)(?:\s+([\d-]+))?)?$"))
async def pimpbunny_command_handler(event):
    if not await check_admin_or_notify(event): return
    url = event.pattern_match.group(1)
    page_range = event.pattern_match.group(2)
    if not url:
        await event.respond(
            "🔥 <b>Viralchut Scraper</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Usage:\n"
            "• <code>/pimpbunny https://pimpbunny.com/video-slug/</code>\n"
            "• <code>/pimpbunny https://pimpbunny.com/category/desi-sex-scandal/</code>\n"
            "• <code>/pimpbunny https://pimpbunny.com/tag/bhabhi-sex-mms/</code>\n"
            "• <code>/pimpbunny https://pimpbunny.com/ 1-3</code>\n\n"
            "Or simply paste any Viralchut link directly into this chat!",
            parse_mode='html'
        )
        return
    dest_id = STORAGE_CHANNEL_ID
    topic_id = None
    if event.is_group:
        dest_id = event.chat_id
        if getattr(event.message, 'reply_to', None):
            topic_id = getattr(event.message.reply_to, 'reply_to_top_id', None) or getattr(event.message.reply_to, 'reply_to_msg_id', None)
    asyncio.create_task(download_and_upload(event, url, page_range, dest_id, topic_id=topic_id))

@client.on(events.NewMessage(pattern=r"^/uxseries(?:\s+(https?://\S+)(?:\s+([\d-]+))?)?$"))
async def uxseries_command_handler(event):
    if not await check_admin_or_notify(event): return
    url = event.pattern_match.group(1)
    page_range = event.pattern_match.group(2)
    if not url:
        await event.respond(
            "✨ <b>UXSeries Scraper</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Scrape HD Web Series, Actresses, Episodes & Platforms from UXSeries!\n\n"
            "<b>Usage:</b>\n"
            "• <code>/uxseries https://uxseries.co/actress/bharti-jha/</code>\n"
            "• <code>/uxseries https://uxseries.co/actress/bharti-jha/ 1-2</code>\n"
            "• <code>/uxseries https://uxseries.co/series/shatir/</code>\n"
            "• <code>/uxseries https://uxseries.co/episode/shatir-episode-1/</code>\n"
            "• <code>/uxseries https://uxseries.co/platform/ullu/</code>\n\n"
            "Or simply paste any UXSeries link directly into this chat!",
            parse_mode='html'
        )
        return
    dest_id = STORAGE_CHANNEL_ID
    topic_id = None
    if event.is_group:
        dest_id = event.chat_id
        if getattr(event.message, 'reply_to', None):
            topic_id = getattr(event.message.reply_to, 'reply_to_top_id', None) or getattr(event.message.reply_to, 'reply_to_msg_id', None)
    asyncio.create_task(download_and_upload(event, url, page_range, dest_id, topic_id=topic_id))

@client.on(events.NewMessage(pattern=r"^/hdmaal(?:\s+(https?://\S+)(?:\s+([\d-]+))?)?$"))
async def hdmaal_command_handler(event):
    if not await check_admin_or_notify(event): return
    url = event.pattern_match.group(1)
    page_range = event.pattern_match.group(2)
    if not url:
        await event.respond(
            "🎬 <b>HDMaal Scraper</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Scrape 18+ Web Series, Models, Episodes & OTTs from HDMaal!\n\n"
            "<b>Usage:</b>\n"
            "• <code>/hdmaal https://hdmaal.movie/model/bharti-jha/</code>\n"
            "• <code>/hdmaal https://hdmaal.movie/model/bharti-jha/ 1-2</code>\n"
            "• <code>/hdmaal https://hdmaal.movie/jaan-bujh-kar-s2-episode-5/</code>\n"
            "• <code>/hdmaal https://hdmaal.movie/ott/voovi/</code>\n\n"
            "Or simply paste any HDMaal link directly into this chat!",
            parse_mode='html'
        )
        return
    dest_id = STORAGE_CHANNEL_ID
    topic_id = None
    if event.is_group:
        dest_id = event.chat_id
        if getattr(event.message, 'reply_to', None):
            topic_id = getattr(event.message.reply_to, 'reply_to_top_id', None) or getattr(event.message.reply_to, 'reply_to_msg_id', None)
    asyncio.create_task(download_and_upload(event, url, page_range, dest_id, topic_id=topic_id))

@client.on(events.NewMessage(pattern=r"^/xmovies(?:\s+(https?://\S+)(?:\s+([\d-]+))?)?$"))
async def xmovies_command_handler(event):
    if not await check_admin_or_notify(event): return
    url = event.pattern_match.group(1)
    page_range = event.pattern_match.group(2)
    if not url:
        await event.respond(
            "🎬 <b>XMoviesForYou Scraper</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Scrape HD Videos, Studios, Pornstars, Categories & Playlists from XMoviesForYou!\n\n"
            "<b>Usage:</b>\n"
            "• <code>/xmovies https://xmoviesforyou.com/page/7</code>\n"
            "• <code>/xmovies https://xmoviesforyou.com/page/7 7-8</code>\n"
            "• <code>/xmovies https://xmoviesforyou.com/devilsfilm-serena-sterling-deepthroat-diagnosis</code>\n"
            "• <code>/xmovies https://xmoviesforyou.com/studio/devilsfilm</code>\n"
            "• <code>/xmovies https://xmoviesforyou.com/pornstar/serena-sterling</code>\n\n"
            "Or simply paste any XMoviesForYou link directly into this chat!",
            parse_mode='html'
        )
        return
    dest_id = STORAGE_CHANNEL_ID
    topic_id = None
    if event.is_group:
        dest_id = event.chat_id
        if getattr(event.message, 'reply_to', None):
            topic_id = getattr(event.message.reply_to, 'reply_to_top_id', None) or getattr(event.message.reply_to, 'reply_to_msg_id', None)
    asyncio.create_task(download_and_upload(event, url, page_range, dest_id, topic_id=topic_id))

@client.on(events.NewMessage(pattern=r"^/crawl(?:\s+(https?://\S+)(?:\s+([\d-]+))?)?$"))
async def crawl_command_handler(event):
    if not await check_admin_or_notify(event): return
    url = event.pattern_match.group(1)
    page_range = event.pattern_match.group(2)
    if not url:
        await event.respond(
            "🕷️ <b>Universal Web Video Crawler</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "Crawl and scrape video links from <b>any</b> website, gallery, blog, tube, or profile!\n\n"
            "<b>Usage:</b>\n"
            "• <code>/crawl https://example.com/gallery/</code>\n"
            "• <code>/crawl https://example.com/videos/ 1-3</code>\n\n"
            "Or simply paste any URL directly into this chat!",
            parse_mode='html'
        )
        return

    user_id = event.sender_id
    USER_STATES[user_id] = {'url': url, 'range': page_range, 'dest': STORAGE_CHANNEL_ID}
    status_m = await event.respond("⏳ <b>Crawling Webpage for Videos...</b>\nScanning source and extracting file list, please wait...", parse_mode='html')
    try:
        domain, service, cur_id, post_id = parse_media_url(url)
        pr = parse_page_range(page_range) if page_range else None
        posts = await fetch_all_posts(domain, service, cur_id, page_range=pr, post_id=None)
        if not posts:
            await status_m.edit("❌ <b>No videos found on this webpage.</b>", parse_mode='html')
            return
        videos = extract_video_urls(domain, posts)
        if not videos:
            await status_m.edit("⚠️ <b>No downloadable videos found on this webpage.</b>", parse_mode='html')
            return
        USER_STATES[user_id]['videos'] = videos
        USER_STATES[user_id]['selected'] = set()
        USER_STATES[user_id]['browser_page'] = 0
        text, buttons = render_album_browser(user_id)
        await status_m.edit(text, parse_mode='html', buttons=buttons)
    except Exception as e:
        logger.error(f"Error crawling webpage: {e}")
        await status_m.edit(f"❌ <b>Error crawling webpage:</b> {e}", parse_mode='html')

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
    dest_id = STORAGE_CHANNEL_ID
    topic_id = None
    if event.is_group:
        dest_id = event.chat_id
        if getattr(event.message, 'reply_to', None):
            topic_id = getattr(event.message.reply_to, 'reply_to_top_id', None) or getattr(event.message.reply_to, 'reply_to_msg_id', None)
    asyncio.create_task(download_and_upload(event, url, page_range, dest_id, topic_id=topic_id))

@client.on(events.NewMessage(pattern=r"^/stats$"))
async def stats_command_handler(event):
    if not await check_admin_or_notify(event): return
    total_idx = await _get_supabase_media_count()
    stat_text = (
        "📊 <b>Uploader & Storage Overview</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"• <b>Active Tasks:</b> <code>{len(RUNNING_TASKS)}</code>\n"
        f"• <b>Storage Channel:</b> <code>{STORAGE_CHANNEL_ID}</code>\n"
        f"• <b>Indexed Videos:</b> <code>{total_idx}</code>\n"
        f"• <b>Mistral AI:</b> {'⚡ Active' if MISTRAL_API_KEY else '⚪ Disabled'}\n"
        f"• <b>Stream Mode:</b> {'⚡ Zero-Disk Streaming' if ENABLE_STREAM_UPLOAD else '💾 Disk Queue'}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    await event.respond(stat_text, parse_mode='html', buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]])

@client.on(events.NewMessage(pattern=r"^/newtopic(?:@\S+)?(?:\s+(.+))?$"))
async def newtopic_command_handler(event):
    if not await check_admin_or_notify(event): return
    title = event.pattern_match.group(1)
    if not title:
        await event.respond(
            "ℹ️ <b>Usage:</b> <code>/newtopic &lt;Title&gt;</code>\n\n"
            "Creates a new forum topic in your forum group.\n"
            "<i>Example:</i> <code>/newtopic Serena Sterling</code>",
            parse_mode='html'
        )
        return

    dest_id = STORAGE_CHANNEL_ID
    if event.is_group:
        dest_id = event.chat_id
    else:
        for c in STORAGE_CHANNELS:
            if await is_forum_chat(c):
                dest_id = c
                break

    status_m = await event.respond(f"⏳ Creating topic <b>{title}</b> in group <code>{dest_id}</code>...", parse_mode='html')
    try:
        topic_id = await create_forum_topic_helper(dest_id, title)
        if topic_id:
            save_forum_topic(dest_id, topic_id, title)
            clean_id = str(abs(dest_id))
            if clean_id.startswith("100"): clean_id = clean_id[3:]
            topic_link = f"https://t.me/c/{clean_id}/{topic_id}"
            await status_m.edit(
                f"✅ <b>Topic Created Successfully!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"• <b>Title:</b> {title}\n"
                f"• <b>Topic ID:</b> <code>{topic_id}</code>\n"
                f"• <b>Group:</b> <code>{dest_id}</code>\n"
                f"• <b>Link:</b> <a href=\"{topic_link}\">Open Topic</a>\n\n"
                "Saved! It will now appear in 📑 <b>Choose Existing Topic</b>.",
                parse_mode='html'
            )
        else:
            await status_m.edit("❌ Failed to obtain topic ID. Check bot permissions in the group.")
    except Exception as e:
        await status_m.edit(
            f"❌ <b>Error creating topic:</b> <code>{e}</code>\n\n"
            "<i>Ensure the bot is an Admin with 'Manage Topics' permission in the group.</i>",
            parse_mode='html'
        )

@client.on(events.NewMessage(pattern=r"^/addtopic(?:@\S+)?(?:\s+(\d+)(?:\s+(.+))?)?$"))
async def addtopic_command_handler(event):
    if not await check_admin_or_notify(event): return
    topic_id_str = event.pattern_match.group(1)
    topic_title = event.pattern_match.group(2)
    if not topic_id_str:
        await event.respond(
            "ℹ️ <b>Usage:</b> <code>/addtopic &lt;Topic_ID&gt; [Title]</code>\n\n"
            "Registers an existing forum topic so it appears in the bot's topic selector.\n"
            "<i>Example:</i> <code>/addtopic 8 Charli O</code>",
            parse_mode='html'
        )
        return

    topic_id = int(topic_id_str)
    topic_title = topic_title.strip() if topic_title else f"Topic #{topic_id}"
    dest_id = event.chat_id if event.is_group else STORAGE_CHANNEL_ID
    if not event.is_group:
        for c in STORAGE_CHANNELS:
            if await is_forum_chat(c):
                dest_id = c
                break

    save_forum_topic(dest_id, topic_id, topic_title)
    await event.respond(
        f"✅ <b>Topic Registered!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"• <b>Group:</b> <code>{dest_id}</code>\n"
        f"• <b>Topic ID:</b> <code>{topic_id}</code>\n"
        f"• <b>Title:</b> {topic_title}\n\n"
        "It will now appear in the 📑 <b>Choose Existing Topic</b> menu!",
        parse_mode='html'
    )

@client.on(events.NewMessage(pattern=r"^/deltopic(?:@\S+)?(?:\s+(\d+))?$"))
async def deltopic_command_handler(event):
    if not await check_admin_or_notify(event): return
    topic_id_str = event.pattern_match.group(1)
    if not topic_id_str:
        await event.respond(
            "ℹ️ <b>Usage:</b> <code>/deltopic &lt;Topic_ID&gt;</code>\n\n"
            "Removes a topic from the bot's selector list.\n"
            "<i>Example:</i> <code>/deltopic 8</code>",
            parse_mode='html'
        )
        return

    topic_id = int(topic_id_str)
    dest_id = event.chat_id if event.is_group else STORAGE_CHANNEL_ID
    if not event.is_group:
        for c in STORAGE_CHANNELS:
            if await is_forum_chat(c):
                dest_id = c
                break

    delete_forum_topic(dest_id, topic_id)
    await event.respond(
        f"🗑️ Removed Topic ID <code>{topic_id}</code> from group <code>{dest_id}</code>.",
        parse_mode='html'
    )

@client.on(events.NewMessage(pattern=r"^/topics(?:@\S+)?$"))
async def topics_command_handler(event):
    if not await check_admin_or_notify(event): return
    dest_id = STORAGE_CHANNEL_ID
    if event.is_group:
        dest_id = event.chat_id
    else:
        for c in STORAGE_CHANNELS:
            if await is_forum_chat(c):
                dest_id = c
                break

    status_m = await event.respond(f"⏳ Fetching topics from group <code>{dest_id}</code>...", parse_mode='html')
    try:
        topics = await get_forum_topics_list(dest_id, limit=30)
        if not topics:
            await status_m.edit(
                f"⚠️ <b>No topics found for group <code>{dest_id}</code>.</b>\n\n"
                "• Use <code>/addtopic &lt;id&gt; &lt;name&gt;</code> to register an existing topic.\n"
                "• Or use <code>/newtopic &lt;title&gt;</code> to create a new one.\n"
                "• Or talk in that topic in the group so the bot auto-registers it!",
                parse_mode='html'
            )
            return

        chat_name = await get_chat_title_cached(dest_id)
        clean_id = str(abs(dest_id))
        if clean_id.startswith("100"): clean_id = clean_id[3:]

        text = (
            f"📑 <b>Forum Topics in {chat_name} (<code>{dest_id}</code>):</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        )

        for t in topics:
            link = f"https://t.me/c/{clean_id}/{t['id']}"
            status = "🔒 Closed" if t.get('closed') else "🟢 Active"
            text += f"• <a href=\"{link}\"><b>{t['title']}</b></a> (ID: <code>{t['id']}</code>) - {status}\n"

        text += (
            "\n💡 <i>Commands:</i>\n"
            "• <code>/addtopic &lt;id&gt; &lt;title&gt;</code> - Register existing topic\n"
            "• <code>/newtopic &lt;title&gt;</code> - Create new topic\n"
            "• <code>/deltopic &lt;id&gt;</code> - Remove from list"
        )
        await status_m.edit(text, parse_mode='html')
    except Exception as e:
        await status_m.edit(f"❌ <b>Error fetching topics:</b> <code>{e}</code>", parse_mode='html')

@client.on(events.NewMessage(pattern=r"^/id(?:@\S+)?$"))
async def id_command_handler(event):
    chat = await event.get_chat()
    chat_title = getattr(chat, 'title', None) or getattr(chat, 'first_name', 'Private Chat')
    chat_id = event.chat_id
    user_id = event.sender_id
    is_group = event.is_group
    is_forum = getattr(chat, 'forum', False)

    reply_to = getattr(event.message, 'reply_to', None)
    topic_id = None
    if reply_to:
        topic_id = getattr(reply_to, 'reply_to_top_id', None) or getattr(reply_to, 'reply_to_msg_id', None)
        if topic_id and is_group and is_forum:
            stored = load_forum_topics().get(str(chat_id), {})
            if str(topic_id) not in stored:
                save_forum_topic(chat_id, topic_id, f"Topic #{topic_id}")

    text = (
        "🆔 <b>Chat & ID Information</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"• <b>Chat Name:</b> {chat_title}\n"
        f"• <b>Chat ID:</b> <code>{chat_id}</code>\n"
        f"• <b>Chat Type:</b> {'Supergroup / Channel' if is_group else 'Private Chat'}\n"
        f"• <b>Forum Topics Enabled:</b> {'🟢 Yes' if is_forum else '⚪ No'}\n"
    )
    if topic_id:
        text += f"• <b>Current Topic ID:</b> <code>{topic_id}</code>\n"
    text += (
        f"• <b>Your User ID:</b> <code>{user_id}</code>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 <i>Copy the Chat ID above to use in <code>STORAGE_CHANNEL_ID</code> in <code>.env</code>.</i>"
    )
    await event.respond(text, parse_mode='html')

@client.on(events.NewMessage())
async def auto_track_topics_in_groups(event):
    if not event.is_group:
        return
    chat_id = event.chat_id
    action = getattr(event.message, 'action', None)
    if isinstance(action, types.MessageActionTopicCreate):
        save_forum_topic(chat_id, event.message.id, action.title)
        logger.info(f"Auto-saved forum topic in {chat_id}: '{action.title}' (ID: {event.message.id})")
    elif isinstance(action, types.MessageActionTopicEdit):
        if getattr(action, 'title', None):
            save_forum_topic(chat_id, event.message.id, action.title)
    elif getattr(event.message, 'reply_to', None) and getattr(event.message.reply_to, 'forum_topic', False):
        top_id = getattr(event.message.reply_to, 'reply_to_top_id', None) or getattr(event.message.reply_to, 'reply_to_msg_id', None)
        if top_id:
            stored = load_forum_topics().get(str(chat_id), {})
            if str(top_id) not in stored:
                save_forum_topic(chat_id, top_id, f"Topic #{top_id}")

@client.on(events.NewMessage(pattern=r"^/help$"))
async def help_command_handler(event):
    if not await check_admin_or_notify(event): return
    help_text = (
        "📖 <b>Help Guide</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>Commands:</b>\n"
        "• /start - 🚀 Open main menu & status\n"
        "• /download [url] - 📥 Download all items\n"
        "• /crawl [url] - 🕷️ Crawl & select videos from any webpage\n"
        "• /newtopic &lt;title&gt; - ➕ Create a new forum topic in group\n"
        "• /addtopic &lt;id&gt; [title] - 📌 Register an existing topic in group\n"
        "• /deltopic &lt;id&gt; - 🗑️ Remove topic from list\n"
        "• /topics - 📑 List active forum topics in group\n"
        "• /id - 🆔 Get chat ID, topic ID & user ID\n"
        "• /uxseries [url] - ✨ Quick UXSeries scraper\n"
        "• /hdmaal [url] - 🎬 Quick HDMaal scraper\n"
        "• /xmovies [url] - 🎥 Quick XMoviesForYou scraper\n"
        "• /viralchut [url] - 🔥 Quick Viralchut scraper\n"
        "• /bunkr [url] - ⚡ Quick Bunkr scraper\n"
        "• /stats - 📊 Storage & bot stats\n"
        "• /help - ❓ Show this guide\n\n"
        "<b>Topic Support:</b>\n"
        "When uploading to a Telegram group with topics enabled, you can choose to create a new topic, pick an existing one, or upload to General chat."
    )
    await event.respond(help_text, parse_mode='html')

# ── Main ─────────────────────────────────────────────────────────────────────

async def set_bot_commands():
    commands = [
        BotCommand(command="start", description="🚀 Open main menu & status"),
        BotCommand(command="download", description="📥 Download from any URL"),
        BotCommand(command="crawl", description="🕷️ Crawl & select from any webpage"),
        BotCommand(command="newtopic", description="➕ Create a new topic in group"),
        BotCommand(command="addtopic", description="📌 Register an existing topic in group"),
        BotCommand(command="deltopic", description="🗑️ Remove topic from list"),
        BotCommand(command="topics", description="📑 List forum topics in group"),
        BotCommand(command="id", description="🆔 Get chat ID, topic ID & user ID"),
        BotCommand(command="xmovies", description="🎥 Quick XMoviesForYou scraper"),
        BotCommand(command="uxseries", description="✨ Quick UXSeries scraper"),
        BotCommand(command="hdmaal", description="🎬 Quick HDMaal scraper"),
        BotCommand(command="pimpbunny", description="🐰 Quick PimpBunny scraper"),
        BotCommand(command="viralchut", description="🔥 Quick Viralchut scraper"),
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
    if MISTRAL_API_KEY:
        logger.info("Mistral AI naming & metadata: active")
    else:
        logger.info("Mistral AI naming & metadata: inactive (MISTRAL_API_KEY not set)")
    await client.connect()
    if not await client.is_user_authorized():
        await client.sign_in(bot_token=BOT_TOKEN)
    print("✅ Setting commands...")
    await set_bot_commands()
    print("✅ Online.")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
