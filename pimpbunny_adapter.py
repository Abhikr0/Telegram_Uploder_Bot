"""Adapter layer between the PimpBunny scraper and the Telegram Uploader Bot pipeline.

This module bridges the pimpbunny_scraper package (which returns ``Video``/``Category``
dataclasses) with the Telegram_Uploder_Bot pipeline (which expects a list of video
dicts with fields: ``url, name, id, service, title, content, thumbnail, poster,
actors, platform, duration, user``).

Integration points:
- ``parse_pimpbunny_url``          -- URL -> (domain, service, user_id, post_id)
- ``pimpbunny_to_video_dict``      -- Video dataclass -> dict for the bot pipeline
- ``fetch_pimpbunny_all_posts``    -- uniform fetcher used by scraper.fetch_all_posts

Usage in bot.py:
    from pimpbunny_adapter import parse_pimpbunny_url, pimpbunny_to_video_dict
    from pimpbunny_adapter import fetch_pimpbunny_all_posts

    domain, service, cur_id, post_id = parse_pimpbunny_url(url)
    # then route service == "pimpbunny" through fetch_pimpbunny_all_posts
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pimpbunny_scraper import (
    Category,
    Model,
    Video,
    PimpBunnyScraper,
    CatalogScraper,
)

DEFAULT_DOMAIN = "pimpbunny.com"


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

def parse_pimpbunny_url(url: str) -> Tuple[str, str, str, Optional[str]]:
    """Parse a PimpBunny URL into ``(domain, service, user_id, post_id)``.

    - ``https://pimpbunny.com/videos/<slug>``       -> single video (post_id = slug)
    - ``https://pimpbunny.com/categories/<slug>``   -> catalog listing (post_id None)
    - ``https://pimpbunny.com/onlyfans-creators/<slug>`` -> model listing (post_id None)
    - homepage / anything else                      -> catalog "home"
    """
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    from urllib.parse import urlparse

    parsed = urlparse(url)
    domain = parsed.netloc or DEFAULT_DOMAIN
    path = parsed.path.rstrip("/")
    parts = [p for p in path.split("/") if p]

    if not parts:
        return domain, "catalog", "home", None

    if parts[0] == "videos":
        slug = parts[1] if len(parts) > 1 else "unknown"
        return domain, "pimpbunny", slug, slug  # post_id = slug -> single video

    if parts[0] in ("categories", "onlyfans-creators", "tags"):
        slug = parts[1] if len(parts) > 1 else parts[0]
        return domain, "pimpbunny", slug, None  # listing

    # homepage or unrecognised path -> catalog home
    return domain, "pimpbunny", "home", None


# ---------------------------------------------------------------------------
# Video -> dict conversion
# ---------------------------------------------------------------------------

def _sanitize_filename(name: str, ext: str = ".mp4") -> str:
    """Sanitize string into a safe file name with correct extension (mirrors ai_caption.py)."""
    clean = re.sub(r'[<>:\"/\\\\|?*\\x00-\\x1f]', "", str(name))
    clean = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", clean)
    clean = re.sub(r"\s+", " ", clean).strip(" ._")
    if not clean:
        clean = "video"
    if not clean.lower().endswith(ext.lower()):
        clean = f"{clean}{ext}"
    return clean[:80]


def _best_download_url(video: Video) -> str:
    """Pick the highest-quality playable download URL from a PimpBunny Video."""
    dl = getattr(video, "download_urls", None) or []
    if not dl:
        return video.url or ""
    # Prefer formats with an explicit height, highest first
    def sort_key(fmt: Dict[str, Any]):
        height = fmt.get("height") or 0
        try:
            filesize = float(fmt.get("filesize_approx") or 0)
        except (TypeError, ValueError):
            filesize = 0
        return (height, filesize)

    ranked = sorted(dl, key=sort_key, reverse=True)
    for fmt in ranked:
        url = fmt.get("url")
        if url:
            return url
    return video.url or ""


def pimpbunny_to_video_dict(video: Video, service: str = "pimpbunny") -> Dict[str, Any]:
    """Convert a PimpBunny ``Video`` dataclass into the dict format the bot pipeline expects."""
    if not video:
        return {
            "url": "", "name": "video.mp4", "id": "unk", "service": service,
            "title": "video", "content": "", "thumbnail": None, "poster": None,
            "actors": [], "platform": "pimpbunny", "duration": 0, "user": "pimpbunny",
        }

    download_url = _best_download_url(video)
    title = video.title or (download_url.split("/")[-1].split("-")[-1] if download_url else "video")
    # strip query strings / formatting from the fallback slug
    slug = Path(title).stem or "video"
    name = _sanitize_filename(slug)
    if not name.lower().endswith(".mp4"):
        name = f"{name}.mp4"

    return {
        "url": download_url or video.url or "",
        "name": name,
        "id": video.video_id or video.url.rstrip("/").split("/")[-1],
        "service": service,
        "title": video.title or name,
        "content": (video.description or "")[:300],
        "thumbnail": video.thumbnail,
        "poster": video.thumbnail,
        "actors": [video.model] if video.model else [],
        "platform": "pimpbunny",
        "duration": video.duration or 0,
        "user": video.model or "pimpbunny",
        # raw metadata available for the AI captioner
        "raw": video.raw,
    }


# ---------------------------------------------------------------------------
# Content fetching (single video + catalog)
# ---------------------------------------------------------------------------

def _catalog_to_video_dicts(videos: List[Video], service: str = "pimpbunny") -> List[Dict[str, Any]]:
    """Convert an iterable of PimpBunny ``Video`` dataclasses into the bot's video dicts."""
    out = []
    for v in videos:
        out.append(pimpbunny_to_video_dict(v, service))
    return out


async def fetch_pimpbunny_all_posts(
    url: str,
    page_range: Optional[Tuple[int, int]] = None,
    service: str = "pimpbunny",
) -> List[Dict[str, Any]]:
    """Uniform content fetcher for PimpBunny.

    A single /videos/<slug> URL is treated as one post; any /categories/ or
    /onlyfans-creators/ URL is treated as a catalog listing.
    """
    parsed = parse_pimpbunny_url(url)
    domain, service_name, user_id, post_id = parsed

    scraper = PimpBunnyScraper()
    items: List[Dict[str, Any]] = []

    if post_id:
        # Single video page
        try:
            video = scraper.get_video(url)
            items.append(pimpbunny_to_video_dict(video, service_name))
        except Exception as e:
            # fall back to catalog-only metadata if yt-dlp extraction fails
            logger_warning = __import__("logging").getLogger(__name__)
            logger_warning.warning(f"PimpBunny single-video scrape failed: {e}")
            try:
                cat = scraper.get_category(user_id, limit=1)
                items.extend(_catalog_to_video_dicts(cat, service_name))
            except Exception as e2:
                raise RuntimeError(f"Could not scrape PimpBunny video: {e}; {e2}") from e
    else:
        # Catalog listing (category / model / homepage)
        try:
            # Models are referenced by slug
            if service_name == "pimpbunny" and "/" in user_id:
                # treat as model profile
                slug = user_id
                model = scraper.get_model(slug)
                items.extend(_catalog_to_video_dicts(model.videos, service_name))
            else:
                cat = scraper.get_category(user_id, limit=20)
                items.extend(_catalog_to_video_dicts(cat.videos, service_name))
        except Exception as e:
            raise RuntimeError(f"Could not scrape PimpBunny catalog: {e}") from e

    return items


# ---------------------------------------------------------------------------
# Public convenience accessors
# ---------------------------------------------------------------------------

__all__ = [
    "parse_pimpbunny_url",
    "pimpbunny_to_video_dict",
    "fetch_pimpbunny_all_posts",
    "DEFAULT_DOMAIN",
]
