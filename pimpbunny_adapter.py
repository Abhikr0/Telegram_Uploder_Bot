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

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from ai_caption import sanitize_filename as _ai_sanitize_filename
from pimpbunny_scraper import (
    Category,
    Model,
    Video,
    PimpBunnyScraper,
    CatalogScraper,
)

DEFAULT_DOMAIN = "pimpbunny.com"
logger = logging.getLogger(__name__)


def _pimpbunny_headers(referer: str = f"https://{DEFAULT_DOMAIN}/") -> Dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
    }


def _is_bunny_shield_response(status_code: int, html: str) -> bool:
    if status_code in (403, 429, 503):
        low = html.lower()
        return (
            status_code == 403
            or "bunny-shield" in low
            or "shield-challenge" in low
            or "establishing a secure connection" in low
        )
    return False


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

def parse_pimpbunny_url(url: str) -> Tuple[str, str, str, Optional[str]]:
    """Parse a PimpBunny URL into ``(domain, service, user_id, post_id)``.

    - ``https://pimpbunny.com/videos/<slug>``          -> single video (post_id = slug)
    - ``https://pimpbunny.com/categories/<slug>``      -> catalog listing (post_id None)
    - ``https://pimpbunny.com/onlyfans-creators/<slug>`` -> model listing (post_id None)
    - ``https://pimpbunny.com/tags/<slug>``            -> tag listing (post_id None)
    - homepage / bare section                          -> catalog listing (post_id None)
    """
    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    parsed = urlparse(url)
    domain = parsed.netloc or DEFAULT_DOMAIN
    path = parsed.path.rstrip("/")
    parts = [p for p in path.split("/") if p]

    if not parts or parts[0].isdigit():
        return domain, "pimpbunny", "home", None

    if parts[0] == "videos":
        if len(parts) > 1 and not parts[1].isdigit():
            slug = parts[1]
            return domain, "pimpbunny", slug, slug  # post_id = slug -> single video
        return domain, "pimpbunny", "videos", None

    if parts[0] in ("categories", "onlyfans-creators", "tags"):
        if len(parts) > 1 and not parts[1].isdigit():
            return domain, "pimpbunny", f"{parts[0]}/{parts[1]}", None
        return domain, "pimpbunny", parts[0], None

    return domain, "pimpbunny", "/".join(parts), None


# ---------------------------------------------------------------------------
# Video -> dict conversion
# ---------------------------------------------------------------------------

def _sanitize_filename(name: str, ext: str = ".mp4") -> str:
    """Sanitize string into a safe file name with correct extension (mirrors ai_caption.py)."""
    return _ai_sanitize_filename(name, ext=ext)


def _parse_iso_duration(value: Any) -> int:
    if isinstance(value, (int, float)):
        return max(0, int(value))
    if not isinstance(value, str):
        return 0
    m = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", value.strip(), re.I)
    if not m:
        return 0
    return int(m.group(1) or 0) * 3600 + int(m.group(2) or 0) * 60 + int(m.group(3) or 0)


def extract_kvs_download_urls(html: str) -> List[Dict[str, Any]]:
    """Extract tokenized KVS player video URLs (ordered with explicit heights)."""
    labels: Dict[str, str] = {}
    for m in re.finditer(r"(video_(?:alt_)?url\d*)_text\s*:\s*['\"]([^'\"]+)['\"]", html):
        labels[m.group(1)] = m.group(2).strip()

    urls: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for m in re.finditer(r"(video_(?:alt_)?url\d*)\s*:\s*['\"](https?://[^'\"]+)['\"]", html):
        key, raw_url = m.group(1), m.group(2).replace("\\/", "/").strip()
        if not raw_url or raw_url in seen or "_preview.mp4" in raw_url.lower():
            continue
        seen.add(raw_url)
        label = labels.get(key, "")
        hm = re.search(r"(\d{3,4})p", label) or re.search(r"_(\d{3,4})p\.(?:mp4|webm)", raw_url, re.I)
        height = int(hm.group(1)) if hm else 0
        urls.append({
            "quality": label or (f"{height}p" if height else key),
            "url": raw_url,
            "format_id": key,
            "height": height,
            "ext": "mp4",
        })
    return urls


def _parse_pimpbunny_video_html(url: str, html: str, service: str = "pimpbunny") -> Dict[str, Any]:
    """Parse a live PimpBunny video page HTML into the bot's video dict."""
    soup = BeautifulSoup(html, "html.parser")
    title = ""
    description = ""
    thumbnail = None
    duration = 0
    content_url = ""

    for s in soup.find_all("script", type="application/ld+json"):
        txt = (s.string or s.text or "").strip()
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if isinstance(item, dict) and "video" in str(item.get("@type", "")).lower():
                title = title or str(item.get("name") or "").strip()
                description = description or str(item.get("description") or "").strip()
                th = item.get("thumbnailUrl")
                if not thumbnail and th:
                    thumbnail = th[0] if isinstance(th, list) else str(th)
                if not duration and item.get("duration"):
                    duration = _parse_iso_duration(item.get("duration"))
                if not content_url and item.get("contentUrl"):
                    content_url = str(item.get("contentUrl")).strip()

    if not title:
        og = soup.find("meta", property="og:title") or soup.find("h1")
        title = (og.get("content") if og and og.has_attr("content") else og.get_text(strip=True)) if og else ""
    if not thumbnail:
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            thumbnail = urljoin(url, og_img["content"].strip())

    model = None
    model_link = soup.select_one("a[href*='/onlyfans-creators/']")
    if model_link:
        href_parts = [p for p in urlparse(model_link["href"]).path.split("/") if p]
        if len(href_parts) >= 2 and href_parts[0] == "onlyfans-creators":
            model = href_parts[1].replace("-", " ").title()

    download_urls = extract_kvs_download_urls(html)
    # Only fall back to JSON-LD contentUrl if JS player URLs were absent
    if not download_urls and content_url:
        download_urls = [{"quality": "default", "url": content_url, "height": 0, "ext": "mp4"}]

    vid_id = None
    if download_urls:
        idm = re.search(r"/(\d+)(?:_\d+p)?\.mp4", download_urls[0]["url"])
        if idm:
            vid_id = idm.group(1)

    video = Video(
        url=url,
        video_id=vid_id or url.rstrip("/").split("/")[-1],
        title=title or None,
        model=model,
        duration=duration or None,
        thumbnail=thumbnail,
        description=description or None,
        download_urls=download_urls,
        source="html",
    )
    return pimpbunny_to_video_dict(video, service=service)


def _parse_pimpbunny_listing_html(
    base_url: str, html: str, service: str = "pimpbunny", fallback_model: str = ""
) -> Tuple[List[Dict[str, Any]], set[int]]:
    """Parse a PimpBunny catalog HTML page into video dicts and available page numbers."""
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, Any]] = []
    seen_urls: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "?" in href or "#" in href:
            continue
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        parts = [p for p in parsed.path.split("/") if p]
        if len(parts) < 2 or parts[0] != "videos" or parts[1].isdigit():
            continue
        canonical = f"{parsed.scheme}://{parsed.netloc}/videos/{parts[1]}/"
        if canonical in seen_urls:
            continue

        parent = a.parent
        img = a.find("img") or (parent.find("img") if parent else None)
        title = (a.get("title") or "").strip()
        if not title and img and img.get("alt"):
            title = img["alt"].strip()
        if not title:
            title = a.get_text(" ", strip=True)
            title = re.sub(r"^(?:\d+K\s+)?\d+:\d{2}\s+", "", title).strip()
        if not title:
            title = parts[1].replace("-", " ").title()

        thumb = None
        if img:
            raw_thumb = img.get("data-original") or img.get("data-src") or img.get("src")
            if raw_thumb and not raw_thumb.startswith("data:"):
                thumb = urljoin(base_url, raw_thumb)

        model = fallback_model
        if not model and parent:
            m_link = parent.find("a", href=re.compile(r"/onlyfans-creators/[^/]+"))
            if m_link:
                mparts = [p for p in urlparse(m_link["href"]).path.split("/") if p]
                if len(mparts) >= 2:
                    model = mparts[1].replace("-", " ").title()

        seen_urls.add(canonical)
        video = Video(
            url=canonical,
            video_id=parts[1],
            title=title,
            model=model or None,
            thumbnail=thumb,
            source="html_listing",
        )
        items.append(pimpbunny_to_video_dict(video, service=service))

    page_nums: set[int] = set()
    base_path = urlparse(base_url).path.rstrip("/")
    for a in soup.find_all("a", href=True):
        p_path = urlparse(urljoin(base_url, a["href"])).path.rstrip("/")
        m = re.match(rf"^{re.escape(base_path)}/(\d+)$", p_path)
        if m:
            page_nums.add(int(m.group(1)))

    return items, page_nums


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
    name = _sanitize_filename(title)
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
    client: Optional[httpx.AsyncClient] = None,
) -> List[Dict[str, Any]]:
    """Uniform content fetcher for PimpBunny.

    Fetches live PimpBunny pages over HTTP (never falling back to stale local caches).
    Raises RuntimeError if PimpBunny blocks automated requests with HTTP 403 / Bunny Shield.
    """
    domain, service_name, user_id, post_id = parse_pimpbunny_url(url)
    base_url = url if url.startswith(("http://", "https://")) else f"https://{domain}/{url.lstrip('/')}"

    close_client = False
    if client is None:
        client = httpx.AsyncClient(
            headers=_pimpbunny_headers(f"https://{domain}/"),
            follow_redirects=True,
            timeout=30.0,
        )
        close_client = True

    try:
        if post_id:
            resp = await client.get(base_url)
            if _is_bunny_shield_response(resp.status_code, resp.text):
                raise RuntimeError(
                    f"PimpBunny returned HTTP {resp.status_code} (Bunny Shield anti-bot challenge) for {base_url}"
                )
            resp.raise_for_status()
            return [_parse_pimpbunny_video_html(str(resp.url), resp.text, service=service_name)]

        fallback_model = ""
        if user_id.startswith("onlyfans-creators/"):
            fallback_model = user_id.split("/", 1)[1].replace("-", " ").title()

        if page_range:
            start_p, end_p = page_range
        else:
            start_p, end_p = 1, 20

        clean_base = base_url.split("?")[0].rstrip("/")
        if clean_base == f"https://{domain}" or user_id == "home":
            clean_base = f"https://{domain}"
        elif not clean_base.endswith(user_id.strip("/")):
            clean_base = f"https://{domain}/{user_id.strip('/')}"

        items: List[Dict[str, Any]] = []
        seen_ids: set[str] = set()

        for page_num in range(start_p, end_p + 1):
            page_url = f"{clean_base}/" if page_num == 1 else f"{clean_base}/{page_num}/"
            resp = await client.get(page_url)
            if _is_bunny_shield_response(resp.status_code, resp.text):
                if items:
                    break
                raise RuntimeError(
                    f"PimpBunny returned HTTP {resp.status_code} (Bunny Shield anti-bot challenge) for {page_url}"
                )
            if resp.status_code != 200:
                if page_num == start_p:
                    resp.raise_for_status()
                break

            page_items, available_pages = _parse_pimpbunny_listing_html(
                page_url, resp.text, service=service_name, fallback_model=fallback_model
            )
            added = 0
            for item in page_items:
                if item["url"] not in seen_ids:
                    seen_ids.add(item["url"])
                    items.append(item)
                    added += 1

            if added == 0:
                break
            if not page_range and (page_num + 1) not in available_pages:
                break

        return items
    finally:
        if close_client:
            await client.aclose()


# ---------------------------------------------------------------------------
# Public convenience accessors
# ---------------------------------------------------------------------------

__all__ = [
    "parse_pimpbunny_url",
    "pimpbunny_to_video_dict",
    "fetch_pimpbunny_all_posts",
    "DEFAULT_DOMAIN",
]
