#!/usr/bin/env python3
"""
Coomer.st Video Scraper (Asynchronous & Enhanced)
=================================================
Downloads videos from any coomer.st creator profile.
Features: Crawler, Async downloading, Resume/Auto-retry, Gzip support.

Usage:
    python scraper.py --url "https://coomer.st/onlyfans/user/12345"
"""

import argparse
import os
import re
import sys
import time
import asyncio
import logging
import gzip
import json
from pathlib import Path
from urllib.parse import urlparse, urljoin, quote, unquote, parse_qs
from typing import List, Optional, Tuple, Dict, Any, Callable

import httpx
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_DOMAIN = "coomer.st"
POSTS_PER_PAGE = 50  # coomer.st returns 50 posts per API page
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".webm", ".m4v", ".flv", ".ts", ".m2ts"}
STREAM_EXTENSIONS = VIDEO_EXTENSIONS | {".m3u8", ".mpd"}

try:
    import yt_dlp
    HAS_YTDLP = True
except ImportError:
    HAS_YTDLP = False

def is_direct_media_url(url: str) -> bool:
    """Check if URL points directly to a video/stream file."""
    clean = urlparse(url).path.lower()
    return any(clean.endswith(ext) for ext in STREAM_EXTENSIONS) or bool(re.search(r'\.(?:mp4|m3u8|mpd|webm|mov|mkv|avi|flv)(?:[?&#]|$)', url, re.I))

MAX_RETRIES = 10
RETRY_DELAY = 10        # seconds between retries
API_DELAY = 1.0        # seconds between API calls
CHUNK_SIZE = 256 * 1024  # 256KB chunks for better throughput
MAX_FILE_SIZE = 2000 * 1024 * 1024  # 2000MB limit for Telegram MTProto
MIN_FILE_SIZE = 5000  # 5KB minimum to skip small error pages

# Bunkr API Endpoints (adapted from Lysagxra/BunkrDownloader)
BUNKR_SIGN_API = "https://glb-apisign.cdn.cr/sign"
BUNKR_DL_API = "https://dl.bunkr.cr/api/_001_v2"
BUNKR_DL_REFERER = "https://dl.bunkrr.cr/"

logger = logging.getLogger(__name__)

def decode_cf_email(cf_hex: str) -> str:
    """Decode Cloudflare email-protection XOR obfuscated string."""
    try:
        raw = bytes.fromhex(cf_hex)
        key = raw[0]
        return bytes(b ^ key for b in raw[1:]).decode('utf-8', errors='ignore')
    except Exception:
        return ""

def get_headers(domain=DEFAULT_DOMAIN, referer=None, is_media: bool = False):
    is_bunkr = any(x in domain.lower() for x in ["bunkr", "balbums", "cdn.cr"])
    is_viralchut = any(x in domain.lower() for x in ["viralchut", "desibp"])
    is_coomer = any(x in domain.lower() for x in ["coomer", "kemono"])

    if is_bunkr:
        accept_val = "*/*"
        default_referer = BUNKR_DL_REFERER if is_media else f"https://{domain}/"
    elif is_viralchut:
        accept_val = "*/*" if is_media else "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        default_referer = "https://viralchut.com/"
    elif is_coomer:
        accept_val = "text/css" if not is_media else "*/*"
        default_referer = f"https://{domain}/"
    else:
        accept_val = "*/*" if is_media else "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        default_referer = f"https://{domain}/"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Accept": accept_val,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer or default_referer,
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "video" if is_media else ("empty" if is_coomer else "document"),
        "Sec-Fetch-Mode": "navigate" if (not is_media and not is_coomer) else "cors",
        "Sec-Fetch-Site": "cross-site" if (is_bunkr or is_viralchut or not is_coomer) else "same-origin",
    }
    if is_bunkr:
        headers["Origin"] = "https://bunkr.cr"
    elif is_coomer:
        headers["Origin"] = f"https://{domain}"
    return headers

# ── URL Parsing ──────────────────────────────────────────────────────────────

def parse_media_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Parse any URL (coomer, kemono, bunkr, viralchut, direct media, or ANY generic website)."""
    if not url.startswith("http"):
        url = "https://" + url

    parsed = urlparse(url)
    domain = parsed.netloc or DEFAULT_DOMAIN
    clean_path = parsed.path.rstrip("/")
    query_params = parse_qs(parsed.query)
    domain_lower = domain.lower()

    # 1. Support Viralchut URLs
    if "viralchut" in domain_lower:
        if "s" in query_params:
            q = query_params["s"][0].strip()
            return domain, "viralchut", f"search/{q}", None

        cat_match = re.match(r"^/categor(?:y|ies)/([^/?#]+)", clean_path)
        if cat_match:
            cat_name = cat_match.group(1)
            return domain, "viralchut", f"category/{cat_name}", None

        tag_match = re.match(r"^/tags?/([^/?#]+)", clean_path)
        if tag_match:
            tag_name = tag_match.group(1)
            return domain, "viralchut", f"tag/{tag_name}", None

        if re.match(r"^/page/\d+", clean_path) or clean_path in ("", "/"):
            return domain, "viralchut", "latest", None

        slug = clean_path.strip("/")
        if slug and slug not in ("categories", "tags"):
            return domain, "viralchut", slug, slug

    # 2. Support Bunkr URLs (albums and single items across all mirrors)
    is_bunkr = any(x in domain_lower for x in ["bunkr", "balbums"])
    if is_bunkr:
        bunkr_album_match = re.match(r"^/a/([^/?#]+)", clean_path)
        if bunkr_album_match:
            return domain, "bunkr", bunkr_album_match.group(1), None

        bunkr_item_match = re.match(r"^/(v|f|d|i)/([^/?#]+)", clean_path)
        if bunkr_item_match:
            item_type, item_id = bunkr_item_match.group(1), bunkr_item_match.group(2)
            return domain, "bunkr", item_id, f"{item_type}_{item_id}"

    # 3. Support Coomer / Kemono URLs
    is_coomer = any(x in domain_lower for x in ["coomer", "kemono"])
    if is_coomer:
        single_post_match = re.match(r"^/([^/]+)/user/([^/]+)/post/([^/?#]+)", clean_path)
        if single_post_match:
            return domain, single_post_match.group(1), single_post_match.group(2), single_post_match.group(3)
            
        profile_match = re.match(r"^/([^/]+)/user/([^/?#]+)", clean_path)
        if profile_match:
            return domain, profile_match.group(1), profile_match.group(2), None

    # 4. Direct Media File on ANY domain
    if is_direct_media_url(url):
        filename = unquote(Path(clean_path).name)
        rel_path = clean_path.lstrip("/")
        if parsed.query:
            rel_path += f"?{parsed.query}"
        return domain, "direct", rel_path, filename

    # 5. Universal Fallback for ANY Other Website
    srv = domain.replace("www.", "").split(".")[0] or "generic"
    listing_patterns = re.compile(r'(/categor(y|ies)/|/tags?/|/channels?/|/users?/|/search|/browse|/gallery|/albums?|/playlists?|/page/\d+)', re.I)
    has_search_q = any(k in query_params for k in ("s", "q", "query", "search"))

    rel_path = clean_path.lstrip("/")
    if parsed.query:
        rel_path += f"?{parsed.query}"

    if listing_patterns.search(clean_path) or has_search_q or clean_path in ("", "/"):
        cur_id = rel_path or "browse"
        return domain, srv, cur_id, None

    # Single video post / page
    slug = clean_path.strip("/").split("/")[-1] if clean_path else "video"
    return domain, srv, rel_path, slug


def parse_profile_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Backwards-compatible wrapper returning (domain, service, user_id)."""
    domain, service, user_id, _ = parse_media_url(url)
    return domain, service, user_id


def parse_page_range(pages_str: str) -> tuple[int, int] | None:
    """Parse page range string like '1-5' or '3'."""
    if not pages_str:
        return None
    pages_str = str(pages_str).strip()
    if "-" in pages_str:
        try:
            start, end = map(int, pages_str.split("-", 1))
            if start < 1 or end < start:
                return None
            return start, end
        except ValueError:
            return None
    else:
        try:
            p = int(pages_str)
            if p < 1:
                return None
            return p, p
        except ValueError:
            return None

# ── Crawler & API Fetching ───────────────────────────────────────────────────

async def fetch_creators(domain: str, service_filter: str = None, name_filter: str = None) -> list[dict]:
    """Fetch all creators and filter by service/name."""
    url = f"https://{domain}/api/v1/creators"
    print(f"\n🕷️  Crawling creator list on {domain}...")
    headers = get_headers(domain)
    try:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            
            content = resp.content
            if content.startswith(b'\x1f\x8b'):
                content = gzip.decompress(content)
            
            creators = json.loads(content)
            
            filtered = []
            for c in creators:
                s_match = not service_filter or c.get("service") == service_filter
                n_match = not name_filter or name_filter.lower() in c.get("name", "").lower()
                if s_match and n_match:
                    filtered.append(c)
            return filtered
    except Exception as e:
        print(f"❌ Failed to crawl creators on {domain}: {e}")
        return []

async def fetch_posts(domain: str, service: str, user_id: str, offset: int = 0, client: httpx.AsyncClient = None) -> list[dict]:
    """Fetch a page of posts from the API."""
    url = f"https://{domain}/api/v1/{service}/user/{user_id}/posts"
    params = {"o": offset}
    referer = f"https://{domain}/{service}/user/{user_id}"
    
    headers = get_headers(domain, referer)
    
    # Internal helper to handle the request
    async def _fetch(c: httpx.AsyncClient):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = await c.get(url, params=params, timeout=30)
                if resp.status_code == 404:
                    return []
                resp.raise_for_status()
                
                content = resp.content
                if content.startswith(b'\x1f\x8b'):
                    content = gzip.decompress(content)
                
                return json.loads(content)
            except (httpx.HTTPError, json.JSONDecodeError) as e:
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    logger.error(f"API Error after {MAX_RETRIES} attempts: {e}")
        return []

    if client:
        return await _fetch(client)
    else:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True) as c:
            return await _fetch(c)

async def fetch_bunkr_album(domain: str, album_id: str, page_range: tuple[int, int] | None = None) -> list[dict]:
    """Scrape all items from a Bunkr album across all pages (modern Tailwind & legacy)."""
    all_items = []
    headers = get_headers(domain, f"https://{domain}/a/{album_id}")
    
    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30) as client:
        url_p1 = f"https://{domain}/a/{album_id}?page=1"
        resp_p1 = await client.get(url_p1)
        resp_p1.raise_for_status()
        
        soup = BeautifulSoup(resp_p1.text, 'html.parser')
        max_page = 1

        # Check <nav class="pagination"> or pagination navigation
        pagination_nav = soup.find('nav', class_=re.compile(r'pagination', re.I))
        if pagination_nav:
            p_nums = [int(p) for p in re.findall(r'\b\d+\b', pagination_nav.text)]
            if p_nums:
                max_page = max(p_nums)

        for a in soup.find_all('a', href=True):
            m = re.search(r'[?&]page=(\d+)', a['href'])
            if m:
                p = int(m.group(1))
                if p > max_page:
                    max_page = p
                    
        start_p = 1
        end_p = max_page
        if page_range:
            start_p, end_p = page_range
            end_p = min(end_p, max_page)
            
        seen_slugs = set()
        for p in range(start_p, end_p + 1):
            if p == 1:
                page_soup = soup
            else:
                print(f"   Fetching page {p}/{max_page}...", end="\r")
                resp = await client.get(f"https://{domain}/a/{album_id}?page={p}")
                resp.raise_for_status()
                page_soup = BeautifulSoup(resp.text, 'html.parser')

            # Decode Cloudflare email obfuscation on any filenames
            for cf in page_soup.find_all(class_=re.compile(r'__cf_email__')):
                cf_hex = cf.get("data-cfemail")
                if cf_hex:
                    cf.replace_with(decode_cf_email(cf_hex))

            items_found = 0
            # Strategy A: Modern Tailwind layout (links to /v/, /f/, /d/, /i/)
            for a_link in page_soup.find_all('a', href=re.compile(r'/(?:v|f|d|i)/([^/?#]+)')):
                href = a_link['href']
                m = re.search(r'/(v|f|d|i)/([^/?#]+)', href)
                if not m:
                    continue
                item_type, slug = m.group(1), m.group(2)
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                items_found += 1

                card = a_link.find_parent('div') or a_link
                title = ""
                name_elem = card.find(class_=re.compile(r'(text-subs|theName|truncate|font-semibold)'))
                if name_elem:
                    title = name_elem.get_text(strip=True)
                if not title:
                    title = a_link.get('title') or card.get('title') or a_link.get_text(strip=True)
                if not title or title.lower() in ["download", "view", "play"]:
                    title = unquote(slug)

                if item_type == 'v' and not Path(title).suffix:
                    title += ".mp4"

                full_url = urljoin(f"https://{domain}", href)
                all_items.append({
                    "id": slug,
                    "service": "bunkr",
                    "bunkr_f_url": full_url,
                    "name": sanitize(title),
                    "file": {
                        "path": href,
                        "name": sanitize(title)
                    }
                })

            # Strategy B: Legacy div.theItem layout
            if items_found == 0:
                for item_div in page_soup.find_all('div', class_='theItem'):
                    a_link = item_div.find('a', href=re.compile(r'^/(?:v|f|d|i)/'))
                    if not a_link:
                        continue
                    href = a_link['href']
                    slug = href.split('/')[-1]
                    if slug in seen_slugs:
                        continue
                    seen_slugs.add(slug)
                    name_elem = item_div.find(class_='theName')
                    title = name_elem.text.strip() if name_elem else item_div.get('title', '')
                    if not title:
                        title = unquote(slug)
                    all_items.append({
                        "id": slug,
                        "service": "bunkr",
                        "bunkr_f_url": urljoin(f"https://{domain}", href),
                        "name": sanitize(title),
                        "file": {
                            "path": href,
                            "name": sanitize(title)
                        }
                    })
                
    print(f"\n[OK] Fetched {len(all_items)} items from Bunkr album.")
    return all_items

async def fetch_single_post(domain: str, service: str, user_id: str, post_id: str) -> list[dict]:
    """Fetch a single post from the API."""
    url = f"https://{domain}/api/v1/{service}/user/{user_id}/post/{post_id}"
    referer = f"https://{domain}/{service}/user/{user_id}/post/{post_id}"
    headers = get_headers(domain, referer)
    try:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30) as client:
            resp = await client.get(url)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
            content = resp.content
            if content.startswith(b'\x1f\x8b'):
                content = gzip.decompress(content)
            data = json.loads(content)
            if isinstance(data, dict):
                return [data]
            elif isinstance(data, list):
                return data
            return []
    except Exception as e:
        logger.error(f"Failed to fetch single post {post_id}: {e}")
        return []

def extract_viralchut_post_data(html: str, page_url: str) -> dict:
    """Extract metadata, direct CDN video URL, poster, and title from a Viralchut post page."""
    soup = BeautifulSoup(html, "html.parser")

    # Title
    title = ""
    h1 = soup.find("h1")
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        og_title = soup.find("meta", property="og:title")
        if og_title:
            title = og_title.get("content", "").strip()
    if not title and soup.title:
        title = soup.title.get_text(strip=True).replace(" - Viralchut", "").strip()

    # Video Source
    video_url = None
    for v in soup.find_all("video"):
        for s in v.find_all("source"):
            src = s.get("src")
            if src:
                video_url = src
                break
        if not video_url and v.get("src"):
            video_url = v.get("src")
        if video_url:
            break

    # Check iframes if no direct video
    if not video_url:
        for ifr in soup.find_all("iframe"):
            src = ifr.get("src")
            if src and any(x in src.lower() for x in ["video", "embed", "player", "dood", "stream", "desibp"]):
                video_url = src
                break

    # Check script or regex fallback
    if not video_url:
        mp4_matches = re.findall(r'https?://[^\s\"\'<>]+\.(?:mp4|m3u8)[^\s\"\'<>]*', html)
        if mp4_matches:
            video_url = mp4_matches[0]

    # Normalize video_url if domain has trailing dot e.g. cdn.desibp.cam./
    if video_url:
        video_url = re.sub(r'(https?://[^/]+)\./', r'\1/', video_url)

    # Poster / Thumb
    poster = None
    video_tag = soup.find("video")
    if video_tag and video_tag.get("poster"):
        poster = video_tag.get("poster")
    if not poster:
        og_img = soup.find("meta", property="og:image")
        if og_img:
            poster = og_img.get("content")

    # Post ID
    post_id = None
    body = soup.find("body")
    if body and body.get("class"):
        for cls in body.get("class"):
            m = re.match(r"postid-(\d+)", cls)
            if m:
                post_id = m.group(1)
                break
    if not post_id:
        m = re.search(r"['\"]post_id['\"]\s*:\s*['\"]?(\d+)", html) or re.search(r"data-post-id=['\"](\d+)['\"]", html)
        if m:
            post_id = m.group(1)
    if not post_id:
        slug = urlparse(page_url).path.strip("/").split("/")[-1]
        post_id = slug or "unk"

    # Description
    description = ""
    for p in soup.find_all("p", class_=re.compile(r"wp-block-paragraph|description|entry-content")):
        t = p.get_text(strip=True)
        if t and len(t) > len(description):
            description = t
    if not description:
        og_desc = soup.find("meta", property="og:description")
        if og_desc:
            description = og_desc.get("content", "").strip()

    # Filename
    filename = ""
    if video_url:
        filename = unquote(Path(urlparse(video_url).path).name)
    if not filename or filename in ["video.mp4", "player.mp4"]:
        filename = f"{title}.mp4" if title else f"viralchut_{post_id}.mp4"

    return {
        "id": post_id,
        "title": title,
        "url": video_url,
        "poster": poster,
        "description": description,
        "filename": filename
    }

async def fetch_viralchut_post(domain: str, post_id: str, client: httpx.AsyncClient = None) -> list[dict]:
    """Fetch and parse a single Viralchut post page into standard post item format."""
    url = f"https://{domain}/{post_id}/" if not post_id.startswith("http") else post_id
    headers = get_headers(domain, f"https://{domain}/")
    close_client = False
    if client is None:
        client = httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30)
        close_client = True
    try:
        resp = await client.get(url)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = extract_viralchut_post_data(resp.text, url)
        if not data.get("url"):
            return []
        return [{
            "id": data["id"],
            "service": "viralchut",
            "name": sanitize(data["filename"]),
            "title": data["title"],
            "content": data["description"][:300],
            "url": data["url"],
            "viralchut_url": url,
            "thumbnail": data["poster"],
            "poster": data["poster"],
            "file": {
                "path": data["url"],
                "name": sanitize(data["filename"])
            }
        }]
    except Exception as e:
        logger.error(f"Failed to fetch Viralchut post {post_id}: {e}")
        return []
    finally:
        if close_client:
            await client.aclose()

async def fetch_viralchut_listing(domain: str, target: str, page_range: tuple[int, int] | None = None, client: httpx.AsyncClient = None) -> list[dict]:
    """Fetch video items across pages from a Viralchut category, tag, search, or home listing."""
    all_items = []
    headers = get_headers(domain, f"https://{domain}/")
    close_client = False
    if client is None:
        client = httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30)
        close_client = True

    try:
        def get_page_url(p: int) -> str:
            if target.startswith("category/"):
                cat = target.split("/", 1)[1]
                return f"https://{domain}/category/{cat}/page/{p}/" if p > 1 else f"https://{domain}/category/{cat}/"
            elif target.startswith("tag/"):
                tag = target.split("/", 1)[1]
                return f"https://{domain}/tag/{tag}/page/{p}/" if p > 1 else f"https://{domain}/tag/{tag}/"
            elif target.startswith("search/"):
                q = target.split("/", 1)[1]
                return f"https://{domain}/page/{p}/?s={quote(q)}" if p > 1 else f"https://{domain}/?s={quote(q)}"
            else:
                return f"https://{domain}/page/{p}/" if p > 1 else f"https://{domain}/"

        url_p1 = get_page_url(1)
        resp_p1 = await client.get(url_p1)
        resp_p1.raise_for_status()
        soup_p1 = BeautifulSoup(resp_p1.text, "html.parser")

        # Determine max_page from pagination element
        max_page = 1
        pagination = soup_p1.find(class_=re.compile(r"pagination|nav-links|page-numbers"))
        if pagination:
            nums = [int(x) for x in re.findall(r'\b\d+\b', pagination.text)]
            if nums:
                max_page = max(nums)

        start_p = 1
        end_p = max_page
        if page_range:
            start_p, end_p = page_range
            end_p = min(end_p, max_page)

        seen_links = set()
        for p in range(start_p, end_p + 1):
            if p == 1:
                soup = soup_p1
            else:
                p_url = get_page_url(p)
                p_resp = await client.get(p_url)
                if p_resp.status_code != 200:
                    break
                soup = BeautifulSoup(p_resp.text, "html.parser")

            cards = soup.find_all(class_="video-block")
            if not cards:
                cards = soup.find_all("div", class_=lambda c: c and "video-block" in c)

            for card in cards:
                a_info = card.find("a", class_="infos") or card.find("a", class_="thumb") or card.find("a", href=True)
                if not a_info or not a_info.get("href"):
                    continue
                href = a_info["href"]
                if href in seen_links:
                    continue
                seen_links.add(href)

                card_id = card.get("data-post-id") or urlparse(href).path.strip("/").split("/")[-1]
                title_elem = card.find(class_="title")
                title = title_elem.get_text(strip=True) if title_elem else (a_info.get("title") or card_id)
                img = card.find("img")
                thumb = img.get("data-src") or img.get("src") if img else None

                all_items.append({
                    "id": str(card_id),
                    "service": "viralchut",
                    "name": sanitize(title) + ".mp4",
                    "title": title,
                    "url": href,
                    "viralchut_url": href,
                    "thumbnail": thumb,
                    "poster": thumb,
                    "file": {
                        "path": href,
                        "name": sanitize(title) + ".mp4"
                    }
                })
        print(f"\n[OK] Fetched {len(all_items)} items from Viralchut.")
        return all_items
    except Exception as e:
        logger.error(f"Error scraping Viralchut listing for {target}: {e}")
        return all_items
    finally:
        if close_client:
            await client.aclose()

async def resolve_viralchut_direct_url(page_url: str, headers: dict = None, client: httpx.AsyncClient = None) -> tuple[str, str, dict]:
    """Resolve a Viralchut post page URL to the direct CDN video URL and filename."""
    clean = page_url.split("?")[0].lower()
    if any(clean.endswith(ext) for ext in VIDEO_EXTENSIONS) and "viralchut.com" not in clean:
        filename = unquote(Path(urlparse(page_url).path).name)
        return page_url, sanitize(filename), {}

    close_client = False
    if client is None:
        if headers is None:
            headers = get_headers("viralchut.com", "https://viralchut.com/")
        client = httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30)
        close_client = True
    try:
        resp = await client.get(page_url)
        resp.raise_for_status()
        data = extract_viralchut_post_data(resp.text, page_url)
        direct_url = data.get("url")
        filename = data.get("filename") or "video.mp4"
        return direct_url, sanitize(filename), data
    finally:
        if close_client:
            await client.aclose()

def find_next_page_url(html: str, current_url: str, target_page_num: int) -> str | None:
    """Find the next page URL in HTML using pagination tags, rel='next', or URL pattern heuristics."""
    soup = BeautifulSoup(html, "html.parser")
    # 1. <link rel="next"> or <a rel="next">
    next_link = soup.find(["link", "a"], rel=lambda r: r and "next" in str(r).lower())
    if next_link and next_link.get("href"):
        return urljoin(current_url, next_link["href"])

    # 2. Pagination container
    pagination = soup.find(class_=re.compile(r"pagination|nav-links|page-numbers|pager", re.I))
    if pagination:
        for a in pagination.find_all("a", href=True):
            if a.get_text(strip=True) == str(target_page_num):
                return urljoin(current_url, a["href"])
        for a in pagination.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            if any(k in txt for k in ["next", "»", "▶", ">"]):
                return urljoin(current_url, a["href"])

    # 3. Fallback URL pattern replacement
    parsed = urlparse(current_url)
    clean_path = parsed.path.rstrip("/")
    if re.search(r'/page/\d+', clean_path):
        new_path = re.sub(r'/page/\d+', f'/page/{target_page_num}', clean_path)
        return parsed._replace(path=new_path).geturl()
    elif "page=" in parsed.query:
        new_query = re.sub(r'page=\d+', f'page={target_page_num}', parsed.query)
        return parsed._replace(query=new_query).geturl()
    elif "p=" in parsed.query:
        new_query = re.sub(r'p=\d+', f'p={target_page_num}', parsed.query)
        return parsed._replace(query=new_query).geturl()
    else:
        new_path = f"{clean_path}/page/{target_page_num}/"
        return parsed._replace(path=new_path).geturl()

def crawl_html_for_videos(html: str, base_url: str) -> list[dict]:
    """Universal crawler that scans any webpage HTML and returns discovered video cards and media links."""
    soup = BeautifulSoup(html, "html.parser")
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc.lower()
    srv = base_domain.replace("www.", "").split(".")[0] or "web"

    discovered = []
    seen_urls = set()

    # 1. Direct <video> tags on the page itself
    for v in soup.find_all("video"):
        src = v.get("src")
        if not src:
            for s in v.find_all("source"):
                if s.get("src"):
                    src = s.get("src")
                    break
        if src:
            full_src = urljoin(base_url, src)
            if full_src not in seen_urls:
                seen_urls.add(full_src)
                poster = urljoin(base_url, v.get("poster", "")) if v.get("poster") else None
                title = v.get("title") or unquote(Path(urlparse(full_src).path).stem)
                discovered.append({
                    "id": str(len(discovered) + 1),
                    "title": sanitize(title),
                    "url": full_src,
                    "thumbnail": poster,
                    "poster": poster,
                    "name": sanitize(title) + ".mp4",
                    "service": srv,
                    "file": {"path": full_src, "name": sanitize(title) + ".mp4"}
                })

    # 2. Search for video card containers
    card_selectors = [
        "article",
        ".video-block", ".video-item", ".video-card", ".video_card", ".thumb-block",
        "[class*='video-item']", "[class*='video-card']", "[class*='video_item']",
        "[class*='thumb']", "[class*='card']"
    ]
    cards = []
    for sel in card_selectors:
        found = soup.select(sel)
        if 2 <= len(found) <= 200:
            cards = found
            break

    EXCLUDE_PATTERNS = re.compile(r'(login|register|signup|signin|contact|privacy|terms|about|faq|cart|checkout|#|javascript:)', re.I)
    VIDEO_HREF_PATTERNS = re.compile(r'(\.(mp4|m3u8|webm)|/video/|/watch|/v/|/post/|/clip/|/play/|/media/|/detail/|/item/)', re.I)

    if cards:
        for idx, card in enumerate(cards):
            a_tag = card.find("a", href=True)
            if not a_tag: continue
            href = a_tag["href"].strip()
            if EXCLUDE_PATTERNS.search(href): continue

            full_href = urljoin(base_url, href)
            if full_href in seen_urls or full_href == base_url: continue

            # Title
            title = ""
            title_elem = card.find(class_=re.compile(r'(title|name|heading|header)', re.I))
            if title_elem:
                title = title_elem.get_text(strip=True)
            if not title:
                title = a_tag.get("title") or a_tag.get_text(strip=True)
            img = card.find("img")
            if not title and img and img.get("alt"):
                title = img.get("alt").strip()
            if not title:
                title = unquote(urlparse(full_href).path.strip("/").split("/")[-1])

            # Thumbnail
            thumb = None
            if img:
                thumb = img.get("data-src") or img.get("data-original") or img.get("src")
                if thumb:
                    thumb = urljoin(base_url, thumb)

            seen_urls.add(full_href)
            discovered.append({
                "id": str(idx + 1),
                "title": sanitize(title),
                "url": full_href,
                "thumbnail": thumb,
                "poster": thumb,
                "name": sanitize(title) + ".mp4",
                "service": srv,
                "file": {"path": full_href, "name": sanitize(title) + ".mp4"}
            })

    # 3. Fallback: Scan all <a> links for video patterns
    if len(discovered) < 2:
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if EXCLUDE_PATTERNS.search(href): continue
            full_href = urljoin(base_url, href)
            if full_href in seen_urls or full_href == base_url: continue

            parsed_href = urlparse(full_href)
            if parsed_href.netloc.lower() != base_domain and not any(ext in full_href.lower() for ext in STREAM_EXTENSIONS):
                continue

            if VIDEO_HREF_PATTERNS.search(full_href) or a.find("img"):
                img = a.find("img")
                title = a.get("title") or a.get_text(strip=True) or (img.get("alt") if img else "")
                if not title:
                    title = unquote(parsed_href.path.strip("/").split("/")[-1])
                if len(title) < 3: continue

                thumb = (img.get("data-src") or img.get("src")) if img else None
                if thumb: thumb = urljoin(base_url, thumb)

                seen_urls.add(full_href)
                discovered.append({
                    "id": str(len(discovered) + 1),
                    "title": sanitize(title),
                    "url": full_href,
                    "thumbnail": thumb,
                    "poster": thumb,
                    "name": sanitize(title) + ".mp4",
                    "service": srv,
                    "file": {"path": full_href, "name": sanitize(title) + ".mp4"}
                })

    return discovered

async def extract_native_video(html: str, page_url: str) -> dict:
    """Extract single video metadata from raw HTML using DOM, JSON-LD, OpenGraph, JS configs, and regex."""
    soup = BeautifulSoup(html, "html.parser")
    # Title
    title = ""
    for meta_prop in ["og:title", "twitter:title"]:
        el = soup.find("meta", property=re.compile(f"^{meta_prop}$", re.I)) or soup.find("meta", attrs={"name": re.compile(f"^{meta_prop}$", re.I)})
        if el and el.get("content"):
            title = el["content"].strip()
            break
    if not title:
        h1 = soup.find("h1")
        if h1: title = h1.get_text(strip=True)
    if not title and soup.title:
        title = soup.title.get_text(strip=True)
    if title:
        title = re.sub(r"\s*[-|–]\s*(YouTube|Vimeo|Viralchut|Watch Video|Pornhub|XVIDEOS|SpankBang).*$", "", title, flags=re.I).strip()

    # Poster
    poster = ""
    for meta_prop in ["og:image", "og:image:secure_url", "twitter:image"]:
        el = soup.find("meta", property=re.compile(f"^{meta_prop}$", re.I)) or soup.find("meta", attrs={"name": re.compile(f"^{meta_prop}$", re.I)})
        if el and el.get("content"):
            poster = urljoin(page_url, el["content"].strip())
            break

    # Description
    description = ""
    for meta_prop in ["og:description", "description"]:
        el = soup.find("meta", property=re.compile(f"^{meta_prop}$", re.I)) or soup.find("meta", attrs={"name": re.compile(f"^{meta_prop}$", re.I)})
        if el and el.get("content"):
            description = el["content"].strip()
            break

    # JSON-LD
    video_url = ""
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            txt = s.string or s.text or ""
            if not txt.strip(): continue
            data = json.loads(txt)
            items = data if isinstance(data, list) else [data]
            if isinstance(data, dict) and "@graph" in data:
                items.extend(data["@graph"])
            for item in items:
                if isinstance(item, dict) and ("video" in str(item.get("@type", "")).lower() or item.get("@type") == "VideoObject"):
                    c_url = item.get("contentUrl") or item.get("embedUrl")
                    if c_url:
                        video_url = urljoin(page_url, c_url)
                        if not title and item.get("name"): title = item["name"]
                        if not poster and item.get("thumbnailUrl"):
                            th = item["thumbnailUrl"]
                            poster = urljoin(page_url, th[0] if isinstance(th, list) else th)
                        break
        except Exception:
            pass
        if video_url: break

    # HTML5 Video
    if not video_url:
        for v in soup.find_all("video"):
            if v.get("poster") and not poster:
                poster = urljoin(page_url, v["poster"])
            if v.get("src"):
                video_url = urljoin(page_url, v["src"])
                break
            for s in v.find_all("source"):
                if s.get("src"):
                    video_url = urljoin(page_url, s["src"])
                    break
            if video_url: break

    # OpenGraph Video
    if not video_url:
        for prop in ["og:video", "og:video:url", "og:video:secure_url", "twitter:player:stream"]:
            og_v = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
            if og_v and og_v.get("content"):
                cand = urljoin(page_url, og_v["content"].strip())
                if is_direct_media_url(cand) or cand.startswith("http"):
                    video_url = cand
                    break

    # JS Player Configs
    if not video_url:
        for s in soup.find_all("script"):
            txt = s.string or s.text or ""
            if len(txt) < 10: continue
            m = re.search(r'["\']?(?:file|src|url|source|source_file)["\']?\s*:\s*["\'](https?://[^"\']+\.(?:mp4|m3u8|webm)[^"\']*)["\']', txt, re.I)
            if m:
                video_url = m.group(1).replace("\\/", "/")
                break
            m2 = re.search(r'(?:video_?url|videoSrc|streamUrl|hlsUrl|mediaUrl)\s*=\s*["\'](https?://[^"\']+)["\']', txt, re.I)
            if m2 and any(ext in m2.group(1).lower() for ext in [".mp4", ".m3u8", "/media/", "/video/"]):
                video_url = m2.group(1).replace("\\/", "/")
                break

    # Direct Regex Fallback
    if not video_url:
        direct_matches = re.findall(r'https?://[^\s"\'<>]+\.(?:mp4|m3u8|webm)[^\s"\'<>]*', html)
        valid = [m for m in direct_matches if not any(x in m.lower() for x in ["doubleclick", "googleads", "analytics", "pixel"])]
        if valid:
            video_url = valid[0]

    if video_url:
        video_url = re.sub(r'(https?://[^/]+)\./', r'\1/', video_url)

    filename = ""
    if video_url:
        path_name = unquote(Path(urlparse(video_url).path).name)
        if any(path_name.lower().endswith(ext) for ext in STREAM_EXTENSIONS):
            filename = path_name
    if not filename or filename in ["video.mp4", "player.mp4", "master.m3u8", "index.m3u8"]:
        slug = urlparse(page_url).path.strip("/").split("/")[-1]
        base_name = title or slug or "video"
        filename = f"{sanitize(base_name)}.mp4"

    return {
        "title": title or sanitize(filename).replace(".mp4", ""),
        "url": video_url,
        "poster": poster,
        "description": description[:300] if description else "",
        "filename": sanitize(filename),
    }

async def extract_universal_video(page_url: str, client: httpx.AsyncClient = None) -> dict:
    """Universal video extractor for any website using deep HTML inspection + yt-dlp fallback."""
    if is_direct_media_url(page_url):
        filename = unquote(Path(urlparse(page_url).path).name)
        srv = urlparse(page_url).netloc.replace("www.", "").split(".")[0] or "direct"
        return {
            "id": sanitize(Path(filename).stem),
            "service": srv,
            "name": sanitize(filename),
            "title": sanitize(Path(filename).stem),
            "content": "",
            "url": page_url,
            "thumbnail": None,
            "poster": None,
            "file": {"path": page_url, "name": sanitize(filename)}
        }

    close_client = False
    if client is None:
        headers = get_headers(urlparse(page_url).netloc, page_url)
        client = httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=25)
        close_client = True
    try:
        resp = await client.get(page_url)
        if resp.status_code == 200:
            native_data = await extract_native_video(resp.text, page_url)
            if native_data.get("url"):
                srv = urlparse(page_url).netloc.replace("www.", "").split(".")[0] or "web"
                return {
                    "id": sanitize(native_data["filename"]).replace(".mp4", ""),
                    "service": srv,
                    "name": native_data["filename"],
                    "title": native_data["title"],
                    "content": native_data["description"],
                    "url": native_data["url"],
                    "thumbnail": native_data["poster"],
                    "poster": native_data["poster"],
                    "file": {"path": native_data["url"], "name": native_data["filename"]}
                }

        # Fallback to yt-dlp
        if HAS_YTDLP:
            def _ytdl_extract():
                ydl_opts = {'skip_download': True, 'quiet': True, 'no_warnings': True}
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(page_url, download=False)
            try:
                info = await asyncio.to_thread(_ytdl_extract)
                if info:
                    v_url = info.get("url")
                    if not v_url and info.get("formats"):
                        v_url = info["formats"][-1].get("url")
                    title = info.get("title") or "video"
                    poster = info.get("thumbnail")
                    desc = info.get("description") or ""
                    srv = info.get("extractor") or urlparse(page_url).netloc.replace("www.", "").split(".")[0]
                    fname = f"{sanitize(title)}.mp4"
                    return {
                        "id": str(info.get("id") or sanitize(title)),
                        "service": srv,
                        "name": fname,
                        "title": title,
                        "content": desc[:300],
                        "url": v_url or page_url,
                        "thumbnail": poster,
                        "poster": poster,
                        "file": {"path": v_url or page_url, "name": fname}
                    }
            except Exception as e:
                logger.debug(f"yt-dlp extract failed for {page_url}: {e}")

        return {}
    finally:
        if close_client:
            await client.aclose()

async def crawl_universal_page(target_url: str, page_range: tuple[int, int] | None = None, client: httpx.AsyncClient = None) -> list[dict]:
    """Crawl any gallery, channel, profile, or website page and return discovered video entries."""
    all_items = []
    seen_urls = set()
    close_client = False
    domain = urlparse(target_url).netloc
    if client is None:
        headers = get_headers(domain, target_url)
        client = httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30)
        close_client = True

    try:
        # 1. Check if yt-dlp can flat-extract as a playlist/channel
        if HAS_YTDLP:
            def _ytdl_flat():
                ydl_opts = {'skip_download': True, 'quiet': True, 'no_warnings': True, 'extract_flat': True}
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    return ydl.extract_info(target_url, download=False)
            try:
                info = await asyncio.to_thread(_ytdl_flat)
                if info and info.get("entries"):
                    entries = list(info["entries"])
                    srv = info.get("extractor") or domain.replace("www.", "").split(".")[0]
                    for idx, ent in enumerate(entries):
                        if not ent: continue
                        ent_url = ent.get("url") or ent.get("webpage_url")
                        if not ent_url or ent_url in seen_urls: continue
                        seen_urls.add(ent_url)
                        title = ent.get("title") or f"Video {idx + 1}"
                        thumb = ent.get("thumbnail") or (ent.get("thumbnails")[0].get("url") if ent.get("thumbnails") else None)
                        all_items.append({
                            "id": str(ent.get("id") or (idx + 1)),
                            "service": srv,
                            "name": sanitize(title) + ".mp4",
                            "title": title,
                            "url": ent_url,
                            "thumbnail": thumb,
                            "poster": thumb,
                            "file": {"path": ent_url, "name": sanitize(title) + ".mp4"}
                        })
                    if all_items:
                        print(f"\n[OK] yt-dlp extracted {len(all_items)} entries from {domain}.")
                        return all_items
            except Exception as e:
                logger.debug(f"yt-dlp flat extraction skipped: {e}")

        # 2. Universal HTML Crawler across pages
        start_p = 1
        end_p = 1
        if page_range:
            start_p, end_p = page_range

        current_url = target_url
        for p in range(start_p, end_p + 1):
            if p > start_p:
                next_url = find_next_page_url(last_html, current_url, p)
                if not next_url or next_url == current_url:
                    break
                current_url = next_url

            resp = await client.get(current_url)
            if resp.status_code != 200:
                break
            last_html = resp.text
            page_items = crawl_html_for_videos(last_html, current_url)
            for itm in page_items:
                if itm["url"] not in seen_urls:
                    seen_urls.add(itm["url"])
                    all_items.append(itm)

        print(f"\n[OK] Universal crawler discovered {len(all_items)} videos on {domain}.")
        return all_items
    except Exception as e:
        logger.error(f"Universal crawl error for {target_url}: {e}")
        return all_items
    finally:
        if close_client:
            await client.aclose()

async def fetch_all_posts(domain: str, service: str, user_id: str, page_range: tuple[int, int] | None = None, post_id: str = None) -> list[dict]:
    """Fetch all creator posts, optionally limited by range or single post ID."""
    # 1. Viralchut
    is_viralchut = (service == "viralchut" or "viralchut" in domain.lower())
    if is_viralchut:
        if post_id:
            return await fetch_viralchut_post(domain, post_id)
        return await fetch_viralchut_listing(domain, user_id, page_range)

    # 2. Bunkr
    is_bunkr = (service == "bunkr" or any(x in domain.lower() for x in ["bunkr", "balbums"]))
    if is_bunkr:
        if post_id:
            item_type = post_id.split('_')[0] if '_' in post_id else 'v'
            item_url = f"https://{domain}/{item_type}/{user_id}"
            return [{
                "id": user_id,
                "service": "bunkr",
                "bunkr_f_url": item_url,
                "name": f"{user_id}.mp4",
                "title": f"Bunkr {item_type.upper()} {user_id}",
                "file": {
                    "path": f"/{item_type}/{user_id}",
                    "name": f"{user_id}.mp4"
                }
            }]
        return await fetch_bunkr_album(domain, user_id, page_range)

    # 3. Coomer / Kemono API
    is_coomer = any(x in domain.lower() for x in ["coomer", "kemono"])
    if is_coomer:
        if post_id:
            return await fetch_single_post(domain, service, user_id, post_id)
            
        all_posts = []
        headers = get_headers(domain, f"https://{domain}/{service}/user/{user_id}")
        
        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30) as client:
            if page_range:
                start, end = page_range
                offsets = range((start - 1) * POSTS_PER_PAGE, end * POSTS_PER_PAGE, POSTS_PER_PAGE)
            else:
                offsets = iter(range(0, 500000, POSTS_PER_PAGE))

            for offset in offsets:
                page_num = (offset // POSTS_PER_PAGE) + 1
                print(f"   Fetching page {page_num}...", end="\r")
                posts = await fetch_posts(domain, service, user_id, offset, client=client)
                if not posts:
                    break
                all_posts.extend(posts)
                if len(posts) < POSTS_PER_PAGE:
                    break
                await asyncio.sleep(API_DELAY)
                
        print(f"\n[OK] Fetched {len(all_posts)} posts.")
        return all_posts

    # 4. Direct Media File on ANY domain
    target_url = user_id if str(user_id).startswith("http") else (post_id if (post_id and str(post_id).startswith("http")) else f"https://{domain}/{user_id}")
    if service == "direct" or is_direct_media_url(target_url):
        filename = unquote(Path(urlparse(target_url).path).name) or "video.mp4"
        stem = Path(filename).stem
        return [{
            "id": sanitize(stem),
            "service": "direct",
            "name": sanitize(filename),
            "title": sanitize(stem),
            "content": "",
            "url": target_url,
            "thumbnail": None,
            "poster": None,
            "file": {"path": target_url, "name": sanitize(filename)}
        }]

    # 5. Universal Web Scraper & Crawler for ANY other website
    if post_id and not any(x in str(user_id).lower() for x in ("browse", "category", "tag", "search")):
        video_data = await extract_universal_video(target_url)
        if video_data and video_data.get("url"):
            return [video_data]
        return await crawl_universal_page(target_url, page_range)
    else:
        return await crawl_universal_page(target_url, page_range)

# ── Video Extraction ─────────────────────────────────────────────────────────

def extract_video_urls(domain: str, posts: list[dict]) -> list[dict]:
    """Extract unique video URLs from posts with rich metadata."""
    videos = []
    urls = set()
    for p in posts:
        post_title = p.get("title", "") or ""
        post_content = p.get("content", "") or ""
        service = p.get("service", "")
        creator_id = p.get("user", "")

        # Universal Web Videos (including Viralchut, direct media, and crawled items)
        direct_url = p.get("url") or (p.get("file", {}).get("path") if str(p.get("file", {}).get("path", "")).startswith("http") else None) or p.get("viralchut_url")
        if direct_url or p.get("service") in ("viralchut", "generic", "direct"):
            url = direct_url or p.get("url")
            name = p.get("name") or (sanitize(post_title) + ".mp4")
            if url and url not in urls:
                videos.append({
                    "url": url,
                    "name": sanitize(name),
                    "id": str(p.get("id", "unk")),
                    "service": p.get("service") or service or "generic",
                    "title": post_title.strip() or name,
                    "content": post_content.strip()[:300],
                    "thumbnail": p.get("thumbnail") or p.get("poster"),
                    "poster": p.get("poster") or p.get("thumbnail"),
                    "user": creator_id or p.get("user") or "web"
                })
                urls.add(url)
            continue

        if p.get("service") == "bunkr" or p.get("bunkr_f_url"):
            f_url = p["bunkr_f_url"]
            name = p.get("name") or "video.mp4"
            suffix = Path(name).suffix.lower()
            # If it's a bunkr item: include if video extension OR no extension (unknown yet) OR /v/ in url
            is_vid = suffix in VIDEO_EXTENSIONS or suffix == "" or "/v/" in f_url
            if is_vid and suffix not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".zip", ".rar", ".pdf"}:
                if f_url not in urls:
                    videos.append({
                        "url": f_url,
                        "bunkr_f_url": f_url,
                        "name": sanitize(name),
                        "id": p.get("id", "unk"),
                        "service": "bunkr",
                        "title": post_title.strip() or name,
                        "content": post_content.strip()[:300],
                        "user": creator_id
                    })
                    urls.add(f_url)
            continue
            
        target_files = []
        if p.get("file", {}).get("path"): target_files.append(p["file"])
        target_files.extend(p.get("attachments", []))
        
        for f in target_files:
            path = f.get("path", "")
            if path and Path(path).suffix.lower() in VIDEO_EXTENSIONS:
                url = f"https://{domain}/data{path}"
                if url not in urls:
                    raw_name = f.get("name") or Path(path).name
                    videos.append({
                        "url": url,
                        "name": sanitize(raw_name),
                        "id": p.get("id", "unk"),
                        "title": post_title.strip(),
                        "content": post_content.strip()[:300],
                        "service": service,
                        "user": creator_id
                    })
                    urls.add(url)
    return videos


def sanitize(n: str) -> str:
    n = re.sub(r'[<>:"/\\|?*]', "_", n)
    n = re.sub(r"[_\s]+", "_", n).strip("_")
    return n[:200] or "video.mp4"

# ── Downloading ──────────────────────────────────────────────────────────────

async def resolve_redirect_url(url: str, headers: dict) -> str:
    """Follow redirects to get the final CDN URL (e.g. n2.coomer.st)."""
    try:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=20) as c:
            async with c.stream("GET", url) as resp:
                return str(resp.url)
    except Exception as e:
        logger.warning(f"Redirect resolution failed for {url}: {e}")
        return url
async def resolve_bunkr_direct_url(f_url: str, headers: dict, client: httpx.AsyncClient = None) -> tuple[str, str]:
    """Resolve Bunkr file page URL to a direct signed CDN download link using Lysagxra/BunkrDownloader algorithms."""
    close_client = False
    if client is None:
        client = httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=25)
        close_client = True
    try:
        resp = await client.get(f_url)
        resp.raise_for_status()
        html = resp.text

        soup = BeautifulSoup(html, 'html.parser')

        # 1. Decode Cloudflare email obfuscation on any titles/text
        for cf in soup.find_all(class_=re.compile(r'__cf_email__')):
            cf_hex = cf.get("data-cfemail")
            if cf_hex:
                cf.replace_with(decode_cf_email(cf_hex))

        # 2. Extract resolved title / filename from h1 or title
        resolved_name = ""
        h1 = soup.find('h1', class_=re.compile(r'text-subs|font-semibold')) or soup.find('h1')
        if h1:
            resolved_name = h1.get_text(strip=True)
            try:
                resolved_name = resolved_name.encode("latin1").decode("utf-8")
            except (UnicodeEncodeError, UnicodeDecodeError):
                pass
        if not resolved_name:
            title_tag = soup.find('title')
            if title_tag:
                resolved_name = title_tag.get_text(strip=True)

        base_url = None
        media_path = None

        # 3. Strategy A: Check inline scripts for var jsCDN = "..." or mediafiles CDN
        js_cdn_match = re.search(r'var\s+jsCDN\s*=\s*["\']([^"\']+)["\']', html)
        if not js_cdn_match:
            js_cdn_match = re.search(r'["\'](https://[^"\']*?(?:bunkr|cdn\.cr)[^"\']*?/media/[^"\']+)["\']', html)

        if js_cdn_match:
            cdn_full = js_cdn_match.group(1).replace('\\/', '/')
            parsed = urlparse(cdn_full)
            base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            media_path = parsed.path
            logger.info(f"Bunkr resolved via inline jsCDN: {base_url}")

        # 4. Strategy B: Fallback to dl.bunkr.cr/api/_001_v2 API
        if not base_url:
            file_id = None
            script_tag = soup.find(lambda tag: tag.name == "script" and tag.has_attr("data-file-id"))
            if script_tag:
                file_id = script_tag.get("data-file-id")
            if not file_id:
                id_match = re.search(r'data-file-id=["\'](\d+)["\']', html) or re.search(r'[\'"]/file/(\d+)[\'"]', html)
                if id_match:
                    file_id = id_match.group(1)
            if not file_id:
                for a in soup.find_all('a', href=True):
                    m = re.search(r'/file/(\d+)', a['href'])
                    if m:
                        file_id = m.group(1)
                        break

            if file_id:
                api_headers = {
                    "Referer": BUNKR_DL_REFERER,
                    "Origin": "https://bunkr.cr",
                    "Accept": "application/json",
                    "Accept-Encoding": "gzip, deflate"
                }
                api_res = await client.post(BUNKR_DL_API, json={'id': file_id}, headers=api_headers)
                if api_res.status_code == 200:
                    meta_res = api_res.json()
                    raw_cdn = meta_res.get('mediafiles', '') + meta_res.get('path', '')
                    if raw_cdn:
                        parsed = urlparse(raw_cdn)
                        base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                        media_path = parsed.path
                        if meta_res.get('original') and not resolved_name:
                            resolved_name = meta_res['original']
                        logger.info(f"Bunkr resolved via API: {base_url}")

        # 5. Strategy C: Check HTML video/source/a download tags
        if not base_url:
            for src_el in soup.find_all(['source', 'video', 'a'], href=True) + soup.find_all(['source', 'video'], src=True):
                href = src_el.get('src') or src_el.get('href')
                if href and any(x in href for x in ['media-files', 'cdn.cr', '/media/']):
                    parsed = urlparse(href)
                    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                    media_path = parsed.path
                    break

        if not base_url or not media_path:
            raise ValueError(f"Could not extract media CDN URL for {f_url}")

        # 6. Request signature from glb-apisign.cdn.cr
        clean_path = unquote(media_path)
        if not clean_path.startswith('/'):
            clean_path = '/' + clean_path

        sign_headers = {"Referer": BUNKR_DL_REFERER, "Origin": "https://bunkr.cr"}
        sign_resp = await client.get(f"{BUNKR_SIGN_API}?path={quote(clean_path)}", headers=sign_headers)
        if sign_resp.status_code == 200:
            sign_data = sign_resp.json()
            token = sign_data.get('token')
            ex = sign_data.get('ex')
            if token and ex:
                sep = "&" if "?" in base_url else "?"
                direct_url = f"{base_url}{sep}token={token}&ex={ex}"
            else:
                direct_url = base_url
        else:
            logger.warning(f"Signature API returned {sign_resp.status_code}, using base URL")
            direct_url = base_url

        if not resolved_name:
            resolved_name = Path(clean_path).name or f_url.split('/')[-1]

        return direct_url, sanitize(resolved_name)
    finally:
        if close_client:
            await client.aclose()


async def resolve_media_stream_info(domain: str, video: dict, client: httpx.AsyncClient = None) -> dict:
    """
    Resolves real direct CDN URL, headers, filename, and checks Content-Length
    for streaming without downloading the payload to disk.
    """
    url = video.get("url") or video.get("bunkr_f_url")
    referer = f"https://{domain}/"
    headers = get_headers(domain, referer, is_media=True)

    # 1. If Viralchut, resolve direct CDN URL if it's still a post page
    if video.get("service") == "viralchut" or "viralchut" in domain.lower():
        raw_url = video.get("url") or video.get("viralchut_url")
        if raw_url and "viralchut.com" in raw_url and not any(raw_url.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
            try:
                direct_url, resolved_name, meta = await resolve_viralchut_direct_url(raw_url, client=client)
                if direct_url:
                    url = direct_url
                    video["url"] = direct_url
                    if resolved_name:
                        video["name"] = sanitize(resolved_name)
                    if meta.get("poster") and not video.get("thumbnail"):
                        video["thumbnail"] = meta["poster"]
                    logger.info(f"Resolved Viralchut direct video URL: {raw_url} -> {url[:60]}...")
            except Exception as e:
                logger.error(f"Failed to resolve Viralchut direct video URL for {raw_url}: {e}")
                return {"status": "error_html", "error": str(e)}

        headers["Referer"] = f"https://{domain}/"
        headers["Accept"] = "*/*"

    # 2. If Bunkr, resolve direct CDN signed URL dynamically
    elif video.get("service") == "bunkr" or video.get("bunkr_f_url") or "bunkr" in domain:
        f_url = video.get("bunkr_f_url") or url
        try:
            url, resolved_name = await resolve_bunkr_direct_url(f_url, headers, client=client)
            if resolved_name:
                video["name"] = sanitize(resolved_name)
            logger.info(f"Resolved Bunkr direct URL for streaming: {f_url} -> {url[:60]}...")
            headers["Referer"] = BUNKR_DL_REFERER
            headers["Origin"] = "https://bunkr.cr"
            headers["Sec-Fetch-Site"] = "cross-site"
            headers["Accept"] = "*/*"
        except Exception as e:
            logger.error(f"Failed to resolve Bunkr direct URL for {f_url}: {e}")
            return {"status": "error_html", "error": str(e)}

    # 3. Universal Web Video Resolution for any other website
    elif url and not is_direct_media_url(url):
        try:
            item = await extract_universal_video(url, client=client)
            if item and item.get("url"):
                url = item["url"]
                video["url"] = item["url"]
                if item.get("name"): video["name"] = sanitize(item["name"])
                if item.get("thumbnail") and not video.get("thumbnail"): video["thumbnail"] = item["thumbnail"]
                if item.get("title") and not video.get("title"): video["title"] = item["title"]
                logger.info(f"Resolved universal direct video URL: {video.get('name')} -> {url[:60]}...")
        except Exception as e:
            logger.warning(f"Universal stream resolution failed for {video.get('name')}: {e}")
            return {"status": "error_html", "error": str(e)}

    # If stream is HLS (.m3u8, .mpd) or fragmented: signal fallback to disk download pipeline
    if any(x in url.lower() for x in [".m3u8", ".mpd"]):
        logger.info(f"HLS/Segmented stream detected for {video.get('name')}. Routing to disk downloader pipeline...")
        return {"status": "error_html", "error": "HLS stream requires disk download pipeline"}

    # 2. Resolve redirects
    resolved_url = await resolve_redirect_url(url, headers)
    if resolved_url and resolved_url != url:
        logger.info(f"Redirect resolved: {url} -> {resolved_url}")
        url = resolved_url
        cdn_host = urlparse(url).netloc
        headers = get_headers(cdn_host, referer, is_media=True)
        if any(x in cdn_host.lower() for x in ["bunkr", "balbums", "cdn.cr"]):
            headers["Referer"] = BUNKR_DL_REFERER
            headers["Origin"] = "https://bunkr.cr"
            headers["Sec-Fetch-Site"] = "cross-site"

    # 3. Probe headers to get Content-Length and validate status
    close_client = False
    if client is None:
        client = httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30.0)
        close_client = True

    try:
        # Send lightweight stream request to inspect response headers without downloading body
        req_headers = headers.copy()
        if any(x in url.lower() for x in ["bunkr", "balbums", "cdn.cr"]):
            req_headers["Referer"] = BUNKR_DL_REFERER
            req_headers["Origin"] = "https://bunkr.cr"

        async with client.stream("GET", url, headers=req_headers, timeout=30) as resp:
            if resp.status_code in (401, 403, 404):
                logger.error(f"HTTP {resp.status_code} error when inspecting {video.get('name')}")
                return {"status": "error_html", "error": f"HTTP {resp.status_code}"}

            content_type = resp.headers.get("Content-Type", "").lower()
            if "text/html" in content_type:
                logger.error(f"Skipping {video.get('name')} (Received HTML instead of media)")
                return {"status": "error_html", "error": "HTML response"}

            total_server_size = 0
            content_range = resp.headers.get("Content-Range", "")
            if content_range and "/" in content_range:
                try:
                    total_server_size = int(content_range.split("/")[-1])
                except ValueError:
                    total_server_size = 0
            elif "Content-Length" in resp.headers:
                try:
                    total_server_size = int(resp.headers.get("Content-Length", 0))
                except ValueError:
                    total_server_size = 0

            if total_server_size > MAX_FILE_SIZE:
                logger.warning(f"Skipping {video.get('name')} (Size: {total_server_size} bytes exceeds 2000MB limit)")
                return {"status": "skipped_size", "file_size": total_server_size}

            if 0 < total_server_size < MIN_FILE_SIZE:
                logger.warning(f"Skipping {video.get('name')} (Size: {total_server_size} bytes too small, likely broken)")
                return {"status": "skipped_small", "file_size": total_server_size}

            return {
                "status": "ok",
                "url": url,
                "headers": req_headers,
                "name": video.get("name") or "video.mp4",
                "file_size": total_server_size
            }
    except Exception as e:
        logger.error(f"Error inspecting media stream for {video.get('name')}: {e}")
        return {"status": "error_html", "error": str(e)}
    finally:
        if close_client:
            await client.aclose()


async def download_video(domain: str, video: dict, output_dir: Path, pos: int = 0, client: httpx.AsyncClient = None, progress_callback: Callable = None) -> str | bool:
    """Download video with resume support and progress bar (asynchronous)."""
    url = video["url"]
    filepath = output_dir / f"{video['id']}_{video['name']}"
    referer = f"https://{domain}/"
    
    headers = get_headers(domain, referer, is_media=True)

    # If this is a Viralchut video, resolve direct CDN link if it's still a post page
    if video.get("service") == "viralchut" or "viralchut" in domain.lower():
        raw_url = video.get("url") or video.get("viralchut_url")
        if raw_url and "viralchut.com" in raw_url and not any(raw_url.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
            try:
                direct_url, resolved_name, meta = await resolve_viralchut_direct_url(raw_url, client=client)
                if direct_url:
                    url = direct_url
                    video["url"] = direct_url
                    if resolved_name:
                        video["name"] = sanitize(resolved_name)
                        filepath = output_dir / f"{video['id']}_{video['name']}"
                    if meta.get("poster") and not video.get("thumbnail"):
                        video["thumbnail"] = meta["poster"]
                    logger.info(f"Resolved Viralchut direct URL: {raw_url} -> {url[:60]}...")
            except Exception as e:
                logger.error(f"Failed to resolve Viralchut direct URL for {raw_url}: {e}")
                return "error_html"

        headers["Referer"] = f"https://{domain}/"
        headers["Accept"] = "*/*"

    # If this is a Bunkr video, resolve the direct CDN signed URL dynamically
    elif video.get("service") == "bunkr" or video.get("bunkr_f_url") or "bunkr" in domain:
        f_url = video.get("bunkr_f_url") or url
        try:
            url, resolved_name = await resolve_bunkr_direct_url(f_url, headers, client=client)
            if resolved_name:
                video["name"] = sanitize(resolved_name)
                filepath = output_dir / f"{video['id']}_{video['name']}"
            logger.info(f"Resolved Bunkr direct URL: {f_url} -> {url[:60]}...")
            
            # Apply Lysagxra/BunkrDownloader CDN download headers
            headers["Referer"] = BUNKR_DL_REFERER
            headers["Origin"] = "https://bunkr.cr"
            headers["Sec-Fetch-Site"] = "cross-site"
            headers["Accept"] = "*/*"
        except Exception as e:
            logger.error(f"Failed to resolve Bunkr direct URL for {f_url}: {e}")
            return "error_html"

    # Universal Web Video Resolution if still a webpage URL
    elif url and not is_direct_media_url(url):
        try:
            item = await extract_universal_video(url, client=client)
            if item and item.get("url"):
                url = item["url"]
                video["url"] = item["url"]
                if item.get("name"):
                    video["name"] = sanitize(item["name"])
                    filepath = output_dir / f"{video['id']}_{video['name']}"
                if item.get("thumbnail") and not video.get("thumbnail"): video["thumbnail"] = item["thumbnail"]
                logger.info(f"Resolved universal direct URL: {video.get('name')} -> {url[:60]}...")
        except Exception as e:
            logger.error(f"Failed to resolve universal direct URL for {url}: {e}")
            return "error_html"

    # If stream is HLS/DASH (.m3u8, .mpd), download and mux via yt-dlp
    if any(x in url.lower() for x in (".m3u8", ".mpd")) and HAS_YTDLP:
        try:
            os.makedirs(output_dir, exist_ok=True)
            logger.info(f"Downloading HLS/segmented stream via yt-dlp: {url[:60]}...")
            def _ytdl_download():
                last_hook_time = [0.0]
                def _hook(d):
                    if d.get('status') == 'downloading' and progress_callback:
                        now = time.time()
                        if now - last_hook_time[0] >= 1.5:
                            last_hook_time[0] = now
                            dl = d.get('downloaded_bytes', 0)
                            tot = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
                            try:
                                res = progress_callback(dl, tot)
                                if asyncio.iscoroutine(res):
                                    asyncio.run_coroutine_threadsafe(res, asyncio.get_event_loop())
                            except Exception: pass

                ydl_opts = {
                    'outtmpl': str(filepath),
                    'quiet': True,
                    'no_warnings': True,
                    'overwrites': True,
                    'format': 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
                    'progress_hooks': [_hook],
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])

            await asyncio.to_thread(_ytdl_download)
            if filepath.exists() and filepath.stat().st_size >= MIN_FILE_SIZE:
                return True
        except Exception as e:
            logger.error(f"yt-dlp download failed for {video.get('name')}: {e}")

    # Resolve the actual CDN URL by following redirects once upfront
    resolved_url = await resolve_redirect_url(url, headers)
    if resolved_url and resolved_url != url:
        logger.info(f"Redirect resolved: {url} -> {resolved_url}")
        url = resolved_url
        cdn_host = urlparse(url).netloc
        headers = get_headers(cdn_host, referer, is_media=True)
        if any(x in cdn_host.lower() for x in ["bunkr", "balbums", "cdn.cr"]):
            headers["Referer"] = BUNKR_DL_REFERER
            headers["Origin"] = "https://bunkr.cr"
            headers["Sec-Fetch-Site"] = "cross-site"
    
    async def _do_download(c: httpx.AsyncClient):
        for attempt in range(1, MAX_RETRIES + 1):
            total_server_size = 0
            try:
                start_pos = 0
                if filepath.exists():
                    start_pos = filepath.stat().st_size
                
                # Prepare Range request
                req_headers = headers.copy()
                if start_pos > 0:
                    req_headers["Range"] = f"bytes={start_pos}-"
                if any(x in url.lower() for x in ["bunkr", "balbums", "cdn.cr"]):
                    req_headers["Referer"] = BUNKR_DL_REFERER
                    req_headers["Origin"] = "https://bunkr.cr"
                
                async with c.stream("GET", url, headers=req_headers, timeout=60) as resp:
                    if resp.status_code == 416: 
                        # Range not satisfiable: file might already be complete
                        if filepath.exists() and filepath.stat().st_size >= MIN_FILE_SIZE:
                            return True
                        else:
                            if filepath.exists(): os.remove(filepath)
                            raise ValueError("Range 416 error but file size mismatch. Restarting.")
                    
                    if resp.status_code in (401, 403, 404):
                        logger.error(f"HTTP {resp.status_code} error when downloading {video['name']}")
                        return "error_html"

                    content_type = resp.headers.get("Content-Type", "").lower()
                    if "text/html" in content_type:
                        logger.error(f"Skipping {video['name']} (Received HTML instead of media)")
                        return "error_html"

                    content_range = resp.headers.get("Content-Range", "")
                    if content_range and "/" in content_range:
                        try:
                            total_server_size = int(content_range.split("/")[-1])
                        except ValueError:
                            total_server_size = 0
                    elif resp.status_code == 206:
                        content_len = int(resp.headers.get("Content-Length", 0))
                        total_server_size = start_pos + content_len
                    else:
                        total_server_size = int(resp.headers.get("Content-Length", 0))
                    
                    if total_server_size > MAX_FILE_SIZE:
                        logger.warning(f"Skipping {video['name']} (Size: {total_server_size} bytes exceeds 2000MB limit)")
                        return "skipped_size"
                    
                    if 0 < total_server_size < MIN_FILE_SIZE and start_pos == 0:
                        logger.warning(f"Skipping {video['name']} (Size: {total_server_size} bytes too small, likely broken)")
                        return "skipped_small"

                    if start_pos >= total_server_size and total_server_size > 0:
                        return True

                    is_resume = (start_pos > 0 and resp.status_code == 206)
                    mode = "ab" if is_resume else "wb"
                    if mode == "wb": 
                        start_pos = 0

                    downloaded_bytes = start_pos
                    last_cb_time = 0.0
                    os.makedirs(output_dir, exist_ok=True)
                    with open(filepath, mode) as f:
                        with tqdm(
                            total=total_server_size if total_server_size > 0 else None,
                            initial=start_pos,
                            unit="B",
                            unit_scale=True,
                            unit_divisor=1024,
                            desc=f"   {video['name'][:30]}",
                            position=pos,
                            leave=False,
                            ncols=80
                        ) as pbar:
                            async for chunk in resp.aiter_bytes(CHUNK_SIZE):
                                f.write(chunk)
                                chunk_len = len(chunk)
                                pbar.update(chunk_len)
                                downloaded_bytes += chunk_len
                                if progress_callback:
                                    now = time.time()
                                    if now - last_cb_time >= 1.5:
                                        last_cb_time = now
                                        try:
                                            res = progress_callback(downloaded_bytes, total_server_size)
                                            if asyncio.iscoroutine(res):
                                                await res
                                        except Exception:
                                            pass
                    if progress_callback:
                        try:
                            res = progress_callback(downloaded_bytes, total_server_size)
                            if asyncio.iscoroutine(res):
                                await res
                        except Exception:
                            pass
                
                # Final verification
                if filepath.exists():
                    final_size = filepath.stat().st_size
                    if total_server_size > 0 and final_size < total_server_size:
                        raise ValueError(f"Incomplete download: {final_size}/{total_server_size} bytes")
                
                return True

            except Exception as e:
                if attempt < MAX_RETRIES:
                    logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {video['name']}: {e}. Retrying in {RETRY_DELAY}s...")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Failed to download {video['name']} after {MAX_RETRIES} attempts: {e}")
                    if filepath.exists() and total_server_size > 0 and filepath.stat().st_size < total_server_size:
                        try: os.remove(filepath) 
                        except: pass
                    return False
        return False

    if client:
        return await _do_download(client)
    else:
        async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=60) as c:
            return await _do_download(c)

# ── CLI ──────────────────────────────────────────────────────────────────────

async def amain():
    parser = argparse.ArgumentParser(description="🎬 Enhanced Coomer.st Scraper (Async)")
    parser.add_argument("--url", "-u", help="Profile URL")
    parser.add_argument("--pages", "-p", help="Page range (e.g. 1-3)")
    parser.add_argument("--output", "-o", help="Output directory")
    parser.add_argument("--threads", "-t", type=int, default=4, help="Download concurrency (default: 4)")
    parser.add_argument("--crawl", action="store_true", help="Crawl creators list")
    parser.add_argument("--service", help="Filter crawler by service (onlyfans, fansly)")
    parser.add_argument("--name", help="Filter crawler by creator name")
    
    args = parser.parse_args()
    
    if not args.url and not args.crawl:
        print("\n🎬 Coomer.st Scraper - Interactive Mode")
        print("-" * 40)
        args.url = input("🔗 Enter Profile URL: ").strip()
        if not args.url:
            print("❌ URL is required.")
            return
            
        crawl_choice = input("🕷️  Crawl creators instead? (y/n): ").lower()
        if crawl_choice == 'y':
            args.crawl = True
            args.service = input("   Filter by service (onlyfans/fansly/etc, leave blank for all): ").strip() or None
            args.name = input("   Filter by name: ").strip() or None

    if args.crawl:
        domain = DEFAULT_DOMAIN
        found = await fetch_creators(domain, args.service, args.name)
        if not found:
            print("❌ No creators found matching filters.")
            return
        print(f"✅ Found {len(found)} creators. Top results:")
        for c in found[:15]:
            print(f"   [{c.get('service')}] {c.get('name')} -> https://{domain}/{c['service']}/user/{c['id']}")
        return

    if not args.pages:
        args.pages = input("📄 Enter page range (e.g., 1-3, 5, or leave blank for all): ").strip() or None

    domain, service, user_id, post_id = parse_media_url(args.url)
    if not domain: return
        
    page_range = parse_page_range(args.pages)
    output_dir = Path(args.output or f"downloads/{service}_{user_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"🚀 Scraping: {service}/{user_id} on {domain} | Concurrency: {args.threads}")
    print("=" * 60)

    posts = await fetch_all_posts(domain, service, user_id, page_range, post_id=post_id)
    if not posts: return
    
    videos = extract_video_urls(domain, posts)
    if not videos:
        print("⚠ No videos found.")
        return

    print(f"📦 Found {len(videos)} videos. Initializing async download...")
    
    # Concurrent downloads with semaphore
    semaphore = asyncio.Semaphore(args.threads)
    
    async def wrapped_download(vid, i):
        async with semaphore:
            return await download_video(domain, vid, output_dir, (i % args.threads) + 1)

    overall_pbar = tqdm(total=len(videos), desc="Overall Progress", unit="vid", position=0, leave=True, ncols=80)
    
    tasks = [wrapped_download(vid, i) for i, vid in enumerate(videos)]
    
    success_count = 0
    for f in asyncio.as_completed(tasks):
        if await f:
            success_count += 1
        overall_pbar.update(1)
    
    overall_pbar.close()

    print("\n" + "=" * 60)
    print(f"🏁 Finished! Successfully downloaded: {success_count}/{len(videos)}")
    print(f"📂 Saved to: {output_dir.resolve()}")
    print("=" * 60)

if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass
