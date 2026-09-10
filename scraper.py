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
from urllib.parse import urlparse, urljoin, quote, unquote
from typing import List, Optional, Tuple, Dict, Any, Callable

import httpx
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_DOMAIN = "coomer.st"
POSTS_PER_PAGE = 50  # coomer.st returns 50 posts per API page
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".webm", ".m4v", ".flv", ".ts", ".m2ts"}

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
    # coomer.st uses DDoS-Guard which requires Accept: text/css to bypass challenges
    if is_bunkr:
        accept_val = "*/*"
        default_referer = BUNKR_DL_REFERER if is_media else f"https://{domain}/"
    else:
        accept_val = "text/css" if ("coomer" in domain or not is_media) else "*/*"
        default_referer = f"https://{domain}/"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Accept": accept_val,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer or default_referer,
        "Origin": f"https://{domain}",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "video" if is_media else "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site" if is_bunkr else "same-origin",
    }
    return headers

# ── URL Parsing ──────────────────────────────────────────────────────────────

def parse_media_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Parse coomer, kemono, or bunkr URL to get domain, service, user ID, and optional post ID."""
    if not url.startswith("http"):
        url = "https://" + url

    parsed = urlparse(url)
    domain = parsed.netloc or DEFAULT_DOMAIN
    clean_path = parsed.path.rstrip("/")
    
    # Support Bunkr URLs (albums and single items across all mirrors)
    is_bunkr = any(x in domain.lower() for x in ["bunkr", "balbums"])
    if is_bunkr:
        # Bunkr album: /a/<album_id>
        bunkr_album_match = re.match(r"^/a/([^/?#]+)", clean_path)
        if bunkr_album_match:
            return domain, "bunkr", bunkr_album_match.group(1), None

        # Bunkr single item: /v/<id>, /f/<id>, /d/<id>, /i/<id>
        bunkr_item_match = re.match(r"^/(v|f|d|i)/([^/?#]+)", clean_path)
        if bunkr_item_match:
            item_type, item_id = bunkr_item_match.group(1), bunkr_item_match.group(2)
            return domain, "bunkr", item_id, f"{item_type}_{item_id}"

    # Support single post URLs: https://coomer.st/<service>/user/<user_id>/post/<post_id>
    single_post_match = re.match(r"^/([^/]+)/user/([^/]+)/post/([^/?#]+)", clean_path)
    if single_post_match:
        return domain, single_post_match.group(1), single_post_match.group(2), single_post_match.group(3)
        
    # Support creator profile URLs: https://coomer.st/<service>/user/<user_id>
    profile_match = re.match(r"^/([^/]+)/user/([^/?#]+)", clean_path)
    if profile_match:
        return domain, profile_match.group(1), profile_match.group(2), None

    print(f"\n❌ Invalid media or profile URL: {url}")
    print("   Expected format: https://coomer.st/<service>/user/<id> or https://bunkr.cr/a/<id> or https://bunkr.cr/v/<id>")
    return None, None, None, None


def parse_profile_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Backwards-compatible wrapper returning (domain, service, user_id)."""
    domain, service, user_id, _ = parse_media_url(url)
    return domain, service, user_id


def parse_page_range(pages_str: str) -> tuple[int, int] | None:
    """Parse page range string like '1-5' or '3'."""
    if not pages_str:
        return None
    pages_str = pages_str.strip()
    if "-" in pages_str:
        try:
            start, end = map(int, pages_str.split("-", 1))
            if start < 1 or end < start: raise ValueError
            return start, end
        except ValueError:
            print(f"❌ Invalid range: {pages_str}. Use '1-5'.")
            sys.exit(1)
    else:
        try:
            p = int(pages_str)
            if p < 1: raise ValueError
            return p, p
        except ValueError:
            print(f"❌ Invalid page: {pages_str}.")
            sys.exit(1)

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

async def fetch_all_posts(domain: str, service: str, user_id: str, page_range: tuple[int, int] | None = None, post_id: str = None) -> list[dict]:
    """Fetch all creator posts, optionally limited by range or single post ID."""
    is_bunkr = (service == "bunkr" or any(x in domain.lower() for x in ["bunkr", "balbums"]))
    if is_bunkr:
        if post_id:
            # Single Bunkr item (e.g. v_AbCd123)
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

    if post_id:
        return await fetch_single_post(domain, service, user_id, post_id)
        
    all_posts = []
    headers = get_headers(domain, f"https://{domain}/{service}/user/{user_id}")
    
    async with httpx.AsyncClient(headers=headers, follow_redirects=True, timeout=30) as client:
        if page_range:
            start, end = page_range
            offsets = range((start-1)*POSTS_PER_PAGE, end*POSTS_PER_PAGE, POSTS_PER_PAGE)
        else:
            offsets = iter(range(0, 500000, POSTS_PER_PAGE))

        for offset in offsets:
            page_num = (offset // POSTS_PER_PAGE) + 1
            print(f"   Fetching page {page_num}...", end="\r")
            posts = await fetch_posts(domain, service, user_id, offset, client=client)
            if not posts: break
            all_posts.extend(posts)
            if len(posts) < POSTS_PER_PAGE: break
            await asyncio.sleep(API_DELAY)
            
    print(f"\n[OK] Fetched {len(all_posts)} posts.")
    return all_posts

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

    # 1. If Bunkr, resolve direct CDN signed URL dynamically
    if video.get("service") == "bunkr" or video.get("bunkr_f_url") or "bunkr" in domain:
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

    # If this is a Bunkr video, resolve the direct CDN signed URL dynamically
    if video.get("service") == "bunkr" or video.get("bunkr_f_url") or "bunkr" in domain:
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

    domain, service, user_id = parse_profile_url(args.url)
    if not domain: return
        
    page_range = parse_page_range(args.pages)
    output_dir = Path(args.output or f"downloads/{service}_{user_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print(f"🚀 Scraping: {service}/{user_id} on {domain} | Concurrency: {args.threads}")
    print("=" * 60)

    posts = await fetch_all_posts(domain, service, user_id, page_range)
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
