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
from urllib.parse import urlparse
from typing import List, Optional, Tuple, Dict, Any, Callable

import httpx
from tqdm.asyncio import tqdm

# ── Constants ────────────────────────────────────────────────────────────────

DEFAULT_DOMAIN = "coomer.st"
POSTS_PER_PAGE = 50  # coomer.st returns 50 posts per API page
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".wmv", ".webm", ".m4v", ".flv"}

MAX_RETRIES = 10
RETRY_DELAY = 10        # seconds between retries
API_DELAY = 1.0        # seconds between API calls
CHUNK_SIZE = 256 * 1024  # 256KB chunks for better throughput

logger = logging.getLogger(__name__)

def get_headers(domain=DEFAULT_DOMAIN, referer=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        "Accept": "text/css",  # Special header to bypass DDG on coomer.st
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer or f"https://{domain}/",
        "Origin": f"https://{domain}",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }
    return headers

# ── URL Parsing ──────────────────────────────────────────────────────────────

def parse_profile_url(url: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse coomer profile URL to get domain, service, and user ID."""
    if not url.startswith("http"):
        url = "https://" + url

    parsed = urlparse(url)
    domain = parsed.netloc or DEFAULT_DOMAIN
    match = re.match(r"^/([^/]+)/user/([^/]+)", parsed.path)
    if not match:
        print(f"\n❌ Invalid profile URL: {url}")
        print("   Expected format: https://coomer.st/<service>/user/<id>")
        return None, None, None

    return domain, match.group(1), match.group(2)

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

async def fetch_all_posts(domain: str, service: str, user_id: str, page_range: tuple[int, int] | None = None) -> list[dict]:
    """Fetch all creator posts, optionally limited by range."""
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
            
    print(f"\n✅ Fetched {len(all_posts)} posts.")
    return all_posts

# ── Video Extraction ─────────────────────────────────────────────────────────

def extract_video_urls(domain: str, posts: list[dict]) -> list[dict]:
    """Extract unique video URLs from posts."""
    videos = []
    urls = set()
    for p in posts:
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
                        "id": p.get("id", "unk")
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
            resp = await c.head(url)
            return str(resp.url)  # final URL after all redirects
    except Exception:
        return url  # fallback to original on any error


async def download_video(domain: str, video: dict, output_dir: Path, pos: int = 0, client: httpx.AsyncClient = None) -> bool:
    """Download video with resume support and progress bar (asynchronous)."""
    url = video["url"]
    filepath = output_dir / f"{video['id']}_{video['name']}"
    # Build a safe referer from the base domain (data URLs are not profile URLs)
    referer = f"https://{domain}/"
    
    headers = get_headers(domain, referer)

    # Resolve the actual CDN URL by following redirects once upfront
    resolved_url = await resolve_redirect_url(url, headers)
    if resolved_url != url:
        logger.info(f"Redirect resolved: {url} -> {resolved_url}")
        url = resolved_url
        # Update domain/headers if CDN host differs (e.g. n2.coomer.st)
        cdn_host = urlparse(url).netloc
        headers = get_headers(cdn_host, referer)
    
    async def _do_download(c: httpx.AsyncClient):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                start_pos = 0
                if filepath.exists():
                    start_pos = filepath.stat().st_size
                
                # GET info via HEAD on the already-resolved CDN URL (no redirect expected)
                head_resp = await c.head(url, headers=headers, timeout=30)
                total_server_size = int(head_resp.headers.get("Content-Length", 0))
                
                if start_pos >= total_server_size and total_server_size > 0:
                    return True
                
                # Prepare Range request
                req_headers = headers.copy()
                if start_pos > 0:
                    req_headers["Range"] = f"bytes={start_pos}-"
                
                async with c.stream("GET", url, headers=req_headers, timeout=60) as resp:
                    if resp.status_code == 416: 
                        if filepath.exists() and total_server_size > 0 and filepath.stat().st_size >= total_server_size:
                            return True
                        else:
                            if filepath.exists(): os.remove(filepath)
                            raise ValueError("Range 416 error but file size mismatch. Restarting.")
                    
                    is_resume = (start_pos > 0 and resp.status_code == 206)
                    mode = "ab" if is_resume else "wb"
                    if mode == "wb": start_pos = 0

                    os.makedirs(output_dir, exist_ok=True)
                    with open(filepath, mode) as f:
                        with tqdm(
                            total=total_server_size,
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
                                pbar.update(len(chunk))
                
                # Final verification
                if filepath.exists():
                    final_size = filepath.stat().st_size
                    if total_server_size > 0 and final_size < total_server_size:
                        raise ValueError(f"Incomplete download: {final_size}/{total_server_size} bytes")
                
                return True

            except Exception as e:
                if attempt < MAX_RETRIES:
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
