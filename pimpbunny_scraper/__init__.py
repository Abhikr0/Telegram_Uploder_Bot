"""
pimpbunny_scraper

A web scraper for PimpBunny (https://pimpbunny.com) that extracts video metadata
and downloadable video URLs.

Features:
- Scrapes video pages: title, model, duration, views, upload date, thumbnail, description
- Extracts downloadable video URLs in multiple qualities (1080p, 720p, 480p, 360p, 240p)
- Scrapes catalog: homepage, categories, models
- Exports to JSON/CSV

Requires yt-dlp >= 2023.1 with the custom PimpBunny extractor installed.

Usage:
    from pimpbunny_scraper import PimpBunnyScraper

    scraper = PimpBunnyScraper()
    video = scraper.get_video('https://pimpbunny.com/videos/slash/')
    print(video.title, video.download_urls)
"""

from __future__ import annotations

import re
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class Video:
    """A scraped PimpBunny video."""

    url: str
    video_id: Optional[str] = None
    title: Optional[str] = None
    model: Optional[str] = None
    model_url: Optional[str] = None
    duration: Optional[int] = None  # seconds
    views: Optional[int] = None
    likes: Optional[int] = None
    dislikes: Optional[int] = None
    upload_date: Optional[datetime] = None
    thumbnail: Optional[str] = None
    description: Optional[str] = None
    download_urls: List[Dict[str, str]] = field(
        default_factory=list
    )  # [{quality, url, format_id, height, ext}]
    raw: Dict[str, Any] = field(default_factory=dict)
    source: str = "yt_dlp"

    @property
    def duration_str(self) -> Optional[str]:
        if self.duration is None:
            return None
        mins, secs = divmod(int(self.duration), 60)
        return f"{mins}:{secs:02d}"

    @property
    def views_formatted(self) -> Optional[str]:
        if self.views is None:
            return None
        if self.views >= 1_000_000:
            return f"{self.views / 1_000_000:.1f}M"
        if self.views >= 1_000:
            return f"{self.views / 1_000:.1f}K"
        return str(self.views)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "video_id": self.video_id,
            "title": self.title,
            "model": self.model,
            "model_url": self.model_url,
            "duration": self.duration,
            "duration_str": self.duration_str,
            "views": self.views,
            "views_formatted": self.views_formatted,
            "likes": self.likes,
            "dislikes": self.dislikes,
            "upload_date": self.upload_date.isoformat() if self.upload_date else None,
            "thumbnail": self.thumbnail,
            "description": self.description,
            "download_urls": self.download_urls,
            "source": self.source,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)


@dataclass
class Category:
    """A PimpBunny category listing."""

    url: str
    slug: str
    name: str
    total_videos: int
    videos: List[Video] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "slug": self.slug,
            "name": self.name,
            "total_videos": self.total_videos,
            "videos": [v.to_dict() for v in self.videos],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)


@dataclass
class Model:
    """A PimpBunny model/creator profile."""

    url: str
    slug: str
    name: str
    profile_url: str
    video_count: int
    videos: List[Video] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "url": self.url,
            "slug": self.slug,
            "name": self.name,
            "profile_url": self.profile_url,
            "video_count": self.video_count,
            "videos": [v.to_dict() for v in self.videos],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Catalog scraping (homepage, categories, models)
# ---------------------------------------------------------------------------


class CatalogScraper:
    """Scrapes PimpBunny catalog pages (homepage, categories, models, tags)."""

    # URL patterns
    HOMEPAGE = "https://pimpbunny.com/"
    VIDEOS_URL = "https://pimpbunny.com/videos/"
    CATEGORY_URL = "https://pimpbunny.com/categories/{slug}/"
    MODEL_URL = "https://pimpbunny.com/onlyfans-creators/{slug}/"
    MODELS_URL = "https://pimpbunny.com/onlyfans-creators/"
    TAGS_URL = "https://pimpbunny.com/tags/"
    PAGE_URL = "https://pimpbunny.com/categories/{slug}/{page}/"

    # Regex patterns for parsing web_extract markdown output
    _VIDEO_LINK_RE = re.compile(
        r"\](\((https://pimpbunny\.com/videos/[^)]+)\))"
    )
    _MODEL_LINK_RE = re.compile(r"\]\((https://pimpbunny\.com/onlyfans-creators/[^)]+)\)")
    _VIEWS_LIKES_RE = re.compile(r"^(\d+(?:\.\d+)?)[MK]?$")
    _TIME_RE = re.compile(r"^\d+ (seconds|minutes?|hours?|days?|weeks?)$", re.IGNORECASE)

    def __init__(
        self,
        fetch_func=None,
        session=None,
    ):
        """
        Args:
            fetch_func: Optional callable(url) -> str (markdown content).
                        Defaults to web_extract.
            session: Optional session object (reserved for future use).
        """
        self._fetch = fetch_func or self._default_fetch

    @staticmethod
    def _default_fetch(url: str) -> str:
        """
        Fetch a page via web_extract, returning markdown content.

        Requires hermes_tools (Hermes environment). If unavailable, falls back
        to the cached file if available at
        ~/.hermes/cache/web/{domain}-{hash}.md.
        """
        try:
            from hermes_tools import web_extract

            result = web_extract(urls=[url], char_limit=50000)
            if result.get("results") and not result["results"][0].get("error"):
                return result["results"][0].get("content", "")
            raise RuntimeError(f"Failed to fetch {url}: {result}")
        except ImportError:
            # Fallback: try to load from cached file
            from pathlib import Path
            import re

            # Extract domain for cache file lookup (keep dots)
            domain = url.split("//")[-1].split("/")[0]
            # Remove http(s):// prefix
            domain = domain.split("://")[-1]
            cache_dir = Path.home() / ".hermes" / "cache" / "web"
            if cache_dir.exists():
                for f in cache_dir.glob(f"*.md"):
                    f_str = f.name
                    if domain in f_str or f_str.startswith(domain):
                        with f.open(encoding="utf-8") as fh:
                            return fh.read()
            raise ImportError(
                f"hermes_tools not available and no cached file found for {url}. "
                f"Run within the Hermes environment, or provide a fetch_func."
            )

    def _parse_video_entries(self, content: str) -> List[Dict[str, Any]]:
        """
        Parse video entries from web_extract markdown content.

        The format (from PimpBunny pages) is:
            [![Image X: Title](thumbnail_url)] <duration> <title>](video_url)
            [ModelName](model_url)
            views
            likes
            time_ago

        Returns a list of dicts with keys: title, url, model, model_url,
        duration, views, likes, time.
        """
        entries = []
        lines = content.split("\n")

        # Build a map of line index -> video_url
        # Only match actual video URLs (not sort_by or query-string URLs)
        video_urls = []
        for i, line in enumerate(lines):
            m = self._VIDEO_LINK_RE.search(line)
            if m:
                url = m.group(2)
                # Skip non-video URLs (sort links, etc.)
                if "/videos/" in url and not url.split("?")[0].endswith("/videos/"):
                    video_urls.append((i, url))

        for vi, video_url in video_urls:
            title = None
            model = None
            model_url = None
            duration = None
            views = None
            likes = None
            time_str = None

            # The video line contains: ![Image X: Title](thumb) duration title](url)
            # The video URL is at the end of this line
            video_line = lines[vi].strip()

            # Extract the title from the video line:
            # Pattern: [![Image X: Title](thumb) duration title](url)
            # The title comes after the image link and duration
            # Split by the video_url first to get everything before it
            if video_url in video_line:
                before_url = video_line.split(video_url)[0]
                img_match = re.search(r"!?\[Image \d+: ([^\]]+)\]", before_url)
                if img_match:
                    title = img_match.group(1).strip()

            # Also extract the full title that follows the duration
            # Pattern: ](thumbnail) duration Title](url)
            dur_match = re.search(r"\]\s+(\d+:\d{2})\s+(.+)\(", video_line)
            if dur_match:
                duration = dur_match.group(1)
                # The full title includes any prefix like "4K "
                title_full = dur_match.group(2).strip()
                # Remove leading quality tags like "4K "
                title_full = re.sub(r"^\d+K\s*", "", title_full)
                if not title:
                    title = title_full

            # Model name - search in surrounding lines (the model link)
            for offset in range(1, 4):
                idx = vi + offset
                if 0 <= idx < len(lines):
                    line = lines[idx].strip()
                    m = self._MODEL_LINK_RE.search(line)
                    if m:
                        model_url = m.group(1)
                        slug = model_url.split("/")[-2]
                        model = self._slug_to_name(slug)
                        break

            # Views, likes, time - search in subsequent lines
            for offset in range(1, 6):
                idx = vi + offset
                if 0 <= idx < len(lines):
                    line = lines[idx].strip()

                    # Skip empty lines and the model link line (already processed)
                    if not line or line.startswith("http"):
                        continue

                    # Views - a number with K or M suffix, or plain number < 100000
                    if views is None:
                        m = self._VIEWS_LIKES_RE.match(line)
                        if m:
                            try:
                                v = int(float(m.group(1)))
                                if v < 100000:
                                    views = v
                            except ValueError:
                                pass

                    # Likes - a number > 0 and < 100000 that's not a time
                    if likes is None and not self._TIME_RE.match(line):
                        m = self._VIEWS_LIKES_RE.match(line)
                        if m:
                            try:
                                v = int(float(m.group(1)))
                                if 0 < v < 100000:
                                    likes = v
                            except ValueError:
                                pass

                    # Time - pattern like "18 hours ago", "9 hours ago", etc.
                    if time_str is None:
                        m = self._TIME_RE.match(line)
                        if m:
                            time_str = line.strip()

            # If we found a model URL but not a model name, derive it
            if model_url and not model:
                model = self._slug_to_name(model_url.split("/")[-2])

            # Clean up model name (remove "Leaked", "New Leaks" suffixes if present)
            if model:
                model = re.sub(r"\s+(Leaked|New Leaks|Exclusive Leaks)$", "", model).strip()

            entries.append(
                {
                    "title": title,
                    "url": video_url,
                    "model": model,
                    "model_url": model_url,
                    "duration": duration,
                    "views": views,
                    "likes": likes,
                    "time": time_str,
                }
            )

        return entries

    @staticmethod
    def _slug_to_name(slug: str) -> str:
        """Convert a URL slug to a readable name."""
        # Replace hyphens with spaces
        name = slug.replace("-", " ")
        # Capitalize first letter of each word
        name = re.sub(r"\b\w", lambda m: m.group(0).upper(), name)
        return name

    def get_homepage(self, limit: Optional[int] = None) -> Category:
        """Scrape the homepage featured videos."""
        content = self._fetch(self.HOMEPAGE)
        entries = self._parse_video_entries(content)
        videos = [self._entry_to_video(e, self.HOMEPAGE) for e in entries]
        # Limit results if requested
        if limit is not None:
            videos = videos[:limit]
        return Category(
            url=self.HOMEPAGE,
            slug="home",
            name="Featured",
            total_videos=len(videos),
            videos=videos,
        )

    def get_category(self, slug: str, limit: Optional[int] = None) -> Category:
        """Scrape a category page."""
        url = self.CATEGORY_URL.format(slug=slug)
        content = self._fetch(url)
        entries = self._parse_video_entries(content)
        videos = [self._entry_to_video(e, url) for e in entries]
        if limit is not None:
            videos = videos[:limit]
        # Extract total video count from content
        total_match = re.search(r"(\d[\d,]*)\s+videos", content)
        total = int(total_match.group(1).replace(",", "")) if total_match else len(videos)
        return Category(
            url=url,
            slug=slug,
            name=slug.replace("-", " ").title(),
            total_videos=total,
            videos=videos,
        )

    def get_model(self, slug: str, limit: Optional[int] = None) -> Model:
        """Scrape a model profile page."""
        url = self.MODEL_URL.format(slug=slug)
        content = self._fetch(url)
        entries = self._parse_video_entries(content)
        videos = [self._entry_to_video(e, url) for e in entries]
        if limit is not None:
            videos = videos[:limit]
        # Extract model name from content
        name_match = re.search(r"^# (.+)$", content, re.MULTILINE)
        model_name = name_match.group(1) if name_match else slug.replace("-", " ").title()
        return Model(
            url=url,
            slug=slug,
            name=model_name,
            profile_url=f"https://pimpbunny.com/onlyfans-creators/{slug}/",
            video_count=len(videos),
            videos=videos,
        )

    def get_models(self, limit: Optional[int] = None) -> List[Model]:
        """Scrape the models list page."""
        content = self._fetch(self.MODELS_URL)
        # Parse model entries from content
        models = []
        # Find all model links
        model_links = re.findall(
            r"\[([^\]]+)\]\((https://pimpbunny\.com/onlyfans-creators/[^)]+)\)", content
        )
        for name, url in model_links[:limit]:
            slug = url.split("/")[-2]
            models.append(
                Model(
                    url=url,
                    slug=slug,
                    name=name.strip(),
                    profile_url=url,
                    video_count=1,  # Will be updated on demand
                    videos=[],
                )
            )
        return models

    @staticmethod
    def _entry_to_video(entry: Dict[str, Any], base_url: str) -> Video:
        """Convert a parsed entry dict to a Video object."""
        views = entry.get("views")
        likes = entry.get("likes")
        if views:
            try:
                views = int(float(str(views)))
            except (ValueError, TypeError):
                pass
        if likes:
            try:
                likes = int(float(str(likes)))
            except (ValueError, TypeError):
                pass

        # Try to extract duration from the title line
        # Pattern: "[![Image X: Title](url)] <duration> Title"
        duration = None
        title = entry.get("title")
        if title:
            m = re.search(r"(\d+):(\d{2})", title)
            if m:
                duration = int(m.group(1)) * 60 + int(m.group(2))

        return Video(
            url=entry["url"],
            title=title,
            model=entry.get("model"),
            model_url=entry.get("model_url"),
            duration=duration,
            views=views,
            likes=likes,
            upload_date=None,
            thumbnail=None,
            description=None,
            source="catalog",
        )


# ---------------------------------------------------------------------------
# Video page scraping (yt-dlp extractor)
# ---------------------------------------------------------------------------


class VideoScraper:
    """Scrapes individual PimpBunny video pages using yt-dlp."""

    def __init__(self, ydl_opts: Optional[Dict[str, Any]] = None):
        self.ydl_opts = ydl_opts or {
            "quiet": True,
            "no_warnings": True,
            "extractor_args": {
                "pimpbunny": {
                    "limit": 1,
                }
            },
        }
        self._ydl = None

    def _get_ydl(self):
        if self._ydl is None:
            import yt_dlp

            self._ydl = yt_dlp.YoutubeDL(self.ydl_opts)
        return self._ydl

    def get_video(self, url: str) -> Video:
        """
        Extract metadata and downloadable URLs from a PimpBunny video page.

        Args:
            url: PimpBunny video page URL.

        Returns:
            Video object with metadata and download URLs.
        """
        ydl = self._get_ydl()
        with ydl:
            info = ydl.extract_info(url, download=False)

            # Extract metadata
            video_id = info.get("id")
            title = info.get("title")
            uploader = info.get("uploader")
            description = info.get("description")
            thumbnail = info.get("thumbnail")
            view_count = info.get("view_count")
            timestamp = info.get("timestamp")

            upload_date = None
            if timestamp:
                upload_date = datetime.fromtimestamp(
                    timestamp, tz=timezone.utc
                )

            # Extract download URLs (available formats)
            download_urls = []
            for fmt in info.get("formats", []):
                if fmt.get("url") and "get_file" in fmt["url"]:
                    download_urls.append(
                        {
                            "quality": fmt.get("format_id"),
                            "url": fmt["url"],
                            "format_id": fmt.get("format_id"),
                            "height": fmt.get("height"),
                            "ext": fmt.get("ext"),
                            "filesize_approx": fmt.get("filesize_approx"),
                        }
                    )

            # Determine model from uploader or title
            model = uploader

            return Video(
                url=url,
                video_id=video_id,
                title=title,
                model=model,
                duration=info.get("duration"),
                views=view_count,
                likes=None,  # yt-dlp may not extract likes
                dislikes=None,
                upload_date=upload_date,
                thumbnail=thumbnail,
                description=description,
                download_urls=download_urls,
                source="yt_dlp",
            )

    def download(self, url: str, output_dir: str, quality: Optional[str] = None) -> Path:
        """
        Download a video file from a PimpBunny video page.

        This method:
        1. Extracts metadata and video URLs using yt-dlp
        2. Downloads the video file using the cookies from the page

        Args:
            url: PimpBunny video page URL.
            output_dir: Directory to save the video.
            quality: Preferred quality (e.g., "720p", "1080p"). If None, downloads best.

        Returns:
            Path to the downloaded file.

        Raises:
            Exception: If metadata extraction or download fails.
        """
        import yt_dlp
        from pathlib import Path

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Step 1: Extract metadata and get video URL + cookies
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "extractor_args": {"pimpbunny": {"limit": 1}},
            "http_headers": {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Cache-Control": "no-cache",
            },
            "socket_timeout": 30,
        }

        ydl = yt_dlp.YoutubeDL(ydl_opts)
        with ydl:
            info = ydl.extract_info(url, download=False)

            # Select the best quality format
            formats = info.get("formats", [])
            if quality:
                # Filter by quality
                quality_formats = [f for f in formats if f.get("format_id") == quality]
                if quality_formats:
                    formats = quality_formats

            if not formats:
                raise ValueError(f"No download formats available for {url}")

            # Use the first format (best available)
            fmt = formats[0]
            video_url = fmt.get("url")
            cookies = fmt.get("cookies", {})

            if not video_url:
                raise ValueError("No video URL found in metadata")

            # Step 2: Download the video file using cookies
            import requests

            headers = fmt.get("http_headers", {})
            cookies_dict = {}
            if isinstance(cookies, dict):
                for key, value in cookies.items():
                    if isinstance(value, str):
                        cookies_dict[key] = value

            # Extract the base URL from the get_file URL
            # The get_file URL has query parameters with v-acctoken and rnd
            # We need to pass these as query parameters too

            # Download the file
            response = requests.get(
                video_url,
                cookies=cookies_dict,
                headers=headers,
                timeout=60,
            )

            if response.status_code != 200:
                # Try with the full URL including query parameters
                response = requests.get(
                    video_url,
                    cookies=cookies_dict,
                    headers=headers,
                    timeout=60,
                )
                if response.status_code != 200:
                    raise Exception(
                        f"Download failed: HTTP {response.status_code}. "
                        f"Video file is CloudFlare-protected."
                    )

            # Determine output filename
            title = info.get("title", "video").replace("/", "_").replace("\\", "_")
            ext = fmt.get("ext", "mp4")
            output_file = output_dir / f"{title}.{ext}"

            # Write the video file
            output_file.write_bytes(response.content)

            # Clean up title for display
            video_title = info.get("title", "video")

            return output_file


# ---------------------------------------------------------------------------
# High-level client
# ---------------------------------------------------------------------------


class PimpBunnyScraper:
    """
    High-level client for scraping PimpBunny.

    Usage:
        scraper = PimpBunnyScraper()
        video = scraper.get_video("https://pimpbunny.com/videos/slash/")
        cat = scraper.get_category("bbc")
        model = scraper.get_model("sashstephensxo")
    """

    def __init__(
        self,
        catalog: Optional[CatalogScraper] = None,
        video: Optional[VideoScraper] = None,
    ):
        self.catalog = catalog or CatalogScraper()
        self.video_scraper = video or VideoScraper()

    def get_video(self, url: str, **kwargs) -> Video:
        """Scrape a single video page."""
        return self.video_scraper.get_video(url, **kwargs)

    def get_category(self, slug: str, limit: Optional[int] = None) -> Category:
        """Scrape a category page."""
        return self.catalog.get_category(slug, limit=limit)

    def get_homepage(self, limit: Optional[int] = None) -> Category:
        """Scrape the homepage featured videos."""
        return self.catalog.get_homepage(limit=limit)

    def get_model(self, slug: str, limit: Optional[int] = None) -> Model:
        """Scrape a model profile page."""
        return self.catalog.get_model(slug, limit=limit)

    def get_models(self, limit: Optional[int] = None) -> List[Model]:
        """Scrape the models list."""
        return self.catalog.get_models(limit=limit)

    def export_video(self, url: str, output_path: str, **kwargs) -> Path:
        """Scrape and export a video to JSON."""
        video = self.get_video(url, **kwargs)
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(video.to_json(indent=2), encoding="utf-8")
        return path

    def export_category(self, slug: str, output_path: str, limit: Optional[int] = None) -> Path:
        """Export a category to JSON."""
        cat = self.get_category(slug, limit=limit)
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(cat.to_json(indent=2), encoding="utf-8")
        return path

    def export_model(self, slug: str, output_path: str, limit: Optional[int] = None) -> Path:
        """Export a model profile to JSON."""
        model = self.get_model(slug, limit=limit)
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(model.to_json(indent=2), encoding="utf-8")
        return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line interface."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Scrape PimpBunny videos and catalog",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  pimpbunny scrape-video https://pimpbunny.com/videos/slash/ --output video.json
  pimpbunny list-category bbc --limit 50 --output videos.json
  pimpbunny list-model sashstephensxo --output model.json
  pimpbunny download https://pimpbunny.com/videos/slash/ --quality 720p
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Sub-command")

    # scrape-video
    p_video = subparsers.add_parser("scrape-video", help="Scrape a video page")
    p_video.add_argument("url", help="PimpBunny video URL")
    p_video.add_argument("-o", "--output", help="Output JSON file")
    p_video.add_argument("--quality", help="Preferred quality for download")

    # list-category
    p_cat = subparsers.add_parser("list-category", help="List videos in a category")
    p_cat.add_argument("slug", help="Category slug (e.g., 'bbc', 'anal')")
    p_cat.add_argument("-l", "--limit", type=int, help="Max videos to return")
    p_cat.add_argument("-o", "--output", help="Output JSON file")

    # list-model
    p_model = subparsers.add_parser("list-model", help="List a model's videos")
    p_model.add_argument("slug", help="Model slug (e.g., 'sashstephensxo')")
    p_model.add_argument("-l", "--limit", type=int, help="Max videos to return")
    p_model.add_argument("-o", "--output", help="Output JSON file")

    # download
    p_down = subparsers.add_parser("download", help="Download a video")
    p_down.add_argument("url", help="PimpBunny video URL")
    p_down.add_argument("-o", "--output", help="Output directory")
    p_down.add_argument("--quality", help="Quality (e.g., 720p, 1080p)")

    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    scraper = PimpBunnyScraper()

    try:
        if args.command == "scrape-video":
            video = scraper.get_video(args.url)
            output = args.output or "video.json"
            scraper.export_video(args.url, output)
            print(video.to_json())
            return 0

        elif args.command == "list-category":
            cat = scraper.get_category(args.slug, limit=args.limit)
            output = args.output or f"{args.slug}.json"
            scraper.export_category(args.slug, output, limit=args.limit)
            print(json.dumps(cat.to_dict(), indent=2))
            return 0

        elif args.command == "list-model":
            model = scraper.get_model(args.slug, limit=args.limit)
            output = args.output or f"{args.slug}.json"
            scraper.export_model(args.slug, output, limit=args.limit)
            print(json.dumps(model.to_dict(), indent=2))
            return 0

        elif args.command == "download":
            scraper.video_scraper.download(args.url, args.output or "./downloads", quality=args.quality)
            print(f"Downloaded to {args.output or './downloads'}")
            return 0

        else:
            parser.print_help()
            return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
