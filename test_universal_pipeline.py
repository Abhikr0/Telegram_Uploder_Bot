import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import asyncio
from pathlib import Path
import os

from scraper import (
    parse_media_url,
    fetch_all_posts,
    extract_video_urls,
    resolve_media_stream_info,
    download_video,
    extract_universal_video,
    crawl_universal_page,
    is_direct_media_url
)
from ai_caption import generate_ai_caption

async def main():
    print("=" * 60)
    print("🚀 Running Universal Web Scraper & Crawler Test Suite")
    print("=" * 60)

    # 1. URL Parsing Tests
    test_urls = [
        ("https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4", "direct", True),
        ("https://viralchut.com/first-time-sex-video-with-hot-vergin-girl-tight-pussy-fucking/", "viralchut", True),
        ("https://viralchut.com/category/desi-sex-scandal/", "viralchut", False),
        ("https://bunkr.cr/a/album123", "bunkr", False),
        ("https://bunkr.cr/v/video123", "bunkr", True),
        ("https://coomer.st/onlyfans/user/example", "onlyfans", False),
        ("https://any-random-video-site.com/watch/awesome-video", "any-random-video-site", True),
        ("https://another-gallery.com/category/nature/", "another-gallery", False),
        ("https://cool-tube.net/browse?page=2", "cool-tube", False),
    ]

    for url, exp_srv, exp_single in test_urls:
        dom, srv, uid, pid = parse_media_url(url)
        print(f"URL: {url[:45]}... -> dom={dom}, srv={srv}, uid={uid}, pid={pid}")
        assert srv == exp_srv, f"Expected {exp_srv}, got {srv}"
        if exp_single:
            assert pid is not None, f"Expected single for {url}"
        else:
            assert pid is None, f"Expected listing for {url}"

    print("\n✅ Step 1: URL Parsing Passed!")

    # 2. Universal Single Video Extraction
    print("\n--- Step 2: Testing Universal Single Video Extraction ---")
    direct_url = "https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4"
    dom, srv, uid, pid = parse_media_url(direct_url)
    posts = await fetch_all_posts(dom, srv, uid, post_id=pid)
    assert len(posts) >= 1, "Expected at least 1 post"
    print(f"Direct media extracted: {posts[0]['name']}")
    assert posts[0]["url"] == direct_url

    # Test webpage with video
    web_url = "https://viralchut.com/first-time-sex-video-with-hot-vergin-girl-tight-pussy-fucking/"
    dom2, srv2, uid2, pid2 = parse_media_url(web_url)
    posts2 = await fetch_all_posts(dom2, srv2, uid2, post_id=pid2)
    assert len(posts2) >= 1, "Expected at least 1 post from webpage"
    print(f"Webpage video extracted: {posts2[0]['title']}")
    print(f"Direct stream URL: {posts2[0]['url'][:60]}...")
    assert posts2[0]["url"].startswith("http")

    print("\n✅ Step 2: Single Video Extraction Passed!")

    # 3. Universal Web Crawler
    print("\n--- Step 3: Testing Universal Web Crawler (Listing / Gallery) ---")
    crawl_target = "https://viralchut.com/category/desi-sex-scandal/"
    dom3, srv3, uid3, pid3 = parse_media_url(crawl_target)
    crawled_posts = await fetch_all_posts(dom3, srv3, uid3, page_range=(1, 1), post_id=None)
    print(f"Crawler found {len(crawled_posts)} videos on {crawl_target}")
    assert len(crawled_posts) > 0, "Expected at least 1 crawled video"
    
    videos = extract_video_urls(dom3, crawled_posts)
    print(f"Extracted {len(videos)} playable video items")
    assert len(videos) == len(crawled_posts)
    print(f"Sample item 0: {videos[0]['title']} -> {videos[0]['url']}")

    print("\n✅ Step 3: Universal Crawler Passed!")

    # 4. Stream Resolution & Zero-Disk Pipeline
    print("\n--- Step 4: Testing Stream Resolution for Telegram ---")
    stream_info = await resolve_media_stream_info(dom3, videos[0])
    print(f"Stream resolution status: {stream_info.get('status')}")
    print(f"Stream direct URL: {stream_info.get('url')[:60]}...")
    print(f"Stream file size: {stream_info.get('file_size')} bytes")
    assert stream_info.get("status") == "ok", f"Expected ok status, got {stream_info}"
    assert stream_info.get("file_size", 0) > 0, "Expected positive file size"

    print("\n✅ Step 4: Stream Resolution Passed!")

    # 5. Download Video Test (first small chunk)
    print("\n--- Step 5: Testing Download Video Pipeline ---")
    temp_dir = Path("test_universal_downloads")
    temp_dir.mkdir(exist_ok=True)
    test_vid = {
        "id": "sample_test_01",
        "name": "flower.mp4",
        "url": "https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4",
        "service": "generic"
    }
    
    progress_recorded = []
    def on_progress(current, total):
        progress_recorded.append((current, total))

    dl_success = await download_video("interactive-examples.mdn.mozilla.net", test_vid, temp_dir, progress_callback=on_progress)
    assert dl_success == True, "Expected download to succeed"
    downloaded_file = temp_dir / f"{test_vid['id']}_{test_vid['name']}"
    assert downloaded_file.exists(), "Expected downloaded file to exist"
    print(f"Downloaded file size: {downloaded_file.stat().st_size} bytes")
    print(f"Progress callbacks received: {len(progress_recorded)}")

    # Clean up test download
    if downloaded_file.exists():
        os.remove(downloaded_file)
    temp_dir.rmdir()

    print("\n✅ Step 5: Download Video Pipeline Passed!")

    # 6. AI Caption & Metadata
    print("\n--- Step 6: Testing AI Caption Generation ---")
    ai_meta = await generate_ai_caption(test_vid, "generic", "sample_creator")
    print(f"Generated clean title: {ai_meta['clean_title']}")
    print(f"Generated file name: {ai_meta['file_name']}")
    print(f"Generated hashtags: {ai_meta['hashtags'][:5]}")
    assert ai_meta["clean_title"]

    print("\n============================================================")
    print("🎉 ALL UNIVERSAL SCRAPER & CRAWLER TESTS PASSED SUCCESSFULLY!")
    print("============================================================")

if __name__ == "__main__":
    asyncio.run(main())
