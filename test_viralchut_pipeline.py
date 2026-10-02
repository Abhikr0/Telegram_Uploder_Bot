import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import asyncio
from pathlib import Path
from scraper import (
    parse_media_url,
    fetch_all_posts,
    extract_video_urls,
    resolve_media_stream_info,
    download_video
)
from ai_caption import generate_ai_caption

async def main():
    print("========================================")
    print("Testing Viralchut Pipeline Integration")
    print("========================================")

    # 1. URL Parsing Tests
    test_urls = [
        ("https://viralchut.com/first-time-sex-video-with-hot-vergin-girl-tight-pussy-fucking/", True),
        ("https://viralchut.com/category/desi-sex-scandal/", False),
        ("https://viralchut.com/tag/bhabhi-sex-mms/", False),
        ("https://viralchut.com/?s=tamil", False),
        ("https://viralchut.com/", False),
        ("https://viralchut.com/page/2/", False),
    ]

    for url, expect_single in test_urls:
        domain, service, cur_id, post_id = parse_media_url(url)
        print(f"URL: {url[:50]}... -> domain={domain}, service={service}, id={cur_id}, post_id={post_id}")
        assert domain == "viralchut.com", f"Expected viralchut.com, got {domain}"
        assert service == "viralchut", f"Expected viralchut, got {service}"
        if expect_single:
            assert post_id is not None, f"Expected post_id to be set for single post {url}"
        else:
            assert post_id is None, f"Expected post_id to be None for listing {url}"

    print("\n✅ URL Parsing Passed!")

    # 2. Single Post Fetching
    single_url = "https://viralchut.com/first-time-sex-video-with-hot-vergin-girl-tight-pussy-fucking/"
    domain, service, cur_id, post_id = parse_media_url(single_url)
    posts = await fetch_all_posts(domain, service, cur_id, post_id=post_id)
    assert len(posts) == 1, f"Expected 1 post, got {len(posts)}"
    post = posts[0]
    print(f"\nFetched Single Post:")
    print(f"  Title: {post['title']}")
    print(f"  ID: {post['id']}")
    print(f"  Direct CDN URL: {post['url']}")
    print(f"  Poster: {post.get('poster')}")
    print(f"  Filename: {post['name']}")

    # 3. Extract Video URLs
    videos = extract_video_urls(domain, posts)
    assert len(videos) == 1, f"Expected 1 video, got {len(videos)}"
    vid = videos[0]
    print(f"\nExtracted Video: {vid['name']} (Service: {vid['service']})")

    # 4. Resolve Media Stream Info
    print("\nTesting resolve_media_stream_info for Stream-Upload Pipeline...")
    stream_info = await resolve_media_stream_info(domain, vid)
    print(f"  Status: {stream_info.get('status')}")
    print(f"  URL: {stream_info.get('url')[:60]}...")
    print(f"  File size: {stream_info.get('file_size')} bytes")
    print(f"  Filename: {stream_info.get('name')}")
    assert stream_info.get("status") == "ok", f"Stream info failed: {stream_info}"
    assert stream_info.get("file_size", 0) > 0, "Expected file_size > 0"

    # 5. AI Caption / Metadata Generation
    print("\nTesting AI Caption / Metadata Generation...")
    ai_meta = await generate_ai_caption(vid, service, cur_id)
    print(f"  Clean Title: {ai_meta.get('clean_title')}")
    print(f"  File Name: {ai_meta.get('file_name')}")
    print(f"  Hashtags: {ai_meta.get('hashtags')[:5]}")
    print(f"  Caption Preview:\n{ai_meta.get('rich_caption')}")
    assert ai_meta.get("clean_title"), "Expected clean title"

    # 6. Listing / Album Crawl Test (Category Page 1)
    print("\nTesting Category Album Crawl (Browse & Select Files)...")
    cat_url = "https://viralchut.com/category/desi-sex-scandal/"
    dom, srv, c_id, p_id = parse_media_url(cat_url)
    cat_posts = await fetch_all_posts(dom, srv, c_id, page_range=(1, 1))
    print(f"  Total items found on category page 1: {len(cat_posts)}")
    assert len(cat_posts) > 0, "Expected at least 1 item in category"
    cat_videos = extract_video_urls(dom, cat_posts)
    print(f"  Total videos extracted: {len(cat_videos)}")
    assert len(cat_videos) == len(cat_posts)

    # Test resolving direct URL from one category item
    print("  Testing lazy resolution of category item stream info...")
    item_stream_info = await resolve_media_stream_info(dom, cat_videos[0])
    print(f"  Item 0 resolved status: {item_stream_info.get('status')}")
    print(f"  Item 0 direct URL: {item_stream_info.get('url')[:60]}...")
    print(f"  Item 0 file size: {item_stream_info.get('file_size')} bytes")
    assert item_stream_info.get("status") == "ok"
    assert item_stream_info.get("file_size", 0) > 0

    print("\n========================================")
    print("🎉 ALL VIRALCHUT PIPELINE TESTS PASSED!")
    print("========================================")

if __name__ == "__main__":
    asyncio.run(main())
