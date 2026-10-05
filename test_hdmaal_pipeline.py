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
    print("=" * 60)
    print("🎬 Testing HDMaal Pipeline Integration (Bharti Jha & Web Series)")
    print("=" * 60)

    # 1. URL Parsing Tests
    test_urls = [
        ("https://hdmaal.movie/model/bharti-jha/", "model/bharti-jha", False),
        ("https://hdmaal.movie/model/bharti-jha/page/2/", "model/bharti-jha", False),
        ("https://hdmaal.movie/jaan-bujh-kar-s2-episode-5/", "jaan-bujh-kar-s2-episode-5", True),
        ("https://hdmaal.movie/betaab-ishq-episode-2/", "betaab-ishq-episode-2", True),
        ("https://hdmaal.movie/ott/voovi/", "ott/voovi", False),
        ("https://hdmaal.movie/topic/hot-web-series/", "topic/hot-web-series", False),
        ("https://hdmaal.movie/web-series/", "web-series", False),
    ]

    for url, exp_id, expect_single in test_urls:
        domain, service, cur_id, post_id = parse_media_url(url)
        print(f"URL: {url[:45]}... -> dom={domain}, srv={service}, id={cur_id}, post_id={post_id}")
        assert domain == "hdmaal.movie", f"Expected hdmaal.movie, got {domain}"
        assert service == "hdmaal", f"Expected hdmaal, got {service}"
        assert cur_id == exp_id, f"Expected cur_id={exp_id}, got {cur_id}"
        if expect_single:
            assert post_id is not None, f"Expected post_id to be set for single post {url}"
        else:
            assert post_id is None, f"Expected post_id to be None for listing {url}"

    print("\n✅ Step 1: URL Parsing Passed!")

    # 2. Single Video Fetching (with automatic video.maalcdn.com -> cdn.azmaal.com routing)
    single_url = "https://hdmaal.movie/jaan-bujh-kar-s2-episode-5/"
    domain, service, cur_id, post_id = parse_media_url(single_url)
    posts = await fetch_all_posts(domain, service, cur_id, post_id=post_id)
    assert len(posts) == 1, f"Expected 1 post, got {len(posts)}"
    post = posts[0]
    print(f"\nFetched Single Episode:")
    print(f"  Title: {post['title']}")
    print(f"  ID: {post['id']}")
    print(f"  Direct CDN URL: {post['url']}")
    print(f"  Poster: {post.get('poster')}")
    print(f"  Filename: {post['name']}")
    print(f"  Actors: {post.get('actors')}")
    assert post['url'].startswith("https://"), f"Expected direct HTTPS video URL, got {post['url']}"

    print("\n✅ Step 2: Single Video Fetching Passed!")

    # 3. Extract Video URLs
    videos = extract_video_urls(domain, posts)
    assert len(videos) == 1, f"Expected 1 video, got {len(videos)}"
    vid = videos[0]
    print(f"\nExtracted Video: {vid['name']} (Service: {vid['service']})")
    assert vid['service'] == "hdmaal"
    assert vid['title'] == post['title']

    print("\n✅ Step 3: Video Extraction Passed!")

    # 4. Resolve Media Stream Info (Zero-Disk Streaming Validation)
    print("\nTesting resolve_media_stream_info for Stream-Upload Pipeline...")
    stream_info = await resolve_media_stream_info(domain, vid)
    print(f"  Status: {stream_info.get('status')}")
    print(f"  URL: {stream_info.get('url')[:60]}...")
    print(f"  File size: {stream_info.get('file_size')} bytes (~{stream_info.get('file_size', 0) // (1024*1024)} MB)")
    print(f"  Filename: {stream_info.get('name')}")
    assert stream_info.get("status") == "ok", f"Stream info failed: {stream_info}"
    assert stream_info.get("file_size", 0) > 0, "Expected file_size > 0"

    print("\n✅ Step 4: Stream Info Resolution Passed!")

    # 5. AI Caption / Metadata Generation
    print("\nTesting AI Caption / Metadata Generation...")
    ai_meta = await generate_ai_caption(vid, service, "Bharti Jha")
    print(f"  Clean Title: {ai_meta.get('clean_title')}")
    print(f"  File Name: {ai_meta.get('file_name')}")
    print(f"  Hashtags: {ai_meta.get('hashtags')}")
    assert ai_meta.get("clean_title"), "Expected clean title"
    assert any("Bharti" in h for h in ai_meta.get("hashtags", [])), "Expected actress hashtag in tags"

    print("\n✅ Step 5: AI Caption Generation Passed!")

    # 6. Listing / Album Crawl Test (Bharti Jha Model Page)
    print("\nTesting Model Album Crawl (https://hdmaal.movie/model/bharti-jha/)...")
    model_url = "https://hdmaal.movie/model/bharti-jha/"
    dom, srv, c_id, p_id = parse_media_url(model_url)
    model_posts = await fetch_all_posts(dom, srv, c_id, page_range=(1, 1))
    print(f"  Total episodes found on page 1: {len(model_posts)}")
    assert len(model_posts) >= 20, f"Expected at least 20 items, got {len(model_posts)}"
    model_videos = extract_video_urls(dom, model_posts)
    print(f"  Total videos extracted: {len(model_videos)}")
    assert len(model_videos) == len(model_posts)

    # Test resolving direct URL from one listing item
    print("  Testing lazy resolution of listing item 0 stream info...")
    item_stream_info = await resolve_media_stream_info(dom, model_videos[0])
    print(f"  Item 0 resolved status: {item_stream_info.get('status')}")
    print(f"  Item 0 direct URL: {item_stream_info.get('url')[:60]}...")
    print(f"  Item 0 file size: {item_stream_info.get('file_size')} bytes")
    assert item_stream_info.get("status") == "ok", f"Expected ok status, got {item_stream_info}"
    assert item_stream_info.get("file_size", 0) > 0

    print("\n============================================================")
    print("🎉 ALL HDMAAL PIPELINE TESTS PASSED SUCCESSFULLY!")
    print("============================================================")

if __name__ == "__main__":
    asyncio.run(main())
