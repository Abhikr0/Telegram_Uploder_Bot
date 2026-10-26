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
    resolve_xmovies_direct_url,
    download_video
)
from ai_caption import generate_ai_caption

async def main():
    print("=" * 65)
    print("🎬 Testing XMoviesForYou Pipeline Integration")
    print("=" * 65)

    # 1. URL Parsing Tests
    test_urls = [
        ("https://xmoviesforyou.com/page/7", "page/7", False),
        ("https://xmoviesforyou.com/devilsfilm-serena-sterling-deepthroat-diagnosis", "devilsfilm-serena-sterling-deepthroat-diagnosis", True),
        ("https://xmoviesforyou.com/studio/devilsfilm", "studio/devilsfilm", False),
        ("https://xmoviesforyou.com/pornstar/serena-sterling", "pornstar/serena-sterling", False),
        ("https://xmoviesforyou.com/category/blonde", "category/blonde", False),
        ("https://xmoviesforyou.com/tag/interracial", "tag/interracial", False),
        ("https://xmoviesforyou.com/search?q=serena", "search/serena", False),
    ]

    for url, exp_id, expect_single in test_urls:
        domain, service, cur_id, post_id = parse_media_url(url)
        print(f"URL: {url[:45]}... -> dom={domain}, srv={service}, id={cur_id}, post_id={post_id}")
        assert domain == "xmoviesforyou.com", f"Expected xmoviesforyou.com, got {domain}"
        assert service == "xmoviesforyou", f"Expected xmoviesforyou, got {service}"
        assert cur_id == exp_id, f"Expected cur_id={exp_id}, got {cur_id}"
        if expect_single:
            assert post_id is not None, f"Expected post_id for single post {url}"
        else:
            assert post_id is None, f"Expected post_id to be None for listing {url}"

    print("\n✅ Step 1: URL Parsing Passed!")

    # 2. Listing Scraper Test (page 7)
    listing_url = "https://xmoviesforyou.com/page/7"
    print(f"\n--- Step 2: Testing Listing Scraper on {listing_url} ---")
    domain, service, cur_id, post_id = parse_media_url(listing_url)
    posts = await fetch_all_posts(domain, service, cur_id, post_id=post_id)
    print(f"Fetched {len(posts)} videos from page 7.")
    assert len(posts) >= 20, f"Expected at least 20 videos on page 7, got {len(posts)}"

    first_post = posts[0]
    print(f"\nFirst video on page 7:")
    print(f"  Title: {first_post['title']}")
    print(f"  ID: {first_post['id']}")
    print(f"  URL: {first_post['url']}")
    print(f"  Studio: {first_post.get('studio')}")
    print(f"  Thumbnail: {first_post.get('thumbnail')}")

    print("\n✅ Step 2: Listing Scraping Passed!")

    # 3. Single Video Fetching & Direct Stream Resolution
    single_url = "https://xmoviesforyou.com/devilsfilm-serena-sterling-deepthroat-diagnosis"
    print(f"\n--- Step 3: Testing Single Post Fetching on {single_url} ---")
    domain, service, cur_id, post_id = parse_media_url(single_url)
    single_posts = await fetch_all_posts(domain, service, cur_id, post_id=post_id)
    assert len(single_posts) == 1, f"Expected 1 post, got {len(single_posts)}"
    post = single_posts[0]
    print(f"Fetched Video Details:")
    print(f"  Title: {post['title']}")
    print(f"  Direct Stream (m3u8): {post['url']}")
    print(f"  Studio: {post.get('studio')}")
    print(f"  Actors: {post.get('actors')}")
    print(f"  Categories: {post.get('categories')}")
    print(f"  Tags: {post.get('tags')}")
    print(f"  Mirrors: {post.get('mirrors')}")

    assert post["url"].startswith("http"), f"Expected HTTP stream URL, got {post['url']}"
    assert "eternalslumber.online" in post["url"] or "bestcdnfree" in post["url"] or ".m3u8" in post["url"]

    print("\n✅ Step 3: Single Video Stream Resolution Passed!")

    # 4. Extract Video URLs
    print("\n--- Step 4: Testing Video URL Extraction ---")
    videos = extract_video_urls(domain, single_posts)
    assert len(videos) == 1, f"Expected 1 video, got {len(videos)}"
    vid = videos[0]
    print(f"Extracted Video Object: {vid['name']} (Service: {vid['service']})")
    assert vid['service'] == "xmoviesforyou"

    print("\n✅ Step 4: Video Extraction Passed!")

    # 5. AI Caption / Metadata Generation
    print("\n--- Step 5: Testing AI Caption Generation ---")
    ai_meta = await generate_ai_caption(vid, service, post.get("studio") or "DevilsFilm")
    print(f"  Clean Title: {ai_meta.get('clean_title')}")
    print(f"  File Name: {ai_meta.get('file_name')}")
    print(f"  Hashtags: {ai_meta.get('hashtags')[:8]}")
    assert ai_meta.get("clean_title"), "Expected clean title"

    print("\n✅ Step 5: AI Caption Generation Passed!")

    print("\n" + "=" * 65)
    print("🎉 ALL XMOVIESFORYOU PIPELINE TESTS PASSED SUCCESSFULLY!")
    print("=" * 65)

if __name__ == "__main__":
    asyncio.run(main())
