import asyncio
from scraper import parse_profile_url, fetch_all_posts, extract_video_urls
import sys

async def test():
    # Using a common URL
    url = "https://coomer.st/fansly/user/615146431358967808" 
    domain, service, user_id = parse_profile_url(url)
    if not domain:
        print("❌ Invalid URL")
        return
    print(f"Testing {service}/{user_id} on {domain}...")
    posts = await fetch_all_posts(domain, service, user_id, page_range=(1, 1))
    print(f"Posts found: {len(posts)}")
    if posts:
        videos = extract_video_urls(domain, posts)
        print(f"Videos found: {len(videos)}")
        if videos:
            v = videos[0]
            print(f"Testing download for: {v['name']}")
            import os
            from pathlib import Path
            output_dir = Path("test_downloads")
            success = await fetch_all_posts.__globals__['download_video'](domain, v, output_dir)
            if success:
                print(f"✅ Download successful: {v['name']}")
                file_path = output_dir / f"{v['id']}_{v['name']}"
                if file_path.exists():
                    print(f"   Size: {os.path.getsize(file_path)} bytes")
            else:
                print(f"❌ Download failed")
    else:
        print("❌ No posts found at all.")

if __name__ == "__main__":
    try:
        asyncio.run(test())
    except KeyboardInterrupt:
        pass
