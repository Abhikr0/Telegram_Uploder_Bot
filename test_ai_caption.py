import asyncio
import os
import sys
from dotenv import load_dotenv

# Fix Windows console UTF-8 emoji printing
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Load UploderBot .env
load_dotenv()

from ai_caption import generate_ai_caption, _build_fallback_caption, _escape

async def test_caption():
    print("🧪 Running AI Caption & Metadata Tests...\n")

    # Sample mock video payload
    mock_vid = {
        "id": "12345678",
        "name": "VID_20240901_183021_f98a2b3c.mp4",
        "title": "Beach Sunset Photoshoot Behind The Scenes [RAW].mp4",
        "content": "Had so much fun shooting at the beach today! Here is the full unedited behind the scenes footage."
    }
    service = "onlyfans"
    creator = "claire_xo"

    # Test 1: Fallback generation
    print("--- Test 1: Fallback Caption Generator ---")
    fallback = _build_fallback_caption(mock_vid["title"], mock_vid["content"], service, creator, mock_vid["id"])
    print(f"Clean Title: {fallback['clean_title']}")
    print(f"Hashtags: {fallback['hashtags']}")
    print(f"DB Title: {fallback['db_title']}")
    print("Rich Caption:\n" + fallback["rich_caption"])
    print(f"Caption Length: {len(fallback['rich_caption'])} chars (Telegram limit is 1024)\n")
    assert len(fallback["rich_caption"]) <= 1024, "Fallback caption exceeds 1024 chars!"
    assert "#Onlyfans" in fallback["hashtags"] or "#onlyfans" in [t.lower() for t in fallback["hashtags"]]
    print("✅ Test 1 Passed!\n")

    # Test 2: generate_ai_caption (with whatever env is currently configured)
    print("--- Test 2: generate_ai_caption() (Fallback/Live) ---")
    res = await generate_ai_caption(mock_vid, service, creator)
    print(f"Clean Title: {res['clean_title']}")
    print(f"Hashtags: {res['hashtags']}")
    print(f"DB Title: {res['db_title']}")
    print(f"Caption Length: {len(res['rich_caption'])} chars")
    assert len(res["rich_caption"]) <= 1024, "Generated caption exceeds 1024 chars!"
    assert "<b>" in res["rich_caption"] and "</b>" in res["rich_caption"], "Missing HTML tags"
    print("✅ Test 2 Passed!\n")

    # Test 3: Simulated AI Response with special HTML chars & long text
    print("--- Test 3: Simulated AI Response Handling & HTML Escaping ---")
    import unittest.mock as mock
    ai_json = {
        "choices": [{
            "message": {
                "content": '{"clean_title": "Sunset Beach Shoot <Exclusive> & BTS", "summary": "Exclusive photoshoot on the beach with sunset lighting & behind-the-scenes moments.", "hashtags": ["#BeachVibes", "#SunsetShoot", "#Exclusive", "#OnlyFans", "#BTS", "#ModelLife"]}'
            }
        }]
    }

    class MockResponse:
        status_code = 200
        def json(self):
            return ai_json

    with mock.patch("os.getenv", side_effect=lambda k, default="": "fake_key" if k == "MISTRAL_API_KEY" else default):
        with mock.patch("httpx.AsyncClient.post", return_value=MockResponse()):
            mocked_res = await generate_ai_caption(mock_vid, service, creator)
            print(f"Mocked Clean Title: {mocked_res['clean_title']}")
            print(f"Mocked Hashtags: {mocked_res['hashtags']}")
            print("Mocked Caption:\n" + mocked_res["rich_caption"])
            assert "&lt;Exclusive&gt;" in mocked_res["rich_caption"], "HTML angle brackets not escaped!"
            assert "&amp;" in mocked_res["rich_caption"], "HTML ampersand not escaped!"
            assert len(mocked_res["rich_caption"]) <= 1024
            print("✅ Test 3 Passed!\n")

    print("🎉 All AI Caption tests completed successfully!")

if __name__ == "__main__":
    asyncio.run(test_caption())
