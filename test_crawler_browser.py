import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from bot import render_album_browser, USER_STATES

def test_album_browser():
    print("🧪 Running Album Browser & Crawler Selection Tests...\n")

    user_id = 999999
    # Create 13 mock videos to test pagination across 3 pages (5 per page)
    mock_videos = [
        {"id": f"vid_{i}", "name": f"Clip_{i}_Highlights_Summer.mp4", "title": f"Video #{i} Exclusive"}
        for i in range(1, 14)
    ]

    USER_STATES[user_id] = {
        'url': "https://bunkr.cr/a/example_album",
        'videos': mock_videos,
        'selected': set(),
        'browser_page': 0
    }

    # Test 1: Page 1 rendering (indices 0 to 4)
    print("--- Test 1: Page 1 Initial Render ---")
    text, buttons = render_album_browser(user_id)
    print(text)
    print(f"Total Button Rows: {len(buttons)}")
    
    # 5 file rows + 1 nav row + 1 control row + 1 download selected + 1 download all/cancel = 9 rows
    assert len(buttons) >= 8, f"Expected at least 8 button rows, got {len(buttons)}"
    # First row should be unselected
    first_btn_text = buttons[0][0].text
    print(f"Row 0 Button: {first_btn_text}")
    assert "◻️ #1" in first_btn_text
    print("✅ Test 1 Passed!\n")

    # Test 2: Toggle Selection (select items 0 and 2)
    print("--- Test 2: Toggle Selection ---")
    USER_STATES[user_id]['selected'].add(0)
    USER_STATES[user_id]['selected'].add(2)
    text, buttons = render_album_browser(user_id)
    assert "✅ #1" in buttons[0][0].text
    assert "◻️ #2" in buttons[1][0].text
    assert "✅ #3" in buttons[2][0].text
    assert "<b>Selected:</b> <b>2</b> / 13" in text
    print(f"Selected Count in text verified.")
    print("✅ Test 2 Passed!\n")

    # Test 3: Pagination (navigate to page 2: indices 5 to 9)
    print("--- Test 3: Page Navigation ---")
    USER_STATES[user_id]['browser_page'] = 1
    text, buttons = render_album_browser(user_id)
    assert "◻️ #6" in buttons[0][0].text
    assert "◻️ #10" in buttons[4][0].text
    # Check navigation row
    nav_buttons = [b.text for b in buttons[5]]
    print(f"Nav Row: {nav_buttons}")
    assert "◀️ Prev" in nav_buttons
    assert "📄 2/3" in nav_buttons
    assert "Next ▶️" in nav_buttons
    print("✅ Test 3 Passed!\n")

    # Test 4: Page 3 (last page with 3 items: indices 10, 11, 12)
    print("--- Test 4: Final Page ---")
    USER_STATES[user_id]['browser_page'] = 2
    text, buttons = render_album_browser(user_id)
    assert "◻️ #11" in buttons[0][0].text
    assert "◻️ #13" in buttons[2][0].text
    nav_buttons = [b.text for b in buttons[3]]
    print(f"Final Page Nav Row: {nav_buttons}")
    assert "Next ▶️" not in nav_buttons, "Last page should not have Next button!"
    assert "◀️ Prev" in nav_buttons
    print("✅ Test 4 Passed!\n")

    # Test 5: Selection extraction for download
    print("--- Test 5: Selection Extraction ---")
    selected_indices = USER_STATES[user_id]['selected']  # {0, 2}
    chosen = [mock_videos[i] for i in sorted(selected_indices)]
    assert len(chosen) == 2
    assert chosen[0]["id"] == "vid_1"
    assert chosen[1]["id"] == "vid_3"
    print(f"Extracted Videos: {[v['id'] for v in chosen]}")
    print("✅ Test 5 Passed!\n")

    print("🎉 All Album Browser tests passed successfully!")

if __name__ == "__main__":
    test_album_browser()
