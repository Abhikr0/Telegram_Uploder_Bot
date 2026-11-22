"""Tests for the pimpbunny_scraper package."""

import unittest
import os
import sys
import json

# Add the parent directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pimpbunny_scraper import (
    PimpBunnyScraper,
    Video,
    Category,
    Model,
    CatalogScraper,
)


class TestPimpBunnyScraper(unittest.TestCase):
    """Integration tests for PimpBunnyScraper."""

    def setUp(self):
        self.scraper = PimpBunnyScraper()

    def test_video_metadata(self):
        """Test that video metadata extraction works."""
        video = self.scraper.get_video(
            "https://pimpbunny.com/videos/sashstephensxo-spreads-pussy-for-her-dildo/"
        )
        self.assertIsInstance(video, Video)
        self.assertIsNotNone(video.title)
        self.assertIsNotNone(video.model)
        self.assertIsNotNone(video.duration)
        self.assertIsNotNone(video.views)
        self.assertIsNotNone(video.thumbnail)
        self.assertIsNotNone(video.description)
        self.assertGreater(len(video.download_urls), 0)

    def test_category_parsing(self):
        """Test that category pages are parsed correctly."""
        cat = self.scraper.get_category("bbc", limit=3)
        self.assertIsInstance(cat, Category)
        self.assertEqual(cat.slug, "bbc")
        self.assertGreater(cat.total_videos, 0)
        self.assertEqual(len(cat.videos), 3)
        for v in cat.videos:
            self.assertIsInstance(v, Video)
            self.assertIsNotNone(v.url)
            self.assertIsNotNone(v.model)

    def test_category_to_dict(self):
        """Test Category.to_dict() output."""
        cat = self.scraper.get_category("bbc", limit=1)
        d = cat.to_dict()
        self.assertIn("url", d)
        self.assertIn("slug", d)
        self.assertIn("name", d)
        self.assertIn("total_videos", d)
        self.assertIn("videos", d)
        self.assertGreater(len(d["videos"]), 0)

    def test_video_to_dict(self):
        """Test Video.to_dict() output."""
        video = self.scraper.get_video(
            "https://pimpbunny.com/videos/sashstephensxo-spreads-pussy-for-her-dildo/"
        )
        d = video.to_dict()
        self.assertIn("url", d)
        self.assertIn("video_id", d)
        self.assertIn("title", d)
        self.assertIn("download_urls", d)
        self.assertGreater(len(d["download_urls"]), 0)

    def test_to_json_output(self):
        """Test that to_json() produces valid JSON."""
        video = self.scraper.get_video(
            "https://pimpbunny.com/videos/sashstephensxo-spreads-pussy-for-her-dildo/"
        )
        json_str = video.to_json()
        data = json.loads(json_str)
        self.assertIsInstance(data, dict)
        self.assertIn("title", data)
        self.assertIn("download_urls", data)


class TestCatalogScraper(unittest.TestCase):
    """Unit tests for CatalogScraper."""

    def setUp(self):
        # Use mock content in the real format (with markdown images)
        self.mock_content = """![Image 1: Logo](https://pimpbunny.com/static/v2/images/popup/age-verification/age-verification.png)

# BBC Porn Videos

[Aaron Rock Verified](https://pimpbunny.com/onlyfans-creators/aaron-rock/)

34K

33

6 days ago

[![Image 5: BBC Hookup Gets Wild — Intense Boy Girl Hardcore Action](https://pimpbunny.com/contents/videos_screenshots/625000/625974/800x450/2.jpg?format=avif&quality=100) 51:53 Steve Policeman And His Friends Fuck A Group Of Girls In A Bitch Street](https://pimpbunny.com/videos/steve-policeman-and-his-friends-fuck-a-group-of-girls-in-a-bitch-street/)

[Steve Policeman](https://pimpbunny.com/onlyfans-creators/steve-policeman/)

29K

26

2 days ago
"""
        self.scraper = CatalogScraper(fetch_func=lambda url: self.mock_content)

    def test_parse_video_entries(self):
        """Test that video entries are parsed correctly."""
        entries = self.scraper._parse_video_entries(self.mock_content)
        self.assertGreater(len(entries), 0)
        for e in entries:
            self.assertIsNotNone(e["title"])
            self.assertIsNotNone(e["url"])
            self.assertIsNotNone(e["model"])
            self.assertIsNotNone(e["views"])
            self.assertIsNotNone(e["likes"])

    def test_parse_video_entries_filters_correct_urls(self):
        """Test that non-video URLs are not parsed as videos."""
        entries = self.scraper._parse_video_entries(self.mock_content)
        for e in entries:
            self.assertTrue("/videos/" in e["url"])
            self.assertFalse("?sort_by" in e["url"])


class TestModels(unittest.TestCase):
    """Tests for data models."""

    def test_video_duration_str(self):
        """Test Video.duration_str property."""
        video = Video(url="https://example.com", duration=161)
        self.assertEqual(video.duration_str, "2:41")

    def test_video_views_formatted(self):
        """Test Video.views_formatted property."""
        video = Video(url="https://example.com", views=3101)
        self.assertEqual(video.views_formatted, "3.1K")

    def test_category_to_json(self):
        """Test Category.to_json() produces valid JSON."""
        cat = Category(url="https://example.com", slug="test", name="Test", total_videos=5)
        json_str = cat.to_json()
        data = json.loads(json_str)
        self.assertIn("url", data)
        self.assertIn("slug", data)

    def test_model_to_json(self):
        """Test Model.to_json() produces valid JSON."""
        model = Model(url="https://example.com", slug="test", name="Test", profile_url="https://example.com", video_count=5)
        json_str = model.to_json()
        data = json.loads(json_str)
        self.assertIn("url", data)
        self.assertIn("slug", data)


if __name__ == "__main__":
    unittest.main()
