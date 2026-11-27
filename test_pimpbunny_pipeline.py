import asyncio
import unittest
from pathlib import Path
from unittest import mock

import httpx

import bot
import pimpbunny_adapter
import scraper


class TestPimpBunnyPipeline(unittest.TestCase):
    def test_url_parsing_preserves_listing_sections(self):
        cases = [
            (
                "https://pimpbunny.com/onlyfans-creators/dainty-wilder/",
                ("pimpbunny.com", "pimpbunny", "onlyfans-creators/dainty-wilder", None),
            ),
            (
                "https://pimpbunny.com/onlyfans-creators/dainty-wilder/2/",
                ("pimpbunny.com", "pimpbunny", "onlyfans-creators/dainty-wilder", None),
            ),
            (
                "https://pimpbunny.com/models/comatozze/",
                ("pimpbunny.com", "pimpbunny", "models/comatozze", None),
            ),
            (
                "https://pimpbunny.com/models/comatozze/2/",
                ("pimpbunny.com", "pimpbunny", "models/comatozze", None),
            ),
            (
                "https://pimpbunny.com/models/comatozze/category/4k/2/",
                ("pimpbunny.com", "pimpbunny", "models/comatozze/category/4k", None),
            ),
            (
                "https://pimpbunny.com/categories/bbc/",
                ("pimpbunny.com", "pimpbunny", "categories/bbc", None),
            ),
            (
                "https://pimpbunny.com/tags/anal/",
                ("pimpbunny.com", "pimpbunny", "tags/anal", None),
            ),
            (
                "https://pimpbunny.com/videos/xenadreamx-gets-her-pussy-railed/",
                ("pimpbunny.com", "pimpbunny", "xenadreamx-gets-her-pussy-railed", "xenadreamx-gets-her-pussy-railed"),
            ),
            (
                "https://pimpbunny.com/",
                ("pimpbunny.com", "pimpbunny", "home", None),
            ),
            (
                "https://pimpbunny.com/2/",
                ("pimpbunny.com", "pimpbunny", "home", None),
            ),
            (
                "https://pimpbunny.com/videos/2/",
                ("pimpbunny.com", "pimpbunny", "videos", None),
            ),
        ]
        for url, expected in cases:
            self.assertEqual(scraper.parse_media_url(url), expected)
            self.assertEqual(pimpbunny_adapter.parse_pimpbunny_url(url), expected)

    def test_sanitize_filename_preserves_uppercase_digits_x_f(self):
        out = pimpbunny_adapter._sanitize_filename("Xenadreamx Gets Her Pussy Railed While Being Handcuffed")
        self.assertEqual(out, "Xenadreamx Gets Her Pussy Railed While Being Handcuffed.mp4")
        out2 = pimpbunny_adapter._sanitize_filename("BBC Hookup 1080p")
        self.assertEqual(out2, "BBC Hookup 1080p.mp4")

    def test_is_direct_media_url_handles_trailing_slash_get_file(self):
        self.assertTrue(
            scraper.is_direct_media_url(
                "https://pimpbunny.com/get_file/55/abc/626000/626103/626103_720p.mp4/?v-acctoken=tok"
            )
        )
        self.assertTrue(
            scraper.is_direct_media_url(
                "https://pimpbunny.com/get_file/55/abc/626000/626103/626103_720p.mp4/"
            )
        )
        self.assertFalse(scraper.is_direct_media_url("https://pimpbunny.com/videos/some-slug/"))
        self.assertFalse(scraper.is_direct_media_url("https://pimpbunny.com/embed/625974/"))

    def test_extract_native_video_prefers_tokenized_kvs_over_untokenized_jsonld_and_embed(self):
        html_untokenized = """
        <html><head>
        <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"VideoObject",
         "name":"Xenadreamx Gets Her Pussy Railed While Being Handcuffed",
         "contentUrl":"https://pimpbunny.com/get_file/55/hash/626000/626103/626103_720p.mp4/"}
        </script>
        </head><body>
        <script>
        var flashvars = {
            video_url: 'https://pimpbunny.com/get_file/55/hash/626000/626103/626103_360p.mp4/?v-acctoken=tok360',
            video_alt_url2: 'https://pimpbunny.com/get_file/55/hash/626000/626103/626103_720p.mp4/?v-acctoken=tok720',
            video_alt_url3: 'https://pimpbunny.com/get_file/55/hash/626000/626103/626103_1080p.mp4/?v-acctoken=tok1080'
        };
        </script></body></html>
        """
        res = asyncio.run(
            scraper.extract_native_video(
                html_untokenized,
                "https://pimpbunny.com/videos/xenadreamx-gets-her-pussy-railed-while-being-handcuffed/",
            )
        )
        self.assertIn("v-acctoken=tok1080", res["url"])
        self.assertIn("1080p.mp4", res["url"])

        html_embed = """
        <html><head>
        <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"VideoObject",
         "name":"BBC Hookup Gets Wild",
         "embedUrl":"https://pimpbunny.com/embed/625974"}
        </script>
        </head><body>
        <script>
        var flashvars = {
            video_url: 'https://pimpbunny.com/get_file/55/hash/625000/625974/625974_pb_360p.mp4/?v-acctoken=tok360',
            video_alt_url2: 'https://pimpbunny.com/get_file/55/hash/625000/625974/625974_pb_720p.mp4/?v-acctoken=tok720'
        };
        </script></body></html>
        """
        res2 = asyncio.run(
            scraper.extract_native_video(
                html_embed,
                "https://pimpbunny.com/videos/bbc-hookup-gets-wild/",
            )
        )
        self.assertNotIn("/embed/", res2["url"])
        self.assertIn("v-acctoken=tok720", res2["url"])

    def test_fetch_pimpbunny_listing_live_and_bunny_shield_block(self):
        creator_html = """
        <html><body>
          <div class="item">
            <a href="https://pimpbunny.com/videos/dainty-wilder-clip-one/" title="Dainty Wilder Clip One">
              <img data-original="https://pimpbunny.com/thumbs/1.jpg" alt="Dainty Wilder Clip One"/>
            </a>
          </div>
          <div class="item">
            <a href="https://pimpbunny.com/videos/?sort_by=rating">Top Rated</a>
          </div>
        </body></html>
        """
        transport = httpx.MockTransport(lambda req: httpx.Response(200, text=creator_html))
        async def run_ok():
            async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
                return await pimpbunny_adapter.fetch_pimpbunny_all_posts(
                    "https://pimpbunny.com/onlyfans-creators/dainty-wilder/",
                    client=client,
                )

        items = asyncio.run(run_ok())
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["url"], "https://pimpbunny.com/videos/dainty-wilder-clip-one/")
        self.assertEqual(items[0]["name"], "Dainty Wilder Clip One.mp4")
        self.assertEqual(items[0]["user"], "Dainty Wilder")

        shield_html = '<html><head><title>Establishing a secure connection ...</title><script src="/.bunny-shield/assets/shield-challenge.js"></script></head></html>'
        shield_transport = httpx.MockTransport(lambda req: httpx.Response(403, text=shield_html))
        async def run_shield():
            async with httpx.AsyncClient(transport=shield_transport, follow_redirects=True) as client:
                return await pimpbunny_adapter.fetch_pimpbunny_all_posts(
                    "https://pimpbunny.com/onlyfans-creators/dainty-wilder/",
                    client=client,
                )

        with self.assertRaises(RuntimeError) as ctx:
            asyncio.run(run_shield())
        self.assertIn("Bunny Shield", str(ctx.exception))

    def test_fetch_pimpbunny_single_video_html_and_403(self):
        video_html = """
        <html><head>
        <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"VideoObject",
         "name":"Xenadreamx Gets Her Pussy Railed While Being Handcuffed",
         "description":"BDSM session",
         "duration":"PT0H12M46S",
         "thumbnailUrl":"https://pimpbunny.com/thumbs/626103.jpg",
         "contentUrl":"https://pimpbunny.com/get_file/55/hash/626000/626103/626103_720p.mp4/"}
        </script></head><body>
        <a href="https://pimpbunny.com/onlyfans-creators/xenadreamx/">Xenadreamx</a>
        <script>
        var flashvars = {
            video_url: 'https://pimpbunny.com/get_file/55/hash/626000/626103/626103_360p.mp4/?v-acctoken=tok360',
            video_alt_url2: 'https://pimpbunny.com/get_file/55/hash/626000/626103/626103_720p.mp4/?v-acctoken=tok720',
            video_alt_url3: 'https://pimpbunny.com/get_file/55/hash/626000/626103/626103_1080p.mp4/?v-acctoken=tok1080'
        };
        </script></body></html>
        """
        transport = httpx.MockTransport(lambda req: httpx.Response(200, text=video_html))

        async def run_ok():
            async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
                return await pimpbunny_adapter.fetch_pimpbunny_all_posts(
                    "https://pimpbunny.com/videos/xenadreamx-gets-her-pussy-railed-while-being-handcuffed/",
                    client=client,
                )

        items = asyncio.run(run_ok())
        self.assertEqual(len(items), 1)
        self.assertIn("v-acctoken=tok1080", items[0]["url"])
        self.assertEqual(items[0]["name"], "Xenadreamx Gets Her Pussy Railed While Being Handcuffed.mp4")
        self.assertEqual(items[0]["duration"], 766)
        self.assertEqual(items[0]["actors"], ["Xenadreamx"])

    def test_stream_fallback_clears_active_uploads(self):
        edits = []
        fallback_calls = []

        async def fake_resolve(domain, vid):
            return {"status": "error_html", "error": "HTTP 403"}

        async def fake_download(domain, vid, temp_dir, pos=0, progress_callback=None):
            fallback_calls.append(vid["name"])
            return "error_html"

        class DummyMsg:
            id = 1
            async def edit(self, text, *args, **kwargs):
                edits.append(text)
                return None

        class DummyEvent:
            sender_id = 12345
            id = 99
            async def respond(self, *args, **kwargs):
                return DummyMsg()

        async def run():
            with (
                mock.patch.object(bot, "ENABLE_STREAM_UPLOAD", True),
                mock.patch.object(bot, "resolve_media_stream_info", side_effect=fake_resolve),
                mock.patch.object(bot, "download_video", side_effect=fake_download),
            ):
                await bot.download_and_upload(
                    DummyEvent(),
                    "https://pimpbunny.com/videos/test-clip/",
                    None,
                    selected_videos=[{"id": "1", "name": "Test_Clip.mp4", "url": "https://pimpbunny.com/videos/test-clip/", "service": "pimpbunny"}],
                )

        asyncio.run(run())
        self.assertEqual(fallback_calls, ["Test_Clip.mp4"])
        self.assertTrue(any("Task Completed!" in e and "Skipped:</b> 1" in e for e in edits))

    def test_fetch_pimpbunny_multipage_model_returns_all_95_items(self):
        def make_page(start_idx: int, count: int, pag_links: str) -> str:
            cards = []
            for i in range(start_idx, start_idx + count):
                cards.append(
                    f'<div class="wrapper"><a href="/videos/comatozze-clip-{i}/">'
                    f'<img alt="Comatozze Clip {i}" src="/thumbs/{i}.jpg"/>'
                    f'<div class="_duration_1">10:05</div></a>'
                    f'<div><a href="/models/comatozze/">Comatozze</a></div></div>'
                )
            return f"<html><body>{''.join(cards)}<ul>{pag_links}</ul></body></html>"

        pages = {
            "/models/comatozze/": make_page(
                1, 32,
                '<li><a aria-label="Go to page 2" href="/models/comatozze/2/">2</a></li>'
                '<li><a aria-label="Go to page 3" href="/models/comatozze/3/">3</a></li>'
                '<li><a aria-label="Next page" href="/models/comatozze/2/"></a></li>',
            ),
            "/models/comatozze/2/": make_page(
                33, 32,
                '<li><a aria-label="Previous page" href="/models/comatozze/"></a></li>'
                '<li><a aria-label="Go to page 1" href="/models/comatozze/">1</a></li>'
                '<li><a aria-label="Go to page 3" href="/models/comatozze/3/">3</a></li>'
                '<li><a aria-label="Next page" href="/models/comatozze/3/"></a></li>',
            ),
            "/models/comatozze/3/": make_page(
                65, 31,
                '<li><a aria-label="Previous page" href="/models/comatozze/2/"></a></li>'
                '<li><a aria-label="Go to page 1" href="/models/comatozze/">1</a></li>'
                '<li><a aria-label="Go to page 2" href="/models/comatozze/2/">2</a></li>',
            ),
        }

        def handler(req: httpx.Request) -> httpx.Response:
            html = pages.get(req.url.path)
            if html is None:
                return httpx.Response(404, text="Not Found")
            return httpx.Response(200, text=html)

        transport = httpx.MockTransport(handler)

        async def run():
            async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
                return await pimpbunny_adapter.fetch_pimpbunny_all_posts(
                    "https://pimpbunny.com/models/comatozze/", client=client
                )

        items = asyncio.run(run())
        self.assertEqual(len(items), 95)
        self.assertEqual(items[0]["actors"], ["Comatozze"])
        self.assertEqual(items[-1]["id"], "comatozze-clip-95")

    def test_extract_astro_get_video_and_native_fallback_clean(self):
        astro_html = """
        <html><head>
        <meta property="og:title" content="Comatozze Gets Drilled | PimpBunny"/>
        <meta property="og:image" content="https://pimpbunny.com/thumbs/573901.jpg"/>
        </head><body>
        <a href="/models/">Models</a>
        <a href="/models/comatozze/">Comatozze</a>
        <astro-island props="{&quot;sources&quot;:[1,[[0,{&quot;name&quot;:[0,&quot;1080p&quot;],&quot;src&quot;:[0,&quot;https://pimpbunny.com/get_video/43/hash/573000/573901/573901_pb_1080p.mp4/?v-acctoken=tok1080&quot;]}],[0,{&quot;name&quot;:[0,&quot;720p&quot;],&quot;src&quot;:[0,&quot;https://pimpbunny.com/get_video/43/hash/573000/573901/573901_pb_720p.mp4/?v-acctoken=tok720&quot;]}]]]}"></astro-island>
        </body></html>
        """
        parsed = pimpbunny_adapter._parse_pimpbunny_video_html(
            "https://pimpbunny.com/videos/comatozze-gets-drilled/", astro_html
        )
        self.assertEqual(
            parsed["url"],
            "https://pimpbunny.com/get_video/43/hash/573000/573901/573901_pb_1080p.mp4/?v-acctoken=tok1080",
        )
        self.assertEqual(parsed["name"], "Comatozze Gets Drilled.mp4")
        self.assertEqual(parsed["actors"], ["Comatozze"])
        self.assertEqual(parsed["id"], "573901")

        native = asyncio.run(
            scraper.extract_native_video(
                astro_html, "https://pimpbunny.com/videos/comatozze-gets-drilled/"
            )
        )
        self.assertNotIn("&quot;", native["url"])
        self.assertTrue(native["url"].endswith("v-acctoken=tok1080"))


if __name__ == "__main__":
    unittest.main()
