import asyncio
import importlib
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx

import ai_caption
import bot


class TestThumbnailPipeline(unittest.TestCase):
    def test_sanitize_filename_preserves_extension_over_80_chars(self):
        long_title = "Very Long Video Title " * 6
        out = ai_caption.sanitize_filename(long_title, ".mp4")
        self.assertLessEqual(len(out), 80)
        self.assertTrue(out.endswith(".mp4"))

        out_mkv = ai_caption.sanitize_filename(long_title, ".mkv")
        self.assertLessEqual(len(out_mkv), 80)
        self.assertTrue(out_mkv.endswith(".mkv"))

    def test_format_ffmpeg_seek_handles_long_durations(self):
        self.assertEqual(bot._format_ffmpeg_seek(0), "00:00:00.000")
        self.assertEqual(bot._format_ffmpeg_seek(65), "00:01:05.000")
        self.assertEqual(bot._format_ffmpeg_seek(1056), "00:17:36.000")
        self.assertEqual(bot._format_ffmpeg_seek(3661), "01:01:01.000")

    def test_windows_drive_download_dir_ignored_on_posix(self):
        if os.name == "nt":
            self.skipTest("POSIX-only guard")
        clean_env = {k: v for k, v in os.environ.items() if k != "DOWNLOAD_DIR"}
        clean_env["DOWNLOAD_DIR"] = "E:/.bot_downloads"
        with mock.patch.dict(os.environ, clean_env, clear=True):
            with mock.patch("dotenv.load_dotenv"):
                reloaded = importlib.reload(bot)
                self.assertEqual(Path(reloaded.DOWNLOAD_DIR), Path("bot_downloads").resolve())
        importlib.reload(bot)

    def test_generate_thumbnail_with_colon_dir_and_large_duration(self):
        ff = bot._get_ffmpeg()
        with tempfile.TemporaryDirectory() as td:
            colon_dir = Path(td) / "E:" / ".bot_downloads"
            colon_dir.mkdir(parents=True)
            vid_path = colon_dir / "sample.mp4"
            thumb_path = colon_dir / "thumb.jpg"
            subprocess.run(
                [ff, "-y", "-f", "lavfi", "-i", "testsrc=duration=4:size=640x360:rate=10", "-c:v", "libx264", str(vid_path)],
                check=True,
                capture_output=True,
            )
            rel_vid = os.path.relpath(vid_path)
            rel_thumb = Path(os.path.relpath(thumb_path))
            # Pass a large duration (>120s) to verify midpoint seek formatting doesn't fail with invalid -ss
            ok = asyncio.run(bot.generate_thumbnail(rel_vid, rel_thumb, duration=2112))
            self.assertTrue(ok)
            self.assertTrue(thumb_path.exists())
            self.assertGreater(thumb_path.stat().st_size, 0)

    def test_fetch_fallback_thumbnail_transcodes_webp_to_jpeg(self):
        ff = bot._get_ffmpeg()
        with tempfile.TemporaryDirectory() as td:
            src_webp = Path(td) / "poster.webp"
            out_jpg = Path(td) / "thumb.jpg"
            subprocess.run(
                [ff, "-y", "-f", "lavfi", "-i", "testsrc=duration=1:size=640x360:rate=1", "-vframes", "1", str(src_webp)],
                check=True,
                capture_output=True,
            )
            webp_bytes = src_webp.read_bytes()
            self.assertGreater(len(webp_bytes), 500)

            transport = httpx.MockTransport(
                lambda req: httpx.Response(200, content=webp_bytes, headers={"Content-Type": "image/webp"})
            )
            orig_async_client = httpx.AsyncClient

            async def run():
                with mock.patch(
                    "bot.httpx.AsyncClient",
                    side_effect=lambda *a, **kw: orig_async_client(*a, transport=transport, **kw),
                ):
                    return await bot._fetch_fallback_thumbnail(
                        {"thumbnail": "https://example.com/poster.webp"},
                        out_jpg,
                    )

            self.assertTrue(asyncio.run(run()))
            self.assertTrue(out_jpg.exists())
            # JPEG magic bytes FF D8 FF
            self.assertEqual(out_jpg.read_bytes()[:3], b"\xff\xd8\xff")
            self.assertLessEqual(out_jpg.stat().st_size, 200 * 1024)

    def test_send_video_with_thumb_passes_resolved_thumb_and_mime(self):
        with tempfile.TemporaryDirectory() as td:
            thumb_path = Path(td) / "thumb.jpg"
            thumb_path.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 100)
            captured = {}

            async def fake_send_file(entity, file, **kwargs):
                captured["entity"] = entity
                captured["file"] = file
                captured["kwargs"] = kwargs
                return "ok"

            with mock.patch.object(bot.client, "send_file", side_effect=fake_send_file):
                res = asyncio.run(
                    bot._send_video_with_thumb(
                        -100123456,
                        "fake_uploaded_file",
                        "clean_video_name.mp4",
                        "<b>caption</b>",
                        {"duration": 120, "width": 1920, "height": 1080},
                        thumb_path,
                        True,
                        topic_id=8,
                    )
                )

            self.assertEqual(res, "ok")
            self.assertEqual(captured["kwargs"]["thumb"], str(thumb_path.resolve()))
            self.assertEqual(captured["kwargs"]["mime_type"], "video/mp4")
            self.assertEqual(captured["kwargs"]["reply_to"], 8)
            self.assertTrue(captured["kwargs"]["supports_streaming"])


if __name__ == "__main__":
    unittest.main()
