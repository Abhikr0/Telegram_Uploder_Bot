import asyncio
import importlib
import inspect
import os
import unittest
from unittest import mock

import httpx

import bot
import fasttelethon


class TestTermuxCompatibility(unittest.TestCase):
    def test_supabase_rest_client_insert_and_count(self):
        captured = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append((request.method, str(request.url), dict(request.headers), request.content))
            if request.method == "POST":
                return httpx.Response(201)
            if request.method == "GET":
                return httpx.Response(206, headers={"Content-Range": "0-0/42"})
            return httpx.Response(404)

        transport = httpx.MockTransport(handler)
        orig_async_client = httpx.AsyncClient

        def mock_async_client(*args, **kwargs):
            kwargs["transport"] = transport
            return orig_async_client(*args, **kwargs)

        async def run():
            client = bot.SupabaseRestClient("https://example.supabase.co/", "test-key")
            with mock.patch("bot.httpx.AsyncClient", side_effect=mock_async_client):
                await client.insert("media", {"id": "1", "title": "hello"})
                total = await client.count("media")
            return total

        total = asyncio.run(run())
        self.assertEqual(total, 42)
        self.assertEqual(len(captured), 2)
        self.assertEqual(captured[0][0], "POST")
        self.assertEqual(captured[0][1], "https://example.supabase.co/rest/v1/media")
        self.assertEqual(captured[0][2]["apikey"], "test-key")
        self.assertEqual(captured[1][0], "GET")
        self.assertEqual(captured[1][1], "https://example.supabase.co/rest/v1/media?select=id")

    def test_supabase_rest_client_empty_table_416(self):
        transport = httpx.MockTransport(
            lambda req: httpx.Response(416, headers={"Content-Range": "*/0"})
        )
        orig_async_client = httpx.AsyncClient

        async def run():
            client = bot.SupabaseRestClient("https://example.supabase.co", "test-key")
            with mock.patch(
                "bot.httpx.AsyncClient",
                side_effect=lambda *a, **kw: orig_async_client(*a, transport=transport, **kw),
            ):
                return await client.count("media")

        self.assertEqual(asyncio.run(run()), 0)

    def test_fasttelethon_accepts_connection_count(self):
        for fn in (fasttelethon.upload_file, fasttelethon.upload_stream, fasttelethon.upload_http_stream):
            sig = inspect.signature(fn)
            self.assertIn("connection_count", sig.parameters)

    def test_termux_env_defaults(self):
        env_patch = {
            "TERMUX_VERSION": "0.118.0",
            "ENABLE_STREAM_UPLOAD": "",
            "STREAM_CONCURRENCY": "",
            "DOWNLOAD_CONCURRENCY": "",
            "MTPROTO_CONNECTIONS": "",
            "STREAM_PROBE_BYTES": "",
        }
        clean_env = {k: v for k, v in os.environ.items() if k not in env_patch}
        clean_env["TERMUX_VERSION"] = "0.118.0"
        with mock.patch.dict(os.environ, clean_env, clear=True):
            with mock.patch("dotenv.load_dotenv"):
                reloaded = importlib.reload(bot)
                self.assertTrue(reloaded.IS_TERMUX)
                self.assertFalse(reloaded.ENABLE_STREAM_UPLOAD)
                self.assertEqual(reloaded.STREAM_CONCURRENCY, 1)
                self.assertEqual(reloaded.DOWNLOAD_CONCURRENCY, 2)
                self.assertEqual(reloaded.MTPROTO_CONNECTIONS, 2)
                self.assertEqual(reloaded.STREAM_PROBE_BYTES, 6 * 1024 * 1024)
        # Restore desktop defaults after test
        importlib.reload(bot)


if __name__ == "__main__":
    unittest.main()
