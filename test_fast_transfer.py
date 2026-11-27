import asyncio
import hashlib
import importlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import httpx
from telethon import errors

import bot
import fasttelethon
import scraper


class _FakeSender:
    def __init__(self, delay: float = 0.0, fail_once_part: int | None = None):
        self._user_connected = True
        self.delay = delay
        self.fail_once_part = fail_once_part
        self.disconnected = False
        self.in_flight = 0
        self.max_in_flight = 0

    async def disconnect(self):
        self._user_connected = False
        self.disconnected = True


class _FakeClient:
    def __init__(self, senders: list[_FakeSender], flood_wait_once_part: int | None = None):
        self.loop = None
        self.session = type("Sess", (), {"dc_id": 2, "auth_key": b"k" * 256})()
        self._log = {}
        self._proxy = None
        self._init_request = type("InitReq", (), {"query": None})()
        self._senders = list(senders)
        self._sender_idx = 0
        self.parts: dict[int, bytes] = {}
        self.sender_counts: list[int] = [0] * len(senders)
        self.flood_wait_once_part = flood_wait_once_part
        self._flood_triggered = False

    async def _get_dc(self, dc_id):
        return type("DC", (), {"ip_address": "127.0.0.1", "port": 443, "id": dc_id})()

    def _connection(self, *args, **kwargs):
        return None

    async def _call(self, sender: _FakeSender, request, flood_sleep_threshold=None):
        sender.in_flight += 1
        sender.max_in_flight = max(sender.max_in_flight, sender.in_flight)
        try:
            part_idx = request.file_part
            if sender.fail_once_part is not None and part_idx == sender.fail_once_part:
                sender.fail_once_part = None
                raise ConnectionError("simulated socket drop")
            if self.flood_wait_once_part is not None and part_idx == self.flood_wait_once_part and not self._flood_triggered:
                self._flood_triggered = True
                err = errors.FloodWaitError(request=request, capture=0)
                err.seconds = 0
                raise err
            if sender.delay:
                await asyncio.sleep(sender.delay)
            self.parts[part_idx] = request.bytes
            return True
        finally:
            sender.in_flight -= 1


class TestFastTransfer(unittest.TestCase):
    def _patch_transferrer_senders(self, client: _FakeClient, replacement_senders=None):
        idx = [0]

        async def _fake_create_sender(self_pt):
            i = idx[0]
            idx[0] += 1
            if i < len(client._senders):
                return client._senders[i]
            if replacement_senders:
                return replacement_senders.pop(0)
            return _FakeSender()

        return mock.patch.object(fasttelethon.ParallelTransferrer, "_create_sender", _fake_create_sender)

    def test_upload_file_pipelined_free_sender_and_recovery(self):
        payload = os.urandom(3 * 1024 * 1024 + 12345)  # > 3 MB
        # Sender 0 is slow and drops once; Sender 1 is fast.
        s0 = _FakeSender(delay=0.05, fail_once_part=0)
        s1 = _FakeSender(delay=0.002)
        replacement = _FakeSender(delay=0.005)
        client = _FakeClient([s0, s1], flood_wait_once_part=3)

        progress_points = []

        async def prog(cur, tot):
            progress_points.append((cur, tot))

        async def run():
            client.loop = asyncio.get_running_loop()
            with (
                self._patch_transferrer_senders(client, [replacement]),
                mock.patch("fasttelethon._sleep", new=mock.AsyncMock()),
            ):
                bio = io.BytesIO(payload)
                bio.name = "sample.mp4"
                res = await fasttelethon.upload_file(
                    client,
                    bio,
                    progress_callback=prog,
                    file_name="sample.mp4",
                    connection_count=2,
                    pipeline_depth=2,
                )
                return res

        res = asyncio.run(run())
        reassembled = b"".join(client.parts[i] for i in sorted(client.parts))
        self.assertEqual(reassembled, payload)
        self.assertEqual(res.md5_checksum, hashlib.md5(payload).hexdigest())
        # Verify pipelining > 1 concurrent in-flight part occurred on the fast sender
        self.assertGreaterEqual(s1.max_in_flight, 2)
        # Progress should be monotonic and end at exact payload size
        self.assertTrue(progress_points)
        self.assertEqual(progress_points[-1], (len(payload), len(payload)))
        vals = [c for c, _ in progress_points]
        self.assertEqual(vals, sorted(vals))

    def test_upload_http_stream_multi_range_concurrent_and_md5(self):
        payload = os.urandom(3 * 1024 * 1024 + 54321)  # ~3.05 MB
        total = len(payload)
        active = [0]
        max_active = [0]

        async def handler(request: httpx.Request) -> httpx.Response:
            active[0] += 1
            max_active[0] = max(max_active[0], active[0])
            try:
                await asyncio.sleep(0.015)
                rng = request.headers.get("Range")
                if rng and rng.startswith("bytes="):
                    start_s, end_s = rng[6:].split("-", 1)
                    start = int(start_s)
                    end = min(total - 1, int(end_s)) if end_s else total - 1
                    chunk = payload[start : end + 1]
                    return httpx.Response(
                        206,
                        content=chunk,
                        headers={
                            "Content-Range": f"bytes {start}-{end}/{total}",
                            "Content-Length": str(len(chunk)),
                        },
                    )
                return httpx.Response(200, content=payload, headers={"Content-Length": str(total)})
            finally:
                active[0] -= 1

        transport = httpx.MockTransport(handler)
        orig_async_client = httpx.AsyncClient
        client = _FakeClient([_FakeSender() for _ in range(4)])

        async def run():
            client.loop = asyncio.get_running_loop()
            with (
                self._patch_transferrer_senders(client),
                mock.patch(
                    "httpx.AsyncClient",
                    side_effect=lambda *a, **kw: orig_async_client(*a, transport=transport, **kw),
                ),
            ):
                return await fasttelethon.upload_http_stream(
                    client,
                    "https://cdn.example.com/video.mp4",
                    "video.mp4",
                    connection_count=4,
                    http_connections=4,
                    pipeline_depth=2,
                )

        inp, size = asyncio.run(run())
        self.assertEqual(size, total)
        reassembled = b"".join(client.parts[i] for i in sorted(client.parts))
        self.assertEqual(reassembled, payload)
        self.assertEqual(inp.md5_checksum, hashlib.md5(payload).hexdigest())
        self.assertGreaterEqual(max_active[0], 2)

    def test_upload_http_stream_200_fallback_and_token_refresh(self):
        payload = os.urandom(1500 * 1024)
        total = len(payload)
        calls = {"v1": 0, "v2": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path == "/expired.mp4":
                calls["v1"] += 1
                return httpx.Response(403, text="expired")
            calls["v2"] += 1
            # Ignore Range header and return 200 OK
            return httpx.Response(200, content=payload, headers={"Content-Length": str(total)})

        transport = httpx.MockTransport(handler)
        orig_async_client = httpx.AsyncClient
        client = _FakeClient([_FakeSender(), _FakeSender()])
        refreshed = [0]

        async def refresh_cb():
            refreshed[0] += 1
            return "https://cdn.example.com/fresh.mp4", {"Referer": "https://example.com/"}

        async def run():
            client.loop = asyncio.get_running_loop()
            with (
                self._patch_transferrer_senders(client),
                mock.patch(
                    "httpx.AsyncClient",
                    side_effect=lambda *a, **kw: orig_async_client(*a, transport=transport, **kw),
                ),
            ):
                return await fasttelethon.upload_http_stream(
                    client,
                    "https://cdn.example.com/expired.mp4",
                    "fresh.mp4",
                    connection_count=2,
                    http_connections=4,
                    url_refresh_cb=refresh_cb,
                )

        inp, size = asyncio.run(run())
        self.assertEqual(size, total)
        self.assertEqual(refreshed[0], 1)
        self.assertEqual(calls["v2"], 1)
        reassembled = b"".join(client.parts[i] for i in sorted(client.parts))
        self.assertEqual(reassembled, payload)
        self.assertEqual(inp.md5_checksum, hashlib.md5(payload).hexdigest())

    def test_upload_http_stream_adaptive_rate_limit_downgrade(self):
        payload = os.urandom(3 * 1024 * 1024 + 777)
        total = len(payload)
        in_flight = [0]
        rejected_429 = [0]

        async def handler(request: httpx.Request) -> httpx.Response:
            in_flight[0] += 1
            try:
                await asyncio.sleep(0.01)
                if in_flight[0] > 1:
                    rejected_429[0] += 1
                    return httpx.Response(429, text="slow down")
                rng = request.headers.get("Range")
                start_s, end_s = rng[6:].split("-", 1)
                start = int(start_s)
                end = min(total - 1, int(end_s)) if end_s else total - 1
                chunk = payload[start : end + 1]
                return httpx.Response(
                    206,
                    content=chunk,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{total}",
                        "Content-Length": str(len(chunk)),
                    },
                )
            finally:
                in_flight[0] -= 1

        transport = httpx.MockTransport(handler)
        orig_async_client = httpx.AsyncClient
        client = _FakeClient([_FakeSender(), _FakeSender()])

        async def run():
            client.loop = asyncio.get_running_loop()
            with (
                self._patch_transferrer_senders(client),
                mock.patch("fasttelethon._sleep", new=mock.AsyncMock()),
                mock.patch(
                    "httpx.AsyncClient",
                    side_effect=lambda *a, **kw: orig_async_client(*a, transport=transport, **kw),
                ),
            ):
                return await fasttelethon.upload_http_stream(
                    client,
                    "https://cdn.example.com/throttled.mp4",
                    "throttled.mp4",
                    connection_count=2,
                    http_connections=4,
                )

        inp, size = asyncio.run(run())
        self.assertEqual(size, total)
        self.assertGreater(rejected_429[0], 0)
        reassembled = b"".join(client.parts[i] for i in sorted(client.parts))
        self.assertEqual(reassembled, payload)

    def test_scraper_download_video_multi_range(self):
        payload = os.urandom(5 * 1024 * 1024 + 9876)  # > 4 MB triggers multi-range
        total = len(payload)
        range_requests = [0]

        def handler(request: httpx.Request) -> httpx.Response:
            rng = request.headers.get("Range")
            if rng and rng.startswith("bytes="):
                range_requests[0] += 1
                start_s, end_s = rng[6:].split("-", 1)
                start = int(start_s)
                end = min(total - 1, int(end_s)) if end_s else total - 1
                chunk = payload[start : end + 1]
                return httpx.Response(
                    206,
                    content=chunk,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{total}",
                        "Content-Length": str(len(chunk)),
                    },
                )
            return httpx.Response(200, content=payload, headers={"Content-Length": str(total)})

        transport = httpx.MockTransport(handler)
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp)
            vid = {"id": "123", "name": "clip.mp4", "url": "https://cdn.example.com/clip.mp4"}

            async def run():
                async with httpx.AsyncClient(transport=transport, follow_redirects=True) as c:
                    return await scraper.download_video(
                        "example.com", vid, out_dir, client=c, http_connections=4
                    )

            with mock.patch("scraper.resolve_redirect_url", new=mock.AsyncMock(return_value=vid["url"])):
                ok = asyncio.run(run())
            self.assertTrue(ok)
            dest = out_dir / "123_clip.mp4"
            self.assertTrue(dest.exists())
            self.assertEqual(dest.read_bytes(), payload)
            self.assertFalse(dest.with_name(dest.name + ".mrange").exists())
            self.assertGreaterEqual(range_requests[0], 3)

    def test_bot_pipeline_env_defaults_desktop_and_termux(self):
        clean_desktop = {
            k: v
            for k, v in os.environ.items()
            if k not in (
                "TERMUX_VERSION",
                "PREFIX",
                "MTPROTO_PIPELINE_DEPTH",
                "HTTP_STREAM_CONNECTIONS",
                "UPLOAD_CONCURRENCY",
            )
        }
        with mock.patch.dict(os.environ, clean_desktop, clear=True):
            with mock.patch("dotenv.load_dotenv"):
                reloaded = importlib.reload(bot)
                self.assertFalse(reloaded.IS_TERMUX)
                self.assertEqual(reloaded.MTPROTO_PIPELINE_DEPTH, 2)
                self.assertEqual(reloaded.HTTP_STREAM_CONNECTIONS, 6)
                self.assertEqual(reloaded.UPLOAD_CONCURRENCY, 2)

        clean_termux = dict(clean_desktop)
        clean_termux["TERMUX_VERSION"] = "0.118.0"
        with mock.patch.dict(os.environ, clean_termux, clear=True):
            with mock.patch("dotenv.load_dotenv"):
                reloaded = importlib.reload(bot)
                self.assertTrue(reloaded.IS_TERMUX)
                self.assertEqual(reloaded.MTPROTO_PIPELINE_DEPTH, 2)
                self.assertEqual(reloaded.HTTP_STREAM_CONNECTIONS, 3)
                self.assertEqual(reloaded.UPLOAD_CONCURRENCY, 1)

        importlib.reload(bot)


if __name__ == "__main__":
    unittest.main()
