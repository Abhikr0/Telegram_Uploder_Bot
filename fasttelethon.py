# copied from https://github.com/tulir/mautrix-telegram/blob/master/mautrix_telegram/util/parallel_file_transfer.py
# Copyright (C) 2021 Tulir Asokan
import asyncio
import hashlib
import inspect
import logging
import math
import os
import re
import time
from collections import defaultdict
from typing import (
    AsyncGenerator,
    AsyncIterable,
    Awaitable,
    BinaryIO,
    Callable,
    DefaultDict,
    List,
    Optional,
    Tuple,
    Union,
)

from telethon import TelegramClient, errors, helpers, utils
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
import telethon.network.connection.connection as telethon_conn
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import (
    GetFileRequest,
    SaveBigFilePartRequest,
    SaveFilePartRequest,
)
from telethon.tl.types import (
    Document,
    InputDocumentFileLocation,
    InputFile,
    InputFileBig,
    InputFileLocation,
    InputPeerPhotoFileLocation,
    InputPhotoFileLocation,
    TypeInputFile,
)

log: logging.Logger = logging.getLogger("fasttelethon")

TypeLocation = Union[
    Document,
    InputDocumentFileLocation,
    InputPeerPhotoFileLocation,
    InputFileLocation,
    InputPhotoFileLocation,
]

_RETRYABLE_UPLOAD_ERRORS = (
    ConnectionError,
    OSError,
    TimeoutError,
    asyncio.TimeoutError,
    BufferError,
    telethon_conn.InvalidChecksumError,
    telethon_conn.InvalidBufferError,
    errors.ServerError,
    errors.RpcCallFailError,
    errors.RpcMcgetFailError,
    errors.InterdcCallErrorError,
    errors.InterdcCallRichErrorError,
    errors.TimedOutError,
    ValueError,
)

_ERR_SENTINEL = object()


async def _sleep(seconds: float) -> None:
    await asyncio.sleep(seconds)


class DownloadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: GetFileRequest
    remaining: int
    stride: int

    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        file: TypeLocation,
        offset: int,
        limit: int,
        stride: int,
        count: int,
    ) -> None:
        self.sender = sender
        self.client = client
        self.request = GetFileRequest(file, offset=offset, limit=limit)
        self.stride = stride
        self.remaining = count

    async def next(self) -> Optional[bytes]:
        if not self.remaining:
            return None
        result = await self.client._call(self.sender, self.request)
        self.remaining -= 1
        self.request.offset += self.stride
        return result.bytes

    async def disconnect(self) -> None:
        try:
            await self.sender.disconnect()
        except Exception:
            pass


class UploadSender:
    """Manages pipelined part uploads over a single MTProtoSender with reconnect recovery."""

    client: TelegramClient
    sender: Optional[MTProtoSender]
    sender_factory: Callable[[], Awaitable[MTProtoSender]]
    file_id: int
    part_count: int
    is_big: bool
    loop: asyncio.AbstractEventLoop
    pipeline_depth: int
    in_flight: set[asyncio.Task]

    def __init__(
        self,
        client: TelegramClient,
        sender: MTProtoSender,
        sender_factory: Callable[[], Awaitable[MTProtoSender]],
        file_id: int,
        part_count: int,
        big: bool,
        loop: asyncio.AbstractEventLoop,
        pipeline_depth: int = 2,
    ) -> None:
        self.client = client
        self.sender = sender
        self.sender_factory = sender_factory
        self.file_id = file_id
        self.part_count = part_count
        self.is_big = big
        self.loop = loop
        self.pipeline_depth = max(1, int(pipeline_depth))
        self.in_flight = set()
        self._reconnect_lock = asyncio.Lock()
        self._closing = False
        try:
            self._supports_flood_kw = (
                "flood_sleep_threshold" in inspect.signature(client._call).parameters
            )
        except (TypeError, ValueError):
            self._supports_flood_kw = False

    def has_capacity(self) -> bool:
        return len(self.in_flight) < self.pipeline_depth

    async def _ensure_sender(self) -> MTProtoSender:
        if self.sender is not None and getattr(self.sender, "_user_connected", True):
            return self.sender
        async with self._reconnect_lock:
            if self.sender is None or not getattr(self.sender, "_user_connected", True):
                self.sender = await self.sender_factory()
            return self.sender

    async def _safe_disconnect_sender(self, target_sender: Optional[MTProtoSender] = None) -> None:
        async with self._reconnect_lock:
            if target_sender is not None and self.sender is not target_sender:
                return
            s = self.sender
            self.sender = None
        if s is not None:
            try:
                await s.disconnect()
            except Exception:
                pass

    async def _call_part(self, sender: MTProtoSender, request) -> None:
        if self._supports_flood_kw:
            await self.client._call(sender, request, flood_sleep_threshold=0)
        else:
            await self.client._call(sender, request)

    async def _send_part_with_retry(self, part_idx: int, data: bytes) -> int:
        max_part_retries = 5
        for attempt in range(1, max_part_retries + 1):
            sender = None
            try:
                sender = await self._ensure_sender()
                if self.is_big:
                    request = SaveBigFilePartRequest(self.file_id, part_idx, self.part_count, data)
                else:
                    request = SaveFilePartRequest(self.file_id, part_idx, data)
                await self._call_part(sender, request)
                return len(data)
            except asyncio.CancelledError:
                cur_task = asyncio.current_task()
                cancelling = getattr(cur_task, "cancelling", lambda: 0)() > 0
                if self._closing or cancelling or attempt == max_part_retries:
                    raise
                await self._safe_disconnect_sender(sender)
                await _sleep(1)
            except errors.FloodWaitError as e:
                if attempt == max_part_retries:
                    raise
                wait_s = min(max(1, e.seconds) + 1, 300)
                log.warning(
                    "Telegram FloodWait (%ds) on part %d (attempt %d/%d); sleeping %ds...",
                    e.seconds,
                    part_idx,
                    attempt,
                    max_part_retries,
                    wait_s,
                )
                await _sleep(wait_s)
            except _RETRYABLE_UPLOAD_ERRORS as e:
                await self._safe_disconnect_sender(sender)
                if attempt == max_part_retries:
                    raise
                is_429 = getattr(e, "code", None) == 429
                delay = min((3 if is_429 else 1) * (2 ** (attempt - 1)), 15)
                log.warning(
                    "Upload part %d failed (attempt %d/%d): %s. Reconnecting sender in %ds...",
                    part_idx,
                    attempt,
                    max_part_retries,
                    e,
                    delay,
                )
                await _sleep(delay)
        raise IOError(f"Failed to upload part {part_idx}")

    def start_part(self, part_idx: int, data: bytes) -> asyncio.Task:
        task = self.loop.create_task(self._send_part_with_retry(part_idx, data))
        self.in_flight.add(task)
        return task

    def reap_done(self) -> int:
        done = [t for t in self.in_flight if t.done()]
        completed_bytes = 0
        for t in done:
            self.in_flight.discard(t)
            completed_bytes += t.result()
        return completed_bytes

    async def disconnect(self) -> None:
        self._closing = True
        if self.in_flight:
            pending = list(self.in_flight)
            self.in_flight.clear()
            for t in pending:
                if not t.done():
                    t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        await self._safe_disconnect_sender()


async def _invoke_progress(progress_callback: Optional[Callable], current: int, total: int) -> None:
    if progress_callback:
        r = progress_callback(current, total)
        if inspect.isawaitable(r):
            await r


class ParallelTransferrer:
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    dc_id: int
    senders: Optional[List[Union[DownloadSender, UploadSender]]]
    auth_key: Optional[AuthKey]
    upload_ticker: int
    uploaded_bytes: int
    _next_part_idx: int

    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (
            None
            if dc_id and self.client.session.dc_id != dc_id
            else self.client.session.auth_key
        )
        self.senders = None
        self.upload_ticker = 0
        self.uploaded_bytes = 0
        self._next_part_idx = 0

    async def _cleanup(self) -> None:
        if self.senders:
            to_disconnect = self.senders
            self.senders = None
            await asyncio.gather(
                *[sender.disconnect() for sender in to_disconnect if sender],
                return_exceptions=True,
            )

    @staticmethod
    def _get_connection_count(
        file_size: int,
        max_count: int = 4,
        full_size: int = 100 * 1024 * 1024,
    ) -> int:
        if file_size > full_size:
            return max_count
        return min(max_count, max(1, math.ceil((file_size / full_size) * max_count)))

    async def _init_download(
        self, connections: int, file: TypeLocation, part_count: int, part_size: int
    ) -> None:
        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        # The first cross-DC sender exports/imports auth before remaining senders connect.
        self.senders = [
            await self._create_download_sender(
                file, 0, part_size, connections * part_size, get_part_count()
            ),
            *await asyncio.gather(
                *[
                    self._create_download_sender(
                        file, i, part_size, connections * part_size, get_part_count()
                    )
                    for i in range(1, connections)
                ]
            ),
        ]

    async def _create_download_sender(
        self, file: TypeLocation, index: int, part_size: int, stride: int, part_count: int
    ) -> DownloadSender:
        return DownloadSender(
            self.client,
            await self._create_sender(),
            file,
            index * part_size,
            part_size,
            stride,
            part_count,
        )

    async def _init_upload(
        self,
        connections: int,
        file_id: int,
        part_count: int,
        big: bool,
        pipeline_depth: int = 2,
    ) -> None:
        self.senders = []
        try:
            first_sender = await self._create_upload_sender(
                file_id, part_count, big, pipeline_depth=pipeline_depth
            )
            self.senders.append(first_sender)
            if connections > 1:
                results = await asyncio.gather(
                    *[
                        self._create_upload_sender(
                            file_id, part_count, big, pipeline_depth=pipeline_depth
                        )
                        for _ in range(1, connections)
                    ],
                    return_exceptions=True,
                )
                for res in results:
                    if isinstance(res, UploadSender):
                        self.senders.append(res)
                    else:
                        log.warning(
                            "Optional upload connection failed to initialize (using %d): %s",
                            len(self.senders),
                            res,
                        )
        except Exception:
            await self._cleanup()
            raise

    async def _create_upload_sender(
        self,
        file_id: int,
        part_count: int,
        big: bool,
        pipeline_depth: int = 2,
    ) -> UploadSender:
        return UploadSender(
            self.client,
            await self._create_sender(),
            self._create_sender,
            file_id,
            part_count,
            big,
            loop=self.loop,
            pipeline_depth=pipeline_depth,
        )

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(
            self.client._connection(
                dc.ip_address,
                dc.port,
                dc.id,
                loggers=self.client._log,
                proxy=self.client._proxy,
            )
        )
        if not self.auth_key:
            log.debug("Exporting auth to DC %s", self.dc_id)
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(
                id=auth.id, bytes=auth.bytes
            )
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def init_upload(
        self,
        file_id: int,
        file_size: int,
        part_size_kb: Optional[float] = None,
        connection_count: Optional[int] = None,
        pipeline_depth: int = 2,
    ) -> Tuple[int, int, bool]:
        is_large = file_size > 10 * 1024 * 1024
        if part_size_kb:
            part_size = int(part_size_kb * 1024)
        elif is_large:
            # For files > 10MB, use maximum supported 512KB parts to halve round-trips.
            part_size = 512 * 1024
        else:
            part_size = utils.get_appropriated_part_size(file_size) * 1024
        part_count = max(1, (file_size + part_size - 1) // part_size)
        connection_count = connection_count or self._get_connection_count(file_size)
        connection_count = min(max(1, connection_count), part_count)
        self.uploaded_bytes = 0
        self._next_part_idx = 0
        await self._init_upload(
            connection_count, file_id, part_count, is_large, pipeline_depth=pipeline_depth
        )
        return part_size, part_count, is_large

    def _reap_senders(self) -> int:
        completed = 0
        for sender in self.senders or ():
            if isinstance(sender, UploadSender):
                completed += sender.reap_done()
        self.uploaded_bytes += completed
        return completed

    def _pick_free_sender(self) -> Optional[Tuple[int, UploadSender]]:
        senders = [s for s in (self.senders or ()) if isinstance(s, UploadSender)]
        if not senders:
            return None
        best_idx = None
        best_load = None
        n = len(senders)
        for offset in range(n):
            idx = (self.upload_ticker + offset) % n
            sender = senders[idx]
            if not sender.has_capacity():
                continue
            load = len(sender.in_flight)
            if best_load is None or load < best_load:
                best_idx = idx
                best_load = load
                if load == 0:
                    break
        if best_idx is None:
            return None
        return best_idx, senders[best_idx]

    async def upload_part(
        self,
        part_idx: int,
        part: bytes,
        progress_callback: Optional[Callable] = None,
        file_size: int = 0,
    ) -> None:
        if self._reap_senders() > 0:
            await _invoke_progress(progress_callback, self.uploaded_bytes, file_size)

        picked = self._pick_free_sender()
        while picked is None:
            active = [
                t
                for s in (self.senders or ())
                if isinstance(s, UploadSender)
                for t in s.in_flight
            ]
            if not active:
                raise IOError("No upload senders available")
            await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
            if self._reap_senders() > 0:
                await _invoke_progress(progress_callback, self.uploaded_bytes, file_size)
            picked = self._pick_free_sender()

        best_idx, sender = picked
        self.upload_ticker = (best_idx + 1) % max(1, len(self.senders or ()))
        sender.start_part(part_idx, part)

    async def upload(
        self,
        part: bytes,
        progress_callback: Optional[Callable] = None,
        file_size: int = 0,
    ) -> None:
        part_idx = self._next_part_idx
        self._next_part_idx += 1
        await self.upload_part(
            part_idx, part, progress_callback=progress_callback, file_size=file_size
        )

    async def flush(
        self,
        progress_callback: Optional[Callable] = None,
        file_size: int = 0,
    ) -> None:
        while True:
            active = [
                t
                for s in (self.senders or ())
                if isinstance(s, UploadSender)
                for t in s.in_flight
            ]
            if not active:
                break
            await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
            if self._reap_senders() > 0:
                await _invoke_progress(progress_callback, self.uploaded_bytes, file_size)

    async def finish_upload(self) -> None:
        await self._cleanup()

    async def download(
        self,
        file: TypeLocation,
        file_size: int,
        part_size_kb: Optional[float] = None,
        connection_count: Optional[int] = None,
    ) -> AsyncGenerator[bytes, None]:
        connection_count = connection_count or self._get_connection_count(file_size)
        part_size = (part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024
        part_count = math.ceil(file_size / part_size)
        log.debug(
            "Starting parallel download: %s %s %s %s",
            connection_count,
            part_size,
            part_count,
            file,
        )
        await self._init_download(connection_count, file, part_count, part_size)

        part = 0
        while part < part_count:
            tasks = [self.loop.create_task(sender.next()) for sender in self.senders]
            for task in tasks:
                data = await task
                if not data:
                    break
                yield data
                part += 1
                log.debug("Part %d downloaded", part)

        log.debug("Parallel download finished, cleaning up connections")
        await self._cleanup()


parallel_transfer_locks: DefaultDict[int, asyncio.Lock] = defaultdict(lambda: asyncio.Lock())


def stream_file(file_to_stream: BinaryIO, chunk_size: int = 512 * 1024):
    while True:
        data_read = file_to_stream.read(chunk_size)
        if not data_read:
            break
        yield data_read


def _build_input_file(
    file_id: int,
    part_count: int,
    file_name: str,
    file_size: int,
    is_large: bool,
    hash_md5,
) -> Tuple[TypeInputFile, int]:
    if is_large:
        return InputFileBig(file_id, part_count, file_name), file_size
    return InputFile(file_id, part_count, file_name, hash_md5.hexdigest()), file_size


async def _internal_transfer_to_telegram(
    client: TelegramClient,
    response: BinaryIO,
    progress_callback: Optional[Callable],
    file_name: Optional[str] = None,
    connection_count: Optional[int] = None,
    pipeline_depth: int = 2,
) -> Tuple[TypeInputFile, int]:
    file_id = helpers.generate_random_long()
    try:
        response.seek(0, os.SEEK_END)
        file_size = response.tell()
        response.seek(0)
    except Exception:
        file_size = os.path.getsize(response.name)
        try:
            response.seek(0)
        except Exception:
            pass

    hash_md5 = hashlib.md5()
    uploader = ParallelTransferrer(client)
    part_size, part_count, is_large = await uploader.init_upload(
        file_id,
        file_size,
        connection_count=connection_count,
        pipeline_depth=pipeline_depth,
    )

    try:
        while True:
            data = response.read(part_size)
            if not data:
                break
            if not is_large:
                hash_md5.update(data)
            await uploader.upload(data, progress_callback=progress_callback, file_size=file_size)
        await uploader.flush(progress_callback=progress_callback, file_size=file_size)
    finally:
        await uploader.finish_upload()

    name = (
        file_name
        or (getattr(response, "name", None) and os.path.basename(response.name))
        or "video.mp4"
    )
    return _build_input_file(file_id, part_count, name, file_size, is_large, hash_md5)


async def download_file(
    client: TelegramClient,
    location: TypeLocation,
    out: BinaryIO,
    progress_callback: Optional[Callable] = None,
) -> BinaryIO:
    size = location.size
    dc_id, location = utils.get_input_location(location)
    downloader = ParallelTransferrer(client, dc_id)
    try:
        downloaded = downloader.download(location, size)
        async for x in downloaded:
            out.write(x)
            await _invoke_progress(progress_callback, out.tell(), size)
    finally:
        await downloader._cleanup()
    return out


async def upload_file(
    client: TelegramClient,
    file: BinaryIO,
    progress_callback: Optional[Callable] = None,
    file_name: Optional[str] = None,
    connection_count: Optional[int] = None,
    pipeline_depth: int = 2,
) -> TypeInputFile:
    res = (
        await _internal_transfer_to_telegram(
            client,
            file,
            progress_callback,
            file_name=file_name,
            connection_count=connection_count,
            pipeline_depth=pipeline_depth,
        )
    )[0]
    return res


async def _emit_buffer_parts(
    part_queue: asyncio.Queue,
    buf: bytearray,
    part_size: int,
    part_idx: int,
    is_large: bool,
    hash_md5,
    flush_tail: bool = False,
) -> int:
    while len(buf) >= part_size:
        part = bytes(buf[:part_size])
        del buf[:part_size]
        if not is_large:
            hash_md5.update(part)
        await part_queue.put((part_idx, part))
        part_idx += 1
    if flush_tail and buf:
        part = bytes(buf)
        buf.clear()
        if not is_large:
            hash_md5.update(part)
        await part_queue.put((part_idx, part))
        part_idx += 1
    return part_idx


async def _consume_parts_to_telegram(
    uploader: ParallelTransferrer,
    part_queue: asyncio.Queue,
    file_size: int,
    progress_callback: Optional[Callable] = None,
) -> None:
    while True:
        item = await part_queue.get()
        if item is None:
            break
        part_idx, payload = item
        if part_idx is _ERR_SENTINEL:
            raise payload
        await uploader.upload_part(
            part_idx,
            payload,
            progress_callback=progress_callback,
            file_size=file_size,
        )
    await uploader.flush(progress_callback=progress_callback, file_size=file_size)


async def upload_stream(
    client: TelegramClient,
    stream: AsyncIterable[bytes],
    file_size: int,
    file_name: str,
    progress_callback: Optional[Callable] = None,
    connection_count: Optional[int] = None,
    pipeline_depth: int = 2,
    prefetch_parts: Optional[int] = None,
) -> Tuple[TypeInputFile, int]:
    """
    Upload an async stream of bytes directly to Telegram via ParallelTransferrer
    with a bounded RAM prefetch queue separating stream reads from MTProto sends.
    """
    file_id = helpers.generate_random_long()
    hash_md5 = hashlib.md5()
    uploader = ParallelTransferrer(client)
    part_size, part_count, is_large = await uploader.init_upload(
        file_id,
        file_size,
        connection_count=connection_count,
        pipeline_depth=pipeline_depth,
    )

    queue_cap = prefetch_parts or max(4, min(16, (len(uploader.senders or ()) * pipeline_depth) + 4))
    part_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_cap)

    async def _producer() -> None:
        buf = bytearray()
        part_idx = 0
        produced = 0
        try:
            async for chunk in stream:
                if not chunk:
                    continue
                remaining = file_size - produced
                if remaining <= 0:
                    break
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]
                buf.extend(chunk)
                produced += len(chunk)
                part_idx = await _emit_buffer_parts(
                    part_queue, buf, part_size, part_idx, is_large, hash_md5, flush_tail=False
                )
            if produced < file_size:
                raise IOError(
                    f"Stream ended prematurely: transferred {produced} of {file_size} bytes"
                )
            await _emit_buffer_parts(
                part_queue, buf, part_size, part_idx, is_large, hash_md5, flush_tail=True
            )
            await part_queue.put(None)
        except BaseException as exc:
            await part_queue.put((_ERR_SENTINEL, exc))

    producer_task = asyncio.create_task(_producer())
    try:
        await _consume_parts_to_telegram(
            uploader, part_queue, file_size, progress_callback=progress_callback
        )
        await producer_task
    finally:
        if not producer_task.done():
            producer_task.cancel()
            await asyncio.gather(producer_task, return_exceptions=True)
        await uploader.finish_upload()

    return _build_input_file(file_id, part_count, file_name, file_size, is_large, hash_md5)


class _RetryableStreamError(IOError):
    """Internal signal for a recoverable HTTP stream interruption."""


class _RateLimitSignal(IOError):
    """Signals HTTP 403/429/503 on concurrent range fetch to shrink concurrency."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code


def _parse_http_total_size(resp) -> int:
    content_range = resp.headers.get("Content-Range", "")
    if content_range and "/" in content_range:
        total_part = content_range.split("/")[-1].strip()
        if total_part.isdigit():
            return int(total_part)
    if "Content-Length" in resp.headers:
        try:
            return int(resp.headers["Content-Length"])
        except ValueError:
            return 0
    return 0


def _parse_content_range_start(resp) -> Optional[int]:
    content_range = resp.headers.get("Content-Range", "")
    m = re.match(r"bytes\s+(\d+)-", content_range, re.I)
    return int(m.group(1)) if m else None


def _build_http_headers(base_headers: dict, byte_range: Optional[str] = None) -> dict:
    req = {
        k: v
        for k, v in base_headers.items()
        if k.lower() not in ("range", "accept-encoding")
    }
    req["Accept-Encoding"] = "identity"
    if byte_range is not None:
        req["Range"] = byte_range
    return req


async def _feed_open_response_to_queue(
    resp,
    part_queue: asyncio.Queue,
    part_size: int,
    is_large: bool,
    hash_md5,
    file_size: int,
    skip_remaining: int = 0,
    produced_bytes: int = 0,
    part_idx: int = 0,
    buf: Optional[bytearray] = None,
) -> Tuple[int, int, bytearray]:
    """Read an open httpx response into part_queue without flushing trailing partial part."""
    if buf is None:
        buf = bytearray()
    async for chunk in resp.aiter_bytes(chunk_size=65536):
        if not chunk:
            continue
        if skip_remaining > 0:
            if len(chunk) <= skip_remaining:
                skip_remaining -= len(chunk)
                continue
            chunk = chunk[skip_remaining:]
            skip_remaining = 0

        remaining = file_size - produced_bytes
        if remaining <= 0:
            break
        if len(chunk) > remaining:
            chunk = chunk[:remaining]
        buf.extend(chunk)
        produced_bytes += len(chunk)
        part_idx = await _emit_buffer_parts(
            part_queue, buf, part_size, part_idx, is_large, hash_md5, flush_tail=False
        )
        if produced_bytes >= file_size:
            break
    return produced_bytes, part_idx, buf


async def _resume_single_stream_to_queue(
    http,
    url_state: dict,
    maybe_refresh_cb,
    part_queue: asyncio.Queue,
    part_size: int,
    is_large: bool,
    hash_md5,
    file_size: int,
    produced_bytes: int,
    part_idx: int,
    buf: bytearray,
    max_stream_retries: int,
) -> Tuple[int, int, bytearray]:
    """Resume remaining bytes over sequential HTTP Range/GET into part_queue."""
    import httpx

    total_retries = 0
    stalled_retries = 0
    max_stalled_retries = 4

    while produced_bytes < file_size:
        prev_produced = produced_bytes
        # Keep only un-emitted tail aligned to committed bytes = produced_bytes - len(buf).
        committed_bytes = produced_bytes - len(buf)
        buf.clear()
        produced_bytes = committed_bytes
        seen_url = url_state["url"]
        req_hdrs = _build_http_headers(
            url_state["headers"],
            byte_range=f"bytes={committed_bytes}-" if committed_bytes > 0 else None,
        )
        try:
            async with http.stream("GET", seen_url, headers=req_hdrs) as resp:
                if resp.status_code in (401, 403, 410) and await maybe_refresh_cb(seen_url):
                    continue
                if resp.status_code == 416 and committed_bytes >= file_size:
                    break
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise _RetryableStreamError(f"HTTP {resp.status_code} from stream server")
                resp.raise_for_status()

                skip_remaining = 0
                if committed_bytes > 0:
                    if resp.status_code == 206:
                        cr_start = _parse_content_range_start(resp)
                        if cr_start is not None:
                            if cr_start > committed_bytes:
                                raise IOError(
                                    f"Server returned Range starting at {cr_start}, expected <= {committed_bytes}"
                                )
                            skip_remaining = committed_bytes - cr_start
                    else:
                        skip_remaining = committed_bytes

                produced_bytes, part_idx, buf = await _feed_open_response_to_queue(
                    resp,
                    part_queue,
                    part_size,
                    is_large,
                    hash_md5,
                    file_size,
                    skip_remaining=skip_remaining,
                    produced_bytes=produced_bytes,
                    part_idx=part_idx,
                    buf=buf,
                )

            if produced_bytes >= file_size:
                break
            raise _RetryableStreamError(
                f"Stream ended prematurely: transferred {produced_bytes} of {file_size} bytes"
            )
        except httpx.HTTPStatusError:
            raise
        except (httpx.HTTPError, IOError, OSError, asyncio.TimeoutError) as err:
            if produced_bytes - len(buf) > prev_produced - len(buf):
                stalled_retries = 0
            else:
                stalled_retries += 1
            total_retries += 1
            if stalled_retries >= max_stalled_retries or total_retries > max_stream_retries:
                raise IOError(
                    f"Stream interrupted at {produced_bytes}/{file_size} bytes: {err}"
                ) from err
            delay = min(2 * stalled_retries if stalled_retries else 2, 12)
            log.warning(
                "HTTP stream interrupted for %s at %d/%d bytes (%s); resuming from byte %d in %ds...",
                url_state["url"][:80],
                produced_bytes,
                file_size,
                err,
                produced_bytes - len(buf),
                delay,
            )
            await _sleep(delay)
    return produced_bytes, part_idx, buf


async def upload_http_stream(
    client: TelegramClient,
    url: str,
    file_name: str,
    headers: Optional[dict] = None,
    progress_callback: Optional[Callable] = None,
    timeout: float = 120.0,
    connection_count: Optional[int] = None,
    http_connections: Optional[int] = None,
    pipeline_depth: int = 2,
    url_refresh_cb: Optional[
        Callable[[], Awaitable[Optional[Tuple[str, Optional[dict]]]]]
    ] = None,
    max_stream_retries: int = 12,
) -> Tuple[TypeInputFile, int]:
    """
    Streams directly from an HTTP(S) URL into Telegram MTProto parts with RAM buffering.
    Uses multi-connection HTTP Range streaming when supported, adaptive concurrency fallback,
    decoupled producer/consumer prefetching, and byte-range resume on transient drops.
    """
    import httpx

    t0 = time.monotonic()
    t = httpx.Timeout(connect=30.0, read=timeout, write=30.0, pool=60.0)
    http_conns = max(1, int(http_connections or 4))
    probe_size = 512 * 1024

    url_state = {"url": url, "headers": dict(headers or {})}
    refresh_lock = asyncio.Lock()

    async def _maybe_refresh_url(seen_url: str) -> bool:
        if not url_refresh_cb:
            return False
        async with refresh_lock:
            if url_state["url"] != seen_url:
                return True
            refreshed = await url_refresh_cb()
            if refreshed and refreshed[0]:
                url_state["url"] = refreshed[0]
                if refreshed[1] is not None:
                    url_state["headers"] = dict(refreshed[1])
                return True
        return False

    file_id = helpers.generate_random_long()
    hash_md5 = hashlib.md5()
    uploader: Optional[ParallelTransferrer] = None
    file_size = 0
    part_size = 0
    part_count = 0
    is_large = False
    mode_desc = "single-stream"
    background_task: Optional[asyncio.Task] = None

    try:
        async with httpx.AsyncClient(timeout=t, follow_redirects=True) as http:
            first_bytes = bytearray()
            use_multi_range = False

            for init_attempt in range(1, 4):
                seen_url = url_state["url"]
                use_probe_range = http_conns > 1 and init_attempt == 1
                req_hdrs = _build_http_headers(
                    url_state["headers"],
                    byte_range=f"bytes=0-{probe_size - 1}" if use_probe_range else None,
                )

                async with http.stream("GET", seen_url, headers=req_hdrs) as resp:
                    if resp.status_code in (401, 403, 410) and await _maybe_refresh_url(seen_url):
                        continue
                    if resp.status_code == 416 and use_probe_range:
                        http_conns = 1
                        continue
                    if resp.status_code in (429, 500, 502, 503, 504) and init_attempt < 3:
                        await _sleep(2 * init_attempt)
                        continue
                    resp.raise_for_status()

                    file_size = _parse_http_total_size(resp)
                    if not file_size or file_size <= 0:
                        if resp.status_code == 206:
                            http_conns = 1
                            continue
                        raise ValueError(
                            f"HTTP server for {file_name} did not supply Content-Length. "
                            "Cannot determine total MTProto parts for streaming."
                        )

                    uploader = ParallelTransferrer(client)
                    part_size, part_count, is_large = await uploader.init_upload(
                        file_id,
                        file_size,
                        connection_count=connection_count,
                        pipeline_depth=pipeline_depth,
                    )

                    queue_cap = max(
                        4,
                        min(16, (len(uploader.senders or ()) * pipeline_depth) + 4),
                    )
                    part_queue: asyncio.Queue = asyncio.Queue(maxsize=queue_cap)

                    if resp.status_code == 206 and file_size > probe_size and http_conns > 1:
                        expected_first = min(probe_size, file_size)
                        first_bytes.clear()
                        try:
                            async for chunk in resp.aiter_bytes(chunk_size=65536):
                                if not chunk:
                                    continue
                                need = expected_first - len(first_bytes)
                                if need <= 0:
                                    break
                                first_bytes.extend(chunk[:need])
                        except (httpx.HTTPError, IOError, OSError, asyncio.TimeoutError):
                            await uploader.finish_upload()
                            uploader = None
                            if init_attempt < 3:
                                await _sleep(2 * init_attempt)
                                continue
                            raise
                        if len(first_bytes) == expected_first:
                            use_multi_range = True
                            break
                        await uploader.finish_upload()
                        uploader = None
                        if init_attempt < 3:
                            await _sleep(2 * init_attempt)
                            continue
                        raise IOError(
                            f"Initial segment truncated for {file_name}: {len(first_bytes)}/{expected_first}"
                        )

                    # Single-stream path (200 OK or small file <= probe_size or http_conns == 1).
                    mode_desc = "single-stream"
                    log.info(
                        "Streaming %s (%.1f MB, %s, %d MTProto conns x%d pipeline)",
                        file_name,
                        file_size / (1024 * 1024),
                        mode_desc,
                        len(uploader.senders or ()),
                        pipeline_depth,
                    )
                    background_task = asyncio.create_task(
                        _consume_parts_to_telegram(
                            uploader, part_queue, file_size, progress_callback=progress_callback
                        )
                    )
                    produced_bytes = 0
                    part_idx = 0
                    buf = bytearray()
                    try:
                        try:
                            produced_bytes, part_idx, buf = await _feed_open_response_to_queue(
                                resp,
                                part_queue,
                                part_size,
                                is_large,
                                hash_md5,
                                file_size,
                            )
                        except (httpx.HTTPError, IOError, OSError, asyncio.TimeoutError) as err:
                            log.warning(
                                "Initial single-stream dropped for %s at %d/%d bytes (%s); resuming...",
                                file_name,
                                produced_bytes,
                                file_size,
                                err,
                            )
                        if produced_bytes < file_size:
                            produced_bytes, part_idx, buf = await _resume_single_stream_to_queue(
                                http,
                                url_state,
                                _maybe_refresh_url,
                                part_queue,
                                part_size,
                                is_large,
                                hash_md5,
                                file_size,
                                produced_bytes,
                                part_idx,
                                buf,
                                max_stream_retries,
                            )
                        await _emit_buffer_parts(
                            part_queue, buf, part_size, part_idx, is_large, hash_md5, flush_tail=True
                        )
                        await part_queue.put(None)
                    except BaseException as exc:
                        await part_queue.put((_ERR_SENTINEL, exc))
                        raise
                    await background_task
                    elapsed = max(0.001, time.monotonic() - t0)
                    log.info(
                        "Stream finished %s: %.1f MB in %.1fs (%.2f MB/s, %s)",
                        file_name,
                        file_size / (1024 * 1024),
                        elapsed,
                        (file_size / (1024 * 1024)) / elapsed,
                        mode_desc,
                    )
                    return _build_input_file(
                        file_id, part_count, file_name, file_size, is_large, hash_md5
                    )

            if not use_multi_range:
                raise IOError(f"Failed to initialize HTTP stream for {file_name}")

            # Multi-range streaming path with bounded sliding window.
            segment_size = max(part_size * 2, 1024 * 1024)
            mode_desc = f"multi-range x{http_conns}"
            log.info(
                "Streaming %s (%.1f MB, %s, %d MTProto conns x%d pipeline)",
                file_name,
                file_size / (1024 * 1024),
                mode_desc,
                len(uploader.senders or ()),
                pipeline_depth,
            )

            async def _fetch_range_segment(
                seg_start: int,
                seg_end: int,
                rate_state: dict,
                rate_cond: asyncio.Condition,
            ) -> bytes:
                expected = seg_end - seg_start + 1
                buf = bytearray()
                seg_retries = 0
                while len(buf) < expected:
                    cur_offset = seg_start + len(buf)
                    seen_url = url_state["url"]
                    req_hdrs = _build_http_headers(
                        url_state["headers"],
                        byte_range=f"bytes={cur_offset}-{seg_end}",
                    )
                    try:
                        async with rate_cond:
                            await rate_cond.wait_for(
                                lambda: rate_state["active"] < rate_state["limit"]
                            )
                            rate_state["active"] += 1
                        try:
                            async with http.stream("GET", seen_url, headers=req_hdrs) as r:
                                if r.status_code in (401, 403, 410):
                                    if await _maybe_refresh_url(seen_url):
                                        continue
                                    raise _RateLimitSignal(
                                        r.status_code,
                                        f"HTTP {r.status_code} on range {cur_offset}-{seg_end}",
                                    )
                                if r.status_code in (429, 500, 502, 503, 504):
                                    raise _RateLimitSignal(
                                        r.status_code,
                                        f"HTTP {r.status_code} on range {cur_offset}-{seg_end}",
                                    )
                                r.raise_for_status()

                                if r.status_code != 206:
                                    raise _RetryableStreamError(
                                        f"Expected 206 for range {cur_offset}-{seg_end}, got {r.status_code}"
                                    )

                                cr_start = _parse_content_range_start(r)
                                skip = 0
                                if cr_start is not None:
                                    if cr_start > cur_offset:
                                        raise IOError(
                                            f"Server returned Range starting at {cr_start}, expected <= {cur_offset}"
                                        )
                                    skip = cur_offset - cr_start

                                async for chunk in r.aiter_bytes(chunk_size=65536):
                                    if not chunk:
                                        continue
                                    if skip > 0:
                                        if len(chunk) <= skip:
                                            skip -= len(chunk)
                                            continue
                                        chunk = chunk[skip:]
                                        skip = 0
                                    need = expected - len(buf)
                                    if need <= 0:
                                        break
                                    buf.extend(chunk[:need])
                        finally:
                            async with rate_cond:
                                rate_state["active"] = max(0, rate_state["active"] - 1)
                                rate_cond.notify_all()

                        if len(buf) < expected:
                            raise _RetryableStreamError(
                                f"Range {seg_start}-{seg_end} ended at {len(buf)}/{expected} bytes"
                            )
                    except _RateLimitSignal as rl:
                        seg_retries += 1
                        if seg_retries > max_stream_retries:
                            raise IOError(str(rl)) from rl
                        if rl.status_code in (403, 429, 503) and rate_state["limit"] > 1:
                            async with rate_cond:
                                if rate_state["limit"] > 1:
                                    rate_state["limit"] = max(1, rate_state["limit"] // 2)
                                    log.warning(
                                        "HTTP %d during multi-range stream; throttling HTTP concurrency to %d",
                                        rl.status_code,
                                        rate_state["limit"],
                                    )
                                    rate_cond.notify_all()
                        await _sleep(min(2 * seg_retries, 12))
                    except (httpx.HTTPError, IOError, OSError, asyncio.TimeoutError) as err:
                        seg_retries += 1
                        if seg_retries > max_stream_retries:
                            raise IOError(
                                f"Range {seg_start}-{seg_end} failed after {seg_retries} attempts: {err}"
                            ) from err
                        await _sleep(min(2 * seg_retries, 12))
                return bytes(buf)

            async def _multi_range_producer() -> None:
                tail_buf = bytearray(first_bytes)
                first_bytes.clear()
                part_idx = 0
                in_flight: dict[int, asyncio.Task] = {}
                try:
                    part_idx = await _emit_buffer_parts(
                        part_queue,
                        tail_buf,
                        part_size,
                        part_idx,
                        is_large,
                        hash_md5,
                        flush_tail=False,
                    )
                    offset = probe_size
                    ranges: List[Tuple[int, int]] = []
                    while offset < file_size:
                        end = min(file_size - 1, offset + segment_size - 1)
                        ranges.append((offset, end))
                        offset = end + 1

                    rate_state = {"limit": http_conns, "active": 0}
                    rate_cond = asyncio.Condition()
                    window_size = http_conns + 2
                    next_launch = 0

                    def _maybe_launch() -> None:
                        nonlocal next_launch
                        cap = 1 if rate_state["limit"] <= 1 else min(window_size, rate_state["limit"] + 2)
                        while next_launch < len(ranges) and len(in_flight) < cap:
                            s_start, s_end = ranges[next_launch]
                            in_flight[next_launch] = asyncio.create_task(
                                _fetch_range_segment(s_start, s_end, rate_state, rate_cond)
                            )
                            next_launch += 1

                    _maybe_launch()
                    for seg_idx in range(len(ranges)):
                        _maybe_launch()
                        seg_data = await in_flight.pop(seg_idx)
                        _maybe_launch()
                        tail_buf.extend(seg_data)
                        is_last = seg_idx == len(ranges) - 1
                        part_idx = await _emit_buffer_parts(
                            part_queue,
                            tail_buf,
                            part_size,
                            part_idx,
                            is_large,
                            hash_md5,
                            flush_tail=is_last,
                        )
                    if tail_buf:
                        await _emit_buffer_parts(
                            part_queue,
                            tail_buf,
                            part_size,
                            part_idx,
                            is_large,
                            hash_md5,
                            flush_tail=True,
                        )
                    await part_queue.put(None)
                except BaseException as exc:
                    for t_rem in list(in_flight.values()):
                        if not t_rem.done():
                            t_rem.cancel()
                    if in_flight:
                        await asyncio.gather(*in_flight.values(), return_exceptions=True)
                    await part_queue.put((_ERR_SENTINEL, exc))

            background_task = asyncio.create_task(_multi_range_producer())
            await _consume_parts_to_telegram(
                uploader, part_queue, file_size, progress_callback=progress_callback
            )
            await background_task
    finally:
        if background_task is not None and not background_task.done():
            background_task.cancel()
            await asyncio.gather(background_task, return_exceptions=True)
        if uploader is not None:
            await uploader.finish_upload()

    elapsed = max(0.001, time.monotonic() - t0)
    log.info(
        "Stream finished %s: %.1f MB in %.1fs (%.2f MB/s, %s)",
        file_name,
        file_size / (1024 * 1024),
        elapsed,
        (file_size / (1024 * 1024)) / elapsed,
        mode_desc,
    )
    return _build_input_file(file_id, part_count, file_name, file_size, is_large, hash_md5)
