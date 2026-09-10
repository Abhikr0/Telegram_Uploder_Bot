# copied from https://github.com/tulir/mautrix-telegram/blob/master/mautrix_telegram/util/parallel_file_transfer.py
# Copyright (C) 2021 Tulir Asokan
import asyncio
import hashlib
import inspect
import logging
import math
import os
from collections import defaultdict
from typing import Optional, List, AsyncGenerator, AsyncIterable, Union, Awaitable, DefaultDict, Tuple, BinaryIO

from telethon import utils, helpers, TelegramClient
from telethon.crypto import AuthKey
from telethon.network import MTProtoSender
from telethon.tl.alltlobjects import LAYER
from telethon.tl.functions import InvokeWithLayerRequest
from telethon.tl.functions.auth import ExportAuthorizationRequest, ImportAuthorizationRequest
from telethon.tl.functions.upload import (GetFileRequest, SaveFilePartRequest,
                                          SaveBigFilePartRequest)
from telethon.tl.types import (Document, InputFileLocation, InputDocumentFileLocation,
                               InputPhotoFileLocation, InputPeerPhotoFileLocation, TypeInputFile,
                               InputFileBig, InputFile)

try:
    from mautrix.crypto.attachments import async_encrypt_attachment
except ImportError:
    async_encrypt_attachment = None

log: logging.Logger = logging.getLogger("telethon")

# Monkey-patch Telethon's Connection._recv_loop to prevent "RuntimeError: coroutine ignored GeneratorExit"
# on Python 3.12+ when connections or tasks are cancelled during GC or loop shutdown.
import telethon.network.connection.connection as telethon_conn

_orig_recv_loop = telethon_conn.Connection._recv_loop

async def _safe_recv_loop(self):
    is_gen_exit = False
    try:
        while self._connected:
            try:
                data = await self._recv()
            except asyncio.CancelledError:
                break
            except (IOError, asyncio.IncompleteReadError) as e:
                self._log.warning('Server closed the connection: %s', e)
                await self._recv_queue.put((None, e))
                await self.disconnect()
            except telethon_conn.InvalidChecksumError as e:
                self._log.warning('Server response had invalid checksum: %s', e)
                await self._recv_queue.put((None, e))
            except telethon_conn.InvalidBufferError as e:
                self._log.warning('Server response had invalid buffer: %s', e)
                await self._recv_queue.put((None, e))
            except Exception as e:
                self._log.exception('Unexpected exception in the receive loop')
                await self._recv_queue.put((None, e))
                await self.disconnect()
            else:
                await self._recv_queue.put((data, None))
    except GeneratorExit:
        is_gen_exit = True
        self._connected = False
        return
    finally:
        if not is_gen_exit:
            try:
                await self.disconnect()
            except Exception:
                pass

telethon_conn.Connection._recv_loop = _safe_recv_loop


TypeLocation = Union[Document, InputDocumentFileLocation, InputPeerPhotoFileLocation,
                     InputFileLocation, InputPhotoFileLocation]


class DownloadSender:
    client: TelegramClient
    sender: MTProtoSender
    request: GetFileRequest
    remaining: int
    stride: int

    def __init__(self, client: TelegramClient, sender: MTProtoSender, file: TypeLocation, offset: int, limit: int,
                 stride: int, count: int) -> None:
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
    client: TelegramClient
    sender: MTProtoSender
    file_id: int
    part_count: int
    is_big: bool
    current_part: int
    stride: int
    previous: Optional[asyncio.Task]
    loop: asyncio.AbstractEventLoop

    def __init__(self, client: TelegramClient, sender: MTProtoSender, file_id: int, part_count: int, big: bool,
                 index: int,
                 stride: int, loop: asyncio.AbstractEventLoop) -> None:
        self.client = client
        self.sender = sender
        self.file_id = file_id
        self.part_count = part_count
        self.is_big = big
        self.current_part = index
        self.stride = stride
        self.previous = None
        self.loop = loop

    async def next(self, data: bytes) -> None:
        if self.previous:
            await self.previous
        part_idx = self.current_part
        self.current_part += self.stride
        self.previous = self.loop.create_task(self._next(part_idx, data))

    async def _next(self, part_idx: int, data: bytes) -> None:
        if self.is_big:
            request = SaveBigFilePartRequest(self.file_id, part_idx, self.part_count, data)
        else:
            request = SaveFilePartRequest(self.file_id, part_idx, data)
        await self.client._call(self.sender, request)

    async def disconnect(self) -> None:
        if self.previous and not self.previous.done():
            try:
                await self.previous
            except Exception:
                pass
        try:
            await self.sender.disconnect()
        except Exception:
            pass


class ParallelTransferrer:
    client: TelegramClient
    loop: asyncio.AbstractEventLoop
    dc_id: int
    senders: Optional[List[Union[DownloadSender, UploadSender]]]
    auth_key: AuthKey
    upload_ticker: int

    def __init__(self, client: TelegramClient, dc_id: Optional[int] = None) -> None:
        self.client = client
        self.loop = self.client.loop
        self.dc_id = dc_id or self.client.session.dc_id
        self.auth_key = (None if dc_id and self.client.session.dc_id != dc_id
                         else self.client.session.auth_key)
        self.senders = None
        self.upload_ticker = 0

    async def _cleanup(self) -> None:
        if self.senders:
            to_disconnect = self.senders
            self.senders = None
            await asyncio.gather(*[sender.disconnect() for sender in to_disconnect if sender], return_exceptions=True)

    @staticmethod
    def _get_connection_count(file_size: int, max_count: int = 4,
                              full_size: int = 100 * 1024 * 1024) -> int:
        if file_size > full_size:
            return max_count
        return min(max_count, max(1, math.ceil((file_size / full_size) * max_count)))

    async def _init_download(self, connections: int, file: TypeLocation, part_count: int,
                             part_size: int) -> None:
        minimum, remainder = divmod(part_count, connections)

        def get_part_count() -> int:
            nonlocal remainder
            if remainder > 0:
                remainder -= 1
                return minimum + 1
            return minimum

        # The first cross-DC sender will export+import the authorization, so we always create it
        # before creating any other senders.
        self.senders = [
            await self._create_download_sender(file, 0, part_size, connections * part_size,
                                               get_part_count()),
            *await asyncio.gather(
                *[self._create_download_sender(file, i, part_size, connections * part_size,
                                               get_part_count())
                  for i in range(1, connections)])
        ]

    async def _create_download_sender(self, file: TypeLocation, index: int, part_size: int,
                                      stride: int,
                                      part_count: int) -> DownloadSender:
        return DownloadSender(self.client, await self._create_sender(), file, index * part_size, part_size,
                              stride, part_count)

    async def _init_upload(self, connections: int, file_id: int, part_count: int, big: bool
                           ) -> None:
        self.senders = [
            await self._create_upload_sender(file_id, part_count, big, 0, connections),
            *await asyncio.gather(
                *[self._create_upload_sender(file_id, part_count, big, i, connections)
                  for i in range(1, connections)])
        ]

    async def _create_upload_sender(self, file_id: int, part_count: int, big: bool, index: int,
                                    stride: int) -> UploadSender:
        return UploadSender(self.client, await self._create_sender(), file_id, part_count, big, index, stride,
                            loop=self.loop)

    async def _create_sender(self) -> MTProtoSender:
        dc = await self.client._get_dc(self.dc_id)
        sender = MTProtoSender(self.auth_key, loggers=self.client._log)
        await sender.connect(self.client._connection(dc.ip_address, dc.port, dc.id,
                                                     loggers=self.client._log,
                                                     proxy=self.client._proxy))
        if not self.auth_key:
            log.debug(f"Exporting auth to DC {self.dc_id}")
            auth = await self.client(ExportAuthorizationRequest(self.dc_id))
            self.client._init_request.query = ImportAuthorizationRequest(id=auth.id,
                                                                         bytes=auth.bytes)
            req = InvokeWithLayerRequest(LAYER, self.client._init_request)
            await sender.send(req)
            self.auth_key = sender.auth_key
        return sender

    async def init_upload(self, file_id: int, file_size: int, part_size_kb: Optional[float] = None,
                          connection_count: Optional[int] = None) -> Tuple[int, int, bool]:
        connection_count = connection_count or self._get_connection_count(file_size)
        is_large = file_size > 10 * 1024 * 1024
        if part_size_kb:
            part_size = int(part_size_kb * 1024)
        elif is_large:
            # For files > 10MB, use maximum supported 512KB parts to halve round-trips
            part_size = 512 * 1024
        else:
            part_size = utils.get_appropriated_part_size(file_size) * 1024
        part_count = (file_size + part_size - 1) // part_size
        await self._init_upload(connection_count, file_id, part_count, is_large)
        return part_size, part_count, is_large

    async def upload(self, part: bytes) -> None:
        await self.senders[self.upload_ticker].next(part)
        self.upload_ticker = (self.upload_ticker + 1) % len(self.senders)

    async def finish_upload(self) -> None:
        await self._cleanup()

    async def download(self, file: TypeLocation, file_size: int,
                       part_size_kb: Optional[float] = None,
                       connection_count: Optional[int] = None) -> AsyncGenerator[bytes, None]:
        connection_count = connection_count or self._get_connection_count(file_size)
        part_size = (part_size_kb or utils.get_appropriated_part_size(file_size)) * 1024
        part_count = math.ceil(file_size / part_size)
        log.debug("Starting parallel download: "
                  f"{connection_count} {part_size} {part_count} {file!s}")
        await self._init_download(connection_count, file, part_count, part_size)

        part = 0
        while part < part_count:
            tasks = []
            for sender in self.senders:
                tasks.append(self.loop.create_task(sender.next()))
            for task in tasks:
                data = await task
                if not data:
                    break
                yield data
                part += 1
                log.debug(f"Part {part} downloaded")

        log.debug("Parallel download finished, cleaning up connections")
        await self._cleanup()


parallel_transfer_locks: DefaultDict[int, asyncio.Lock] = defaultdict(lambda: asyncio.Lock())


def stream_file(file_to_stream: BinaryIO, chunk_size=512 * 1024):
    while True:
        data_read = file_to_stream.read(chunk_size)
        if not data_read:
            break
        yield data_read


async def _internal_transfer_to_telegram(client: TelegramClient,
                                         response: BinaryIO,
                                         progress_callback: callable
                                         ) -> Tuple[TypeInputFile, int]:
    file_id = helpers.generate_random_long()
    file_size = os.path.getsize(response.name)

    hash_md5 = hashlib.md5()
    uploader = ParallelTransferrer(client)
    part_size, part_count, is_large = await uploader.init_upload(file_id, file_size)

    try:
        try:
            response.seek(0)
        except Exception:
            pass

        pos = 0
        while True:
            data = response.read(part_size)
            if not data:
                break
            if not is_large:
                hash_md5.update(data)
            await uploader.upload(data)
            pos += len(data)
            if progress_callback:
                r = progress_callback(pos, file_size)
                if inspect.isawaitable(r):
                    await r
    finally:
        await uploader.finish_upload()

    file_name = os.path.basename(response.name)
    if is_large:
        return InputFileBig(file_id, part_count, file_name), file_size
    else:
        return InputFile(file_id, part_count, file_name, hash_md5.hexdigest()), file_size


async def download_file(client: TelegramClient,
                        location: TypeLocation,
                        out: BinaryIO,
                        progress_callback: callable = None
                        ) -> BinaryIO:
    size = location.size
    dc_id, location = utils.get_input_location(location)
    # We lock the transfers because telegram has connection count limits
    downloader = ParallelTransferrer(client, dc_id)
    try:
        downloaded = downloader.download(location, size)
        async for x in downloaded:
            out.write(x)
            if progress_callback:
                r = progress_callback(out.tell(), size)
                if inspect.isawaitable(r):
                    await r
    finally:
        await downloader._cleanup()

    return out


async def upload_file(client: TelegramClient,
                      file: BinaryIO,
                      progress_callback: callable = None,

                      ) -> TypeInputFile:
    res = (await _internal_transfer_to_telegram(client, file, progress_callback))[0]
    return res


async def upload_stream(client: TelegramClient,
                        stream: AsyncIterable[bytes],
                        file_size: int,
                        file_name: str,
                        progress_callback: callable = None) -> Tuple[TypeInputFile, int]:
    """
    Upload an async stream of bytes directly to Telegram via ParallelTransferrer.
    Zero disk usage!
    """
    file_id = helpers.generate_random_long()
    hash_md5 = hashlib.md5()
    uploader = ParallelTransferrer(client)
    part_size, part_count, is_large = await uploader.init_upload(file_id, file_size)

    try:
        pos = 0
        buffer = bytearray()
        async for chunk in stream:
            if not chunk:
                continue
            buffer.extend(chunk)
            while len(buffer) >= part_size:
                part = bytes(buffer[:part_size])
                del buffer[:part_size]
                if not is_large:
                    hash_md5.update(part)
                await uploader.upload(part)
                pos += len(part)
                if progress_callback:
                    r = progress_callback(pos, file_size)
                    if inspect.isawaitable(r):
                        await r

        if buffer:
            part = bytes(buffer)
            buffer.clear()
            if not is_large:
                hash_md5.update(part)
            await uploader.upload(part)
            pos += len(part)
            if progress_callback:
                r = progress_callback(pos, file_size)
                if inspect.isawaitable(r):
                    await r
    finally:
        await uploader.finish_upload()

    if is_large:
        return InputFileBig(file_id, part_count, file_name), file_size
    else:
        return InputFile(file_id, part_count, file_name, hash_md5.hexdigest()), file_size



async def upload_http_stream(client: TelegramClient,
                             url: str,
                             file_name: str,
                             headers: Optional[dict] = None,
                             progress_callback: callable = None,
                             timeout: float = 60.0) -> Tuple[TypeInputFile, int]:
    """
    Streams directly from an HTTP(S) URL into Telegram MTProto parts with RAM buffering.
    Zero disk usage!
    """
    import httpx
    t = httpx.Timeout(connect=20.0, read=timeout, write=20.0, pool=30.0)
    async with httpx.AsyncClient(headers=headers, timeout=t, follow_redirects=True) as http:
        async with http.stream("GET", url) as resp:
            resp.raise_for_status()

            file_size = 0
            if "Content-Length" in resp.headers:
                try:
                    file_size = int(resp.headers["Content-Length"])
                except ValueError:
                    file_size = 0
            if not file_size and "Content-Range" in resp.headers:
                content_range = resp.headers["Content-Range"]
                if "/" in content_range:
                    try:
                        file_size = int(content_range.split("/")[-1])
                    except ValueError:
                        file_size = 0

            if not file_size or file_size <= 0:
                raise ValueError(f"HTTP server for {file_name} did not supply Content-Length. Cannot determine total MTProto parts for streaming.")

            return await upload_stream(
                client=client,
                stream=resp.aiter_bytes(),
                file_size=file_size,
                file_name=file_name,
                progress_callback=progress_callback
            )

