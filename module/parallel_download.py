"""Download one Telegram media file with multiple ranged workers."""

import asyncio
import os
import shutil
from dataclasses import dataclass
from typing import List, Optional

import pyrogram
from loguru import logger
from pyrogram.file_id import FileId

from module.app import TaskNode
from module.download_stat import update_download_status
from module.language import _t
from utils.format import format_byte

CHUNK_SIZE = 1024 * 1024


class ParallelDownloadError(Exception):
    """Base error for a ranged download that can be shown to the user."""

    code = "PARALLEL_DOWNLOAD_ERROR"

    def user_message(self, global_worker_limit: int) -> str:
        """Return a concise user-facing diagnostic."""
        return f"[{self.code}] {self}"


class IncompletePartError(ParallelDownloadError):
    """A worker stopped before its assigned range was fully downloaded."""

    code = "INCOMPLETE_PART"

    def __init__(
        self,
        worker_id: int,
        actual_size: int,
        expected_size: int,
        worker_count: int,
    ):
        super().__init__(worker_id, actual_size, expected_size, worker_count)
        self.worker_id = worker_id
        self.actual_size = actual_size
        self.expected_size = expected_size
        self.worker_count = worker_count

    def __str__(self) -> str:
        return (
            f"worker={self.worker_id}, actual={self.actual_size}, "
            f"expected={self.expected_size}, workers={self.worker_count}"
        )

    def user_message(self, global_worker_limit: int) -> str:
        return (
            f"[{self.code}] {_t('Shard download ended early')}\n"
            f"{_t('Shard')}: {self.worker_id}\n"
            f"{_t('actual')}: {format_byte(self.actual_size)} / "
            f"{_t('Expected')}: {format_byte(self.expected_size)}\n"
            f"{_t('Worker count')}: {self.worker_count} / "
            f"{_t('Global worker limit')}: {global_worker_limit}\n"
            f"{_t('Possible cause')}: "
            f"{_t('Telegram connection was reset or shard concurrency may be too high')}"
        )


@dataclass(frozen=True)
class DownloadPart:
    """A contiguous range of Telegram file chunks assigned to one worker."""

    worker_id: int
    offset: int
    limit: int
    size: int


def build_download_parts(file_size: int, worker_count: int) -> List[DownloadPart]:
    """Split a file into balanced, contiguous 1 MiB chunk ranges."""
    total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE
    actual_worker_count = min(max(worker_count, 1), total_chunks)
    if actual_worker_count == 0:
        return []

    chunks_per_worker, extra_chunks = divmod(total_chunks, actual_worker_count)
    parts = []
    offset = 0
    for worker_index in range(actual_worker_count):
        limit = chunks_per_worker + (1 if worker_index < extra_chunks else 0)
        start_byte = offset * CHUNK_SIZE
        end_byte = min((offset + limit) * CHUNK_SIZE, file_size)
        parts.append(
            DownloadPart(
                worker_id=worker_index + 1,
                offset=offset,
                limit=limit,
                size=end_byte - start_byte,
            )
        )
        offset += limit

    return parts


class ParallelDownloadProgress:
    """Combine worker progress while retaining each worker's real byte count."""

    def __init__(
        self,
        parts: List[DownloadPart],
        total_size: int,
        message_id: int,
        file_name: str,
        start_time: float,
        node: TaskNode,
        client: pyrogram.Client,
    ):
        self.parts = {part.worker_id: part for part in parts}
        self.worker_downloaded = {part.worker_id: 0 for part in parts}
        self.total_size = total_size
        self.message_id = message_id
        self.file_name = file_name
        self.start_time = start_time
        self.node = node
        self.client = client

    async def update(self, current: int, _total: int, worker_id: int):
        """Handle one ranged Pyrogram progress callback."""
        part = self.parts[worker_id]
        start_byte = part.offset * CHUNK_SIZE
        worker_down_byte = min(max(current - start_byte, 0), part.size)
        self.worker_downloaded[worker_id] = worker_down_byte

        await update_download_status(
            sum(self.worker_downloaded.values()),
            self.total_size,
            self.message_id,
            self.file_name,
            self.start_time,
            self.node,
            self.client,
            worker_id=worker_id,
            worker_down_byte=worker_down_byte,
            worker_total=part.size,
            worker_count=len(self.parts),
        )


async def _download_part(
    client: pyrogram.Client,
    file_id: FileId,
    file_size: int,
    part: DownloadPart,
    part_path: str,
    progress: ParallelDownloadProgress,
    worker_semaphore: asyncio.Semaphore,
):
    """Download one part and validate its byte count."""
    async with worker_semaphore:
        with open(part_path, "wb") as part_file:
            async for chunk in client.get_file(
                file_id,
                file_size,
                part.limit,
                part.offset,
                progress.update,
                (part.worker_id,),
            ):
                part_file.write(chunk)

    actual_size = os.path.getsize(part_path)
    if actual_size != part.size:
        raise IncompletePartError(
            part.worker_id,
            actual_size,
            part.size,
            len(progress.parts),
        )


# pylint: disable = R0912, R0914
async def download_media_in_parts(
    client: pyrogram.Client,
    media,
    file_name: str,
    file_size: int,
    worker_count: int,
    message_id: int,
    ui_file_name: str,
    start_time: float,
    node: TaskNode,
    worker_semaphore: asyncio.Semaphore = None,
) -> Optional[str]:
    """Download media concurrently and merge its ordered part files."""
    parts = build_download_parts(file_size, worker_count)
    if worker_semaphore is None:
        worker_semaphore = asyncio.Semaphore(max(len(parts), 1))
    if len(parts) <= 1:
        download_result = await client.download_media(
            media,
            file_name=file_name,
            progress=update_download_status,
            progress_args=(message_id, ui_file_name, start_time, node, client),
        )
        return download_result if isinstance(download_result, str) else None

    target_path = os.path.abspath(file_name)
    temp_path = f"{target_path}.temp"
    part_paths = [f"{temp_path}.part-{part.worker_id}" for part in parts]
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    progress = ParallelDownloadProgress(
        parts,
        file_size,
        message_id,
        ui_file_name,
        start_time,
        node,
        client,
    )
    tasks = [
        asyncio.create_task(
            _download_part(
                client,
                FileId.decode(media.file_id),
                file_size,
                part,
                path,
                progress,
                worker_semaphore,
            )
        )
        for part, path in zip(parts, part_paths)
    ]
    completed = False

    try:
        await asyncio.gather(*tasks)
        with open(temp_path, "wb") as merged_file:
            for part_path in part_paths:
                with open(part_path, "rb") as part_file:
                    shutil.copyfileobj(part_file, merged_file)
        os.replace(temp_path, target_path)
        completed = True
        return target_path
    except asyncio.CancelledError:
        raise
    except pyrogram.StopTransmission:
        return None
    except pyrogram.errors.exceptions.bad_request_400.BadRequest:
        raise
    except pyrogram.errors.exceptions.flood_420.FloodWait:
        raise
    except ParallelDownloadError:
        raise
    except Exception as error:  # pylint: disable = W0703
        logger.exception(f"parallel media download failed: {error}")
        raise ParallelDownloadError(f"{type(error).__name__}: {error}") from error
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for part_path in part_paths:
            if os.path.exists(part_path):
                os.remove(part_path)
        if not completed and os.path.exists(temp_path):
            os.remove(temp_path)
