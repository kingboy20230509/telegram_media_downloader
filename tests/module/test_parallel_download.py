"""Tests for ranged single-file downloads."""

import asyncio
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from module.parallel_download import (
    CHUNK_SIZE,
    IncompletePartError,
    build_download_parts,
    download_media_in_parts,
)


class FakeClient:
    """Minimal client that serves byte ranges from memory."""

    def __init__(self, data: bytes):
        self.data = data
        self.ranges = []
        self.active_downloads = 0
        self.max_active_downloads = 0

    async def get_file(
        self, _file_id, file_size, limit, offset, progress, progress_args
    ):
        self.ranges.append((offset, limit))
        self.active_downloads += 1
        self.max_active_downloads = max(
            self.max_active_downloads, self.active_downloads
        )
        try:
            start = offset * CHUNK_SIZE
            end = min((offset + limit) * CHUNK_SIZE, file_size)
            for chunk_start in range(start, end, CHUNK_SIZE):
                chunk_end = min(chunk_start + CHUNK_SIZE, end)
                yield self.data[chunk_start:chunk_end]
                await progress(chunk_end, file_size, *progress_args)
                await asyncio.sleep(0)
        finally:
            self.active_downloads -= 1


class ParallelDownloadTestCase(unittest.IsolatedAsyncioTestCase):
    def test_build_download_parts_balances_contiguous_ranges(self):
        parts = build_download_parts(10 * CHUNK_SIZE + 123, 6)

        self.assertEqual(6, len(parts))
        self.assertEqual([2, 2, 2, 2, 2, 1], [part.limit for part in parts])
        self.assertEqual(list(range(1, 7)), [part.worker_id for part in parts])
        self.assertEqual(10 * CHUNK_SIZE + 123, sum(part.size for part in parts))

    @mock.patch("module.parallel_download.update_download_status")
    @mock.patch("module.parallel_download.FileId.decode", return_value=object())
    async def test_download_media_in_parts_merges_ranges_in_order(
        self, _mock_decode, mock_update_status
    ):
        data = bytes(range(251)) * ((8 * CHUNK_SIZE // 251) + 1)
        data = data[: 8 * CHUNK_SIZE]
        client = FakeClient(data)
        media = SimpleNamespace(file_id="file-id")
        node = SimpleNamespace()

        with tempfile.TemporaryDirectory() as directory:
            target = os.path.join(directory, "video.mp4")
            result = await download_media_in_parts(
                client,
                media,
                target,
                len(data),
                6,
                8224,
                "video.mp4",
                1.0,
                node,
            )

            self.assertEqual(target, result)
            with open(target, "rb") as downloaded_file:
                self.assertEqual(data, downloaded_file.read())

        self.assertEqual(6, len(client.ranges))
        worker_ids = {
            call.kwargs["worker_id"] for call in mock_update_status.await_args_list
        }
        self.assertEqual(set(range(1, 7)), worker_ids)

    @mock.patch("module.parallel_download.update_download_status")
    @mock.patch("module.parallel_download.FileId.decode", return_value=object())
    async def test_download_media_in_parts_honors_global_worker_limit(
        self, _mock_decode, _mock_update_status
    ):
        data = bytes(range(251)) * ((6 * CHUNK_SIZE // 251) + 1)
        data = data[: 6 * CHUNK_SIZE]
        client = FakeClient(data)

        with tempfile.TemporaryDirectory() as directory:
            await download_media_in_parts(
                client,
                SimpleNamespace(file_id="file-id"),
                os.path.join(directory, "video.mp4"),
                len(data),
                6,
                8224,
                "video.mp4",
                1.0,
                SimpleNamespace(),
                asyncio.Semaphore(2),
            )

        self.assertEqual(2, client.max_active_downloads)

    @mock.patch("module.parallel_download.update_download_status")
    @mock.patch("module.parallel_download.FileId.decode", return_value=object())
    async def test_incomplete_part_exposes_worker_diagnostics(
        self, _mock_decode, _mock_update_status
    ):
        client = FakeClient(b"")

        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(IncompletePartError) as error_context:
                await download_media_in_parts(
                    client,
                    SimpleNamespace(file_id="file-id"),
                    os.path.join(directory, "video.mp4"),
                    6 * CHUNK_SIZE,
                    6,
                    8224,
                    "video.mp4",
                    1.0,
                    SimpleNamespace(),
                    asyncio.Semaphore(2),
                )

        error = error_context.exception
        self.assertEqual("INCOMPLETE_PART", error.code)
        self.assertEqual(1, error.worker_id)
        self.assertEqual(6, error.worker_count)
        self.assertIn("Global worker limit", error.user_message(2))


if __name__ == "__main__":
    unittest.main()
