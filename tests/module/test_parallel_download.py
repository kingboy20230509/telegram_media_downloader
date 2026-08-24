"""Tests for ordered single-file parallel downloads."""

import asyncio
import unittest
from unittest import mock

import pyrogram

from module.pyrogram_extension import ParallelDownloadClient


class ParallelDownloadClientTestCase(unittest.IsolatedAsyncioTestCase):
    """Test per-file range scheduling without Telegram network access."""

    async def test_get_file_downloads_parts_in_parallel_and_yields_in_order(self):
        active = 0
        peak_active = 0
        progress_values = []
        progress_part_speeds = []

        def progress(current, _total, part_speeds=()):
            progress_values.append(current)
            progress_part_speeds.append(part_speeds)

        async def fake_get_file(
            _client,
            _file_id,
            file_size=0,
            limit=0,
            offset=0,
            progress=None,
            progress_args=(),
        ):
            del file_size, limit, progress, progress_args
            nonlocal active, peak_active
            active += 1
            peak_active = max(peak_active, active)
            await asyncio.sleep(0.01 * (3 - offset))
            active -= 1
            yield bytes([offset]) * 1024

        client = ParallelDownloadClient(
            "parallel_download_test",
            api_id=1,
            api_hash="api_hash",
            in_memory=True,
            max_concurrent_transmissions=4,
            single_file_download_workers=3,
        )

        with mock.patch.object(pyrogram.Client, "get_file", new=fake_get_file):
            chunks = [
                chunk
                async for chunk in client.get_file(
                    object(),
                    file_size=3 * client.DOWNLOAD_CHUNK_SIZE,
                    progress=progress,
                )
            ]

        self.assertGreaterEqual(peak_active, 2)
        self.assertEqual([chunk[0] for chunk in chunks], [0, 1, 2])
        self.assertEqual(
            progress_values,
            [
                1024,
                client.DOWNLOAD_CHUNK_SIZE + 1024,
                2 * client.DOWNLOAD_CHUNK_SIZE + 1024,
            ],
        )
        self.assertEqual(len(progress_part_speeds[-1]), 3)
        self.assertTrue(all(speed > 0 for speed in progress_part_speeds[-1]))

    async def test_get_file_falls_back_to_pyrogram_for_one_worker(self):
        calls = []

        async def fake_get_file(
            _client,
            _file_id,
            file_size=0,
            limit=0,
            offset=0,
            progress=None,
            progress_args=(),
        ):
            calls.append((file_size, limit, offset, progress, progress_args))
            yield b"sequential"

        client = ParallelDownloadClient(
            "sequential_download_test",
            api_id=1,
            api_hash="api_hash",
            in_memory=True,
            single_file_download_workers=1,
        )

        with mock.patch.object(pyrogram.Client, "get_file", new=fake_get_file):
            chunks = [
                chunk
                async for chunk in client.get_file(
                    object(),
                    file_size=2 * client.DOWNLOAD_CHUNK_SIZE,
                )
            ]

        self.assertEqual(chunks, [b"sequential"])
        self.assertEqual(calls[0][:3], (2 * client.DOWNLOAD_CHUNK_SIZE, 0, 0))
