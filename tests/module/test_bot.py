"""Tests for the download bot."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest import mock
from unittest.mock import AsyncMock

from module.app import Application, DownloadStatus, TaskNode
from module.bot import (
    DownloadBot,
    direct_download,
    download_forward_media,
    is_direct_download_candidate,
)
from module.pyrogram_extension import _report_bot_status, report_bot_download_status


class DownloadBotTestCase(unittest.TestCase):
    """Test bot client configuration."""

    @mock.patch("module.bot.set_max_concurrent_transmissions")
    @mock.patch("module.bot.pyrogram.Client")
    def test_create_client_uses_application_transmission_limit(
        self, mock_client, mock_set_transmission_limit
    ):
        app = Application("", "")
        app.application_name = "media_downloader"
        app.api_hash = "api_hash"
        app.api_id = "api_id"
        app.bot_token = "bot_token"
        app.session_file_path = "sessions"
        app.proxy = {
            "scheme": "socks5",
            "hostname": "127.0.0.1",
            "port": 1080,
        }
        app.max_concurrent_transmissions = 25

        bot = DownloadBot()
        bot._create_client(app)

        mock_client.assert_called_once_with(
            "media_downloader_bot",
            api_hash="api_hash",
            api_id="api_id",
            bot_token="bot_token",
            workdir="sessions",
            proxy=app.proxy,
        )
        mock_set_transmission_limit.assert_called_once_with(
            mock_client.return_value, 25
        )
        self.assertIs(bot.bot, mock_client.return_value)

    def test_direct_download_candidate_rejects_disabled_media_type(self):
        app = SimpleNamespace(media_types=["video"], file_formats={"video": ["all"]})
        message = SimpleNamespace(
            media=SimpleNamespace(value="photo"),
            photo=SimpleNamespace(),
        )

        self.assertFalse(is_direct_download_candidate(app, message))


class DirectDownloadProgressTestCase(unittest.IsolatedAsyncioTestCase):
    """Test aggregated progress messages for directly forwarded media."""

    @mock.patch("module.bot._t", return_value="跳过")
    @mock.patch("module.bot._bot")
    async def test_disabled_forward_replies_with_skipped(
        self, mock_bot, _mock_translate
    ):
        mock_bot.app = SimpleNamespace(
            media_types=["video"], file_formats={"video": ["all"]}
        )
        client = SimpleNamespace(send_message=AsyncMock())
        message = SimpleNamespace(
            id=8224,
            from_user=SimpleNamespace(id=99),
            media=SimpleNamespace(value="photo"),
            photo=SimpleNamespace(),
        )

        await download_forward_media(client, message)

        client.send_message.assert_awaited_once_with(
            99,
            "跳过",
            reply_to_message_id=8224,
        )
        mock_bot.add_download_task.assert_not_called()

    async def test_new_download_recreates_shared_progress_at_bottom(self):
        bot = DownloadBot()
        events = []

        async def send_message(*_args, **_kwargs):
            events.append("send")
            return SimpleNamespace(id=100 + events.count("send"))

        async def delete_messages(*_args, **_kwargs):
            events.append("delete")

        bot.bot = SimpleNamespace(
            send_message=AsyncMock(side_effect=send_message),
            delete_messages=AsyncMock(side_effect=delete_messages),
        )

        first_node = await bot.get_direct_download_node(1, 99)
        first_node.is_running = True
        second_node = await bot.get_direct_download_node(1, 99)

        self.assertIs(first_node, second_node)
        self.assertEqual(second_node.reply_message_id, 102)
        self.assertFalse(second_node.is_running)
        bot.bot.delete_messages.assert_awaited_once_with(99, 101)
        self.assertEqual(["send", "send", "delete"], events)
        self.assertEqual(len(bot.task_node), 1)

    @mock.patch("module.pyrogram_extension.get_download_result", return_value={})
    async def test_stale_progress_update_does_not_edit_deleted_message(
        self, _mock_download_result
    ):
        client = SimpleNamespace(edit_message_text=AsyncMock())
        node = TaskNode(
            chat_id=99,
            from_user_id=99,
            reply_message_id=101,
            bot=object(),
            task_id=1,
        )
        node.is_direct_download = True
        await node.progress_message_lock.acquire()

        report_task = asyncio.create_task(
            _report_bot_status(client, node, immediate_reply=True)
        )
        await asyncio.sleep(0)
        node.reply_message_id = 102
        node.progress_message_lock.release()
        await report_task

        client.edit_message_text.assert_not_awaited()

    @mock.patch("module.bot._bot")
    async def test_direct_download_tracks_the_corresponding_reply(
        self, mock_global_bot
    ):
        bot = DownloadBot()
        bot.bot = SimpleNamespace(
            send_message=AsyncMock(return_value=SimpleNamespace(id=201))
        )
        node = TaskNode(chat_id=99, from_user_id=99, task_id=1)
        bot.get_direct_download_node = AsyncMock(return_value=node)
        mock_global_bot.add_download_task = AsyncMock()
        message = SimpleNamespace(id=8224, from_user=SimpleNamespace(id=99))

        await direct_download(bot, 99, message, message, client=object())

        self.assertEqual({8224: 201}, node.direct_download_reply_ids)

    @mock.patch("module.pyrogram_extension._t", return_value="完成")
    async def test_successful_download_replaces_direct_reply_with_completed(
        self, _mock_translate
    ):
        client = SimpleNamespace(edit_message_text=AsyncMock())
        node = TaskNode(chat_id=99, from_user_id=99, task_id=1)
        node.direct_download_reply_ids[8224] = 201

        await report_bot_download_status(
            client,
            node,
            DownloadStatus.SuccessDownload,
            message_id=8224,
        )

        client.edit_message_text.assert_awaited_once_with(99, 201, "完成")
        self.assertNotIn(8224, node.direct_download_reply_ids)

    @mock.patch("module.pyrogram_extension._t", side_effect=lambda text: text)
    async def test_failed_download_replaces_direct_reply_with_diagnostic(
        self, _mock_translate
    ):
        client = SimpleNamespace(edit_message_text=AsyncMock())
        node = TaskNode(chat_id=99, from_user_id=99, task_id=1)
        node.direct_download_reply_ids[8224] = 201
        node.download_error_messages[8224] = "[INCOMPLETE_PART] worker=3, workers=6"

        await report_bot_download_status(
            client,
            node,
            DownloadStatus.FailedDownload,
            message_id=8224,
        )

        client.edit_message_text.assert_awaited_once_with(
            99,
            201,
            "Failed\n[INCOMPLETE_PART] worker=3, workers=6",
        )

    async def test_finished_download_removes_progress_and_shared_node(self):
        bot = DownloadBot()
        bot.bot = SimpleNamespace(delete_messages=AsyncMock())
        node = TaskNode(
            chat_id=99,
            from_user_id=99,
            reply_message_id=101,
            task_id=1,
        )
        node.total_task = 1
        node.total_download_task = 1
        node.is_running = True
        bot.direct_download_nodes[99] = node

        await bot.remove_finished_direct_download_node(node)

        bot.bot.delete_messages.assert_awaited_once_with(99, 101)
        self.assertNotIn(99, bot.direct_download_nodes)

    @mock.patch("module.pyrogram_extension.get_download_result")
    async def test_progress_shows_total_waiting_and_each_active_speed(
        self, mock_download_result
    ):
        client = SimpleNamespace(edit_message_text=AsyncMock())
        node = TaskNode(
            chat_id=99,
            from_user_id=99,
            reply_message_id=101,
            bot=object(),
            task_id=1,
        )
        node.is_direct_download = True
        node.total_task = 3
        node.is_running = True
        mock_download_result.return_value = {
            99: {
                8224: {
                    "task_id": 1,
                    "down_byte": 120,
                    "total_size": 1000,
                    "file_name": "one.mp4",
                    "download_speed": 5 * 1024 * 1024,
                    "worker_count": 6,
                    "workers": {
                        worker_id: {"download_speed": worker_id * 1024 * 1024}
                        for worker_id in range(1, 7)
                    },
                },
                8222: {
                    "task_id": 1,
                    "down_byte": 100,
                    "total_size": 1000,
                    "file_name": "two.mp4",
                    "download_speed": 4 * 1024 * 1024,
                },
            }
        }

        await _report_bot_status(client, node, immediate_reply=True)

        progress_message = client.edit_message_text.await_args.args[2]
        self.assertNotIn("task id", progress_message)
        self.assertIn("Total: 3", progress_message)
        self.assertIn("Waiting: 1", progress_message)
        self.assertIn("Active downloads: 2", progress_message)
        self.assertIn("Total speed: 5.0MB/s", progress_message)
        self.assertIn("Worker count: 6", progress_message)
        self.assertIn("Worker speeds:", progress_message)
        self.assertIn("Worker 1: 1.0MB/s", progress_message)
        self.assertIn("Worker 6: 6.0MB/s", progress_message)
        self.assertIn("5.0MB/s", progress_message)
        self.assertIn("4.0MB/s", progress_message)

    def test_direct_download_candidate_accepts_allowed_media_format(self):
        app = SimpleNamespace(media_types=["video"], file_formats={"video": ["mp4"]})
        message = SimpleNamespace(
            media=SimpleNamespace(value="video"),
            video=SimpleNamespace(mime_type="video/mp4"),
        )

        self.assertTrue(is_direct_download_candidate(app, message))

    def test_direct_download_candidate_rejects_disabled_media_format(self):
        app = SimpleNamespace(
            media_types=["document"], file_formats={"document": ["pdf"]}
        )
        message = SimpleNamespace(
            media=SimpleNamespace(value="document"),
            document=SimpleNamespace(mime_type="application/zip"),
        )

        self.assertFalse(is_direct_download_candidate(app, message))
