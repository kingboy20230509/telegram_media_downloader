"""Tests for the download bot."""

import unittest
from types import SimpleNamespace
from unittest import mock
from unittest.mock import AsyncMock

from module.app import Application, TaskNode
from module.bot import DownloadBot, is_direct_download_candidate
from module.pyrogram_extension import _report_bot_status


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

    async def test_new_download_recreates_shared_progress_at_bottom(self):
        bot = DownloadBot()
        bot.bot = SimpleNamespace(
            send_message=AsyncMock(
                side_effect=[SimpleNamespace(id=101), SimpleNamespace(id=102)]
            ),
            delete_messages=AsyncMock(),
        )

        first_node = await bot.get_direct_download_node(1, 99)
        first_node.is_running = True
        second_node = await bot.get_direct_download_node(1, 99)

        self.assertIs(first_node, second_node)
        self.assertEqual(second_node.reply_message_id, 102)
        self.assertFalse(second_node.is_running)
        bot.bot.delete_messages.assert_awaited_once_with(99, 101)
        self.assertEqual(len(bot.task_node), 1)

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
