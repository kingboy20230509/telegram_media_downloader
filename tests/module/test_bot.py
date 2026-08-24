"""Tests for the download bot."""

import unittest
from unittest import mock

from module.app import Application
from module.bot import DownloadBot


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
