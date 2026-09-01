"""Tests for coalesced Pyrogram session restarts."""

import asyncio
import unittest

from module.session_guard import _restart_once


class FakeSession:
    """Weak-referenceable stand-in for a Pyrogram Session."""


class SessionRestartGuardTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_restart_requests_restart_once(self):
        session = FakeSession()
        restart_count = 0
        restart_started = asyncio.Event()
        allow_restart = asyncio.Event()

        async def restart():
            nonlocal restart_count
            restart_count += 1
            restart_started.set()
            await allow_restart.wait()

        tasks = [asyncio.create_task(_restart_once(session, restart)) for _ in range(6)]
        await restart_started.wait()
        allow_restart.set()
        await asyncio.gather(*tasks)

        self.assertEqual(1, restart_count)

    async def test_failed_restart_can_be_retried(self):
        session = FakeSession()
        restart_count = 0

        async def restart():
            nonlocal restart_count
            restart_count += 1
            if restart_count == 1:
                raise ConnectionError("connection reset")

        with self.assertRaises(ConnectionError):
            await _restart_once(session, restart)

        await _restart_once(session, restart)
        self.assertEqual(2, restart_count)


if __name__ == "__main__":
    unittest.main()
