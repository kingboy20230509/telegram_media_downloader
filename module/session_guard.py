"""Coalesce concurrent Pyrogram session restart requests."""

import asyncio
from dataclasses import dataclass, field
from typing import Awaitable, Callable
from weakref import WeakKeyDictionary

import pyrogram
from loguru import logger


@dataclass
class _RestartState:
    """Track restart synchronization state for one Pyrogram session."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    generation: int = 0


_restart_states: "WeakKeyDictionary[object, _RestartState]" = WeakKeyDictionary()


async def _restart_once(
    session,
    restart: Callable[[], Awaitable[None]],
):
    """Run one restart for all callers observing the same session generation."""
    state = _restart_states.get(session)
    if state is None:
        state = _RestartState()
        _restart_states[session] = state

    observed_generation = state.generation
    async with state.lock:
        if observed_generation != state.generation:
            return

        await restart()
        state.generation += 1


def install_session_restart_guard():
    """Install an idempotent restart guard on Pyrogram media sessions."""
    session_class = pyrogram.session.Session
    original_restart = session_class.restart
    if getattr(original_restart, "_tdl_restart_guard", False):
        return

    async def guarded_restart(session):
        try:
            await _restart_once(session, lambda: original_restart(session))
        except Exception as error:
            logger.warning(f"Pyrogram session restart failed: {error}")
            raise

    setattr(guarded_restart, "_tdl_restart_guard", True)
    setattr(session_class, "restart", guarded_restart)
