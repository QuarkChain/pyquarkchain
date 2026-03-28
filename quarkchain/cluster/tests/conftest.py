import asyncio

import pytest

from quarkchain.protocol import AbstractConnection


@pytest.fixture(autouse=True)
def ensure_event_loop():
    """Ensure an event loop exists after each test.
    IsolatedAsyncioTestCase tears down its loop and sets the current loop to None,
    which breaks subsequent sync tests that call asyncio.get_event_loop()."""
    yield
    AbstractConnection.aborted_rpc_count = 0
    try:
        old_loop = asyncio.get_event_loop()
        if old_loop.is_closed():
            old_loop.close()
            asyncio.set_event_loop(asyncio.new_event_loop())
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
