import asyncio

import pytest

from quarkchain.protocol import AbstractConnection


@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Reset shared state and restore event loop after each test.

    IsolatedAsyncioTestCase closes its event loop when done.  Subsequent
    sync tests (or their imports) may call asyncio.get_event_loop(), which
    fails in Python 3.12+ when no loop is set.  Re-create one here.
    """
    yield
    AbstractConnection.aborted_rpc_count = 0
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            asyncio.set_event_loop(asyncio.new_event_loop())
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
