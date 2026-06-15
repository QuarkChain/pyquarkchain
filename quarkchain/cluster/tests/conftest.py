import asyncio

import pytest

from quarkchain.protocol import AbstractConnection

# Track the gap-filling loop we create so we can close it when it is replaced
# by an IsolatedAsyncioTestCase framework loop.  Without this, each replaced
# loop is only closed by CPython's reference-counting GC, which is not
# guaranteed and leaks one fd per async-test / sync-test boundary.
_loop_created_by_fixture = None


@pytest.fixture(autouse=True)
def ensure_event_loop():
    """Ensure an event loop exists after each test.

    IsolatedAsyncioTestCase tears down its loop and sets the current loop to None,
    which breaks subsequent sync tests that call asyncio.get_event_loop().

    This fixture:
    1. Allows tests to run normally with proper cleanup
    2. Resets the event loop state if it's closed
    3. Closes any previously-created gap loop to avoid fd leaks
    4. Clears RPC counters without aggressively cancelling test framework tasks
    """
    global _loop_created_by_fixture
    yield
    AbstractConnection.aborted_rpc_count = 0

    # If our previously-created gap loop has been replaced (e.g. by
    # IsolatedAsyncioTestCase installing its own loop), close it now.
    if _loop_created_by_fixture is not None:
        try:
            current = asyncio.get_event_loop()
        except RuntimeError:
            current = None
        if current is not _loop_created_by_fixture:
            if not _loop_created_by_fixture.is_closed():
                _loop_created_by_fixture.close()
            _loop_created_by_fixture = None

    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            _loop_created_by_fixture = new_loop
    except RuntimeError:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        _loop_created_by_fixture = new_loop
