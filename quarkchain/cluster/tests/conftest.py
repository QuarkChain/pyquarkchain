import asyncio

import pytest

from quarkchain.protocol import AbstractConnection
from quarkchain.utils import _get_or_create_event_loop


@pytest.fixture(autouse=True)
def cleanup_event_loop():
    """Cancel all pending asyncio tasks after each test to prevent inter-test contamination."""
    yield
    loop = _get_or_create_event_loop()
    # Multiple rounds of cleanup: cancelling tasks can spawn new tasks in finally blocks
    for _ in range(3):
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        if not pending:
            break
        for task in pending:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        # Let the loop process any callbacks triggered by cancellation
        loop.run_until_complete(asyncio.sleep(0))
    AbstractConnection.aborted_rpc_count = 0
