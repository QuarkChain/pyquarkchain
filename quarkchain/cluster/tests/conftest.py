import asyncio

import pytest

from quarkchain.utils import _get_or_create_event_loop


@pytest.fixture(autouse=True)
def cleanup_event_loop():
    """Cancel all pending asyncio tasks after each test to prevent inter-test contamination."""
    yield
    loop = _get_or_create_event_loop()
    pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
    for task in pending:
        task.cancel()
    if pending:
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
