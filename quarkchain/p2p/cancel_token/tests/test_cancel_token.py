import asyncio
import functools

import pytest

from quarkchain.p2p.cancel_token.token import (
    CancelToken,
    EventLoopMismatch,
    OperationCancelled,
)


def test_token_single():
    token = CancelToken("token")
    assert not token.triggered
    token.trigger()
    assert token.triggered
    assert token.triggered_token == token


def test_token_chain_event_loop_mismatch():
    token = CancelToken("token")
    token2 = CancelToken("token2", loop=asyncio.new_event_loop())
    with pytest.raises(EventLoopMismatch):
        token.chain(token2)


def test_token_chain_trigger_chain():
    token = CancelToken("token")
    token2 = CancelToken("token2")
    token3 = CancelToken("token3")
    intermediate_chain = token.chain(token2)
    chain = intermediate_chain.chain(token3)
    assert not chain.triggered
    chain.trigger()
    assert chain.triggered
    assert not intermediate_chain.triggered
    assert chain.triggered_token == chain
    assert not token.triggered
    assert not token2.triggered
    assert not token3.triggered


def test_token_chain_trigger_first():
    token = CancelToken("token")
    token2 = CancelToken("token2")
    token3 = CancelToken("token3")
    chain = token.chain(token2).chain(token3)
    assert not chain.triggered
    token.trigger()
    assert chain.triggered
    assert chain.triggered_token == token


def test_token_chain_trigger_middle():
    token = CancelToken("token")
    token2 = CancelToken("token2")
    token3 = CancelToken("token3")
    intermediate_chain = token.chain(token2)
    chain = intermediate_chain.chain(token3)
    assert not chain.triggered
    token2.trigger()
    assert chain.triggered
    assert intermediate_chain.triggered
    assert chain.triggered_token == token2
    assert not token3.triggered
    assert not token.triggered


def test_token_chain_trigger_last():
    token = CancelToken("token")
    token2 = CancelToken("token2")
    token3 = CancelToken("token3")
    intermediate_chain = token.chain(token2)
    chain = intermediate_chain.chain(token3)
    assert not chain.triggered
    token3.trigger()
    assert chain.triggered
    assert chain.triggered_token == token3
    assert not intermediate_chain.triggered


@pytest.mark.asyncio
async def test_token_wait(event_loop):
    token = CancelToken("token")
    event_loop.call_soon(token.trigger)
    done, pending = await asyncio.wait([token.wait()], timeout=0.1)
    assert len(done) == 1
    assert len(pending) == 0
    assert token.triggered


@pytest.mark.asyncio
async def test_wait_cancel_pending_tasks_on_completion(event_loop):
    token = CancelToken("token")
    token2 = CancelToken("token2")
    chain = token.chain(token2)
    event_loop.call_soon(token2.trigger)
    await chain.wait()
    await assert_only_current_task_not_done()


@pytest.mark.asyncio
async def test_wait_cancel_pending_tasks_on_cancellation(event_loop):
    """Test that cancelling a pending CancelToken.wait() coroutine doesn't leave .wait()
    coroutines for any chained tokens behind.
    """
    token = (
        CancelToken("token").chain(CancelToken("token2")).chain(CancelToken("token3"))
    )
    token_wait_coroutine = token.wait()
    done, pending = await asyncio.wait([token_wait_coroutine], timeout=0.1)
    assert len(done) == 0
    assert len(pending) == 1
    pending_task = pending.pop()
    assert pending_task._coro == token_wait_coroutine
    pending_task.cancel()
    await assert_only_current_task_not_done()


@pytest.mark.asyncio
async def test_cancellable_wait(event_loop):
    fut = asyncio.Future()
    event_loop.call_soon(functools.partial(fut.set_result, "result"))
    result = await CancelToken("token").cancellable_wait(fut, timeout=1)
    assert result == "result"
    await assert_only_current_task_not_done()


@pytest.mark.asyncio
async def test_cancellable_wait_future_exception(event_loop):
    fut = asyncio.Future()
    event_loop.call_soon(functools.partial(fut.set_exception, Exception()))
    with pytest.raises(Exception):
        await CancelToken("token").cancellable_wait(fut, timeout=1)
    await assert_only_current_task_not_done()


@pytest.mark.asyncio
async def test_cancellable_wait_cancels_subtasks_when_cancelled(event_loop):
    token = CancelToken("")
    future = asyncio.ensure_future(token.cancellable_wait(asyncio.sleep(2)))
    with pytest.raises(asyncio.TimeoutError):
        # asyncio.wait_for() will timeout and then cancel our cancellable_wait() future, but
        # Task.cancel() doesn't immediately cancels the task
        # (https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancel), so we need
        # the sleep below before we check that the task is actually cancelled.
        await asyncio.wait_for(future, timeout=0.01)
    await asyncio.sleep(0)
    assert future.cancelled()
    await assert_only_current_task_not_done()


@pytest.mark.asyncio
async def test_cancellable_wait_timeout():
    with pytest.raises(TimeoutError):
        await CancelToken("token").cancellable_wait(asyncio.sleep(0.02), timeout=0.01)
    await assert_only_current_task_not_done()


@pytest.mark.asyncio
async def test_cancellable_wait_operation_cancelled(event_loop):
    token = CancelToken("token")
    token.trigger()
    with pytest.raises(OperationCancelled):
        await token.cancellable_wait(asyncio.sleep(0.02))
    await assert_only_current_task_not_done()


async def assert_only_current_task_not_done():
    # This sleep() is necessary because Task.cancel() doesn't immediately cancels the task:
    # https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.cancel
    await asyncio.sleep(0.01)
    for task in asyncio.Task.all_tasks():
        if task == asyncio.Task.current_task():
            # This is the task for this very test, so it will be running
            assert not task.done()
        else:
            assert task.done()
