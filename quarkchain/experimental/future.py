#!/usr/bin/python3

import asyncio


async def slow_operation(future):
    await asyncio.sleep(1)
    future.set_result("Future is done")


async def int_operation(future):
    await asyncio.sleep(0.5)
    future.cancel()


async def next_operation():
    future = asyncio.Future()
    asyncio.run_coroutine_threadsafe(slow_operation(future), asyncio.get_event_loop())
    asyncio.run_coroutine_threadsafe(int_operation(future), asyncio.get_event_loop())
    await future
    print("Done")


loop = asyncio.get_event_loop()

loop.run_until_complete(next_operation())
