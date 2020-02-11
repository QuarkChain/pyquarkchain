import asyncio


async def start():
    print("enter start")
    try:
        await asyncio.wait_for(task1(), timeout=1)
    except asyncio.CancelledError:
        print("cancel in start")
    except asyncio.TimeoutError:
        print("timeout in start")
    print("exit start")


async def task1():
    print("enter task1")
    try:
        await asyncio.sleep(2)
    except asyncio.CancelledError:
        print("cancel in task1")
        pass
    print("exit task1")


async def test_cancel(task):
    print("canceling task")
    task.cancel()


async def task2():
    print("enter task2")
    await asyncio.sleep(2)
    raise TimeoutError()
    print("exit task2")


async def test_wait(task):
    done, pending = await asyncio.wait([task], return_when=asyncio.FIRST_COMPLETED)
    for d in done:
        d.exception()
    print("test_wait done")


# Test group 1
# task = asyncio.get_event_loop().create_task(start())
# asyncio.get_event_loop().create_task(test_cancel(task))
# asyncio.get_event_loop().create_task(test_cancel(task))

# Test group 2
task = asyncio.get_event_loop().create_task(task2())
asyncio.get_event_loop().create_task(test_wait(task))
asyncio.get_event_loop().run_forever()
