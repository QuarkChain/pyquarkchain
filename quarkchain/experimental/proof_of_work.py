#!/usr/bin/python3

import asyncio
import numpy.random
import time


class PoW:
    # hash_power, number of hashs per second
    def __init__(self, hash_power):
        self.hash_power = hash_power

    # Return a realization of mining time in sec
    def mine(self, diff):
        return numpy.random.exponential(1 / diff / self.hash_power)


async def test_po_w():
    hash_power = 100
    diff = 0.01
    # Target block rate is 1sec
    p = PoW(hash_power)
    for i in range(10):
        start_time = time.time()
        await asyncio.sleep(p.mine(diff))
        used_time = time.time() - start_time
        print(used_time)


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_po_w())


if __name__ == "__main__":
    main()
