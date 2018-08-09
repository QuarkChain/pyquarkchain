#!/usr/bin/python3

import asyncio
import numpy.random
import time

class PoW:
    # hashPower, number of hashs per second
    def __init__(self, hashPower):
        self.hashPower = hashPower

    # Return a realization of mining time in sec
    def mine(self, diff):
        return numpy.random.exponential(1 / diff / self.hashPower)


async def test_po_w():
    hashPower = 100
    diff = 0.01
    # Target block rate is 1sec
    p = PoW(hashPower)
    for i in range(10):
        startTime = time.time()
        await asyncio.sleep(p.mine(diff))
        usedTime = time.time() - startTime
        print(usedTime)


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_po_w())


if __name__ == "__main__":
    main()
