import asyncio
import random
import time

import numpy
from aioprocessing import AioProcess, AioQueue

from quarkchain.config import DEFAULT_ENV, NetworkId
from quarkchain.core import MinorBlock, RootBlock
from quarkchain.utils import Logger


class Miner:
    def __init__(self, create_block_async_func, add_block_async_func, get_target_block_time_func, simulate=True):
        """Mining will happen on a subprocess managed by this class

        create_block_async_func: takes no argument, returns a block (either RootBlock or MinorBlock)
        add_block_async_func: takes a block
        get_target_block_time_func: takes no argument, returns the target block time in second
        """
        self.create_block_async_func = create_block_async_func
        self.add_block_async_func = add_block_async_func
        self.get_target_block_time_func = get_target_block_time_func
        self.simulate = simulate
        self.enabled = False
        self.process = None

    def is_enabled(self):
        return self.enabled

    def enable(self):
        self.enabled = True

    def disable(self):
        """Stop the mining process is there is one"""
        self.enabled = False

    def mine_new_block_async(self):
        if not self.enabled:
            return False
        asyncio.ensure_future(self.__mine_new_block())

    async def __mine_new_block(self):
        """Get a new block and start mining.
        If a mining process has already been started, update the process to mine the new block.
        """
        target_block_time = self.get_target_block_time_func()
        block = await self.create_block_async_func()
        if self.process:
            self.input.put((block, target_block_time))
            return

        mine_func = Miner.simulate_mine if self.simulate else Miner.mine
        self.input = AioQueue()
        self.output = AioQueue()
        self.process = AioProcess(target=mine_func, args=(block, target_block_time, self.input, self.output))
        self.process.start()

        asyncio.ensure_future(self.__handle_mined_block())

    async def __handle_mined_block(self):
        while True:
            block = await self.output.coro_get()
            if not block:
                return
            try:
                await self.add_block_async_func(block)
            except Exception:
                Logger.log_exception()
                self.mine_new_block_async()

    @staticmethod
    def __log_status(block):
        is_root = isinstance(block, RootBlock)
        shard = "R" if is_root else block.header.branch.get_shard_id()
        count = len(block.minor_block_header_list) if is_root else len(block.tx_list)
        elapsed = time.time() - block.header.create_time

        Logger.info("[{}] {} [{}] ({:.2f}) {}".format(
            shard, block.header.height, count, elapsed, block.header.get_hash().hex()))

    @staticmethod
    def __check_metric(metric):
        # Testnet does not check difficulty
        if DEFAULT_ENV.config.NETWORK_ID != NetworkId.MAINNET:
            return True
        return metric < 2 ** 256

    @staticmethod
    def mine(block, _, input, output):
        """PoW"""
        while True:
            block.header.nonce += 1
            metric = int.from_bytes(block.header.get_hash(), byteorder="big") * block.header.difficulty
            if Miner.__check_metric(metric):
                Miner.__log_status(block)
                output.put(block)
                block, _ = input.get()
            try:
                block, _ = input.get_nowait()
            except Exception:
                # got nothing from queue
                pass
            if not block:
                output.put(None)
                return

    @staticmethod
    def __get_block_time(block, target_block_time):
        if isinstance(block, MinorBlock):
            # Adjust the target block time to compensate computation time
            gas_used_ratio = block.meta.evm_gas_used / block.meta.evm_gas_limit
            target_block_time = target_block_time * (1 - gas_used_ratio * 0.4)
            Logger.debug("[{}] target block time {:.2f}".format(
                block.header.branch.get_shard_id(), target_block_time))

        return numpy.random.exponential(target_block_time)

    @staticmethod
    def simulate_mine(block, target_block_time, input, output):
        """Sleep until the target time"""
        target_time = block.header.create_time + numpy.random.exponential(target_block_time)
        while True:
            time.sleep(0.1)
            try:
                block, target_block_time = input.get_nowait()  # raises if queue is empty
                if not block:
                    output.put(None)
                    return
                target_time = block.header.create_time + Miner.__get_block_time(block, target_block_time)
            except Exception:
                # got nothing from queue
                pass
            if time.time() > target_time:
                Miner.__log_status(block)
                block.header.nonce = random.randint(0, 2 ** 32 - 1)
                output.put(block)
                block, target_block_time = input.get()  # blocking
                if not block:
                    output.put(None)
                    return
                target_time = block.header.create_time + Miner.__get_block_time(block, target_block_time)


