import asyncio
import random
import time

import numpy
from aioprocessing import AioProcess, AioQueue

from quarkchain.config import DEFAULT_ENV, NetworkId
from quarkchain.core import MinorBlock, RootBlock
from quarkchain.utils import Logger


class Miner:
    def __init__(self, createBlockAsyncFunc, add_blockAsyncFunc, getTargetBlockTimeFunc, simulate=True):
        """Mining will happen on a subprocess managed by this class

        createBlockAsyncFunc: takes no argument, returns a block (either RootBlock or MinorBlock)
        add_blockAsyncFunc: takes a block
        getTargetBlockTimeFunc: takes no argument, returns the target block time in second
        """
        self.createBlockAsyncFunc = createBlockAsyncFunc
        self.add_blockAsyncFunc = add_blockAsyncFunc
        self.getTargetBlockTimeFunc = getTargetBlockTimeFunc
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
        targetBlockTime = self.getTargetBlockTimeFunc()
        block = await self.createBlockAsyncFunc()
        if self.process:
            self.input.put((block, targetBlockTime))
            return

        mineFunc = Miner.simulate_mine if self.simulate else Miner.mine
        self.input = AioQueue()
        self.output = AioQueue()
        self.process = AioProcess(target=mineFunc, args=(block, targetBlockTime, self.input, self.output))
        self.process.start()

        asyncio.ensure_future(self.__handle_mined_block())

    async def __handle_mined_block(self):
        while True:
            block = await self.output.coro_get()
            if not block:
                return
            try:
                await self.add_blockAsyncFunc(block)
            except Exception:
                Logger.logException()
                self.mine_new_block_async()

    @staticmethod
    def __logStatus(block):
        isRoot = isinstance(block, RootBlock)
        shard = "R" if isRoot else block.header.branch.get_shard_id()
        count = len(block.minorBlockHeaderList) if isRoot else len(block.txList)
        elapsed = time.time() - block.header.createTime

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
                Miner.__logStatus(block)
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
    def __get_block_time(block, targetBlockTime):
        if isinstance(block, MinorBlock):
            # Adjust the target block time to compensate computation time
            gasUsedRatio = block.meta.evmGasUsed / block.meta.evmGasLimit
            targetBlockTime = targetBlockTime * (1 - gasUsedRatio * 0.4)
            Logger.debug("[{}] target block time {:.2f}".format(
                block.header.branch.get_shard_id(), targetBlockTime))

        return numpy.random.exponential(targetBlockTime)

    @staticmethod
    def simulate_mine(block, targetBlockTime, input, output):
        """Sleep until the target time"""
        targetTime = block.header.createTime + numpy.random.exponential(targetBlockTime)
        while True:
            time.sleep(0.1)
            try:
                block, targetBlockTime = input.get_nowait()  # raises if queue is empty
                if not block:
                    output.put(None)
                    return
                targetTime = block.header.createTime + Miner.__get_block_time(block, targetBlockTime)
            except Exception:
                # got nothing from queue
                pass
            if time.time() > targetTime:
                Miner.__logStatus(block)
                block.header.nonce = random.randint(0, 2 ** 32 - 1)
                output.put(block)
                block, targetBlockTime = input.get()  # blocking
                if not block:
                    output.put(None)
                    return
                targetTime = block.header.createTime + Miner.__get_block_time(block, targetBlockTime)


