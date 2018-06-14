import asyncio
import multiprocessing
import time
import numpy

from quarkchain.config import DEFAULT_ENV, NetworkId
from quarkchain.cluster.core import RootBlock
from quarkchain.utils import Logger


class Miner:
    def __init__(self, executor, createBlockAsyncFunc, addBlockAsyncFunc, getTargetBlockTimeFunc, simulate=True):
        """Mining will happen on the executor

        createBlockAsyncFunc: takes no argument, returns a block (either RootBlock or MinorBlock)
        addBlockAsyncFunc: takes a block
        getTargetBlockTimeFunc: takes no argument, returns the target block time in second
        """
        self.executor = executor
        self.createBlockAsyncFunc = createBlockAsyncFunc
        self.addBlockAsyncFunc = addBlockAsyncFunc
        self.getTargetBlockTimeFunc = getTargetBlockTimeFunc
        self.queue = multiprocessing.Manager().Queue()
        self.simulate = simulate
        self.enabled = False
        self.isMining = False

    def enable(self):
        self.enabled = True

    def disable(self):
        """Stop the mining process is there is one"""
        self.enabled = False

        if not self.isMining:
            return
        self.queue.put((None, None))

    def mineNewBlockAsync(self):
        if not self.enabled:
            return False
        asyncio.ensure_future(self.__mineNewBlock())

    async def __mineNewBlock(self):
        """Get a new block and start mining.
        If a mining process has already been started, update the process to mine the new block.
        """
        targetBlockTime = self.getTargetBlockTimeFunc()
        block = await self.createBlockAsyncFunc()
        if self.isMining:
            self.queue.put((block, targetBlockTime))
            return

        self.isMining = True
        mineFunc = Miner.simulateMine if self.simulate else Miner.mine
        block = await asyncio.get_event_loop().run_in_executor(
            self.executor, mineFunc, block, targetBlockTime, self.queue)
        self.isMining = False
        if not block:
            return
        try:
            await self.addBlockAsyncFunc(block)
        except Exception:
            Logger.logException()

        self.mineNewBlockAsync()

    @staticmethod
    def __logStatus(block):
        isRoot = isinstance(block, RootBlock)
        shard = "R" if isRoot else block.header.branch.getShardId()
        count = len(block.minorBlockHeaderList) if isRoot else len(block.txList)
        elapsed = time.time() - block.header.createTime

        Logger.info("[{}] {} [{}] ({:.2f}) {}".format(
            shard, block.header.height, count, elapsed, block.header.getHash().hex()))

    @staticmethod
    def __checkMetric(metric):
        # Testnet does not check difficulty
        if DEFAULT_ENV.config.NETWORK_ID != NetworkId.MAINNET:
            return True
        return metric < 2 ** 256

    @staticmethod
    def mine(block, _, q):
        """PoW"""
        while True:
            block.header.nonce += 1
            metric = int.from_bytes(block.header.getHash(), byteorder="big") * block.header.difficulty
            if Miner.__checkMetric(metric):
                Miner.__logStatus(block)
                return block
            try:
                block, _ = q.get_nowait()
                if not block:
                    return None
            except Exception:
                # got nothing from queue
                pass

    @staticmethod
    def simulateMine(block, targetBlockTime, q):
        """Sleep until the target time"""
        targetTime = block.header.createTime + numpy.random.exponential(targetBlockTime)
        while True:
            time.sleep(0.1)
            try:
                block, targetBlockTime = q.get_nowait()
                if not block:
                    return None
                targetTime = block.header.createTime + numpy.random.exponential(targetBlockTime)
            except Exception:
                # got nothing from queue
                pass
            if time.time() > targetTime:
                Miner.__logStatus(block)
                return block


