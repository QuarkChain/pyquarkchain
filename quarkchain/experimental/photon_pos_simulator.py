
from collections import deque
from quarkchain.experimental.event_driven_simulator import Scheduler
from quarkchain.utils import check
import numpy.random
import argparse
import time

ROOT_BLOCK_INTERVAL_SEC = 90
MINOR_BLOCK_INTERVAL_SEC = 10
MINOR_BLOCK_PER_ROOT_BLOCK = ROOT_BLOCK_INTERVAL_SEC // MINOR_BLOCK_INTERVAL_SEC
MINOR_BLOCK_ROOT_HASH_PREVIOUS = 2          # Should be >= 2
MINOR_BLOCK_GENESIS_EPOCH = 1               # Should be >= 1 and <= MINOR_BLOCK_ROOT_HASH_PREVIOUS

ROOT_BLOCK_NUM = 10000          # Number of root blocks to simulate
ROOT_BLOCK_MINOR_BLOCK_LIMIT = MINOR_BLOCK_PER_ROOT_BLOCK


class MinorBlock:
    def __init__(self, rootBlock, rootIndex, blockHeight, minedTime):
        self.rootBlock = rootBlock
        self.rootIndex = rootIndex
        self.blockHeight = blockHeight
        self.minedTime = minedTime


class RootBlock:
    def __init__(self, blockHeight, minorBlockList, miningTime, minedTime):
        self.blockHeight = blockHeight
        self.minorBlockList = minorBlockList
        self.miningTime = miningTime
        self.minedTime = minedTime


def get_next_interval(expectedInterval):
    return numpy.random.exponential(expectedInterval)


class Simulator:
    def __init__(self, args):
        self.pendingMinorBlockQueue = deque()
        self.minorBlockList = []
        self.scheduler = Scheduler()
        self.args = args

        # Create genesis root blocks
        self.rootBlockList = []
        for i in range(args.rprevious):
            self.rootBlockList.append(
                RootBlock(
                    blockHeight=i,
                    minorBlockList=[],
                    miningTime=0,
                    minedTime=0))

        # Create genesis minor blocks
        self.minorBlockList = []
        for i in range(args.minor_genesis_epoch):
            for j in range(MINOR_BLOCK_PER_ROOT_BLOCK):
                mBlock = MinorBlock(
                    rootBlock=self.rootBlockList[i],
                    rootIndex=j,
                    blockHeight=i * MINOR_BLOCK_PER_ROOT_BLOCK + j,
                    minedTime=0)
                self.minorBlockList.append(mBlock)
                self.pendingMinorBlockQueue.append(mBlock)

        self.rootBlockToMine = None
        self.minorBlockToMine = None

    def get_minor_block_tip(self):
        return self.minorBlockList[-1]

    def get_root_block_tip(self):
        return self.rootBlockList[-1]

    def get_genesis_root_block_number(self):
        return self.args.rprevious

    def get_genesis_minor_block_number(self):
        return self.args.minor_genesis_epoch * MINOR_BLOCK_PER_ROOT_BLOCK

    def get_next_minor_block_to_mine(self, ts):
        tip = self.get_minor_block_tip()

        # Produce block in this epoch if the epoch has space
        if tip.rootIndex < MINOR_BLOCK_PER_ROOT_BLOCK - 1:
            check(ts == tip.minedTime)
            return MinorBlock(
                rootBlock=tip.rootBlock,
                rootIndex=tip.rootIndex + 1,
                blockHeight=tip.blockHeight + 1,
                minedTime=ts + MINOR_BLOCK_INTERVAL_SEC)

        # Produce block in next epoch if a root block is available
        rootHeightToConfirm = tip.rootBlock.blockHeight + 1
        if rootHeightToConfirm <= self.get_root_block_tip().blockHeight:
            rootBlock = self.rootBlockList[rootHeightToConfirm]
            check(ts == max(tip.minedTime, rootBlock.minedTime))
            return MinorBlock(
                rootBlock=rootBlock,
                rootIndex=0,
                blockHeight=tip.blockHeight + 1,
                minedTime=ts + MINOR_BLOCK_INTERVAL_SEC)

        # Unable to find a root block to produce the minor block
        return None

    def mine_next_root_block(self, ts):
        check(self.rootBlockToMine is None)
        if len(self.pendingMinorBlockQueue) == 0 or \
                self.pendingMinorBlockQueue[0].rootBlock.blockHeight + self.args.rprevious > \
                self.get_root_block_tip().blockHeight + 1:
            # The root block is not able to mine.  Will wait until a minor block is produced.
            # TODO: the root block may also mine an null minor block with reduced coinbase reward
            return

        self.rootBlockToMine = RootBlock(
            blockHeight=self.get_root_block_tip().blockHeight + 1,
            minorBlockList=[],       # to be fill once mined
            miningTime=ts,
            minedTime=None)

        self.scheduler.schedule_after(
            get_next_interval(ROOT_BLOCK_INTERVAL_SEC),
            self.mine_root_block,
            self.rootBlockToMine)

        if self.args.verbose >= 1:
            print("%0.2f: rootBlock %d mining ..." % (ts, self.rootBlockToMine.blockHeight))

    def mine_root_block(self, ts, rootBlock):
        check(rootBlock == self.rootBlockToMine)

        # Include minor blocks as much as possible
        confirmedList = []
        while len(self.pendingMinorBlockQueue) != 0 and \
                self.pendingMinorBlockQueue[0].rootBlock.blockHeight + self.args.rprevious <= \
                rootBlock.blockHeight:
            mBlock = self.pendingMinorBlockQueue.popleft()
            confirmedList.append(mBlock)
            if len(confirmedList) >= ROOT_BLOCK_MINOR_BLOCK_LIMIT:
                break

        check(len(confirmedList) > 0)
        if (self.args.verbose >= 1):
            print("%0.2f: rootBlock %d mined with %d mblocks" % (ts, rootBlock.blockHeight, len(confirmedList)))

        # Add root block to chain
        self.rootBlockToMine.minorBlockList = confirmedList
        self.rootBlockToMine.minedTime = ts
        self.rootBlockList.append(self.rootBlockToMine)
        self.rootBlockToMine = None

        if rootBlock.blockHeight >= self.args.rblocks:
            self.scheduler.stop()
            return

        # Mine minor block produced by the root block if no minor block is in progress.
        if self.minorBlockToMine is None:
            self.mine_next_minor_block(ts)
            check(self.minorBlockToMine is not None)
            check(self.minorBlockToMine.rootBlock == self.get_root_block_tip())

        self.mine_next_root_block(ts)

    def mine_next_minor_block(self, ts):
        check(self.minorBlockToMine is None)

        mBlock = self.get_next_minor_block_to_mine(ts)
        if mBlock is None:
            return

        self.minorBlockToMine = mBlock
        self.scheduler.schedule_after(
            mBlock.minedTime - ts,
            self.mine_minor_block,
            self.minorBlockToMine)

    def mine_minor_block(self, ts, minorBlock):
        check(minorBlock == self.minorBlockToMine)

        if self.args.verbose >= 1:
            print("%0.2f: minorBlock %d mined" % (ts, minorBlock.blockHeight))

        # Add mined minor block to pending queue and chain
        self.pendingMinorBlockQueue.append(self.minorBlockToMine)
        self.minorBlockList.append(self.minorBlockToMine)
        self.minorBlockToMine = None

        # Should restart root block mining, but actually it doesn't matter
        # because exponential distribution is memory-less
        if self.rootBlockToMine is None:
            self.mine_next_root_block(ts)

        self.mine_next_minor_block(ts)

    def run(self):
        self.mine_next_root_block(0)
        self.mine_next_minor_block(0)

        self.scheduler.loop_until_no_task()


def main():
    global ROOT_BLOCK_NUM
    global MINOR_BLOCK_ROOT_HASH_PREVIOUS
    global MINOR_BLOCK_GENESIS_EPOCH

    parser = argparse.ArgumentParser()
    parser.add_argument("--rblocks", default=ROOT_BLOCK_NUM, type=int)
    parser.add_argument("--rprevious", default=MINOR_BLOCK_ROOT_HASH_PREVIOUS, type=int)
    parser.add_argument("--verbose", default=0, type=int)
    parser.add_argument("--minor_genesis_epoch", default=MINOR_BLOCK_GENESIS_EPOCH, type=int)
    parser.add_argument("--seed", default=int(time.time()), type=int)
    args = parser.parse_args()

    numpy.random.seed(args.seed)
    simulator = Simulator(args)
    simulator.run()

    mBlockFreq = dict()
    for rBlock in simulator.rootBlockList[args.rprevious:]:
        mBlockFreq[len(rBlock.minorBlockList)] = mBlockFreq.get(len(rBlock.minorBlockList), 0) + 1
    for key in mBlockFreq:
        print("%d: %d" % (key, mBlockFreq[key]))

    idleDuration = 0
    for i in range(1, len(simulator.rootBlockList)):
        rBlockPrevous = simulator.rootBlockList[i - 1]
        rBlock = simulator.rootBlockList[i]
        idleDuration += rBlock.miningTime - rBlockPrevous.minedTime
    print("Seed: %d" % (args.seed))
    print("Duration: %0.2f, Root block mining idle duraiton: %0.2f, idle percentage: %0.2f%%" % (
        simulator.rootBlockList[-1].minedTime,
        idleDuration,
        idleDuration / simulator.rootBlockList[-1].minedTime * 100))
    print("Minor block interval: %0.2f" % (
        simulator.minorBlockList[-1].minedTime /
        (simulator.minorBlockList[-1].blockHeight - simulator.get_genesis_minor_block_number())))


if __name__ == '__main__':
    main()
