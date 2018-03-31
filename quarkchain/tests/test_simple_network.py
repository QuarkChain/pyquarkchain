import asyncio
import unittest
from quarkchain.chain import QuarkChainState
from quarkchain.simple_network import ForkResolverManager
from quarkchain.tests.test_utils import get_test_env


class MockSimpleNetwork:

    def __init__(self, qcState):
        self.qcState = qcState


class MockMinorBlockDownloader:

    def __init__(self, minorBlockMap):
        self.minorBlockMap = minorBlockMap

    async def getMinorBlockByHash(self, h):
        return self.minorBlockMap.get(h, None)

    async def getPreviousMinorBlockHeaderList(self, h, maxBlocks=1):
        if h not in self.minorBlockMap:
            return
        h = self.minorBlockMap[h].header.hashPrevMinorBlock

        headerList = []
        for i in range(maxBlocks):
            if h not in self.minorBlockMap:
                break
            header = self.minorBlockMap[h].header
            headerList.append(header)
            h = header.hashPrevMinorBlock
        return headerList


def build_minor_block_map(blockList):
    mbMap = dict()
    for block in blockList:
        mbMap[block.header.getHash()] = block
    return mbMap


class TestSimpleNetwork(unittest.TestCase):

    def testShardForkWithLength1(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100).finalizeMerkleRoot()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockMinorBlockDownloader(build_minor_block_map([b1, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveShardFork(network, None, b1.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getShardTip(0), b1.header)
        self.assertEqual(qcState.getShardTip(1).height, 0)

    def testShardForkWithLength2(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100).finalizeMerkleRoot()
        b2 = b1.createBlockToAppend().finalizeMerkleRoot()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockMinorBlockDownloader(
                build_minor_block_map([b1, b2, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveShardFork(network, None, b2.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getShardTip(0).height, 2)
        self.assertEqual(qcState.getShardTip(0), b2.header)
        self.assertEqual(qcState.getShardTip(1).height, 0)

    def testShardForkWithEqualLength(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))

        b2 = qcState.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockMinorBlockDownloader(
                build_minor_block_map([b2, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveShardFork(network, None, b2.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getShardTip(0).height, 1)
        self.assertEqual(qcState.getShardTip(0), b1.header)
        self.assertEqual(qcState.getShardTip(1).height, 0)

    def testShardForkWithUncommitedBlockAndLength1(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))

        b2 = b1.createBlockToAppend().finalizeMerkleRoot()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockMinorBlockDownloader(
                build_minor_block_map([b1, b2, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveShardFork(network, None, b2.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getShardTip(0).height, 2)
        self.assertEqual(qcState.getShardTip(0), b2.header)
        self.assertEqual(qcState.getShardTip(1).height, 0)

    def testShardForkWithUncommitedBlockAndLength2(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))

        b2 = b1.createBlockToAppend().finalizeMerkleRoot()
        b3 = b2.createBlockToAppend().finalizeMerkleRoot()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockMinorBlockDownloader(
                build_minor_block_map([b2, b3, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveShardFork(network, None, b3.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getShardTip(0).height, 3)
        self.assertEqual(qcState.getShardTip(0), b3.header)
        self.assertEqual(qcState.getShardTip(1).height, 0)

    def testShardForkWithUncommitedBlockAndDiff1(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))

        b2 = qcState.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b3 = b2.createBlockToAppend().finalizeMerkleRoot()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockMinorBlockDownloader(
                build_minor_block_map([b2, b3, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveShardFork(network, None, b3.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getShardTip(0).height, 2)
        self.assertEqual(qcState.getShardTip(0), b3.header)
        self.assertEqual(qcState.getShardTip(1).height, 0)
