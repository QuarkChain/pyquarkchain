import asyncio
import time
import unittest
from quarkchain.chain import QuarkChainState
from quarkchain.commands import *
from quarkchain.simple_network import Downloader, ForkResolverManager, SimpleNetwork
from quarkchain.tests.test_utils import get_test_env
from quarkchain.utils import set_logging_level, call_async


# Disable error log in test
# You want to comment out this if debugging the tests
set_logging_level("critical")


class MockSimpleNetwork:

    def __init__(self, qcState):
        self.qcState = qcState


class MockDownloader:

    def __init__(self, rootBlockMap, minorBlockMap):
        self.rootBlockMap = rootBlockMap
        self.minorBlockMap = minorBlockMap
        self.error = None

    async def getRootBlockByHash(self, h):
        return self.rootBlockMap.get(h, None)

    async def getPreviousRootBlockHeaderList(self, h, maxBlocks=10):
        if h not in self.rootBlockMap:
            return
        h = self.rootBlockMap[h].header.hashPrevBlock

        headerList = []
        for i in range(maxBlocks):
            if h not in self.rootBlockMap:
                break
            header = self.rootBlockMap[h].header
            headerList.append(header)
            h = header.hashPrevBlock
        return headerList

    async def getMinorBlockByHash(self, h):
        return self.minorBlockMap.get(h, None)

    async def getPreviousMinorBlockHeaderList(self, shardId, h, maxBlocks=10):
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

    def closePeerWithError(self, error):
        self.error = error


def build_block_map(blockList):
    mbMap = dict()
    for block in blockList:
        mbMap[block.header.getHash()] = block
    return mbMap


class TestShardFork(unittest.TestCase):
    '''Unit test of shard fork resolver with MockDownloader'''

    def testShardForkWithLength1(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100).finalizeMerkleRoot()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockDownloader(
                dict(),
                build_block_map([b1, qcState.getGenesisMinorBlock(0)])))

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
            lambda peer: MockDownloader(
                dict(),
                build_block_map([b1, b2, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveShardFork(network, None, b2.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getShardTip(0).height, 2)
        self.assertEqual(qcState.getShardTip(0), b2.header)
        self.assertEqual(qcState.getShardTip(1).height, 0)

    def testShardForkWithLength2StateRecovery(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100).finalizeMerkleRoot()
        b2 = b1.createBlockToAppend().finalizeMerkleRoot()
        b2.header.createTime = 0

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockDownloader(
                dict(),
                build_block_map([b1, b2, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveShardFork(network, None, b2.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getShardTip(0).height, 0)
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
            lambda peer: MockDownloader(
                dict(),
                build_block_map([b2, qcState.getGenesisMinorBlock(0)])))

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
            lambda peer: MockDownloader(
                dict(),
                build_block_map([b1, b2, qcState.getGenesisMinorBlock(0)])))

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
            lambda peer: MockDownloader(
                dict(),
                build_block_map([b2, b3, qcState.getGenesisMinorBlock(0)])))

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
            lambda peer: MockDownloader(
                dict(),
                build_block_map([b2, b3, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveShardFork(network, None, b3.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getShardTip(0).height, 2)
        self.assertEqual(qcState.getShardTip(0), b3.header)
        self.assertEqual(qcState.getShardTip(1).height, 0)

    def testShardForkWithStateRecovery(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend(
            quarkash=100).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))

        b2 = qcState.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b3 = b2.createBlockToAppend().finalizeMerkleRoot()
        b3.header.createTime = 0

        network = MockSimpleNetwork(qcState)
        downloader = MockDownloader(
            dict(),
            build_block_map([b2, b3, qcState.getGenesisMinorBlock(0)]))
        frManager = ForkResolverManager(
            lambda peer: downloader)

        loop = asyncio.get_event_loop()
        frManager.tryResolveShardFork(network, None, b3.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getShardTip(0).height, 1)
        self.assertEqual(qcState.getShardTip(0), b1.header)
        self.assertEqual(qcState.getShardTip(1).height, 0)
        self.assertTrue("incorrect create time tip time" in downloader.error)


class TestRootFork(unittest.TestCase):
    '''Unit test of root fork resolver with MockDownloader'''

    def testRootForkWithoutShardFork(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend().finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))

        rB = qcState.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b1.header, b2.header]).finalize()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockDownloader(
                build_block_map([qcState.getGenesisRootBlock(), rB]),
                build_block_map([b1, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveRootFork(network, None, rB.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getRootBlockTip(), rB.header)

    def testRootForkWithMissingMinorBlock(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend().finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))

        rB = qcState.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b1.header, b2.header]).finalize()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockDownloader(
                build_block_map([qcState.getGenesisRootBlock(), rB]),
                build_block_map([b1, b2, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveRootFork(network, None, rB.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getRootBlockTip(), rB.header)
        self.assertEqual(qcState.getMinorBlockTip(0), b1.header)
        self.assertEqual(qcState.getMinorBlockTip(1), b2.header)

    def testRootForkWithShardFork(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend().finalizeMerkleRoot()
        b3 = b2.createBlockToAppend().finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))
        self.assertIsNone(qcState.appendMinorBlock(b3))
        b4 = qcState.getGenesisMinorBlock(1).createBlockToAppend(quarkash=100).finalizeMerkleRoot()

        rB = qcState.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b1.header, b4.header]).finalize()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockDownloader(
                build_block_map([qcState.getGenesisRootBlock(), rB]),
                build_block_map([b1, b4, qcState.getGenesisMinorBlock(0)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveRootFork(network, None, rB.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getRootBlockTip(), rB.header)
        self.assertEqual(qcState.getMinorBlockTip(0), b1.header)
        self.assertEqual(qcState.getMinorBlockTip(1), b4.header)

    def testRootForkWithEqualHeight(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend().finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))
        rB = qcState.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b1.header, b2.header]).finalize()
        self.assertIsNone(qcState.appendRootBlock(rB))

        b3 = qcState.getGenesisMinorBlock(0).createBlockToAppend(quarkash=1).finalizeMerkleRoot()
        b4 = qcState.getGenesisMinorBlock(1).createBlockToAppend(quarkash=2).finalizeMerkleRoot()
        rB1 = qcState.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b3.header, b4.header]).finalize()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockDownloader(
                build_block_map([qcState.getGenesisRootBlock(), rB1]),
                build_block_map([b3, b4, qcState.getGenesisMinorBlock(0), qcState.getGenesisMinorBlock(1)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveRootFork(network, None, rB.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getRootBlockTip(), rB.header)
        self.assertEqual(qcState.getMinorBlockTip(0), b1.header)
        self.assertEqual(qcState.getMinorBlockTip(1), b2.header)

    def testRootForkWithTwoRootForks(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend().finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))
        rB = qcState.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b1.header, b2.header]).finalize()
        self.assertIsNone(qcState.appendRootBlock(rB))

        b3 = qcState.getGenesisMinorBlock(0).createBlockToAppend(quarkash=1).finalizeMerkleRoot()
        b4 = qcState.getGenesisMinorBlock(1).createBlockToAppend(quarkash=2).finalizeMerkleRoot()
        rB1 = qcState.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b3.header, b4.header]).finalize()
        b5 = b3.createBlockToAppend().finalizeMerkleRoot()
        b6 = b4.createBlockToAppend().finalizeMerkleRoot()
        rB2 = rB1.createBlockToAppend().extendMinorBlockHeaderList(
            [b5.header, b6.header]).finalize()

        network = MockSimpleNetwork(qcState)
        frManager = ForkResolverManager(
            lambda peer: MockDownloader(
                build_block_map([qcState.getGenesisRootBlock(), rB1, rB2]),
                build_block_map([b3, b4, b5, b6, qcState.getGenesisMinorBlock(0), qcState.getGenesisMinorBlock(1)])))

        loop = asyncio.get_event_loop()
        frManager.tryResolveRootFork(network, None, rB2.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getRootBlockTip(), rB2.header)
        self.assertEqual(qcState.getMinorBlockTip(0), b5.header)
        self.assertEqual(qcState.getMinorBlockTip(1), b6.header)

    def testRootForkWithStateRecovery(self):
        env = get_test_env()
        qcState = QuarkChainState(env)
        b1 = qcState.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b2 = qcState.getGenesisMinorBlock(1).createBlockToAppend().finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))
        rB = qcState.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b1.header, b2.header]).finalize()
        self.assertIsNone(qcState.appendRootBlock(rB))

        b3 = qcState.getGenesisMinorBlock(0).createBlockToAppend(quarkash=1).finalizeMerkleRoot()
        b4 = qcState.getGenesisMinorBlock(1).createBlockToAppend(quarkash=2).finalizeMerkleRoot()
        rB1 = qcState.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b3.header, b4.header]).finalize()
        b5 = b3.createBlockToAppend().finalizeMerkleRoot()
        b6 = b4.createBlockToAppend().finalizeMerkleRoot()
        b6.header.createTime = 0

        rB2 = rB1.createBlockToAppend().extendMinorBlockHeaderList(
            [b5.header, b6.header]).finalize()

        network = MockSimpleNetwork(qcState)
        downloader = MockDownloader(
            build_block_map([qcState.getGenesisRootBlock(), rB1, rB2]),
            build_block_map([b3, b4, b5, b6, qcState.getGenesisMinorBlock(0), qcState.getGenesisMinorBlock(1)]))
        frManager = ForkResolverManager(lambda peer: downloader)

        loop = asyncio.get_event_loop()
        frManager.tryResolveRootFork(network, None, rB2.header)
        loop.run_until_complete(frManager.getCompletionFuture())
        qcState = network.qcState
        self.assertEqual(qcState.getRootBlockTip(), rB.header)
        self.assertEqual(qcState.getMinorBlockTip(0), b1.header)
        self.assertEqual(qcState.getMinorBlockTip(1), b2.header)
        self.assertTrue("incorrect create time tip time" in downloader.error)


server_port = 51354


def create_network():
    global server_port
    env = get_test_env()
    env.config.P2P_SERVER_PORT = server_port
    server_port += 1
    qcState = QuarkChainState(env)
    network = SimpleNetwork(env, qcState)
    network.startServer()
    return (env, qcState, network)


def assert_true_with_timeout(f):
        async def d():
            deadline = time.time() + 1
            while not f() and time.time() < deadline:
                await asyncio.sleep(0.001)
            assert(f())

        asyncio.get_event_loop().run_until_complete(d())


class TestSimpmleNetwork(unittest.TestCase):
    '''Test of P2P commands with real node servers'''

    def testGetBlockHeaderListRequest(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        b1 = qcState0.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b2 = qcState0.getGenesisMinorBlock(1).createBlockToAppend().finalizeMerkleRoot()
        b3 = b2.createBlockToAppend().finalizeMerkleRoot()
        self.assertIsNone(qcState0.appendMinorBlock(b1))
        self.assertIsNone(qcState0.appendMinorBlock(b2))
        self.assertIsNone(qcState0.appendMinorBlock(b3))
        rB = qcState0.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b1.header, b2.header, b3.header]).finalize()
        self.assertIsNone(qcState0.appendRootBlock(rB))

        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))

        # Forward iteration of root chain
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HEADER_LIST_REQUEST,
            GetBlockHeaderListRequest(
                isRoot=True,
                shardId=0,      # ignore
                blockHash=qcState0.getGenesisRootBlock().header.getHash(),
                maxBlocks=1024,
                direction=1,
            )))

        self.assertEqual(len(resp.blockHeaderList), 2)
        self.assertEqual(resp.blockHeaderList[0], qcState0.getGenesisRootBlock().header.serialize())
        self.assertEqual(resp.blockHeaderList[1], rB.header.serialize())
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())

        # Backward iteration of root chain
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HEADER_LIST_REQUEST,
            GetBlockHeaderListRequest(
                isRoot=True,
                shardId=0,      # ignore
                blockHash=qcState0.getGenesisRootBlock().header.getHash(),
                maxBlocks=1024,
                direction=0,
            )))

        self.assertEqual(len(resp.blockHeaderList), 1)
        self.assertEqual(resp.blockHeaderList[0], qcState0.getGenesisRootBlock().header.serialize())
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())

        # Failed iteration
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HEADER_LIST_REQUEST,
            GetBlockHeaderListRequest(
                isRoot=True,
                shardId=0,      # ignore
                blockHash=bytes(32),
                maxBlocks=1024,
                direction=0,
            )))

        self.assertEqual(len(resp.blockHeaderList), 0)
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())

        # Forward iteration of shard
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HEADER_LIST_REQUEST,
            GetBlockHeaderListRequest(
                isRoot=False,
                shardId=1,
                blockHash=qcState0.getGenesisMinorBlock(1).header.getHash(),
                maxBlocks=1024,
                direction=1,
            )))

        self.assertEqual(len(resp.blockHeaderList), 3)
        self.assertEqual(resp.blockHeaderList[0], qcState0.getGenesisMinorBlock(1).header.serialize())
        self.assertEqual(resp.blockHeaderList[1], b2.header.serialize())
        self.assertEqual(resp.blockHeaderList[2], b3.header.serialize())
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())
        self.assertEqual(resp.shardTip, qcState0.getMinorBlockTip(1))

        # Limiting test
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HEADER_LIST_REQUEST,
            GetBlockHeaderListRequest(
                isRoot=False,
                shardId=1,
                blockHash=qcState0.getGenesisMinorBlock(1).header.getHash(),
                maxBlocks=2,
                direction=1,
            )))

        self.assertEqual(len(resp.blockHeaderList), 2)
        self.assertEqual(resp.blockHeaderList[0], qcState0.getGenesisMinorBlock(1).header.serialize())
        self.assertEqual(resp.blockHeaderList[1], b2.header.serialize())
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())
        self.assertEqual(resp.shardTip, qcState0.getMinorBlockTip(1))

        # Backwaord iteration of shard
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HEADER_LIST_REQUEST,
            GetBlockHeaderListRequest(
                isRoot=False,
                shardId=0,
                blockHash=b1.header.getHash(),
                maxBlocks=1024,
                direction=0,
            )))

        self.assertEqual(len(resp.blockHeaderList), 2)
        self.assertEqual(resp.blockHeaderList[0], b1.header.serialize())
        self.assertEqual(resp.blockHeaderList[1], qcState0.getGenesisMinorBlock(0).header.serialize())
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())
        self.assertEqual(resp.shardTip, qcState0.getMinorBlockTip(0))

        network0.shutdown()
        network1.shutdown()


class TestDownloader(unittest.TestCase):
    '''Test Downloader with real node servers'''

    def setUp(self):
        self.env0, self.qcState0, self.network0 = create_network()
        env1, qcState1, self.network1 = create_network()
        peer = call_async(self.network1.connect("127.0.0.1", self.env0.config.P2P_SERVER_PORT))
        self.downloader = Downloader(peer)
        self.assertFalse(self.downloader.isPeerClosed())

    def tearDown(self):
        self.network0.shutdown()
        self.network1.shutdown()

    def testGetRootBlockByHash(self):
        b1 = self.qcState0.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b2 = self.qcState0.getGenesisMinorBlock(1).createBlockToAppend().finalizeMerkleRoot()
        self.assertIsNone(self.qcState0.appendMinorBlock(b1))
        self.assertIsNone(self.qcState0.appendMinorBlock(b2))
        rB = self.qcState0.getGenesisRootBlock().createBlockToAppend().extendMinorBlockHeaderList(
            [b1.header, b2.header]).finalize()
        self.assertIsNone(self.qcState0.appendRootBlock(rB))

        for block in [rB, self.qcState0.getGenesisRootBlock()]:
            self.assertEqual(call_async(self.downloader.getRootBlockByHash(block.header.getHash())), block)

    def testGetRootBlockByHashNotExist(self):
        self.assertIsNone(call_async(self.downloader.getRootBlockByHash(
            self.qcState0.getGenesisMinorBlock(0).header.getHash())))
        self.assertFalse(self.downloader.isPeerClosed())

    def testGetMinorBlockByHash(self):
        b1 = self.qcState0.getGenesisMinorBlock(0).createBlockToAppend().finalizeMerkleRoot()
        b2 = self.qcState0.getGenesisMinorBlock(1).createBlockToAppend().finalizeMerkleRoot()
        b3 = b2.createBlockToAppend().finalizeMerkleRoot()
        self.assertIsNone(self.qcState0.appendMinorBlock(b1))
        self.assertIsNone(self.qcState0.appendMinorBlock(b2))
        self.assertIsNone(self.qcState0.appendMinorBlock(b3))

        for block in [b1, b2, b3, self.qcState0.getGenesisMinorBlock(0), self.qcState0.getGenesisMinorBlock(1)]:
            self.assertEqual(call_async(self.downloader.getMinorBlockByHash(block.header.getHash())), block)

    def testGetMinorBlockByHashNotExist(self):
        self.assertIsNone(call_async(self.downloader.getMinorBlockByHash(
            self.qcState0.getGenesisRootBlock().header.getHash())))
        self.assertFalse(self.downloader.isPeerClosed())

    def testGetPreviousRootBlockHeaderList(self):
        rootBlocks = [self.qcState0.getGenesisRootBlock()]
        b1 = self.qcState0.getGenesisMinorBlock(0)
        b2 = self.qcState0.getGenesisMinorBlock(1)
        for i in range(5):
            b1 = b1.createBlockToAppend().finalizeMerkleRoot()
            b2 = b2.createBlockToAppend().finalizeMerkleRoot()
            self.assertIsNone(self.qcState0.appendMinorBlock(b1))
            self.assertIsNone(self.qcState0.appendMinorBlock(b2))
            rB = rootBlocks[-1].createBlockToAppend().extendMinorBlockHeaderList(
                [b1.header, b2.header]).finalize()
            self.assertIsNone(self.qcState0.appendRootBlock(rB))
            rootBlocks.append(rB)

        headerList = call_async(self.downloader.getPreviousRootBlockHeaderList(rootBlocks[5].header.getHash()))
        self.assertEqual(len(headerList), 1)
        self.assertEqual(headerList[0], rootBlocks[4].header)

        headerList = call_async(self.downloader.getPreviousRootBlockHeaderList(rootBlocks[4].header.getHash(), 3))
        self.assertEqual(len(headerList), 3)
        self.assertEqual(headerList[0], rootBlocks[3].header)
        self.assertEqual(headerList[1], rootBlocks[2].header)
        self.assertEqual(headerList[2], rootBlocks[1].header)

        headerList = call_async(self.downloader.getPreviousRootBlockHeaderList(rootBlocks[1].header.getHash(), 10))
        self.assertEqual(len(headerList), 1)
        self.assertEqual(headerList[0], rootBlocks[0].header)

        headerList = call_async(self.downloader.getPreviousRootBlockHeaderList(
            self.qcState0.getGenesisRootBlock().header.getHash(), 10))
        self.assertEqual(len(headerList), 0)
        self.assertFalse(self.downloader.isPeerClosed())

    def testGetPreviousRootBlockHeaderListNotExist(self):
        headerList = call_async(self.downloader.getPreviousRootBlockHeaderList(
            self.qcState0.getGenesisMinorBlock(0).header.getHash(), 10))
        self.assertEqual(len(headerList), 0)
        self.assertFalse(self.downloader.isPeerClosed())

    def testGetPreviousMinorBlockHeaderList(self):
        blocks0 = [self.qcState0.getGenesisMinorBlock(0)]
        blocks1 = [self.qcState0.getGenesisMinorBlock(1)]
        for i in range(5):
            b1 = blocks0[-1].createBlockToAppend().finalizeMerkleRoot()
            b2 = blocks1[-1].createBlockToAppend().finalizeMerkleRoot()
            self.assertIsNone(self.qcState0.appendMinorBlock(b1))
            self.assertIsNone(self.qcState0.appendMinorBlock(b2))
            blocks0.append(b1)
            blocks1.append(b2)

        headerList = call_async(self.downloader.getPreviousMinorBlockHeaderList(0, blocks0[5].header.getHash()))
        self.assertEqual(len(headerList), 1)
        self.assertEqual(headerList[0], blocks0[4].header)

        headerList = call_async(self.downloader.getPreviousMinorBlockHeaderList(1, blocks1[4].header.getHash(), 3))
        self.assertEqual(len(headerList), 3)
        self.assertEqual(headerList[0], blocks1[3].header)
        self.assertEqual(headerList[1], blocks1[2].header)
        self.assertEqual(headerList[2], blocks1[1].header)

        headerList = call_async(self.downloader.getPreviousMinorBlockHeaderList(0, blocks0[1].header.getHash(), 10))
        self.assertEqual(len(headerList), 1)
        self.assertEqual(headerList[0], blocks0[0].header)

        headerList = call_async(self.downloader.getPreviousMinorBlockHeaderList(
            0, self.qcState0.getGenesisMinorBlock(0).header.getHash(), 10))
        self.assertEqual(len(headerList), 0)
        self.assertFalse(self.downloader.isPeerClosed())

    def testGetPreviousMinorBlockHeaderListNotExist(self):
        # wrong shard id
        headerList = call_async(self.downloader.getPreviousMinorBlockHeaderList(
            0, self.qcState0.getGenesisMinorBlock(1).header.getHash(), 10))
        self.assertEqual(len(headerList), 0)
        self.assertFalse(self.downloader.isPeerClosed())


class TestShardForkWithTwoNetworks(unittest.TestCase):
    '''Test shard fork resolver with real node servers
       Calling network1.broadcastBlockHeaders will invoke Peer.handleNewMinorBlockHeaderList in network0
    '''

    def addNewBlock(self, qcState, quarkash=0):
        b1 = qcState.getShardTip(0).createBlockToAppend(quarkash=quarkash).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))

    def testAppendBlocksFromLongerChain(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))
        self.assertIsNotNone(peer)

        for i in range(10):
            self.addNewBlock(qcState1)

        assert(qcState1.getShardTip(0).height == 10)

        network1.broadcastBlockHeaders(qcState1.getRootBlockTip(), [qcState1.getShardTip(0)])
        assert_true_with_timeout(
            lambda: network0.qcState.getShardTip(0) == qcState1.getShardTip(0))

        network0.shutdown()
        network1.shutdown()

    def testEqualHeightWithLocalChainDoNothing(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))
        self.assertIsNotNone(peer)

        for i in range(10):
            self.addNewBlock(qcState0)
            self.addNewBlock(qcState1, quarkash=1)

        assert(qcState0.getShardTip(0).height == 10)
        assert(qcState1.getShardTip(0).height == 10)
        assert(qcState0.getShardTip(0) != qcState1.getShardTip(0))

        oldTip = qcState0.getShardTip(0)
        network1.broadcastBlockHeaders(qcState1.getRootBlockTip(), [qcState1.getShardTip(0)])
        assert_true_with_timeout(
            lambda: qcState0.getShardTip(0) == oldTip)

        network0.shutdown()
        network1.shutdown()

    def testLongerChainOverrideShorterChain(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))
        self.assertIsNotNone(peer)

        self.addNewBlock(qcState1, quarkash=1)
        for i in range(10):
            self.addNewBlock(qcState0)
            self.addNewBlock(qcState1)

        assert(qcState0.getShardTip(0).height == 10)
        assert(qcState1.getShardTip(0).height == 11)

        network1.broadcastBlockHeaders(qcState1.getRootBlockTip(), [qcState1.getShardTip(0)])
        assert_true_with_timeout(
            lambda: network0.qcState.getShardTip(0) == qcState1.getShardTip(0))

        network0.shutdown()
        network1.shutdown()

    def testEqualHeightWithBestObservedCloseConnection(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        # Exchange best blocks info
        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))
        self.assertIsNotNone(peer)

        network1.broadcastBlockHeaders(qcState1.getRootBlockTip(), [qcState1.getShardTip(1)])
        assert_true_with_timeout(
            lambda: network0.qcState.getShardTip(0).height == 0)
        assert_true_with_timeout(
            lambda: peer.isClosed())

        network0.shutdown()
        network1.shutdown()


class TestRootForkWithTwoNetworks(unittest.TestCase):
    '''Test root fork resolver with real node servers
       Calling network1.broadcastBlockHeaders will invoke Peer.handleNewMinorBlockHeaderList in network0
    '''

    def addNewBlocks(self, qcState, addRootBlock=True, quarkash=0):
        b1 = qcState.getShardTip(0).createBlockToAppend(quarkash=quarkash).finalizeMerkleRoot()
        b2 = b1.createBlockToAppend(quarkash=quarkash).finalizeMerkleRoot()
        b3 = qcState.getShardTip(1).createBlockToAppend(quarkash=quarkash).finalizeMerkleRoot()

        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertIsNone(qcState.appendMinorBlock(b2))
        self.assertIsNone(qcState.appendMinorBlock(b3))

        if not addRootBlock:
            return

        rB = qcState.getRootBlockTip().createBlockToAppend().extendMinorBlockHeaderList(
            [b1.header, b2.header, b3.header]).finalize()
        self.assertIsNone(qcState.appendRootBlock(rB))

    def testAppendRootBlocksFromLongerChain(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))
        self.assertIsNotNone(peer)

        for i in range(10):
            self.addNewBlocks(qcState0, addRootBlock=False)
            self.addNewBlocks(qcState1)

        assert(qcState0.getRootBlockTip().height == 0)
        assert(qcState0.getShardTip(0).height == 20)
        assert(qcState0.getShardTip(1).height == 10)
        assert(qcState1.getRootBlockTip().height == 10)
        assert(qcState1.getShardTip(0).height == 20)
        assert(qcState0.getShardTip(1).height == 10)

        network1.broadcastBlockHeaders(qcState1.getRootBlockTip(), [])
        assert_true_with_timeout(
            lambda: network0.qcState.getRootBlockTip() == qcState1.getRootBlockTip())
        assert_true_with_timeout(
            lambda: network0.qcState.getShardTip(0) == qcState1.getShardTip(0))
        assert_true_with_timeout(
            lambda: network0.qcState.getShardTip(1) == qcState1.getShardTip(1))

        network0.shutdown()
        network1.shutdown()

    def testAppendRootBlocksAndMinorBlocksFromLongerChain(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))
        self.assertIsNotNone(peer)

        for i in range(10):
            self.addNewBlocks(qcState1)

        assert(qcState0.getRootBlockTip().height == 0)
        assert(qcState0.getShardTip(0).height == 0)
        assert(qcState0.getShardTip(1).height == 0)
        assert(qcState1.getRootBlockTip().height == 10)
        assert(qcState1.getShardTip(0).height == 20)
        assert(qcState1.getShardTip(1).height == 10)

        network1.broadcastBlockHeaders(qcState1.getRootBlockTip(), [])
        assert_true_with_timeout(
            lambda: network0.qcState.getRootBlockTip() == qcState1.getRootBlockTip())
        assert_true_with_timeout(
            lambda: network0.qcState.getShardTip(0) == qcState1.getShardTip(0))
        assert_true_with_timeout(
            lambda: network0.qcState.getShardTip(1) == qcState1.getShardTip(1))

        network0.shutdown()
        network1.shutdown()

    def testLongerChainOverrideShorterChainAndMinorBlocks(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))
        self.assertIsNotNone(peer)

        self.addNewBlocks(qcState1, quarkash=1)

        for i in range(10):
            self.addNewBlocks(qcState0)
            self.addNewBlocks(qcState1)

        assert(qcState0.getRootBlockTip().height == 10)
        assert(qcState0.getShardTip(0).height == 20)
        assert(qcState0.getShardTip(1).height == 10)
        assert(qcState1.getRootBlockTip().height == 11)
        assert(qcState1.getShardTip(0).height == 22)
        assert(qcState1.getShardTip(1).height == 11)
        assert(qcState0.getRootBlockTip() != qcState1.getRootBlockTip())
        assert(qcState0.getShardTip(0) != qcState1.getShardTip(0))
        assert(qcState0.getShardTip(1) != qcState1.getShardTip(1))

        network1.broadcastBlockHeaders(qcState1.getRootBlockTip(), [])
        assert_true_with_timeout(
            lambda: network0.qcState.getRootBlockTip() == qcState1.getRootBlockTip())
        assert_true_with_timeout(
            lambda: network0.qcState.getShardTip(0) == qcState1.getShardTip(0))
        assert_true_with_timeout(
            lambda: network0.qcState.getShardTip(1) == qcState1.getShardTip(1))

        network0.shutdown()
        network1.shutdown()

    def testLowerHeightThanBestObservedCloseConnection(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        self.addNewBlocks(qcState1)

        # Exchange best blocks info
        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))
        self.assertIsNotNone(peer)

        oldTip = qcState0.getRootBlockTip()
        network1.broadcastBlockHeaders(qcState1.getRootBlockHeaderByHeight(0), [])
        assert_true_with_timeout(
            lambda: network0.qcState.getRootBlockTip() == oldTip)
        assert_true_with_timeout(
            lambda: peer.isClosed())

        network0.shutdown()
        network1.shutdown()

    def testEqualHeightWithBestObservedButDifferentHeaderCloseConnection(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        self.addNewBlocks(qcState0)
        self.addNewBlocks(qcState1, quarkash=1)

        # Exchange best blocks info
        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))
        self.assertIsNotNone(peer)

        oldTip = qcState0.getRootBlockTip()

        network1.broadcastBlockHeaders(qcState0.getRootBlockTip(), [])
        assert_true_with_timeout(
            lambda: network0.qcState.getRootBlockTip() == oldTip)
        assert_true_with_timeout(
            lambda: peer.isClosed())

        network0.shutdown()
        network1.shutdown()

    def testEqualHeightWithBestObservedWithoutMinorBlockCloseConnection(self):
        env0, qcState0, network0 = create_network()
        env1, qcState1, network1 = create_network()

        # Exchange best blocks info
        peer = call_async(network1.connect("127.0.0.1", env0.config.P2P_SERVER_PORT))
        self.assertIsNotNone(peer)

        oldTip = qcState0.getRootBlockTip()
        network1.broadcastBlockHeaders(qcState1.getRootBlockTip(), [])
        assert_true_with_timeout(
            lambda: network0.qcState.getRootBlockTip() == oldTip)
        assert_true_with_timeout(
            lambda: peer.isClosed())

        network0.shutdown()
        network1.shutdown()
