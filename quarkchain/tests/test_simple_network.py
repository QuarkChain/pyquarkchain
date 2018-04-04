import asyncio
import unittest
from quarkchain.chain import QuarkChainState
from quarkchain.simple_network import Downloader, ForkResolverManager, SimpleNetwork
from quarkchain.tests.test_utils import get_test_env
from quarkchain.commands import *


class MockSimpleNetwork:

    def __init__(self, qcState):
        self.qcState = qcState


class MockDownloader:

    def __init__(self, rootBlockMap, minorBlockMap):
        self.rootBlockMap = rootBlockMap
        self.minorBlockMap = minorBlockMap

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


def build_block_map(blockList):
    mbMap = dict()
    for block in blockList:
        mbMap[block.header.getHash()] = block
    return mbMap


class TestShardFork(unittest.TestCase):

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


class TestRootFork(unittest.TestCase):

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


def call_async(coro):
    future = asyncio.ensure_future(coro)
    asyncio.get_event_loop().run_until_complete(future)
    return future.result()


class TestSimpmleNetwork(unittest.TestCase):

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

    def connect(self):
        peer = call_async(self.network1.connect("127.0.0.1", self.env0.config.P2P_SERVER_PORT))
        self.downloader = Downloader(peer)
        self.assertFalse(self.downloader.isPeerClosed())

    def setUp(self):
        self.env0, self.qcState0, self.network0 = create_network()
        env1, qcState1, self.network1 = create_network()
        self.connect()

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
        self.assertTrue(self.downloader.isPeerClosed())

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
        self.assertTrue(self.downloader.isPeerClosed())

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
        self.assertTrue(self.downloader.isPeerClosed())

    def testGetPreviousMinorBlockHeaderList(self):
        blocks0 = [self.qcState0.getGenesisMinorBlock(0)]
        blocks1 = [self.qcState0.getGenesisMinorBlock(1)]
        for i in range(5):
            b1 = blocks0[-1].createBlockToAppend().finalizeMerkleRoot()
            b2 = blocks0[-1].createBlockToAppend().finalizeMerkleRoot()
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
        self.assertTrue(self.downloader.isPeerClosed())
