import asyncio
import unittest
from quarkchain.chain import QuarkChainState
from quarkchain.simple_network import ForkResolverManager, SimpleNetwork
from quarkchain.tests.test_utils import get_test_env
from quarkchain.commands import *


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

    def testGetBlockHashListRequest(self):
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
            CommandOp.GET_BLOCK_HASH_LIST_REQUEST,
            GetBlockHashListRequest(
                isRoot=True,
                shardId=0,      # ignore
                blockHash=qcState0.getGenesisRootBlock().header.getHash(),
                maxBlocks=1024,
                direction=1,
            )))

        self.assertEqual(len(resp.blockHashList), 2)
        self.assertEqual(resp.blockHashList[0], qcState0.getGenesisRootBlock().header.getHash())
        self.assertEqual(resp.blockHashList[1], rB.header.getHash())
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())

        # Backward iteration of root chain
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HASH_LIST_REQUEST,
            GetBlockHashListRequest(
                isRoot=True,
                shardId=0,      # ignore
                blockHash=qcState0.getGenesisRootBlock().header.getHash(),
                maxBlocks=1024,
                direction=0,
            )))

        self.assertEqual(len(resp.blockHashList), 1)
        self.assertEqual(resp.blockHashList[0], qcState0.getGenesisRootBlock().header.getHash())
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())

        # Failed iteration
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HASH_LIST_REQUEST,
            GetBlockHashListRequest(
                isRoot=True,
                shardId=0,      # ignore
                blockHash=bytes(32),
                maxBlocks=1024,
                direction=0,
            )))

        self.assertEqual(len(resp.blockHashList), 0)
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())

        # Forward iteration of shard
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HASH_LIST_REQUEST,
            GetBlockHashListRequest(
                isRoot=False,
                shardId=1,
                blockHash=qcState0.getGenesisMinorBlock(1).header.getHash(),
                maxBlocks=1024,
                direction=1,
            )))

        self.assertEqual(len(resp.blockHashList), 3)
        self.assertEqual(resp.blockHashList[0], qcState0.getGenesisMinorBlock(1).header.getHash())
        self.assertEqual(resp.blockHashList[1], b2.header.getHash())
        self.assertEqual(resp.blockHashList[2], b3.header.getHash())
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())
        self.assertEqual(resp.shardTip, qcState0.getMinorBlockTip(1))

        # Limiting test
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HASH_LIST_REQUEST,
            GetBlockHashListRequest(
                isRoot=False,
                shardId=1,
                blockHash=qcState0.getGenesisMinorBlock(1).header.getHash(),
                maxBlocks=2,
                direction=1,
            )))

        self.assertEqual(len(resp.blockHashList), 2)
        self.assertEqual(resp.blockHashList[0], qcState0.getGenesisMinorBlock(1).header.getHash())
        self.assertEqual(resp.blockHashList[1], b2.header.getHash())
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())
        self.assertEqual(resp.shardTip, qcState0.getMinorBlockTip(1))

        # Backwaord iteration of shard
        op, resp, rpcId = call_async(peer.writeRpcRequest(
            CommandOp.GET_BLOCK_HASH_LIST_REQUEST,
            GetBlockHashListRequest(
                isRoot=False,
                shardId=0,
                blockHash=b1.header.getHash(),
                maxBlocks=1024,
                direction=0,
            )))

        self.assertEqual(len(resp.blockHashList), 2)
        self.assertEqual(resp.blockHashList[0], b1.header.getHash())
        self.assertEqual(resp.blockHashList[1], qcState0.getGenesisMinorBlock(0).header.getHash())
        self.assertEqual(resp.rootTip, qcState0.getRootBlockTip())
        self.assertEqual(resp.shardTip, qcState0.getMinorBlockTip(0))

        network0.shutdown()
        network1.shutdown()
