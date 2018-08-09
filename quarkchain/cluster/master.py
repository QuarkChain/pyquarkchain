import argparse
import asyncio
import ipaddress
import json
import random
import socket
import time
from collections import deque
from threading import Thread
from typing import Optional

import psutil

from quarkchain.cluster.jsonrpc import JSONRPCServer
from quarkchain.cluster.miner import Miner
from quarkchain.cluster.p2p_commands import (
    CommandOp, Direction, GetRootBlockHeaderListRequest, GetRootBlockListRequest,
)
from quarkchain.cluster.protocol import (
    ClusterMetadata, ClusterConnection, P2PConnection, ROOT_BRANCH, NULL_CONNECTION,
)
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.rpc import (
    AddMinorBlockHeaderResponse, GetEcoInfoListRequest,
    GetNextBlockToMineRequest, GetUnconfirmedHeadersRequest,
    GetAccountDataRequest, AddTransactionRequest,
    AddRootBlockRequest, AddMinorBlockRequest,
    CreateClusterPeerConnectionRequest,
    DestroyClusterPeerConnectionCommand,
    SyncMinorBlockListRequest,
    GetMinorBlockRequest, GetTransactionRequest,
    ArtificialTxConfig, MineRequest, GenTxRequest,
)
from quarkchain.cluster.rpc import (
    ConnectToSlavesRequest,
    ClusterOp,
    CLUSTER_OP_SERIALIZER_MAP,
    ExecuteTransactionRequest,
    Ping,
    SlaveInfo,
    GetTransactionReceiptRequest,
    GetTransactionListByAddressRequest,
)
from quarkchain.cluster.simple_network import SimpleNetwork
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Branch, ShardMask
from quarkchain.core import Transaction
from quarkchain.db import PersistentDb
from quarkchain.p2p.p2p_network import P2PNetwork, devp2p_app
from quarkchain.utils import set_logging_level, Logger, check


class SyncTask:
    ''' Given a header and a peer, the task will synchronize the local state
    including root chain and shards with the peer up to the height of the header.
    '''

    def __init__(self, header, peer):
        self.header = header
        self.peer = peer
        self.masterServer = peer.masterServer
        self.rootState = peer.rootState
        self.maxStaleness = self.rootState.env.config.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF

    async def sync(self):
        try:
            await self.__run_sync()
        except Exception as e:
            Logger.logException()
            self.peer.close_with_error(str(e))

    async def __run_sync(self):
        """raise on any error so that sync() will close peer connection"""
        if self.__has_block_hash(self.header.get_hash()):
            return

        # descending height
        blockHeaderChain = [self.header]

        while not self.__has_block_hash(blockHeaderChain[-1].hashPrevBlock):
            blockHash = blockHeaderChain[-1].hashPrevBlock
            height = blockHeaderChain[-1].height - 1

            # abort if we have to download super old blocks
            if self.rootState.tip.height - height > self.maxStaleness:
                Logger.warning("[R] abort syncing due to forking at super old block {} << {}".format(
                    height, self.rootState.tip.height))
                return

            Logger.info("[R] downloading block header list from {} {}".format(height, blockHash.hex()))
            blockHeaderList = await self.__download_block_headers(blockHash)
            Logger.info("[R] downloaded headers from peer".format(len(blockHeaderList)))
            if not self.__validate_block_headers(blockHeaderList):
                # TODO: tag bad peer
                raise RuntimeError("Bad peer sending discontinuing block headers")
            for header in blockHeaderList:
                if self.__has_block_hash(header.get_hash()):
                    break
                blockHeaderChain.append(header)

        blockHeaderChain.reverse()

        while len(blockHeaderChain) > 0:
            Logger.info("[R] syncing from {} {}".format(
                blockHeaderChain[0].height, blockHeaderChain[0].get_hash().hex()))
            blockChain = await self.__download_blocks(blockHeaderChain[:100])
            Logger.info("[R] downloaded {} blocks from peer".format(len(blockChain)))
            if len(blockChain) != len(blockHeaderChain[:100]):
                # TODO: tag bad peer
                raise RuntimeError("Bad peer missing blocks for headers they have")

            for block in blockChain:
                await self.__add_block(block)
                blockHeaderChain.pop(0)

    def __has_block_hash(self, blockHash):
        return self.rootState.contain_root_block_by_hash(blockHash)

    def __validate_block_headers(self, blockHeaderList):
        # TODO: check difficulty and other stuff?
        for i in range(len(blockHeaderList) - 1):
            block, prev = blockHeaderList[i:i + 2]
            if block.height != prev.height + 1:
                return False
            if block.hashPrevBlock != prev.get_hash():
                return False
        return True

    async def __download_block_headers(self, blockHash):
        request = GetRootBlockHeaderListRequest(
            blockHash=blockHash,
            limit=100,
            direction=Direction.GENESIS,
        )
        op, resp, rpcId = await self.peer.write_rpc_request(
            CommandOp.GET_ROOT_BLOCK_HEADER_LIST_REQUEST, request)
        return resp.blockHeaderList

    async def __download_blocks(self, blockHeaderList):
        blockHashList = [b.get_hash() for b in blockHeaderList]
        op, resp, rpcId = await self.peer.write_rpc_request(
            CommandOp.GET_ROOT_BLOCK_LIST_REQUEST, GetRootBlockListRequest(blockHashList))
        return resp.rootBlockList

    async def __add_block(self, rootBlock):
        start = time.time()
        await self.__sync_minor_blocks(rootBlock.minorBlockHeaderList)
        await self.masterServer.add_root_block(rootBlock)
        elapse = time.time() - start
        Logger.info("[R] syncing root block {} {} took {:.2f} seconds".format(
            rootBlock.header.height, rootBlock.header.get_hash().hex(), elapse,
        ))

    async def __sync_minor_blocks(self, minorBlockHeaderList):
        minorBlockDownloadMap = dict()
        for mBlockHeader in minorBlockHeaderList:
            mBlockHash = mBlockHeader.get_hash()
            if not self.rootState.is_minor_block_validated(mBlockHash):
                minorBlockDownloadMap.setdefault(mBlockHeader.branch, []).append(mBlockHash)

        futureList = []
        for branch, mBlockHashList in minorBlockDownloadMap.items():
            slaveConn = self.masterServer.get_slave_connection(branch=branch)
            future = slaveConn.write_rpc_request(
                op=ClusterOp.SYNC_MINOR_BLOCK_LIST_REQUEST,
                cmd=SyncMinorBlockListRequest(mBlockHashList, branch, self.peer.get_cluster_peer_id()),
            )
            futureList.append(future)

        resultList = await asyncio.gather(*futureList)
        for result in resultList:
            if result is Exception:
                raise RuntimeError(
                    "Unable to download minor blocks from root block with exception {}".format(result))
            _, result, _ = result
            if result.errorCode != 0:
                raise RuntimeError("Unable to download minor blocks from root block")

        for mHeader in minorBlockHeaderList:
            self.rootState.add_validated_minor_block_hash(mHeader.get_hash())


class Synchronizer:
    ''' Buffer the headers received from peer and sync one by one '''

    def __init__(self):
        self.queue = deque()
        self.running = False

    def add_task(self, header, peer):
        self.queue.append((header, peer))
        if not self.running:
            self.running = True
            asyncio.ensure_future(self.__run())

    async def __run(self):
        while len(self.queue) > 0:
            header, peer = self.queue.popleft()
            task = SyncTask(header, peer)
            await task.sync()
        self.running = False


class ClusterConfig:

    def __init__(self, config):
        self.config = config

    def get_slave_info_list(self):
        results = []
        for slave in self.config["slaves"]:
            ip = int(ipaddress.ip_address(slave["ip"]))
            results.append(SlaveInfo(slave["id"], ip, slave["port"], [ShardMask(v) for v in slave["shard_masks"]]))
        return results


class SlaveConnection(ClusterConnection):
    OP_NONRPC_MAP = {}

    def __init__(self, env, reader, writer, masterServer, slaveId, shardMaskList, name=None):
        super().__init__(env, reader, writer, CLUSTER_OP_SERIALIZER_MAP, self.OP_NONRPC_MAP, OP_RPC_MAP, name=name)
        self.masterServer = masterServer
        self.id = slaveId
        self.shardMaskList = shardMaskList
        check(len(shardMaskList) > 0)

        asyncio.ensure_future(self.active_and_loop_forever())

    def get_connection_to_forward(self, metadata):
        ''' Override ProxyConnection.get_connection_to_forward()
        Forward traffic from slave to peer
        '''
        if metadata.clusterPeerId == 0:
            return None

        peer = self.masterServer.get_peer(metadata.clusterPeerId)
        if peer is None:
            return NULL_CONNECTION

        return peer

    def validate_connection(self, connection):
        return connection == NULL_CONNECTION or isinstance(connection, P2PConnection)

    def has_shard(self, shardId):
        for shardMask in self.shardMaskList:
            if shardMask.contain_shard_id(shardId):
                return True
        return False

    def has_overlap(self, shardMask):
        for localShardMask in self.shardMaskList:
            if localShardMask.has_overlap(shardMask):
                return True
        return False

    async def send_ping(self):
        req = Ping("", [], self.masterServer.rootState.get_tip_block())
        op, resp, rpcId = await self.write_rpc_request(
            op=ClusterOp.PING,
            cmd=req,
            metadata=ClusterMetadata(branch=ROOT_BRANCH, clusterPeerId=0))
        return (resp.id, resp.shardMaskList)

    async def send_connect_to_slaves(self, slaveInfoList):
        ''' Make slave connect to other slaves.
        Returns True on success
        '''
        req = ConnectToSlavesRequest(slaveInfoList)
        op, resp, rpcId = await self.write_rpc_request(ClusterOp.CONNECT_TO_SLAVES_REQUEST, req)
        check(len(resp.resultList) == len(slaveInfoList))
        for i, result in enumerate(resp.resultList):
            if len(result) > 0:
                Logger.info("Slave {} failed to connect to {} with error {}".format(
                    self.id, slaveInfoList[i].id, result))
                return False
        Logger.info("Slave {} connected to other slaves successfully".format(self.id))
        return True

    def close(self):
        Logger.info("Lost connection with slave {}".format(self.id))
        super().close()
        self.masterServer.shutdown()

    def close_with_error(self, error):
        Logger.info("Closing connection with slave {}".format(self.id))
        return super().close_with_error(error)

    async def add_transaction(self, tx):
        request = AddTransactionRequest(tx)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.ADD_TRANSACTION_REQUEST,
            request,
        )
        return resp.errorCode == 0

    async def execute_transaction(self, tx: Transaction, fromAddress):
        request = ExecuteTransactionRequest(tx, fromAddress)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.EXECUTE_TRANSACTION_REQUEST,
            request,
        )
        return resp.result if resp.errorCode == 0 else None

    async def get_minor_block_by_hash(self, blockHash, branch):
        request = GetMinorBlockRequest(branch, minorBlockHash=blockHash)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_MINOR_BLOCK_REQUEST,
            request,
        )
        if resp.errorCode != 0:
            return None
        return resp.minorBlock

    async def get_minor_block_by_height(self, height, branch):
        request = GetMinorBlockRequest(branch, height=height)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_MINOR_BLOCK_REQUEST,
            request,
        )
        if resp.errorCode != 0:
            return None
        return resp.minorBlock

    async def get_transaction_by_hash(self, txHash, branch):
        request = GetTransactionRequest(txHash, branch)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_TRANSACTION_REQUEST,
            request,
        )
        if resp.errorCode != 0:
            return None, None
        return resp.minorBlock, resp.index

    async def get_transaction_receipt(self, txHash, branch):
        request = GetTransactionReceiptRequest(txHash, branch)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_TRANSACTION_RECEIPT_REQUEST,
            request,
        )
        if resp.errorCode != 0:
            return None
        return resp.minorBlock, resp.index, resp.receipt

    async def get_transactions_by_address(self, address, start, limit):
        request = GetTransactionListByAddressRequest(address, start, limit)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST,
            request,
        )
        if resp.errorCode != 0:
            return None
        return resp.txList, resp.next


    # RPC handlers

    async def handle_add_minor_block_header_request(self, req):
        self.masterServer.rootState.add_validated_minor_block_hash(req.minorBlockHeader.get_hash())
        self.masterServer.update_shard_stats(req.shardStats)
        self.masterServer.update_tx_count_history(req.txCount, req.xShardTxCount, req.minorBlockHeader.createTime)
        return AddMinorBlockHeaderResponse(
            errorCode=0,
            artificialTxConfig=self.masterServer.get_artificial_tx_config(),
        )


OP_RPC_MAP = {
    ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST: (
        ClusterOp.ADD_MINOR_BLOCK_HEADER_RESPONSE, SlaveConnection.handle_add_minor_block_header_request),
}


class MasterServer():
    ''' Master node in a cluster
    It does two things to initialize the cluster:
    1. Setup connection with all the slaves in ClusterConfig
    2. Make slaves connect to each other
    '''

    def __init__(self, env, rootState, name="master"):
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.rootState = rootState
        self.network = None  # will be set by SimpleNetwork
        self.clusterConfig = env.clusterConfig.CONFIG

        # branch value -> a list of slave running the shard
        self.branchToSlaves = dict()
        self.slavePool = set()

        self.clusterActiveFuture = self.loop.create_future()
        self.shutdownFuture = self.loop.create_future()
        self.name = name

        self.artificialTxConfig = ArtificialTxConfig(
            targetRootBlockTime=self.env.config.ROOT_BLOCK_INTERVAL_SEC,
            targetMinorBlockTime=self.env.config.MINOR_BLOCK_INTERVAL_SEC,
        )
        self.synchronizer = Synchronizer()

        # branch value -> ShardStats
        self.branchToShardStats = dict()
        # (epoch in minute, txCount in the minute)
        self.txCountHistory = deque()

        self.__init_root_miner()

    def __init_root_miner(self):
        minerAddress = self.env.config.TESTNET_MASTER_ACCOUNT

        async def __create_block():
            while True:
                isRoot, block = await self.get_next_block_to_mine(address=minerAddress, shardMaskValue=0, preferRoot=True)
                if isRoot:
                    return block
                await asyncio.sleep(1)

        async def __add_block(block):
            # Root block should include latest minor block headers while it's being mined
            # This is a hack to get the latest minor block included since testnet does not check difficulty
            # TODO: fix this as it will break real PoW
            block = await __create_block()
            await self.add_root_block(block)

        def __get_target_block_time():
            return self.get_artificial_tx_config().targetRootBlockTime

        self.rootMiner = Miner(
            __create_block,
            __add_block,
            __get_target_block_time,
        )

    def __get_shard_size(self):
        # TODO: replace it with dynamic size
        return self.env.config.SHARD_SIZE

    def get_shard_size(self):
        return self.__get_shard_size()

    def get_artificial_tx_config(self):
        return self.artificialTxConfig

    def __has_all_shards(self):
        ''' Returns True if all the shards have been run by at least one node '''
        return (len(self.branchToSlaves) == self.__get_shard_size() and
                all([len(slaves) > 0 for _, slaves in self.branchToSlaves.items()]))

    async def __connect(self, ip, port):
        ''' Retries until success '''
        Logger.info("Trying to connect {}:{}".format(ip, port))
        while True:
            try:
                reader, writer = await asyncio.open_connection(ip, port, loop=self.loop)
                break
            except Exception as e:
                Logger.info("Failed to connect {} {}: {}".format(ip, port, e))
                await asyncio.sleep(self.env.clusterConfig.MASTER_TO_SLAVE_CONNECT_RETRY_DELAY)
        Logger.info("Connected to {}:{}".format(ip, port))
        return (reader, writer)

    async def __connect_to_slaves(self):
        ''' Master connects to all the slaves '''
        futures = []
        slaves = []
        for slaveInfo in self.clusterConfig.get_slave_info_list():
            ip = str(ipaddress.ip_address(slaveInfo.ip))
            reader, writer = await self.__connect(ip, slaveInfo.port)

            slave = SlaveConnection(
                self.env,
                reader,
                writer,
                self,
                slaveInfo.id,
                slaveInfo.shardMaskList,
                name="{}_slave_{}".format(self.name, slaveInfo.id))
            await slave.wait_until_active()
            futures.append(slave.send_ping())
            slaves.append(slave)

        results = await asyncio.gather(*futures)

        for slave, result in zip(slaves, results):
            # Verify the slave does have the same id and shard mask list as the config file
            id, shardMaskList = result
            if id != slave.id:
                Logger.error("Slave id does not match. expect {} got {}".format(slave.id, id))
                self.shutdown()
            if shardMaskList != slave.shardMaskList:
                Logger.error("Slave {} shard mask list does not match. expect {} got {}".format(
                    slave.id, slave.shardMaskList, shardMaskList))
                self.shutdown()

            self.slavePool.add(slave)
            for shardId in range(self.__get_shard_size()):
                branch = Branch.create(self.__get_shard_size(), shardId)
                if slave.has_shard(shardId):
                    self.branchToSlaves.setdefault(branch.value, []).append(slave)

    async def __setup_slave_to_slave_connections(self):
        ''' Make slaves connect to other slaves.
        Retries until success.
        '''
        for slave in self.slavePool:
            await slave.wait_until_active()
            success = await slave.send_connect_to_slaves(self.clusterConfig.get_slave_info_list())
            if not success:
                self.shutdown()

    async def __send_mining_config_to_slaves(self, mining):
        futures = []
        for slave in self.slavePool:
            request = MineRequest(self.get_artificial_tx_config(), mining)
            futures.append(slave.write_rpc_request(ClusterOp.MINE_REQUEST, request))
        responses = await asyncio.gather(*futures)
        check(all([resp.errorCode == 0 for _, resp, _ in responses]))

    async def start_mining(self):
        await self.__send_mining_config_to_slaves(True)
        self.rootMiner.enable()
        self.rootMiner.mine_new_block_async()
        Logger.warning("Mining started with root block time {} s, minor block time {} s".format(
            self.get_artificial_tx_config().targetRootBlockTime,
            self.get_artificial_tx_config().targetMinorBlockTime,
        ))

    async def stop_mining(self):
        await self.__send_mining_config_to_slaves(False)
        self.rootMiner.disable()
        Logger.warning("Mining stopped")

    def get_slave_connection(self, branch):
        # TODO:  Support forwarding to multiple connections (for replication)
        check(len(self.branchToSlaves[branch.value]) > 0)
        return self.branchToSlaves[branch.value][0]

    def __logSummary(self):
        for branchValue, slaves in self.branchToSlaves.items():
            Logger.info("[{}] is run by slave {}".format(Branch(branchValue).get_shard_id(), [s.id for s in slaves]))

    async def __init_cluster(self):
        await self.__connect_to_slaves()
        self.__logSummary()
        if not self.__has_all_shards():
            Logger.error("Missing some shards. Check cluster config file!")
            return
        await self.__setup_slave_to_slave_connections()

        self.clusterActiveFuture.set_result(None)

    def start(self):
        self.loop.create_task(self.__init_cluster())

    def start_and_loop(self):
        self.start()
        try:
            self.loop.run_until_complete(self.shutdownFuture)
        except KeyboardInterrupt:
            pass

    def wait_until_cluster_active(self):
        # Wait until cluster is ready
        self.loop.run_until_complete(self.clusterActiveFuture)

    def shutdown(self):
        # TODO: May set exception and disconnect all slaves
        if not self.shutdownFuture.done():
            self.shutdownFuture.set_result(None)
        if not self.clusterActiveFuture.done():
            self.clusterActiveFuture.set_exception(RuntimeError("failed to start the cluster"))

    def get_shutdown_future(self):
        return self.shutdownFuture

    async def __create_root_block_to_mine_or_fallback_to_minor_block(self, address):
        ''' Try to create a root block to mine or fallback to create minor block if failed proof-of-progress '''
        futures = []
        for slave in self.slavePool:
            request = GetUnconfirmedHeadersRequest()
            futures.append(slave.write_rpc_request(ClusterOp.GET_UNCONFIRMED_HEADERS_REQUEST, request))
        responses = await asyncio.gather(*futures)

        # Slaves may run multiple copies of the same branch
        # branchValue -> HeaderList
        shardIdToHeaderList = dict()
        for response in responses:
            _, response, _ = response
            if response.errorCode != 0:
                return (None, None)
            for headersInfo in response.headersInfoList:
                if headersInfo.branch.get_shard_size() != self.__get_shard_size():
                    Logger.error("Expect shard size {} got {}".format(
                        self.__get_shard_size(), headersInfo.branch.get_shard_size()))
                    return (None, None)

                height = 0
                for header in headersInfo.headerList:
                    # check headers are ordered by height
                    check(height == 0 or height + 1 == header.height)
                    height = header.height

                    # Filter out the ones unknown to the master
                    if not self.rootState.is_minor_block_validated(header.get_hash()):
                        break
                    shardIdToHeaderList.setdefault(headersInfo.branch.get_shard_id(), []).append(header)

        headerList = []
        # check proof of progress
        for shardId in range(self.__get_shard_size()):
            headers = shardIdToHeaderList.get(shardId, [])
            headerList.extend(headers)
            if len(headers) < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
                # Fallback to create minor block
                block = await self.__get_minor_block_to_mine(Branch.create(self.__get_shard_size(), shardId), address)
                return (None, None) if not block else (False, block)

        return (True, self.rootState.create_block_to_mine(headerList, address))

    async def __get_minor_block_to_mine(self, branch, address):
        request = GetNextBlockToMineRequest(
            branch=branch,
            address=address.address_in_branch(branch),
            artificialTxConfig=self.get_artificial_tx_config(),
        )
        slave = self.get_slave_connection(branch)
        _, response, _ = await slave.write_rpc_request(ClusterOp.GET_NEXT_BLOCK_TO_MINE_REQUEST, request)
        return response.block if response.errorCode == 0 else None

    async def get_next_block_to_mine(self, address, shardMaskValue=0, preferRoot=False, randomizeOutput=True):
        ''' Returns (isRootBlock, block)

        shardMaskValue = 0 means considering root chain and all the shards
        '''
        # Mining old blocks is useless
        if self.synchronizer.running:
            return None, None

        if preferRoot and shardMaskValue == 0:
            return await self.__create_root_block_to_mine_or_fallback_to_minor_block(address)

        shardMask = None if shardMaskValue == 0 else ShardMask(shardMaskValue)
        futures = []

        # Collect EcoInfo from shards
        for slave in self.slavePool:
            if shardMask and not slave.has_overlap(shardMask):
                continue
            request = GetEcoInfoListRequest()
            futures.append(slave.write_rpc_request(ClusterOp.GET_ECO_INFO_LIST_REQUEST, request))
        responses = await asyncio.gather(*futures)

        # Slaves may run multiple copies of the same branch
        # We only need one EcoInfo per branch
        # branchValue -> EcoInfo
        branchValueToEcoInfo = dict()
        for response in responses:
            _, response, _ = response
            if response.errorCode != 0:
                return (None, None)
            for ecoInfo in response.ecoInfoList:
                branchValueToEcoInfo[ecoInfo.branch.value] = ecoInfo

        rootCoinbaseAmount = 0
        for branchValue, ecoInfo in branchValueToEcoInfo.items():
            rootCoinbaseAmount += ecoInfo.unconfirmedHeadersCoinbaseAmount
        rootCoinbaseAmount = rootCoinbaseAmount // 2

        branchValueWithMaxEco = 0 if shardMask is None else None
        maxEco = rootCoinbaseAmount / self.rootState.get_next_block_difficulty()

        dupEcoCount = 1
        blockHeight = 0
        for branchValue, ecoInfo in branchValueToEcoInfo.items():
            if shardMask and not shardMask.contain_branch(Branch(branchValue)):
                continue
            # TODO: Obtain block reward and tx fee
            eco = ecoInfo.coinbaseAmount / ecoInfo.difficulty
            if branchValueWithMaxEco is None or eco > maxEco or \
                    (eco == maxEco and branchValueWithMaxEco > 0 and blockHeight > ecoInfo.height):
                branchValueWithMaxEco = branchValue
                maxEco = eco
                dupEcoCount = 1
                blockHeight = ecoInfo.height
            elif eco == maxEco and randomizeOutput:
                # The current block with max eco has smaller height, mine the block first
                # This should be only used during bootstrap.
                if branchValueWithMaxEco > 0 and blockHeight < ecoInfo.height:
                    continue
                dupEcoCount += 1
                if random.random() < 1 / dupEcoCount:
                    branchValueWithMaxEco = branchValue
                    maxEco = eco

        if branchValueWithMaxEco == 0:
            return await self.__create_root_block_to_mine_or_fallback_to_minor_block(address)

        block = await self.__get_minor_block_to_mine(Branch(branchValueWithMaxEco), address)
        return (None, None) if not block else (False, block)

    async def get_account_data(self, address):
        ''' Returns a dict where key is Branch and value is AccountBranchData '''
        futures = []
        for slave in self.slavePool:
            request = GetAccountDataRequest(address)
            futures.append(slave.write_rpc_request(ClusterOp.GET_ACCOUNT_DATA_REQUEST, request))
        responses = await asyncio.gather(*futures)

        # Slaves may run multiple copies of the same branch
        # We only need one AccountBranchData per branch
        branchToAccountBranchData = dict()
        for response in responses:
            _, response, _ = response
            check(response.errorCode == 0)
            for accountBranchData in response.accountBranchDataList:
                branchToAccountBranchData[accountBranchData.branch] = accountBranchData

        check(len(branchToAccountBranchData) == self.__get_shard_size())
        return branchToAccountBranchData

    async def get_primary_account_data(self, address):
        # TODO: Only query the shard who has the address
        shardId = address.get_shard_id(self.__get_shard_size())
        branch = Branch.create(self.__get_shard_size(), shardId)
        slaves = self.branchToSlaves.get(branch.value, None)
        if not slaves:
            return None
        slave = slaves[0]
        request = GetAccountDataRequest(address)
        _, resp, _ = await slave.write_rpc_request(ClusterOp.GET_ACCOUNT_DATA_REQUEST, request)
        for accountBranchData in resp.accountBranchDataList:
            if accountBranchData.branch == branch:
                return accountBranchData
        return None

    async def add_transaction(self, tx, fromPeer=None):
        ''' Add transaction to the cluster and broadcast to peers '''
        evmTx = tx.code.get_evm_transaction()
        evmTx.set_shard_size(self.__get_shard_size())
        branch = Branch.create(self.__get_shard_size(), evmTx.from_shard_id())
        if branch.value not in self.branchToSlaves:
            return False

        futures = []
        for slave in self.branchToSlaves[branch.value]:
            futures.append(slave.add_transaction(tx))

        success = all(await asyncio.gather(*futures))
        if not success:
            return False

        if self.network is not None:
            for peer in self.network.iterate_peers():
                if peer == fromPeer:
                    continue
                try:
                    peer.send_transaction(tx)
                except Exception:
                    Logger.logException()
        return True

    async def execute_transaction(self, tx: Transaction, fromAddress) -> Optional[bytes]:
        """ Execute transaction without persistence """
        evmTx = tx.code.get_evm_transaction()
        evmTx.set_shard_size(self.__get_shard_size())
        branch = Branch.create(self.__get_shard_size(), evmTx.from_shard_id())
        if branch.value not in self.branchToSlaves:
            return None

        futures = []
        for slave in self.branchToSlaves[branch.value]:
            futures.append(slave.execute_transaction(tx, fromAddress))
        responses = await asyncio.gather(*futures)
        # failed response will return as None
        success = all(r is not None for r in responses) and len(set(responses)) == 1
        if not success:
            return None

        check(len(responses) >= 1)
        return responses[0]

    def handle_new_root_block_header(self, header, peer):
        self.synchronizer.add_task(header, peer)

    async def add_root_block(self, rBlock):
        ''' Add root block locally and broadcast root block to all shards and .
        All update root block should be done in serial to avoid inconsistent global root block state.
        '''
        self.rootState.validate_block(rBlock)  # throw exception if failed
        updateTip = False
        try:
            updateTip = self.rootState.add_block(rBlock)
            success = True
        except ValueError:
            Logger.logException()
            success = False

        try:
            if updateTip and self.network is not None:
                for peer in self.network.iterate_peers():
                    peer.send_updated_tip()
        except Exception:
            pass

        if success:
            futureList = self.broadcast_rpc(
                op=ClusterOp.ADD_ROOT_BLOCK_REQUEST,
                req=AddRootBlockRequest(rBlock, False))
            resultList = await asyncio.gather(*futureList)
            check(all([resp.errorCode == 0 for _, resp, _ in resultList]))

            self.rootMiner.mine_new_block_async()


    async def add_raw_minor_block(self, branch, blockData):
        if branch.value not in self.branchToSlaves:
            return False

        request = AddMinorBlockRequest(blockData)
        # TODO: support multiple slaves running the same shard
        _, resp, _ = await self.get_slave_connection(branch).write_rpc_request(ClusterOp.ADD_MINOR_BLOCK_REQUEST, request)
        return resp.errorCode == 0

    async def add_root_block_from_miner(self, block):
        ''' Should only be called by miner '''
        # TODO: push candidate block to miner
        if block.header.hashPrevBlock != self.rootState.tip.get_hash():
            Logger.info("[R] dropped stale root block {} mined locally".format(block.header.height))
            return False
        await self.add_root_block(block)

    def broadcast_command(self, op, cmd):
        ''' Broadcast command to all slaves.
        '''
        for slaveConn in self.slavePool:
            slaveConn.write_command(
                op=op,
                cmd=cmd,
                metadata=ClusterMetadata(ROOT_BRANCH, 0))

    def broadcast_rpc(self, op, req):
        ''' Broadcast RPC request to all slaves.
        '''
        futureList = []
        for slaveConn in self.slavePool:
            futureList.append(slaveConn.write_rpc_request(
                op=op,
                cmd=req,
                metadata=ClusterMetadata(ROOT_BRANCH, 0)))
        return futureList

    # ------------------------------ Cluster Peer Connnection Management --------------
    def get_peer(self, clusterPeerId):
        if self.network is None:
            return None
        return self.network.get_peer_by_cluster_peer_id(clusterPeerId)

    async def create_peer_cluster_connections(self, clusterPeerId):
        futureList = self.broadcast_rpc(
            op=ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
            req=CreateClusterPeerConnectionRequest(clusterPeerId))
        resultList = await asyncio.gather(*futureList)
        # TODO: Check resultList
        return

    def destroy_peer_cluster_connections(self, clusterPeerId):
        # Broadcast connection lost to all slaves
        self.broadcast_command(
            op=ClusterOp.DESTROY_CLUSTER_PEER_CONNECTION_COMMAND,
            cmd=DestroyClusterPeerConnectionCommand(clusterPeerId))

    async def set_target_block_time(self, rootBlockTime, minorBlockTime):
        rootBlockTime = rootBlockTime if rootBlockTime else self.artificialTxConfig.targetRootBlockTime
        minorBlockTime = minorBlockTime if minorBlockTime else self.artificialTxConfig.targetMinorBlockTime
        self.artificialTxConfig = ArtificialTxConfig(
            targetRootBlockTime=rootBlockTime,
            targetMinorBlockTime=minorBlockTime,
        )
        await self.start_mining()

    async def set_mining(self, mining):
        if mining:
            await self.start_mining()
        else:
            await self.stop_mining()

    async def create_transactions(self, numTxPerShard, xShardPercent, tx: Transaction):
        """Create transactions and add to the network for loadtesting"""
        futures = []
        for slave in self.slavePool:
            request = GenTxRequest(numTxPerShard, xShardPercent, tx)
            futures.append(slave.write_rpc_request(ClusterOp.GEN_TX_REQUEST, request))
        responses = await asyncio.gather(*futures)
        check(all([resp.errorCode == 0 for _, resp, _ in responses]))

    def update_shard_stats(self, shardStats):
        self.branchToShardStats[shardStats.branch.value] = shardStats

    def update_tx_count_history(self, txCount, xShardTxCount, timestamp):
        ''' maintain a list of tuples of (epoch minute, tx count, xshard tx count) of 12 hours window
        Note that this is also counting transactions on forks and thus larger than if only couting the best chains. '''
        minute = int(timestamp / 60) * 60
        if len(self.txCountHistory) == 0 or self.txCountHistory[-1][0] < minute:
            self.txCountHistory.append((minute, txCount, xShardTxCount))
        else:
            old = self.txCountHistory.pop()
            self.txCountHistory.append((old[0], old[1] + txCount, old[2] + xShardTxCount))

        while len(self.txCountHistory) > 0 and self.txCountHistory[0][0] < time.time() - 3600 * 12:
            self.txCountHistory.popleft()

    async def get_stats(self):
        shards = [dict() for i in range(self.__get_shard_size())]
        for shardStats in self.branchToShardStats.values():
            shardId = shardStats.branch.get_shard_id()
            shards[shardId]["height"] = shardStats.height
            shards[shardId]["timestamp"] = shardStats.timestamp
            shards[shardId]["txCount60s"] = shardStats.txCount60s
            shards[shardId]["pendingTxCount"] = shardStats.pendingTxCount
            shards[shardId]["totalTxCount"] = shardStats.totalTxCount
            shards[shardId]["blockCount60s"] = shardStats.blockCount60s
            shards[shardId]["staleBlockCount60s"] = shardStats.staleBlockCount60s
            shards[shardId]["lastBlockTime"] = shardStats.lastBlockTime

        txCount60s = sum([shardStats.txCount60s for shardStats in self.branchToShardStats.values()])
        blockCount60s = sum([shardStats.blockCount60s for shardStats in self.branchToShardStats.values()])
        pendingTxCount = sum([shardStats.pendingTxCount for shardStats in self.branchToShardStats.values()])
        staleBlockCount60s = sum([shardStats.staleBlockCount60s for shardStats in self.branchToShardStats.values()])
        totalTxCount = sum([shardStats.totalTxCount for shardStats in self.branchToShardStats.values()])

        rootLastBlockTime = 0
        if self.rootState.tip.height >= 3:
            prev = self.rootState.db.get_root_block_by_hash(self.rootState.tip.hashPrevBlock)
            rootLastBlockTime = self.rootState.tip.createTime - prev.header.createTime

        txCountHistory = []
        for item in self.txCountHistory:
            txCountHistory.append({
                "timestamp": item[0],
                "txCount": item[1],
                "xShardTxCount": item[2],
            })

        return {
            "shardServerCount": len(self.slavePool),
            "shardSize": self.__get_shard_size(),
            "rootHeight": self.rootState.tip.height,
            "rootTimestamp": self.rootState.tip.createTime,
            "rootLastBlockTime": rootLastBlockTime,
            "txCount60s": txCount60s,
            "blockCount60s": blockCount60s,
            "staleBlockCount60s": staleBlockCount60s,
            "pendingTxCount": pendingTxCount,
            "totalTxCount": totalTxCount,
            "syncing": self.synchronizer.running,
            "mining": self.rootMiner.is_enabled(),
            "shards": shards,
            "cpus": psutil.cpu_percent(percpu=True),
            "txCountHistory": txCountHistory,
        }

    def is_syncing(self):
        return self.synchronizer.running

    def is_mining(self):
        return self.rootMiner.is_enabled()

    async def get_minor_block_by_hash(self, blockHash, branch):
        if branch.value not in self.branchToSlaves:
            return None

        slave = self.branchToSlaves[branch.value][0]
        return await slave.get_minor_block_by_hash(blockHash, branch)

    async def get_minor_block_by_height(self, height: Optional[int], branch):
        if branch.value not in self.branchToSlaves:
            return None

        slave = self.branchToSlaves[branch.value][0]
        # use latest height if not specified
        height = height if height is not None else self.branchToShardStats[branch.value].height
        return await slave.get_minor_block_by_height(height, branch)

    async def get_transaction_by_hash(self, txHash, branch):
        """ Returns (MinorBlock, i) where i is the index of the tx in the block txList """
        if branch.value not in self.branchToSlaves:
            return None

        slave = self.branchToSlaves[branch.value][0]
        return await slave.get_transaction_by_hash(txHash, branch)

    async def get_transaction_receipt(self, txHash, branch):
        if branch.value not in self.branchToSlaves:
            return None

        slave = self.branchToSlaves[branch.value][0]
        return await slave.get_transaction_receipt(txHash, branch)

    async def get_transactions_by_address(self, address, start, limit):
        branch = Branch.create(self.__get_shard_size(), address.get_shard_id(self.__get_shard_size()))
        slave = self.branchToSlaves[branch.value][0]
        return await slave.get_transactions_by_address(address, start, limit)


def parse_args():
    parser = argparse.ArgumentParser()
    # P2P port
    parser.add_argument(
        "--server_port", default=DEFAULT_ENV.config.P2P_SERVER_PORT, type=int)
    # Local port for JSON-RPC, wallet, etc
    parser.add_argument(
        "--enable_local_server", default=False, type=bool)
    parser.add_argument(
        "--local_port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    parser.add_argument(
        "--json_rpc_private_port", default=DEFAULT_ENV.config.PRIVATE_JSON_RPC_PORT, type=int)
    # Seed host which provides the list of available peers
    parser.add_argument(
        "--seed_host", default=DEFAULT_ENV.config.P2P_SEED_HOST, type=str)
    parser.add_argument(
        "--seed_port", default=DEFAULT_ENV.config.P2P_SEED_PORT, type=int)
    parser.add_argument(
        "--cluster_config", default="cluster_config.json", type=str)
    parser.add_argument(
        "--mine", default=False, type=bool)
    parser.add_argument("--in_memory_db", default=False)
    parser.add_argument("--db_path_root", default="./db", type=str)
    parser.add_argument("--clean", default=False, type=bool)
    parser.add_argument("--log_level", default="info", type=str)
    parser.add_argument("--devp2p", default=False, type=bool)
    parser.add_argument("--devp2p_ip", default='', type=str)
    parser.add_argument("--devp2p_port", default=29000, type=int)
    parser.add_argument("--devp2p_bootstrap_host", default=socket.gethostbyname(socket.gethostname()), type=str)
    parser.add_argument("--devp2p_bootstrap_port", default=29000, type=int)
    parser.add_argument("--devp2p_min_peers", default=2, type=int)
    parser.add_argument("--devp2p_max_peers", default=10, type=int)
    parser.add_argument("--devp2p_additional_bootstraps", default='', type=str)
    args = parser.parse_args()

    set_logging_level(args.log_level)

    env = DEFAULT_ENV.copy()
    env.config.P2P_SERVER_PORT = args.server_port
    env.config.P2P_SEED_HOST = args.seed_host
    env.config.P2P_SEED_PORT = args.seed_port
    # TODO: cleanup local server port
    env.config.LOCAL_SERVER_PORT = args.local_port
    env.config.LOCAL_SERVER_ENABLE = args.enable_local_server
    env.config.PUBLIC_JSON_RPC_PORT = args.local_port
    env.config.PRIVATE_JSON_RPC_PORT = args.json_rpc_private_port
    env.config.DEVP2P = args.devp2p
    env.config.DEVP2P_IP = args.devp2p_ip
    env.config.DEVP2P_PORT = args.devp2p_port
    env.config.DEVP2P_BOOTSTRAP_HOST = args.devp2p_bootstrap_host
    env.config.DEVP2P_BOOTSTRAP_PORT = args.devp2p_bootstrap_port
    env.config.DEVP2P_MIN_PEERS = args.devp2p_min_peers
    env.config.DEVP2P_MAX_PEERS = args.devp2p_max_peers
    env.config.DEVP2P_ADDITIONAL_BOOTSTRAPS = args.devp2p_additional_bootstraps
    env.clusterConfig.CONFIG = ClusterConfig(json.load(open(args.cluster_config)))

    # initialize database
    if not args.in_memory_db:
        env.db = PersistentDb(
            "{path}/master.db".format(path=args.db_path_root),
            clean=args.clean,
        )

    return env, args.mine


def main():
    env, mine = parse_args()

    rootState = RootState(env)

    master = MasterServer(env, rootState)
    master.start()
    master.wait_until_cluster_active()

    # kick off mining
    if mine:
        asyncio.ensure_future(master.start_mining())

    network = P2PNetwork(env, master) if env.config.DEVP2P else SimpleNetwork(env, master)
    network.start()

    if env.config.DEVP2P:
        thread = Thread(target = devp2p_app, args = [env, network], daemon=True)
        thread.start()

    publicJsonRpcServer = JSONRPCServer.start_public_server(env, master)
    privateJsonRpcServer = JSONRPCServer.start_private_server(env, master)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(master.shutdownFuture)
    except KeyboardInterrupt:
        pass

    publicJsonRpcServer.shutdown()
    privateJsonRpcServer.shutdown()

    Logger.info("Master server is shutdown")


if __name__ == '__main__':
    main()
