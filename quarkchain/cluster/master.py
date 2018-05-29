import argparse
import asyncio
import ipaddress
import json
import psutil
import random
from collections import deque

from quarkchain.config import DEFAULT_ENV
from quarkchain.cluster.rpc import ConnectToSlavesRequest, ClusterOp, CLUSTER_OP_SERIALIZER_MAP, Ping, SlaveInfo
from quarkchain.cluster.rpc import (
    AddMinorBlockHeaderResponse, GetEcoInfoListRequest,
    GetNextBlockToMineRequest, GetUnconfirmedHeadersRequest,
    GetAccountDataRequest, AddTransactionRequest,
    AddRootBlockRequest, AddMinorBlockRequest,
    CreateClusterPeerConnectionRequest,
    DestroyClusterPeerConnectionCommand,
    SyncMinorBlockListRequest,
    GetMinorBlockRequest, GetTransactionRequest,
    ArtificialTxConfig,
)
from quarkchain.cluster.protocol import (
    ClusterMetadata, ClusterConnection, P2PConnection, ROOT_BRANCH, NULL_CONNECTION,
)
from quarkchain.cluster.p2p_commands import (
    CommandOp, Direction, GetRootBlockHeaderListRequest, GetRootBlockListRequest,
)
from quarkchain.core import Branch, ShardMask
from quarkchain.db import PersistentDb
from quarkchain.cluster.jsonrpc import JSONRPCServer
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.simple_network import SimpleNetwork
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

    async def sync(self):
        try:
            await self.__runSync()
        except Exception as e:
            Logger.logException()
            self.peer.closeWithError(str(e))

    async def __runSync(self):
        if self.__hasBlockHash(self.header.getHash()):
            return

        # descending height
        blockHeaderChain = [self.header]
        blockHash = self.header.hashPrevBlock

        # TODO: Stop if too many headers to revert
        while not self.__hasBlockHash(blockHash):
            blockHeaderList = await self.__downloadBlockHeaders(blockHash)
            Logger.info("[R] downloaded {} headers from peer".format(len(blockHeaderList)))
            if not self.__validateBlockHeaders(blockHeaderList):
                # TODO: tag bad peer
                return self.peer.closeWithError("Bad peer sending discontinuing block headers")
            for header in blockHeaderList:
                if self.__hasBlockHash(header.getHash()):
                    break
                blockHeaderChain.append(header)
            blockHash = blockHeaderChain[-1].hashPrevBlock

        blockHeaderChain.reverse()

        while len(blockHeaderChain) > 0:
            blockChain = await self.__downloadBlocks(blockHeaderChain[:100])
            Logger.info("[R] downloaded {} blocks from peer".format(len(blockChain)))
            if len(blockChain) != len(blockHeaderChain[:100]):
                # TODO: tag bad peer
                return self.peer.closeWithError("Bad peer missing blocks for headers they have")

            for block in blockChain:
                await self.__addBlock(block)
                blockHeaderChain.pop(0)

    def __hasBlockHash(self, blockHash):
        return self.rootState.containRootBlockByHash(blockHash)

    def __validateBlockHeaders(self, blockHeaderList):
        # TODO: check difficulty and other stuff?
        for i in range(len(blockHeaderList) - 1):
            block, prev = blockHeaderList[i:i + 2]
            if block.height != prev.height + 1:
                return False
            if block.hashPrevBlock != prev.getHash():
                return False
        return True

    async def __downloadBlockHeaders(self, blockHash):
        request = GetRootBlockHeaderListRequest(
            blockHash=blockHash,
            limit=10,
            direction=Direction.GENESIS,
        )
        op, resp, rpcId = await self.peer.writeRpcRequest(
            CommandOp.GET_ROOT_BLOCK_HEADER_LIST_REQUEST, request)
        return resp.blockHeaderList

    async def __downloadBlocks(self, blockHeaderList):
        blockHashList = [b.getHash() for b in blockHeaderList]
        op, resp, rpcId = await self.peer.writeRpcRequest(
            CommandOp.GET_ROOT_BLOCK_LIST_REQUEST, GetRootBlockListRequest(blockHashList))
        return resp.rootBlockList

    async def __addBlock(self, rootBlock):
        await self.__syncMinorBlocks(rootBlock.minorBlockHeaderList)
        await self.masterServer.addRootBlock(rootBlock)

    async def __syncMinorBlocks(self, minorBlockHeaderList):
        minorBlockDownloadMap = dict()
        for mBlockHeader in minorBlockHeaderList:
            mBlockHash = mBlockHeader.getHash()
            if not self.rootState.isMinorBlockValidated(mBlockHash):
                minorBlockDownloadMap.setdefault(mBlockHeader.branch, []).append(mBlockHash)

        futureList = []
        for branch, mBlockHashList in minorBlockDownloadMap.items():
            slaveConn = self.masterServer.getSlaveConnection(branch=branch)
            future = slaveConn.writeRpcRequest(
                op=ClusterOp.SYNC_MINOR_BLOCK_LIST_REQUEST,
                cmd=SyncMinorBlockListRequest(mBlockHashList, branch, self.peer.getClusterPeerId()),
            )
            futureList.append(future)

        resultList = await asyncio.gather(*futureList)
        for result in resultList:
            if result is Exception:
                self.peer.closeWithError(
                    "Unable to download minor blocks from root block with exception {}".format(result))
                return
            _, result, _ = result
            if result.errorCode != 0:
                self.peer.closeWithError("Unable to download minor blocks from root block")
                return


class Synchronizer:
    ''' Buffer the headers received from peer and sync one by one '''

    def __init__(self):
        self.queue = deque()
        self.running = False

    def addTask(self, header, peer):
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

    def getSlaveInfoList(self):
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

        asyncio.ensure_future(self.activeAndLoopForever())

    def getConnectionToForward(self, metadata):
        ''' Override ProxyConnection.getConnectionToForward()
        Forward traffic from slave to peer
        '''
        if metadata.clusterPeerId == 0:
            return None

        peer = self.masterServer.getPeer(metadata.clusterPeerId)
        if peer is None:
            return NULL_CONNECTION

        return peer

    def validateConnection(self, connection):
        return connection == NULL_CONNECTION or isinstance(connection, P2PConnection)

    def hasShard(self, shardId):
        for shardMask in self.shardMaskList:
            if shardMask.containShardId(shardId):
                return True
        return False

    def hasOverlap(self, shardMask):
        for localShardMask in self.shardMaskList:
            if localShardMask.hasOverlap(shardMask):
                return True
        return False

    async def sendPing(self):
        req = Ping("", [], self.masterServer.rootState.getTipBlock())
        op, resp, rpcId = await self.writeRpcRequest(
            op=ClusterOp.PING,
            cmd=req,
            metadata=ClusterMetadata(branch=ROOT_BRANCH, clusterPeerId=0))
        return (resp.id, resp.shardMaskList)

    async def sendConnectToSlaves(self, slaveInfoList):
        ''' Make slave connect to other slaves.
        Returns True on success
        '''
        req = ConnectToSlavesRequest(slaveInfoList)
        op, resp, rpcId = await self.writeRpcRequest(ClusterOp.CONNECT_TO_SLAVES_REQUEST, req)
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

    def closeWithError(self, error):
        Logger.info("Closing connection with slave {}".format(self.id))
        return super().closeWithError(error)

    async def addTransaction(self, tx):
        request = AddTransactionRequest(tx)
        _, resp, _ = await self.writeRpcRequest(
            ClusterOp.ADD_TRANSACTION_REQUEST,
            request,
        )
        return resp.errorCode == 0

    async def getMinorBlockByHash(self, blockHash, branch):
        request = GetMinorBlockRequest(branch, minorBlockHash=blockHash)
        _, resp, _ = await self.writeRpcRequest(
            ClusterOp.GET_MINOR_BLOCK_REQUEST,
            request,
        )
        if resp.errorCode != 0:
            return None
        return resp.minorBlock

    async def getMinorBlockByHeight(self, height, branch):
        request = GetMinorBlockRequest(branch, height=height)
        _, resp, _ = await self.writeRpcRequest(
            ClusterOp.GET_MINOR_BLOCK_REQUEST,
            request,
        )
        if resp.errorCode != 0:
            return None
        return resp.minorBlock

    async def getTransactionByHash(self, txHash, branch):
        request = GetTransactionRequest(txHash, branch)
        _, resp, _ = await self.writeRpcRequest(
            ClusterOp.GET_TRANSACTION_REQUEST,
            request,
        )
        if resp.errorCode != 0:
            return None, None
        return (resp.minorBlock, resp.index)

    # RPC handlers

    async def handleAddMinorBlockHeaderRequest(self, req):
        self.masterServer.rootState.addValidatedMinorBlockHash(req.minorBlockHeader.getHash())
        self.masterServer.updateShardStats(req.shardStats)
        return AddMinorBlockHeaderResponse(
            errorCode=0,
        )


OP_RPC_MAP = {
    ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST: (
        ClusterOp.ADD_MINOR_BLOCK_HEADER_RESPONSE, SlaveConnection.handleAddMinorBlockHeaderRequest),
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

        self.defaultArtificialTxConfig = ArtificialTxConfig(0, 0)
        self.artificialTxConfig = None  # None means no loadtest running
        self.synchronizer = Synchronizer()

        # branch value -> ShardStats
        self.branchToShardStats = dict()

    def __getShardSize(self):
        # TODO: replace it with dynamic size
        return self.env.config.SHARD_SIZE

    def getShardSize(self):
        return self.__getShardSize()

    def __hasAllShards(self):
        ''' Returns True if all the shards have been run by at least one node '''
        return (len(self.branchToSlaves) == self.__getShardSize() and
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

    async def __connectToSlaves(self):
        ''' Master connects to all the slaves '''
        for slaveInfo in self.clusterConfig.getSlaveInfoList():
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
            await slave.waitUntilActive()

            # Verify the slave does have the same id and shard mask list as the config file
            id, shardMaskList = await slave.sendPing()
            if id != slaveInfo.id:
                Logger.error("Slave id does not match. expect {} got {}".format(slaveInfo.id, id))
                self.shutdown()
            if shardMaskList != slaveInfo.shardMaskList:
                Logger.error("Slave {} shard mask list does not match. expect {} got {}".format(
                    slaveInfo.id, slaveInfo.shardMaskList, shardMaskList))
                self.shutdown()

            self.slavePool.add(slave)
            for shardId in range(self.__getShardSize()):
                branch = Branch.create(self.__getShardSize(), shardId)
                if slave.hasShard(shardId):
                    self.branchToSlaves.setdefault(branch.value, []).append(slave)

    async def __setupSlaveToSlaveConnections(self):
        ''' Make slaves connect to other slaves.
        Retries until success.
        '''
        for slave in self.slavePool:
            await slave.waitUntilActive()
            success = await slave.sendConnectToSlaves(self.clusterConfig.getSlaveInfoList())
            if not success:
                self.shutdown()

    def getSlaveConnection(self, branch):
        # TODO:  Support forwarding to multiple connections (for replication)
        check(len(self.branchToSlaves[branch.value]) > 0)
        return self.branchToSlaves[branch.value][0]

    def __logSummary(self):
        for branchValue, slaves in self.branchToSlaves.items():
            Logger.info("[{}] is run by slave {}".format(Branch(branchValue).getShardId(), [s.id for s in slaves]))

    async def __initCluster(self):
        await self.__connectToSlaves()
        self.__logSummary()
        if not self.__hasAllShards():
            Logger.error("Missing some shards. Check cluster config file!")
            return
        await self.__setupSlaveToSlaveConnections()

        self.clusterActiveFuture.set_result(None)

    def start(self):
        self.loop.create_task(self.__initCluster())

    def startAndLoop(self):
        self.start()
        try:
            self.loop.run_until_complete(self.shutdownFuture)
        except KeyboardInterrupt:
            pass

    def waitUntilClusterActive(self):
        # Wait until cluster is ready
        self.loop.run_until_complete(self.clusterActiveFuture)

    def shutdown(self):
        # TODO: May set exception and disconnect all slaves
        if not self.shutdownFuture.done():
            self.shutdownFuture.set_result(None)
        if not self.clusterActiveFuture.done():
            self.clusterActiveFuture.set_exception(RuntimeError("failed to start the cluster"))

    def getShutdownFuture(self):
        return self.shutdownFuture

    async def __createRootBlockToMineOrFallbackToMinorBlock(self, address):
        ''' Try to create a root block to mine or fallback to create minor block if failed proof-of-progress '''
        futures = []
        for slave in self.slavePool:
            request = GetUnconfirmedHeadersRequest()
            futures.append(slave.writeRpcRequest(ClusterOp.GET_UNCONFIRMED_HEADERS_REQUEST, request))
        responses = await asyncio.gather(*futures)

        # Slaves may run multiple copies of the same branch
        # branchValue -> HeaderList
        shardIdToHeaderList = dict()
        for response in responses:
            _, response, _ = response
            if response.errorCode != 0:
                return (None, None)
            for headersInfo in response.headersInfoList:
                if headersInfo.branch.getShardSize() != self.__getShardSize():
                    Logger.error("Expect shard size {} got {}".format(
                        self.__getShardSize(), headersInfo.branch.getShardSize()))
                    return (None, None)
                # TODO: check headers are ordered by height
                shardIdToHeaderList[headersInfo.branch.getShardId()] = headersInfo.headerList

        headerList = []
        # check proof of progress
        for shardId in range(self.__getShardSize()):
            headers = shardIdToHeaderList.get(shardId, [])
            headerList.extend(headers)
            if len(headers) < self.env.config.PROOF_OF_PROGRESS_BLOCKS:
                # Fallback to create minor block
                block = await self.__getMinorBlockToMine(Branch.create(self.__getShardSize(), shardId), address)
                return (None, None) if not block else (False, block)

        return (True, self.rootState.createBlockToMine(headerList, address))

    async def __getMinorBlockToMine(self, branch, address):
        request = GetNextBlockToMineRequest(
            branch=branch,
            address=address.addressInBranch(branch),
            artificialTxConfig=self.artificialTxConfig if self.artificialTxConfig else self.defaultArtificialTxConfig,
        )
        slave = self.getSlaveConnection(branch)
        _, response, _ = await slave.writeRpcRequest(ClusterOp.GET_NEXT_BLOCK_TO_MINE_REQUEST, request)
        return response.block if response.errorCode == 0 else None

    async def getNextBlockToMine(self, address, shardMaskValue=0, preferRoot=False, randomizeOutput=True):
        ''' Returns (isRootBlock, block)

        shardMaskValue = 0 means considering root chain and all the shards
        '''
        # Mining old blocks is useless
        if self.synchronizer.running:
            return None, None

        if preferRoot and shardMaskValue == 0:
            return await self.__createRootBlockToMineOrFallbackToMinorBlock(address)

        shardMask = None if shardMaskValue == 0 else ShardMask(shardMaskValue)
        futures = []

        # Collect EcoInfo from shards
        for slave in self.slavePool:
            if shardMask and not slave.hasOverlap(shardMask):
                continue
            request = GetEcoInfoListRequest()
            futures.append(slave.writeRpcRequest(ClusterOp.GET_ECO_INFO_LIST_REQUEST, request))
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
        maxEco = rootCoinbaseAmount / self.rootState.getNextBlockDifficulty()

        dupEcoCount = 1
        blockHeight = 0
        for branchValue, ecoInfo in branchValueToEcoInfo.items():
            if shardMask and not shardMask.containBranch(Branch(branchValue)):
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
            return await self.__createRootBlockToMineOrFallbackToMinorBlock(address)

        block = await self.__getMinorBlockToMine(Branch(branchValueWithMaxEco), address)
        return (None, None) if not block else (False, block)

    async def getAccountData(self, address):
        ''' Returns a dict where key is Branch and value is AccountBranchData '''
        futures = []
        for slave in self.slavePool:
            request = GetAccountDataRequest(address)
            futures.append(slave.writeRpcRequest(ClusterOp.GET_ACCOUNT_DATA_REQUEST, request))
        responses = await asyncio.gather(*futures)

        # Slaves may run multiple copies of the same branch
        # We only need one AccountBranchData per branch
        branchToAccountBranchData = dict()
        for response in responses:
            _, response, _ = response
            check(response.errorCode == 0)
            for accountBranchData in response.accountBranchDataList:
                branchToAccountBranchData[accountBranchData.branch] = accountBranchData

        check(len(branchToAccountBranchData) == self.__getShardSize())
        return branchToAccountBranchData

    async def getPrimaryAccountData(self, address):
        # TODO: Only query the shard who has the address
        shardId = address.getShardId(self.__getShardSize())
        branch = Branch.create(self.__getShardSize(), shardId)
        slaves = self.branchToSlaves.get(branch.value, None)
        if not slaves:
            return None
        slave = slaves[0]
        request = GetAccountDataRequest(address)
        _, resp, _ = await slave.writeRpcRequest(ClusterOp.GET_ACCOUNT_DATA_REQUEST, request)
        for accountBranchData in resp.accountBranchDataList:
            if accountBranchData.branch == branch:
                return accountBranchData
        return None

    async def addTransaction(self, tx, fromPeer=None):
        ''' Add transaction to the cluster and broadcast to peers '''
        branch = Branch(tx.code.getEvmTransaction().branchValue)
        if branch.value not in self.branchToSlaves:
            return False

        futures = []
        for slave in self.branchToSlaves[branch.value]:
            futures.append(slave.addTransaction(tx))

        success = all(await asyncio.gather(*futures))
        if not success:
            return False

        if success and self.network is not None:
            for peer in self.network.iteratePeers():
                if peer == fromPeer:
                    continue
                try:
                    peer.sendTransaction(tx)
                except Exception:
                    Logger.logException()
        return True

    def handleNewRootBlockHeader(self, header, peer):
        self.synchronizer.addTask(header, peer)

    async def addRootBlock(self, rBlock):
        ''' Add root block locally and broadcast root block to all shards and .
        All update root block should be done in serial to avoid inconsistent global root block state.
        '''
        self.rootState.validateBlock(rBlock)     # throw exception if failed
        updateTip = False
        try:
            updateTip = self.rootState.addBlock(rBlock)
            success = True
        except ValueError:
            success = False

        if success:
            futureList = self.broadcastRpc(
                op=ClusterOp.ADD_ROOT_BLOCK_REQUEST,
                req=AddRootBlockRequest(rBlock, False))
            resultList = await asyncio.gather(*futureList)
            check(all([resp.errorCode == 0 for _, resp, _ in resultList]))

        if updateTip and self.network is not None:
            for peer in self.network.iteratePeers():
                peer.sendUpdatedTip()

    async def addRawMinorBlock(self, branch, blockData):
        if branch.value not in self.branchToSlaves:
            return False

        request = AddMinorBlockRequest(blockData)
        # TODO: support multiple slaves running the same shard
        _, resp, _ = await self.getSlaveConnection(branch).writeRpcRequest(ClusterOp.ADD_MINOR_BLOCK_REQUEST, request)
        return resp.errorCode == 0

    async def addRootBlockFromMiner(self, block):
        ''' Should only be called by miner '''
        # TODO: push candidate block to miner
        if block.header.hashPrevBlock != self.rootState.tip.getHash():
            Logger.info("[R] dropped stale root block {} mined locally".format(block.header.height))
            return False
        await self.addRootBlock(block)

    def broadcastCommand(self, op, cmd):
        ''' Broadcast command to all slaves.
        '''
        for slaveConn in self.slavePool:
            slaveConn.writeCommand(
                op=op,
                cmd=cmd,
                metadata=ClusterMetadata(ROOT_BRANCH, 0))

    def broadcastRpc(self, op, req):
        ''' Broadcast RPC request to all slaves.
        '''
        futureList = []
        for slaveConn in self.slavePool:
            futureList.append(slaveConn.writeRpcRequest(
                op=op,
                cmd=req,
                metadata=ClusterMetadata(ROOT_BRANCH, 0)))
        return futureList

    # ------------------------------ Cluster Peer Connnection Management --------------
    def getPeer(self, clusterPeerId):
        if self.network is None:
            return None
        return self.network.getPeerByClusterPeerId(clusterPeerId)

    async def createPeerClusterConnections(self, clusterPeerId):
        futureList = self.broadcastRpc(
            op=ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
            req=CreateClusterPeerConnectionRequest(clusterPeerId))
        resultList = await asyncio.gather(*futureList)
        # TODO: Check resultList
        return

    def destroyPeerClusterConnections(self, clusterPeerId):
        # Broadcast connection lost to all slaves
        self.broadcastCommand(
            op=ClusterOp.DESTROY_CLUSTER_PEER_CONNECTION_COMMAND,
            cmd=DestroyClusterPeerConnectionCommand(clusterPeerId))

    def setArtificialTxConfig(self, numTxPerBlock, xShardTxPercent, seconds):
        ''' If seconds <= 0 udpate the default config and also cancel ongoing loadtest '''
        async def revertLater():
            await asyncio.sleep(seconds)
            self.artificialTxConfig = None

        newConfig = ArtificialTxConfig(numTxPerBlock, xShardTxPercent)
        if seconds > 0:
            if not self.artificialTxConfig:
                asyncio.ensure_future(revertLater())
                self.artificialTxConfig = newConfig
        else:
            self.defaultArtificialTxConfig = newConfig
            self.artificialTxConfig = None

    def updateShardStats(self, shardStats):
        self.branchToShardStats[shardStats.branch.value] = shardStats

    async def getStats(self):
        shards = [dict() for i in range(self.__getShardSize())]
        for shardStats in self.branchToShardStats.values():
            shardId = shardStats.branch.getShardId()
            shards[shardId]["height"] = shardStats.height
            shards[shardId]["timestamp"] = shardStats.timestamp
            shards[shardId]["txCount60s"] = shardStats.txCount60s
            shards[shardId]["pendingTxCount"] = shardStats.pendingTxCount
            shards[shardId]["blockCount60s"] = shardStats.blockCount60s
            shards[shardId]["staleBlockCount60s"] = shardStats.staleBlockCount60s
            shards[shardId]["lastBlockTime"] = shardStats.lastBlockTime

        txCount60s = sum([shardStats.txCount60s for shardStats in self.branchToShardStats.values()])
        blockCount60s = sum([shardStats.blockCount60s for shardStats in self.branchToShardStats.values()])
        staleBlockCount60s = sum([shardStats.staleBlockCount60s for shardStats in self.branchToShardStats.values()])
        pendingTxCount = sum([shardStats.pendingTxCount for shardStats in self.branchToShardStats.values()])
        artificialTxConfig = self.artificialTxConfig if self.artificialTxConfig else self.defaultArtificialTxConfig

        rootLastBlockTime = 0
        if self.rootState.tip.height >= 3:
            prev = self.rootState.db.getRootBlockByHash(self.rootState.tip.hashPrevBlock)
            rootLastBlockTime = self.rootState.tip.createTime - prev.header.createTime

        return {
            "shardServerCount": len(self.slavePool),
            "shardSize": self.__getShardSize(),
            "rootHeight": self.rootState.tip.height,
            "rootTimestamp": self.rootState.tip.createTime,
            "rootLastBlockTime": rootLastBlockTime,
            "txCount60s": txCount60s,
            "blockCount60s": blockCount60s,
            "staleBlockCount60s": staleBlockCount60s,
            "pendingTxCount": pendingTxCount,
            "syncing": self.synchronizer.running,
            "loadtesting": self.artificialTxConfig is not None,
            "numTxPerBlock": artificialTxConfig.numTxPerBlock,
            "xShardTxPercent": artificialTxConfig.xShardTxPercent,
            "shards": shards,
            "cpus": psutil.cpu_percent(percpu=True),
        }

    async def getMinorBlockByHash(self, blockHash, branch):
        if branch.value not in self.branchToSlaves:
            return None

        slave = self.branchToSlaves[branch.value][0]
        return await slave.getMinorBlockByHash(blockHash, branch)

    async def getMinorBlockByHeight(self, height, branch):
        if branch.value not in self.branchToSlaves:
            return None

        slave = self.branchToSlaves[branch.value][0]
        return await slave.getMinorBlockByHeight(height, branch)

    async def getTransactionByHash(self, txHash, branch):
        ''' Returns (MinorBlock, i) where i is the index of the tx in the block txList '''
        if branch.value not in self.branchToSlaves:
            return None

        slave = self.branchToSlaves[branch.value][0]
        return await slave.getTransactionByHash(txHash, branch)


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
    # Seed host which provides the list of available peers
    parser.add_argument(
        "--seed_host", default=DEFAULT_ENV.config.P2P_SEED_HOST, type=str)
    parser.add_argument(
        "--seed_port", default=DEFAULT_ENV.config.P2P_SEED_PORT, type=int)
    # Node port for intra-cluster RPC
    parser.add_argument(
        "--node_port", default=DEFAULT_ENV.clusterConfig.NODE_PORT, type=int)
    parser.add_argument(
        "--cluster_config", default="cluster_config.json", type=str)
    parser.add_argument("--in_memory_db", default=False)
    parser.add_argument("--db_path", default="./db", type=str)
    parser.add_argument("--clean", default=False, type=bool)
    parser.add_argument("--log_level", default="info", type=str)
    args = parser.parse_args()

    set_logging_level(args.log_level)

    env = DEFAULT_ENV.copy()
    env.config.P2P_SERVER_PORT = args.server_port
    env.config.P2P_SEED_HOST = args.seed_host
    env.config.P2P_SEED_PORT = args.seed_port
    env.config.LOCAL_SERVER_PORT = args.local_port
    env.config.LOCAL_SERVER_ENABLE = args.enable_local_server
    env.clusterConfig.NODE_PORT = args.node_port
    env.clusterConfig.CONFIG = ClusterConfig(json.load(open(args.cluster_config)))
    if not args.in_memory_db:
        env.db = PersistentDb(path=args.db_path, clean=args.clean)

    return env


def main():
    env = parse_args()
    env.NETWORK_ID = 1  # testnet

    rootState = RootState(env)

    master = MasterServer(env, rootState)
    master.start()
    master.waitUntilClusterActive()

    network = SimpleNetwork(env, master)
    network.start()

    jsonRpcServer = JSONRPCServer(env, master)
    jsonRpcServer.start()

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(master.shutdownFuture)
    except KeyboardInterrupt:
        pass

    jsonRpcServer.shutdown()

    Logger.info("Server is shutdown")


if __name__ == '__main__':
    main()
