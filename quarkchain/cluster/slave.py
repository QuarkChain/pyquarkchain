import argparse
import asyncio
import errno
import ipaddress

from quarkchain.core import Branch, ShardMask
from quarkchain.config import DEFAULT_ENV
from quarkchain.cluster.core import CrossShardTransactionList, MinorBlock, RootBlock, RootBlockHeader
from quarkchain.cluster.protocol import (
    ClusterConnection, VirtualConnection, ClusterMetadata, ForwardingVirtualConnection
)
from quarkchain.cluster.rpc import ConnectToSlavesResponse, ClusterOp, CLUSTER_OP_SERIALIZER_MAP, Ping, Pong
from quarkchain.cluster.rpc import AddMinorBlockHeaderRequest
from quarkchain.cluster.rpc import (
    AddRootBlockResponse, EcoInfo, GetEcoInfoListResponse, GetNextBlockToMineResponse,
    AddMinorBlockResponse, HeadersInfo, GetUnconfirmedHeadersResponse,
    GetAccountDataResponse, AddTransactionResponse,
    CreateClusterPeerConnectionResponse,
    GetStatsResponse, SyncMinorBlockListResponse,
)
from quarkchain.cluster.rpc import AddXshardTxListRequest, AddXshardTxListResponse
from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.p2p_commands import (
    CommandOp, OP_SERIALIZER_MAP, NewMinorBlockHeaderListCommand, GetMinorBlockListRequest, GetMinorBlockListResponse,
    GetMinorBlockHeaderListRequest, Direction, GetMinorBlockHeaderListResponse,
)
from quarkchain.protocol import Connection
from quarkchain.db import PersistentDb, ShardedDb
from quarkchain.utils import check, set_logging_level, Logger


class Synchronizer:
    ''' Given a header and a shard connection, the synchronizer will synchronize
    the shard state with the peer shard up to the height of the header.
    '''

    def __init__(self, header, shardConn):
        self.header = header
        self.shardConn = shardConn
        self.shardState = shardConn.shardState
        self.slaveServer = shardConn.slaveServer

    async def sync(self):
        try:
            await self.__runSync()
        except Exception as e:
            Logger.logException()
            self.shardConn.closeWithError(str(e))

    async def __runSync(self):
        if self.__hasBlockHash(self.header.getHash()):
            return

        # descending height
        blockHeaderChain = [self.header]
        blockHash = self.header.hashPrevMinorBlock

        # TODO: Stop if too many headers to revert
        while not self.__hasBlockHash(blockHash):
            blockHeaderList = await self.__downloadBlockHeaders(blockHash)
            Logger.info("[{}] downloaded {} headers from peer".format(
                self.shardState.branch.getShardId(), len(blockHeaderList)))
            if not self.__validateBlockHeaders(blockHeaderList):
                # TODO: tag bad peer
                return self.shardConn.closeWithError("Bad peer sending discontinuing block headers")
            for header in blockHeaderList:
                if self.__hasBlockHash(header.getHash()):
                    break
                blockHeaderChain.append(header)
            blockHash = blockHeaderChain[-1].hashPrevMinorBlock

        # ascending height
        blockHeaderChain.reverse()
        while len(blockHeaderChain) > 0:
            blockChain = await self.__downloadBlocks(blockHeaderChain[:100])
            Logger.info("[{}] downloaded {} blocks from peer".format(
                self.shardState.branch.getShardId(), len(blockChain)))
            check(len(blockChain) == len(blockHeaderChain[:100]))

            for block in blockChain:
                # Stop if the block depends on an unknown root block
                # TODO: move this check to early stage to avoid downloading unnecessary headers
                # this requires moving hashPrevRootBlock to header from meta
                if not self.shardState.db.containRootBlockByHash(block.meta.hashPrevRootBlock):
                    return
                await self.slaveServer.addBlock(block)
                blockHeaderChain.pop(0)

    def __hasBlockHash(self, blockHash):
        return self.shardState.db.containMinorBlockByHash(blockHash)

    def __validateBlockHeaders(self, blockHeaderList):
        # TODO: check difficulty and other stuff?
        for i in range(len(blockHeaderList) - 1):
            block, prev = blockHeaderList[i:i + 2]
            if block.height != prev.height + 1:
                return False
            if block.hashPrevMinorBlock != prev.getHash():
                return False
        return True

    async def __downloadBlockHeaders(self, blockHash):
        request = GetMinorBlockHeaderListRequest(
            blockHash=blockHash,
            branch=self.shardState.branch,
            limit=100,
            direction=Direction.GENESIS,
        )
        op, resp, rpcId = await self.shardConn.writeRpcRequest(
            CommandOp.GET_MINOR_BLOCK_HEADER_LIST_REQUEST, request)
        return resp.blockHeaderList

    async def __downloadBlocks(self, blockHeaderList):
        blockHashList = [b.getHash() for b in blockHeaderList]
        op, resp, rpcId = await self.shardConn.writeRpcRequest(
            CommandOp.GET_MINOR_BLOCK_LIST_REQUEST, GetMinorBlockListRequest(blockHashList))
        return resp.minorBlockList


class ShardConnection(VirtualConnection):
    ''' A virtual connection between local shard and remote shard
    '''

    def __init__(self, masterConn, clusterPeerId, shardState, name=None):
        super().__init__(masterConn, OP_SERIALIZER_MAP, OP_NONRPC_MAP, OP_RPC_MAP, name=name)
        self.clusterPeerId = clusterPeerId
        self.shardState = shardState
        self.masterConn = masterConn
        self.slaveServer = masterConn.slaveServer

    def closeWithError(self, error):
        Logger.error("Closing shard connection with error {}".format(error))
        return super().closeWithError(error)

    async def handleGetMinorBlockHeaderListRequest(self, request):
        if request.branch != self.shardState.branch:
            self.closeWithError("Wrong branch from peer")
        if request.limit <= 0:
            self.closeWithError("Bad limit")
        # TODO: support tip direction
        if request.direction != Direction.GENESIS:
            self.closeWithError("Bad direction")

        blockHash = request.blockHash
        headerList = []
        for i in range(request.limit):
            header = self.shardState.db.getMinorBlockHeaderByHash(blockHash)
            headerList.append(header)
            if header.height == 0:
                break
            blockHash = header.hashPrevMinorBlock

        return GetMinorBlockHeaderListResponse(
            self.shardState.rootTip, self.shardState.headerTip, headerList)

    async def handleGetMinorBlockListRequest(self, request):
        mBlockList = []
        for mBlockHash in request.minorBlockHashList:
            mBlock = self.shardState.getBlockByHash(mBlockHash)
            if mBlock is None:
                continue
            # TODO: Check list size to make sure the resp is smaller than limit
            mBlockList.append(mBlock)

        return GetMinorBlockListResponse(mBlockList)

    async def handleNewMinorBlockHeaderListCommand(self, op, cmd, rpcId):
        # TODO:  Make sure the minor block height and root is not decreasing
        # TODO: allow multiple headers if needed
        if len(cmd.minorBlockHeaderList) != 1:
            self.closeWithError("minor block header list must have only one header")
            return
        for mHeader in cmd.minorBlockHeaderList:
            Logger.info("[{}] received new header with height {}".format(
                mHeader.branch.getShardId(), mHeader.height))
            if mHeader.branch != self.shardState.branch:
                self.closeWithError("incorrect branch")
                return

        await self.slaveServer.handleNewMinorBlockHeader(mHeader, self)

    def broadcastNewTip(self):
        # TODO: Compare local best observed and broadcast if the tip is latest
        self.writeCommand(
            op=CommandOp.NEW_MINOR_BLOCK_HEADER_LIST,
            cmd=NewMinorBlockHeaderListCommand(self.shardState.rootTip, [self.shardState.headerTip]))

    def getMetadataToWrite(self, metadata):
        ''' Override VirtualConnection.getMetadataToWrite()
        '''
        return ClusterMetadata(self.shardState.branch, self.clusterPeerId)


# P2P command definitions
OP_NONRPC_MAP = {
    CommandOp.NEW_MINOR_BLOCK_HEADER_LIST: ShardConnection.handleNewMinorBlockHeaderListCommand,
}


OP_RPC_MAP = {
    CommandOp.GET_MINOR_BLOCK_HEADER_LIST_REQUEST:
        (CommandOp.GET_MINOR_BLOCK_HEADER_LIST_RESPONSE, ShardConnection.handleGetMinorBlockHeaderListRequest),
    CommandOp.GET_MINOR_BLOCK_LIST_REQUEST:
        (CommandOp.GET_MINOR_BLOCK_LIST_RESPONSE, ShardConnection.handleGetMinorBlockListRequest),
}


class MasterConnection(ClusterConnection):

    def __init__(self, env, reader, writer, slaveServer, name=None):
        super().__init__(
            env,
            reader,
            writer,
            CLUSTER_OP_SERIALIZER_MAP,
            MASTER_OP_NONRPC_MAP,
            MASTER_OP_RPC_MAP,
            name=name)
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.slaveServer = slaveServer
        self.shardStateMap = slaveServer.shardStateMap

        asyncio.ensure_future(self.activeAndLoopForever())

        # clusterPeerId -> {branchValue -> ShardConn}
        self.vConnMap = dict()

    def getConnectionToForward(self, metadata):
        ''' Override ProxyConnection.getConnectionToForward()
        '''
        if metadata.clusterPeerId == 0:
            # Data from master
            return None

        if metadata.branch.value not in self.shardStateMap:
            self.closeWithError("incorrect forwarding branch")
            return

        connMap = self.vConnMap.get(metadata.clusterPeerId)
        if connMap is None:
            self.closeWithError("cannot find cluster peer id in vConnMap")
            return

        return connMap[metadata.branch.value].getForwardingConnection()

    def validateConnection(self, connection):
        return isinstance(connection, ForwardingVirtualConnection)

    def __getShardSize(self):
        return self.env.config.SHARD_SIZE

    def close(self):
        for clusterPeerId, connMap in self.vConnMap.items():
            for branchValue, conn in connMap.items():
                conn.getForwardingConnection().close()

        Logger.info("Lost connection with master")
        return super().close()

    def closeWithError(self, error):
        Logger.info("Closing connection with master: {}".format(error))
        return super().closeWithError(error)

    def closeConnection(self, conn):
        ''' TODO: Notify master that the connection is closed by local.
        The master should close the peer connection, and notify the other slaves that a close happens
        More hint could be provided so that the master may blacklist the peer if it is mis-behaving
        '''
        pass

    # Cluster RPC handlers

    async def handlePing(self, ping):
        self.slaveServer.initShardStates(ping.rootTip)
        return Pong(self.slaveServer.id, self.slaveServer.shardMaskList)

    async def handleConnectToSlavesRequest(self, connectToSlavesRequest):
        ''' Master sends in the slave list. Let's connect to them.
        Skip self and slaves already connected.
        '''
        resultList = []
        for slaveInfo in connectToSlavesRequest.slaveInfoList:
            if slaveInfo.id == self.slaveServer.id or slaveInfo.id in self.slaveServer.slaveIds:
                resultList.append(bytes())
                continue

            ip = str(ipaddress.ip_address(slaveInfo.ip))
            port = slaveInfo.port
            try:
                reader, writer = await asyncio.open_connection(ip, port, loop=self.loop)
            except Exception as e:
                errMsg = "Failed to connect {}:{} with exception {}".format(ip, port, e)
                Logger.info(errMsg)
                resultList.append(bytes(errMsg, "ascii"))
                continue

            slave = SlaveConnection(self.env, reader, writer, self.slaveServer, slaveInfo.id, slaveInfo.shardMaskList)
            await slave.waitUntilActive()
            # Tell the remote slave who I am
            id, shardMaskList = await slave.sendPing()
            # Verify that remote slave indeed has the id and shard mask list advertised by the master
            if id != slave.id:
                resultList.append(bytes("id does not match. expect {} got {}".format(slave.id, id), "ascii"))
                continue
            if shardMaskList != slave.shardMaskList:
                resultList.append(bytes("shard mask list does not match. expect {} got {}".format(
                    slave.shardMaskList, shardMaskList), "ascii"))
                continue

            self.slaveServer.addSlaveConnection(slave)
            resultList.append(bytes())
        return ConnectToSlavesResponse(resultList)

    # Blockchain RPC handlers

    async def handleAddRootBlockRequest(self, req):
        # TODO: handle expectSwitch
        errorCode = 0
        switched = False
        for branchValue, shardState in self.shardStateMap.items():
            try:
                switched = shardState.addRootBlock(req.rootBlock)
            except ValueError:
                # TODO: May be enum or Unix errno?
                errorCode = errno.EBADMSG
                break

        return AddRootBlockResponse(errorCode, switched)

    async def handleGetEcoInfoListRequest(self, req):
        ecoInfoList = []
        for branchValue, shardState in self.shardStateMap.items():
            ecoInfoList.append(EcoInfo(
                branch=Branch(branchValue),
                height=shardState.headerTip.height + 1,
                coinbaseAmount=shardState.getNextBlockCoinbaseAmount(),
                difficulty=shardState.getNextBlockDifficulty(),
                unconfirmedHeadersCoinbaseAmount=shardState.getUnconfirmedHeadersCoinbaseAmount(),
            ))
        return GetEcoInfoListResponse(
            errorCode=0,
            ecoInfoList=ecoInfoList,
        )

    async def handleGetNextBlockToMineRequest(self, req):
        branchValue = req.branch.value
        if branchValue not in self.shardStateMap:
            return GetNextBlockToMineResponse(errorCode=errno.EBADMSG)

        block = self.shardStateMap[branchValue].createBlockToMine(
            address=req.address,
            artificialTxCount=req.artificialTxCount,
        )
        response = GetNextBlockToMineResponse(
            errorCode=0,
            block=block,
        )
        return response

    async def handleAddMinorBlockRequest(self, req):
        try:
            block = MinorBlock.deserialize(req.minorBlockData)
        except Exception:
            Logger.warning("!@#$!@#@!")
            return AddMinorBlockResponse(
                errorCode=errno.EBADMSG,
            )
        success = await self.slaveServer.addBlock(block)
        return AddMinorBlockResponse(
            errorCode=0 if success else errno.EFAULT,
        )

    async def handleGetUnconfirmedHeaderListRequest(self, req):
        headersInfoList = []
        for branchValue, shardState in self.shardStateMap.items():
            headersInfoList.append(HeadersInfo(
                branch=Branch(branchValue),
                headerList=shardState.getUnconfirmedHeaderList(),
            ))
        return GetUnconfirmedHeadersResponse(
            errorCode=0,
            headersInfoList=headersInfoList,
        )

    async def handleAccountDataRequest(self, req):
        count = self.slaveServer.getTransactionCount(req.address)
        balance = self.slaveServer.getBalance(req.address)
        errorCode = 0
        if count is None or balance is None:
            errorCode = errno.EBADMSG
            count = -1
            balance = -1
        return GetAccountDataResponse(
            errorCode=errorCode,
            transactionCount=count,
            balance=balance,
        )

    async def handleAddTransaction(self, req):
        success = self.slaveServer.addTx(req.tx)
        return AddTransactionResponse(
            errorCode=0 if success else 1,
        )

    async def handleDestroyClusterPeerConnectionCommand(self, op, cmd, rpcId):
        if cmd.clusterPeerId not in self.vConnMap:
            Logger.error("cannot find cluster peer connection to destroy {}".format(cmd.clusterPeerId))
            return
        for branchValue, vConn in self.vConnMap[cmd.clusterPeerId].items():
            vConn.getForwardingConnection().close()
        del self.vConnMap[cmd.clusterPeerId]

    async def handleCreateClusterPeerConnectionRequest(self, req):
        if req.clusterPeerId in self.vConnMap:
            Logger.error("duplicated create cluster peer connection {}".format(req.clusterPeerId))
            return CreateClusterPeerConnectionResponse(errorCode=errno.ENOENT)

        connMap = dict()
        self.vConnMap[req.clusterPeerId] = connMap
        for branchValue, shardState in self.shardStateMap.items():
            conn = ShardConnection(
                masterConn=self,
                clusterPeerId=req.clusterPeerId,
                shardState=shardState,
                name="{}_vconn_{}".format(self.name, req.clusterPeerId))
            asyncio.ensure_future(conn.activeAndLoopForever())
            connMap[branchValue] = conn
        return CreateClusterPeerConnectionResponse(errorCode=0)

    def broadcastNewTip(self, branch):
        for clusterPeerId, connMap in self.vConnMap.items():
            if branch.value not in connMap:
                Logger.error("Cannot find branch {} in conn {}".format(branch.value, clusterPeerId))
                continue

            connMap[branch.value].broadcastNewTip()

    async def handleGetStatsRequest(self, req):
        resp = GetStatsResponse(
            errorCode=0,
            shardStatsList=self.slaveServer.getStats(),
        )
        return resp

    async def handleSyncMinorBlockListRequest(self, req):

        async def __downloadBlocks(blockHashList):
            op, resp, rpcId = await vConn.writeRpcRequest(
                CommandOp.GET_MINOR_BLOCK_LIST_REQUEST, GetMinorBlockListRequest(blockHashList))
            return resp.minorBlockList

        if req.clusterPeerId not in self.vConnMap:
            return SyncMinorBlockListResponse(errorCode=errno.EBADMSG)
        if req.branch.value not in self.vConnMap[req.clusterPeerId]:
            return SyncMinorBlockListResponse(errorCode=errno.EBADMSG)

        vConn = self.vConnMap[req.clusterPeerId][req.branch.value]

        try:
            blockHashList = req.minorBlockHashList
            while len(blockHashList) > 0:
                blockChain = await __downloadBlocks(blockHashList[:100])
                Logger.info("[{}] handling sync request from master ... downloaded {} blocks from peer".format(
                    req.branch.getShardId(), len(blockChain)))
                check(len(blockChain) == len(blockHashList[:100]))

                for block in blockChain:
                    await self.slaveServer.addBlock(block)
                    blockHashList.pop(0)
        except Exception as e:
            Logger.errorException()
            return SyncMinorBlockListResponse(errorCode=1)

        return SyncMinorBlockListResponse(errorCode=0)


MASTER_OP_NONRPC_MAP = {
    ClusterOp.DESTROY_CLUSTER_PEER_CONNECTION_COMMAND: MasterConnection.handleDestroyClusterPeerConnectionCommand,
}


MASTER_OP_RPC_MAP = {
    ClusterOp.CONNECT_TO_SLAVES_REQUEST:
        (ClusterOp.CONNECT_TO_SLAVES_RESPONSE, MasterConnection.handleConnectToSlavesRequest),
    ClusterOp.PING:
        (ClusterOp.PONG, MasterConnection.handlePing),
    ClusterOp.ADD_ROOT_BLOCK_REQUEST:
        (ClusterOp.ADD_ROOT_BLOCK_RESPONSE, MasterConnection.handleAddRootBlockRequest),
    ClusterOp.GET_ECO_INFO_LIST_REQUEST:
        (ClusterOp.GET_ECO_INFO_LIST_RESPONSE, MasterConnection.handleGetEcoInfoListRequest),
    ClusterOp.GET_NEXT_BLOCK_TO_MINE_REQUEST:
        (ClusterOp.GET_NEXT_BLOCK_TO_MINE_RESPONSE, MasterConnection.handleGetNextBlockToMineRequest),
    ClusterOp.ADD_MINOR_BLOCK_REQUEST:
        (ClusterOp.ADD_MINOR_BLOCK_RESPONSE, MasterConnection.handleAddMinorBlockRequest),
    ClusterOp.GET_UNCONFIRMED_HEADERS_REQUEST:
        (ClusterOp.GET_UNCONFIRMED_HEADERS_RESPONSE, MasterConnection.handleGetUnconfirmedHeaderListRequest),
    ClusterOp.GET_ACCOUNT_DATA_REQUEST:
        (ClusterOp.GET_ACCOUNT_DATA_RESPONSE, MasterConnection.handleAccountDataRequest),
    ClusterOp.ADD_TRANSACTION_REQUEST:
        (ClusterOp.ADD_TRANSACTION_RESPONSE, MasterConnection.handleAddTransaction),
    ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST:
        (ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_RESPONSE, MasterConnection.handleCreateClusterPeerConnectionRequest),
    ClusterOp.GET_STATS_REQUEST:
        (ClusterOp.GET_STATS_RESPONSE, MasterConnection.handleGetStatsRequest),
    ClusterOp.SYNC_MINOR_BLOCK_LIST_REQUEST:
        (ClusterOp.SYNC_MINOR_BLOCK_LIST_RESPONSE, MasterConnection.handleSyncMinorBlockListRequest),
}


class SlaveConnection(Connection):

    def __init__(self, env, reader, writer, slaveServer, slaveId, shardMaskList, name=None):
        super().__init__(env, reader, writer, CLUSTER_OP_SERIALIZER_MAP, SLAVE_OP_NONRPC_MAP, SLAVE_OP_RPC_MAP, name=name)
        self.slaveServer = slaveServer
        self.id = slaveId
        self.shardMaskList = shardMaskList
        self.shardStateMap = self.slaveServer.shardStateMap

        asyncio.ensure_future(self.activeAndLoopForever())

    def __getShardSize(self):
        return self.slaveServer.env.config.SHARD_SIZE

    def hasShard(self, shardId):
        for shardMask in self.shardMaskList:
            if shardMask.containShardId(shardId):
                return True
        return False

    def closeWithError(self, error):
        Logger.info("Closing connection with slave {}".format(self.id))
        return super().closeWithError(error)

    async def sendPing(self):
        # TODO: Send real root tip and allow shards to confirm each other
        req = Ping(self.slaveServer.id, self.slaveServer.shardMaskList, RootBlock(RootBlockHeader()))
        op, resp, rpcId = await self.writeRpcRequest(ClusterOp.PING, req)
        return (resp.id, resp.shardMaskList)

    # Cluster RPC handlers

    async def handlePing(self, ping):
        if not self.id:
            self.id = ping.id
            self.shardMaskList = ping.shardMaskList
            self.slaveServer.addSlaveConnection(self)
        if len(self.shardMaskList) == 0:
            return self.closeWithError("Empty shard mask list from slave {}".format(self.id))

        return Pong(self.slaveServer.id, self.slaveServer.shardMaskList)

    # Blockchain RPC handlers

    async def handleAddXshardTxListRequest(self, req):
        if req.branch.getShardSize() != self.__getShardSize():
            Logger.error(
                "add xshard tx list request shard size mismatch! "
                "Expect: {}, actual: {}".format(self.__getShardSize(), req.branch.getShardSize()))
            return AddXshardTxListResponse(errorCode=errno.ESRCH)

        if req.branch.value not in self.shardStateMap:
            Logger.error("cannot find shard id {} locally".format(req.branch.getShardId()))
            return AddXshardTxListResponse(errorCode=errno.ENOENT)

        self.shardStateMap[req.branch.value].addCrossShardTxListByMinorBlockHash(req.minorBlockHash, req.txList)
        return AddXshardTxListResponse(errorCode=0)


SLAVE_OP_NONRPC_MAP = {}


SLAVE_OP_RPC_MAP = {
    ClusterOp.PING:
        (ClusterOp.PONG, SlaveConnection.handlePing),
    ClusterOp.ADD_XSHARD_TX_LIST_REQUEST:
        (ClusterOp.ADD_XSHARD_TX_LIST_RESPONSE, SlaveConnection.handleAddXshardTxListRequest)
}


class SlaveServer():
    """ Slave node in a cluster """

    def __init__(self, env, name="slave"):
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.id = self.env.clusterConfig.ID
        self.shardMaskList = self.env.clusterConfig.SHARD_MASK_LIST

        # shard id -> a list of slave running the shard
        self.shardToSlaves = [[] for i in range(self.__getShardSize())]
        self.slaveConnections = set()
        self.slaveIds = set()

        self.master = None
        self.name = name

        self.__initShardStateMap()
        self.shutdownInProgress = False
        self.slaveId = 0

        self.shardSynchronizerMap = dict()

    def __initShardStateMap(self):
        ''' branchValue -> ShardState mapping '''
        shardSize = self.__getShardSize()
        self.shardStateMap = dict()
        branchValues = set()
        for shardMask in self.shardMaskList:
            for shardId in shardMask.iterate(shardSize):
                branchValue = shardId + shardSize
                branchValues.add(branchValue)

        for branchValue in branchValues:
            self.shardStateMap[branchValue] = ShardState(
                env=self.env,
                shardId=Branch(branchValue).getShardId(),
                db=ShardedDb(
                    db=self.env.db,
                    fullShardId=branchValue,
                )
            )

    def initShardStates(self, rootTip):
        ''' Will be called when master connects to slaves '''
        for _, shardState in self.shardStateMap.items():
            shardState.initFromRootBlock(rootTip)

    def __getShardSize(self):
        return self.env.config.SHARD_SIZE

    def addSlaveConnection(self, slave):
        self.slaveIds.add(slave.id)
        self.slaveConnections.add(slave)
        for shardId in range(self.__getShardSize()):
            if slave.hasShard(shardId):
                self.shardToSlaves[shardId].append(slave)

        self.__logSummary()

    def __logSummary(self):
        for shardId, slaves in enumerate(self.shardToSlaves):
            Logger.info("[{}] is run by slave {}".format(shardId, [s.id for s in slaves]))

    async def __handleMasterConnectionLost(self):
        check(self.master is not None)
        await self.waitUntilClose()

        if not self.shutdownInProgress:
            # TODO: May reconnect
            self.shutdown()

    async def __handleNewConnection(self, reader, writer):
        # The first connection should always come from master
        if not self.master:
            self.master = MasterConnection(self.env, reader, writer, self, name="{}_master".format(self.name))
            return

        self.slaveId += 1
        self.slaveConnections.add(SlaveConnection(
            self.env,
            reader,
            writer,
            self,
            None,
            None,
            name="{}_slave_{}".format(self.name, self.slaveId)))

    async def __startServer(self):
        ''' Run the server until shutdown is called '''
        self.server = await asyncio.start_server(
            self.__handleNewConnection, "0.0.0.0", self.env.clusterConfig.NODE_PORT, loop=self.loop)
        Logger.info("Listening on {} for intra-cluster RPC".format(
            self.server.sockets[0].getsockname()))

    def start(self):
        self.loop.create_task(self.__startServer())

    def startAndLoop(self):
        self.start()
        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass
        self.shutdown()

    def shutdown(self):
        self.shutdownInProgress = True
        if self.master is not None:
            self.master.close()
        for slave in self.slaveConnections:
            slave.close()
        self.server.close()

    def getShutdownFuture(self):
        return self.server.wait_closed()

    # Blockchain functions

    async def handleNewMinorBlockHeader(self, header, shardConn):

        async def __sync():
            ''' Only allow one synchronizer running at a time for a shard'''
            if header.branch.value in self.shardSynchronizerMap:
                return
            synchronizer = Synchronizer(header, shardConn)
            self.shardSynchronizerMap[header.branch.value] = synchronizer
            await synchronizer.sync()
            del self.shardSynchronizerMap[header.branch.value]

        asyncio.ensure_future(__sync())

    async def sendMinorBlockHeaderToMaster(self, minorBlockHeader):
        ''' Update master that a minor block has been appended successfully '''
        request = AddMinorBlockHeaderRequest(minorBlockHeader)
        _, resp, _ = await self.master.writeRpcRequest(ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST, request)
        check(resp.errorCode == 0)

    async def broadcastXshardTxList(self, block, xshardTxList):
        ''' Broadcast x-shard transactions to their recipient shards '''

        xshardMap = dict()
        # TODO: Only broadcast to neighbors
        for shardId in range(self.__getShardSize()):
            xshardMap[shardId + self.__getShardSize()] = []

        for xshardTx in xshardTxList:
            shardId = xshardTx.address.getShardId(self.__getShardSize())
            branchValue = Branch.create(self.__getShardSize(), shardId).value
            xshardMap[branchValue].append(xshardTx)

        blockHash = block.header.getHash()
        rpcFutures = []
        for branchValue, txList in xshardMap.items():
            crossShardTxList = CrossShardTransactionList(txList)
            if branchValue in self.shardStateMap:
                self.shardStateMap[branchValue].addCrossShardTxListByMinorBlockHash(blockHash, crossShardTxList)

            branch = Branch(branchValue)
            request = AddXshardTxListRequest(branch, blockHash, crossShardTxList)

            for slaveConn in self.shardToSlaves[branch.getShardId()]:
                future = slaveConn.writeRpcRequest(ClusterOp.ADD_XSHARD_TX_LIST_REQUEST, request)
                rpcFutures.append(future)
        responses = await asyncio.gather(*rpcFutures)
        check(all([response.errorCode == 0 for _, response, _ in responses]))

    async def addBlock(self, block):
        branchValue = block.header.branch.value
        if branchValue not in self.shardStateMap:
            return False
        try:
            updateTip = self.shardStateMap[branchValue].addBlock(block)
        except Exception as e:
            Logger.errorException()
            return False
        await self.broadcastXshardTxList(block, self.shardStateMap[branchValue].evmState.xshard_list)
        await self.sendMinorBlockHeaderToMaster(block.header)

        if updateTip:
            self.master.broadcastNewTip(block.header.branch)
        return True

    def addTx(self, tx):
        evmTx = tx.code.getEvmTransaction()
        if evmTx.branchValue not in self.shardStateMap:
            return False
        return self.shardStateMap[evmTx.branchValue].addTx(tx)

    def getTransactionCount(self, address):
        branch = Branch.create(self.__getShardSize(), address.getShardId(self.__getShardSize()))
        if branch.value not in self.shardStateMap:
            return None
        return self.shardStateMap[branch.value].getTransactionCount(address.recipient)

    def getBalance(self, address):
        branch = Branch.create(self.__getShardSize(), address.getShardId(self.__getShardSize()))
        if branch.value not in self.shardStateMap:
            return None
        return self.shardStateMap[branch.value].getBalance(address.recipient)

    def getStats(self):
        shardStatsList = []
        for branchValue, shardState in self.shardStateMap.items():
            shardStatsList.append(shardState.getShardStats())
        return shardStatsList


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
    # Unique Id identifying the node in the cluster
    parser.add_argument(
        "--node_id", default=DEFAULT_ENV.clusterConfig.ID, type=str)
    # Node port for intra-cluster RPC
    parser.add_argument(
        "--node_port", default=DEFAULT_ENV.clusterConfig.NODE_PORT, type=int)
    # TODO: support a list shard masks
    parser.add_argument(
        "--shard_mask", default=1, type=int)
    parser.add_argument("--in_memory_db", default=False)
    parser.add_argument("--clean", default=False)
    parser.add_argument("--db_path", default="./db", type=str)
    parser.add_argument("--log_level", default="info", type=str)
    args = parser.parse_args()

    set_logging_level(args.log_level)

    env = DEFAULT_ENV.copy()
    env.config.P2P_SERVER_PORT = args.server_port
    env.config.P2P_SEED_HOST = args.seed_host
    env.config.P2P_SEED_PORT = args.seed_port
    env.config.LOCAL_SERVER_PORT = args.local_port
    env.config.LOCAL_SERVER_ENABLE = args.enable_local_server

    env.clusterConfig.ID = bytes(args.node_id, "ascii")
    env.clusterConfig.NODE_PORT = args.node_port
    env.clusterConfig.SHARD_MASK_LIST = [ShardMask(args.shard_mask)]

    if not args.in_memory_db:
        env.db = PersistentDb(path=args.db_path, clean=args.clean)

    return env


def main():
    env = parse_args()
    env.NETWORK_ID = 1  # testnet

    # qcState = QuarkChainState(env)
    # network = SimpleNetwork(env, qcState)
    # network.start()

    slaveServer = SlaveServer(env)
    slaveServer.startAndLoop()

    Logger.info("Server is shutdown")


if __name__ == '__main__':
    main()
