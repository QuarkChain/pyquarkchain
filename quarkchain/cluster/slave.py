import argparse
import asyncio
import errno
import ipaddress
from typing import Optional, Tuple

from collections import deque

from quarkchain.core import Address, Branch, Code, ShardMask, Transaction
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import (
    CrossShardTransactionList,
    MinorBlock,
    MinorBlockHeader,
    MinorBlockMeta,
    RootBlock,
    RootBlockHeader,
    TransactionReceipt,
)
from quarkchain.cluster.protocol import (
    ClusterConnection, VirtualConnection, ClusterMetadata, ForwardingVirtualConnection,
    NULL_CONNECTION
)
from quarkchain.cluster.rpc import ConnectToSlavesResponse, ClusterOp, CLUSTER_OP_SERIALIZER_MAP, Ping, Pong, \
    ExecuteTransactionResponse, GetTransactionReceiptResponse
from quarkchain.cluster.rpc import AddMinorBlockHeaderRequest
from quarkchain.cluster.rpc import (
    AddRootBlockResponse, EcoInfo, GetEcoInfoListResponse, GetNextBlockToMineResponse,
    AddMinorBlockResponse, HeadersInfo, GetUnconfirmedHeadersResponse,
    GetAccountDataResponse, AddTransactionResponse,
    CreateClusterPeerConnectionResponse,
    SyncMinorBlockListResponse,
    GetMinorBlockResponse, GetTransactionResponse,
    AccountBranchData,
    BatchAddXshardTxListRequest,
    BatchAddXshardTxListResponse,
    MineResponse, GenTxResponse, GetTransactionListByAddressResponse,
    TransactionDetail,
)

from quarkchain.cluster.miner import Miner
from quarkchain.cluster.tx_generator import TransactionGenerator
from quarkchain.cluster.rpc import AddXshardTxListRequest, AddXshardTxListResponse
from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.p2p_commands import (
    CommandOp, OP_SERIALIZER_MAP, NewMinorBlockHeaderListCommand, GetMinorBlockListRequest, GetMinorBlockListResponse,
    GetMinorBlockHeaderListRequest, Direction, GetMinorBlockHeaderListResponse, NewTransactionListCommand
)
from quarkchain.protocol import Connection
from quarkchain.db import PersistentDb
from quarkchain.utils import check, set_logging_level, Logger


class SyncTask:
    ''' Given a header and a shard connection, the synchronizer will synchronize
    the shard state with the peer shard up to the height of the header.
    '''

    def __init__(self, header, shardConn):
        self.header = header
        self.shardConn = shardConn
        self.shardState = shardConn.shardState
        self.slaveServer = shardConn.slaveServer
        self.maxStaleness = self.shardState.env.config.MAX_STALE_MINOR_BLOCK_HEIGHT_DIFF

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

        # TODO: Stop if too many headers to revert
        while not self.__hasBlockHash(blockHeaderChain[-1].hashPrevMinorBlock):
            blockHash = blockHeaderChain[-1].hashPrevMinorBlock
            height = blockHeaderChain[-1].height - 1

            if self.shardState.headerTip.height - height > self.maxStaleness:
                Logger.warning("[{}] abort syncing due to forking at very old block {} << {}".format(
                    self.header.branch.getShardId(), height, self.shardState.headerTip.height))
                return

            if not self.shardState.db.containRootBlockByHash(blockHeaderChain[-1].hashPrevRootBlock):
                return
            Logger.info("[{}] downloading headers from {} {}".format(
                self.shardState.branch.getShardId(), height, blockHash.hex()))
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
                if not self.shardState.db.containRootBlockByHash(block.header.hashPrevRootBlock):
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


class Synchronizer:
    ''' Buffer the headers received from peer and sync one by one '''

    def __init__(self):
        self.queue = deque()
        self.running = False

    def addTask(self, header, shardConn):
        self.queue.append((header, shardConn))
        if not self.running:
            self.running = True
            asyncio.ensure_future(self.__run())

    async def __run(self):
        while len(self.queue) > 0:
            header, shardConn = self.queue.popleft()
            task = SyncTask(header, shardConn)
            await task.sync()
        self.running = False


class ShardConnection(VirtualConnection):
    ''' A virtual connection between local shard and remote shard
    '''

    def __init__(self, masterConn, clusterPeerId, shardState, name=None):
        super().__init__(masterConn, OP_SERIALIZER_MAP, OP_NONRPC_MAP, OP_RPC_MAP, name=name)
        self.clusterPeerId = clusterPeerId
        self.shardState = shardState
        self.masterConn = masterConn
        self.slaveServer = masterConn.slaveServer
        self.synchronizer = Synchronizer()
        self.bestRootBlockHeaderObserved = None
        self.bestMinorBlockHeaderObserved = None

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
            header = self.shardState.db.getMinorBlockHeaderByHash(blockHash, consistencyCheck=False)
            headerList.append(header)
            if header.height == 0:
                break
            blockHash = header.hashPrevMinorBlock

        return GetMinorBlockHeaderListResponse(
            self.shardState.rootTip, self.shardState.headerTip, headerList)

    async def handleGetMinorBlockListRequest(self, request):
        mBlockList = []
        for mBlockHash in request.minorBlockHashList:
            mBlock = self.shardState.db.getMinorBlockByHash(mBlockHash, consistencyCheck=False)
            if mBlock is None:
                continue
            # TODO: Check list size to make sure the resp is smaller than limit
            mBlockList.append(mBlock)

        return GetMinorBlockListResponse(mBlockList)

    async def handleNewMinorBlockHeaderListCommand(self, op, cmd, rpcId):
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

        if self.bestRootBlockHeaderObserved:
            # check root header is not decreasing
            if cmd.rootBlockHeader.height < self.bestRootBlockHeaderObserved.height:
                return self.closeWithError("best observed root header height is decreasing {} < {}".format(
                    cmd.rootBlockHeader.height, self.bestRootBlockHeaderObserved.height))
            if cmd.rootBlockHeader.height == self.bestRootBlockHeaderObserved.height:
                if cmd.rootBlockHeader != self.bestRootBlockHeaderObserved:
                    return self.closeWithError("best observed root header changed with same height {}".format(
                        self.bestRootBlockHeaderObserved.height))

                # check minor header is not decreasing
                if mHeader.height < self.bestMinorBlockHeaderObserved.height:
                    return self.closeWithError("best observed minor header is decreasing {} < {}".format(
                        mHeader.height, self.bestMinorBlockHeaderObserved.height))

        self.bestRootBlockHeaderObserved = cmd.rootBlockHeader
        self.bestMinorBlockHeaderObserved = mHeader

        # Do not download if the new header is not higher than the current tip
        if self.shardState.headerTip.height >= mHeader.height:
            return

        self.synchronizer.addTask(mHeader, self)

    def broadcastNewTip(self):
        if self.bestRootBlockHeaderObserved:
            if self.shardState.rootTip.height < self.bestRootBlockHeaderObserved.height:
                return
            if self.shardState.rootTip == self.bestRootBlockHeaderObserved:
                if self.shardState.headerTip.height < self.bestMinorBlockHeaderObserved.height:
                    return
                if self.shardState.headerTip == self.bestMinorBlockHeaderObserved:
                    return

        self.writeCommand(
            op=CommandOp.NEW_MINOR_BLOCK_HEADER_LIST,
            cmd=NewMinorBlockHeaderListCommand(self.shardState.rootTip, [self.shardState.headerTip]))

    async def handleNewTransactionListCommand(self, opCode, cmd, rpcId):
        self.slaveServer.addTxList(cmd.transactionList, self)

    def broadcastTxList(self, txList):
        self.writeCommand(
            op=CommandOp.NEW_TRANSACTION_LIST,
            cmd=NewTransactionListCommand(txList),
        )

    def getMetadataToWrite(self, metadata):
        ''' Override VirtualConnection.getMetadataToWrite()
        '''
        return ClusterMetadata(self.shardState.branch, self.clusterPeerId)


# P2P command definitions
OP_NONRPC_MAP = {
    CommandOp.NEW_MINOR_BLOCK_HEADER_LIST: ShardConnection.handleNewMinorBlockHeaderListCommand,
    CommandOp.NEW_TRANSACTION_LIST: ShardConnection.handleNewTransactionListCommand,
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
            # Master can close the peer connection at any time
            # TODO: any way to avoid this race?
            Logger.warningEverySec("cannot find cluster peer id in vConnMap {}".format(metadata.clusterPeerId), 1)
            return NULL_CONNECTION

        return connMap[metadata.branch.value].getForwardingConnection()

    def validateConnection(self, connection):
        return connection == NULL_CONNECTION or isinstance(connection, ForwardingVirtualConnection)

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
        """
        Master sends in the slave list. Let's connect to them.
        Skip self and slaves already connected.
        """
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

    async def handleMineRequest(self, request):
        if request.mining:
            self.slaveServer.startMining(request.artificialTxConfig)
        else:
            self.slaveServer.stopMining()
        return MineResponse(errorCode=0)

    async def handleGenTxRequest(self, request):
        self.slaveServer.createTransactions(request.numTxPerShard, request.xShardPercent, request.tx)
        return GenTxResponse(errorCode=0)

    # Blockchain RPC handlers

    async def handleAddRootBlockRequest(self, req):
        # TODO: handle expectSwitch
        errorCode = 0
        switched = False
        for branchValue, shardState in self.shardStateMap.items():
            try:
                switched = shardState.addRootBlock(req.rootBlock)
            except ValueError:
                Logger.logException()
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
        )
        response = GetNextBlockToMineResponse(
            errorCode=0,
            block=block,
        )
        return response

    async def handleAddMinorBlockRequest(self, req):
        ''' For local miner to submit mined blocks through master '''
        try:
            block = MinorBlock.deserialize(req.minorBlockData)
        except Exception:
            return AddMinorBlockResponse(
                errorCode=errno.EBADMSG,
            )
        branchValue = block.header.branch.value
        shardState = self.slaveServer.shardStateMap.get(branchValue, None)
        if not shardState:
            return AddMinorBlockResponse(
                errorCode=errno.EBADMSG,
            )

        if block.header.hashPrevMinorBlock != shardState.headerTip.getHash():
            # Tip changed, don't bother creating a fork
            # TODO: push block candidate to miners than letting them pull
            Logger.info("[{}] dropped stale block {} mined locally".format(
                block.header.branch.getShardId(), block.header.height))
            return AddMinorBlockResponse(errorCode=0)

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

    async def handleGetAccountDataRequest(self, req):
        accountBranchDataList = self.slaveServer.getAccountData(req.address)
        return GetAccountDataResponse(
            errorCode=0,
            accountBranchDataList=accountBranchDataList,
        )

    async def handleAddTransaction(self, req):
        success = self.slaveServer.addTx(req.tx)
        return AddTransactionResponse(
            errorCode=0 if success else 1,
        )

    async def handleExecuteTransaction(self, req):
        res = self.slaveServer.executeTx(req.tx, req.fromAddress)
        fail = res is None
        return ExecuteTransactionResponse(
            errorCode=int(fail),
            result=res if not fail else b''
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
        activeFutures = []
        for branchValue, shardState in self.shardStateMap.items():
            conn = ShardConnection(
                masterConn=self,
                clusterPeerId=req.clusterPeerId,
                shardState=shardState,
                name="{}_vconn_{}".format(self.name, req.clusterPeerId))
            asyncio.ensure_future(conn.activeAndLoopForever())
            connMap[branchValue] = conn
            activeFutures.append(conn.activeFuture)
        # wait for all the connections to become active before return
        await asyncio.gather(*activeFutures)
        return CreateClusterPeerConnectionResponse(errorCode=0)

    def broadcastNewTip(self, branch):
        for clusterPeerId, connMap in self.vConnMap.items():
            if branch.value not in connMap:
                Logger.error("Cannot find branch {} in conn {}".format(branch.value, clusterPeerId))
                continue

            connMap[branch.value].broadcastNewTip()

    def broadcastTxList(self, branch, txList, shardConn=None):
        for clusterPeerId, connMap in self.vConnMap.items():
            if branch.value not in connMap:
                Logger.error("Cannot find branch {} in conn {}".format(branch.value, clusterPeerId))
                continue
            if shardConn == connMap[branch.value]:
                continue
            connMap[branch.value].broadcastTxList(txList)

    async def handleGetMinorBlockRequest(self, req):
        if req.minorBlockHash != bytes(32):
            block = self.slaveServer.getMinorBlockByHash(req.minorBlockHash, req.branch)
        else:
            block = self.slaveServer.getMinorBlockByHeight(req.height, req.branch)

        if not block:
            emptyBlock = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            return GetMinorBlockResponse(errorCode=1, minorBlock=emptyBlock)

        return GetMinorBlockResponse(errorCode=0, minorBlock=block)

    async def handleGetTransactionRequest(self, req):
        minorBlock, i = self.slaveServer.getTransactionByHash(req.txHash, req.branch)
        if not minorBlock:
            emptyBlock = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            return GetTransactionResponse(errorCode=1, minorBlock=emptyBlock, index=0)

        return GetTransactionResponse(errorCode=0, minorBlock=minorBlock, index=i)

    async def handleGetTransactionReceiptRequest(self, req):
        resp = self.slaveServer.getTransactionReceipt(req.txHash, req.branch)
        if not resp:
            emptyBlock = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            emptyReceipt = TransactionReceipt.createEmptyReceipt()
            return GetTransactionReceiptResponse(
                errorCode=1, minorBlock=emptyBlock, index=0, receipt=emptyReceipt)
        minorBlock, i, receipt = resp
        return GetTransactionReceiptResponse(
            errorCode=0, minorBlock=minorBlock, index=i, receipt=receipt)

    async def handleGetTransactionListByAddressRequest(self, req):
        result = self.slaveServer.getTransactionListByAddress(req.address, req.start, req.limit)
        if not result:
            return GetTransactionListByAddressResponse(
                errorCode=1,
                txList=[],
                next=b"",
            )
        return GetTransactionListByAddressResponse(
            errorCode=0,
            txList=result[0],
            next=result[1],
        )

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

        BLOCK_BATCH_SIZE = 100
        try:
            blockHashList = req.minorBlockHashList
            while len(blockHashList) > 0:
                blocksToDownload = blockHashList[:BLOCK_BATCH_SIZE]
                blockChain = await __downloadBlocks(blocksToDownload)
                Logger.info("[{}] sync request from master, downloaded {} blocks ({} - {})".format(
                    req.branch.getShardId(), len(blockChain),
                    blockChain[0].header.height, blockChain[-1].header.height))
                check(len(blockChain) == len(blocksToDownload))

                await self.slaveServer.addBlockListForSync(blockChain)
                blockHashList = blockHashList[BLOCK_BATCH_SIZE:]

        except Exception as e:
            Logger.errorException()
            return SyncMinorBlockListResponse(errorCode=1)

        return SyncMinorBlockListResponse(errorCode=0)


MASTER_OP_NONRPC_MAP = {
    ClusterOp.DESTROY_CLUSTER_PEER_CONNECTION_COMMAND: MasterConnection.handleDestroyClusterPeerConnectionCommand,
}


MASTER_OP_RPC_MAP = {
    ClusterOp.PING:
        (ClusterOp.PONG, MasterConnection.handlePing),
    ClusterOp.CONNECT_TO_SLAVES_REQUEST:
        (ClusterOp.CONNECT_TO_SLAVES_RESPONSE, MasterConnection.handleConnectToSlavesRequest),
    ClusterOp.MINE_REQUEST:
        (ClusterOp.MINE_RESPONSE, MasterConnection.handleMineRequest),
    ClusterOp.GEN_TX_REQUEST:
        (ClusterOp.GEN_TX_RESPONSE, MasterConnection.handleGenTxRequest),
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
        (ClusterOp.GET_ACCOUNT_DATA_RESPONSE, MasterConnection.handleGetAccountDataRequest),
    ClusterOp.ADD_TRANSACTION_REQUEST:
        (ClusterOp.ADD_TRANSACTION_RESPONSE, MasterConnection.handleAddTransaction),
    ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST:
        (ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_RESPONSE, MasterConnection.handleCreateClusterPeerConnectionRequest),
    ClusterOp.GET_MINOR_BLOCK_REQUEST:
        (ClusterOp.GET_MINOR_BLOCK_RESPONSE, MasterConnection.handleGetMinorBlockRequest),
    ClusterOp.GET_TRANSACTION_REQUEST:
        (ClusterOp.GET_TRANSACTION_RESPONSE, MasterConnection.handleGetTransactionRequest),
    ClusterOp.SYNC_MINOR_BLOCK_LIST_REQUEST:
        (ClusterOp.SYNC_MINOR_BLOCK_LIST_RESPONSE, MasterConnection.handleSyncMinorBlockListRequest),
    ClusterOp.EXECUTE_TRANSACTION_REQUEST:
        (ClusterOp.EXECUTE_TRANSACTION_RESPONSE, MasterConnection.handleExecuteTransaction),
    ClusterOp.GET_TRANSACTION_RECEIPT_REQUEST:
        (ClusterOp.GET_TRANSACTION_RECEIPT_RESPONSE, MasterConnection.handleGetTransactionReceiptRequest),
    ClusterOp.GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST:
        (ClusterOp.GET_TRANSACTION_LIST_BY_ADDRESS_RESPONSE, MasterConnection.handleGetTransactionListByAddressRequest)
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

    async def handleBatchAddXshardTxListRequest(self, batchRequest):
        for request in batchRequest.addXshardTxListRequestList:
            response = await self.handleAddXshardTxListRequest(request)
            if response.errorCode != 0:
                return BatchAddXshardTxListResponse(errorCode=response.errorCode)
        return BatchAddXshardTxListResponse(errorCode=0)


SLAVE_OP_NONRPC_MAP = {}


SLAVE_OP_RPC_MAP = {
    ClusterOp.PING:
        (ClusterOp.PONG, SlaveConnection.handlePing),
    ClusterOp.ADD_XSHARD_TX_LIST_REQUEST:
        (ClusterOp.ADD_XSHARD_TX_LIST_RESPONSE, SlaveConnection.handleAddXshardTxListRequest),
    ClusterOp.BATCH_ADD_XSHARD_TX_LIST_REQUEST:
        (ClusterOp.BATCH_ADD_XSHARD_TX_LIST_RESPONSE, SlaveConnection.handleBatchAddXshardTxListRequest)
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

        self.artificialTxConfig = None
        self.txGenMap = dict()
        self.minerMap = dict()  # branchValue -> Miner
        self.shardStateMap = dict()  # branchValue -> ShardState
        self.__initShards()
        self.shutdownInProgress = False
        self.slaveId = 0

        # block hash -> future (that will return when the block is fully propagated in the cluster)
        # the block that has been added locally but not have been fully propagated will have an entry here
        self.addBlockFutures = dict()

    def __initShards(self):
        ''' branchValue -> ShardState mapping '''
        shardSize = self.__getShardSize()
        branchValues = set()
        for shardMask in self.shardMaskList:
            for shardId in shardMask.iterate(shardSize):
                branchValue = shardId + shardSize
                branchValues.add(branchValue)

        for branchValue in branchValues:
            shardId = Branch(branchValue).getShardId()
            db = self.__initShardDb(shardId)
            self.shardStateMap[branchValue] = ShardState(
                env=self.env,
                shardId=shardId,
                db=db,
            )
            self.__initMiner(branchValue)
            self.txGenMap[branchValue] = TransactionGenerator(Branch(branchValue), self)

    def __initShardDb(self, shardId):
        """
        Given a shardId (*not* full shard id), create a PersistentDB or use the env.db if
        DB_PATH_ROOT is not specified in the ClusterConfig.
        """
        if self.env.clusterConfig.DB_PATH_ROOT is None:
            return self.env.db

        dbPath = "{path}/shard-{shardId}.db".format(
            path=self.env.clusterConfig.DB_PATH_ROOT,
            shardId=shardId,
        )
        return PersistentDb(dbPath, clean=self.env.clusterConfig.DB_CLEAN)

    def __initMiner(self, branchValue):
        minerAddress = self.env.config.TESTNET_MASTER_ACCOUNT.addressInBranch(Branch(branchValue))

        def __isSyncing():
            return any([vs[branchValue].synchronizer.running for vs in self.master.vConnMap.values()])

        async def __createBlock():
            # hold off mining if the shard is syncing
            while __isSyncing():
                await asyncio.sleep(0.1)

            return self.shardStateMap[branchValue].createBlockToMine(address=minerAddress)

        async def __addBlock(block):
            # Do not add block if there is a sync in progress
            if __isSyncing():
                return
            # Do not add stale block
            if self.shardStateMap[block.header.branch.value].headerTip.height >= block.header.height:
                return
            await self.addBlock(block)

        def __getTargetBlockTime():
            return self.artificialTxConfig.targetMinorBlockTime

        self.minerMap[branchValue] = Miner(
            __createBlock,
            __addBlock,
            __getTargetBlockTime,
        )

    def initShardStates(self, rootTip):
        ''' Will be called when master connects to slaves '''
        for _, shardState in self.shardStateMap.items():
            shardState.initFromRootBlock(rootTip)

    def startMining(self, artificialTxConfig):
        self.artificialTxConfig = artificialTxConfig
        for branchValue, miner in self.minerMap.items():
            Logger.info("[{}] start mining with target minor block time {} seconds".format(
                Branch(branchValue).getShardId(),
                artificialTxConfig.targetMinorBlockTime,
            ))
            miner.enable()
            miner.mineNewBlockAsync();

    def createTransactions(self, numTxPerShard, xShardPercent, tx: Transaction):
        for generator in self.txGenMap.values():
            generator.generate(numTxPerShard, xShardPercent, tx)

    def stopMining(self):
        for branchValue, miner in self.minerMap.items():
            Logger.info("[{}] stop mining".format(Branch(branchValue).getShardId()))
            miner.disable()

    def __getShardSize(self):
        return self.env.config.SHARD_SIZE

    def addSlaveConnection(self, slave):
        self.slaveIds.add(slave.id)
        self.slaveConnections.add(slave)
        for shardId in range(self.__getShardSize()):
            if slave.hasShard(shardId):
                self.shardToSlaves[shardId].append(slave)

        # self.__logSummary()

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

    async def sendMinorBlockHeaderToMaster(self, minorBlockHeader, txCount, xShardTxCount, shardStats):
        ''' Update master that a minor block has been appended successfully '''
        request = AddMinorBlockHeaderRequest(minorBlockHeader, txCount, xShardTxCount, shardStats)
        _, resp, _ = await self.master.writeRpcRequest(ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST, request)
        check(resp.errorCode == 0)
        self.artificialTxConfig = resp.artificialTxConfig

    def __getBranchToAddXshardTxListRequest(self, blockHash, xshardTxList):
        branchToAddXshardTxListRequest = dict()

        xshardMap = dict()
        for shardId in range(self.__getShardSize()):
            xshardMap[shardId + self.__getShardSize()] = []

        for xshardTx in xshardTxList:
            shardId = xshardTx.toAddress.getShardId(self.__getShardSize())
            branchValue = Branch.create(self.__getShardSize(), shardId).value
            xshardMap[branchValue].append(xshardTx)

        for branchValue, txList in xshardMap.items():
            crossShardTxList = CrossShardTransactionList(txList)

            branch = Branch(branchValue)
            request = AddXshardTxListRequest(branch, blockHash, crossShardTxList)
            branchToAddXshardTxListRequest[branch] = request

        return branchToAddXshardTxListRequest

    async def broadcastXshardTxList(self, block, xshardTxList):
        ''' Broadcast x-shard transactions to their recipient shards '''

        blockHash = block.header.getHash()
        branchToAddXshardTxListRequest = self.__getBranchToAddXshardTxListRequest(blockHash, xshardTxList)
        rpcFutures = []
        for branch, request in branchToAddXshardTxListRequest.items():
            if branch.value in self.shardStateMap:
                self.shardStateMap[branch.value].addCrossShardTxListByMinorBlockHash(blockHash, request.txList)

            for slaveConn in self.shardToSlaves[branch.getShardId()]:
                future = slaveConn.writeRpcRequest(ClusterOp.ADD_XSHARD_TX_LIST_REQUEST, request)
                rpcFutures.append(future)
        responses = await asyncio.gather(*rpcFutures)
        check(all([response.errorCode == 0 for _, response, _ in responses]))

    async def batchBroadcastXshardTxList(self, blockHashToXShardList):
        branchToAddXshardTxListRequestList = dict()
        for blockHash, xShardList in blockHashToXShardList.items():
            branchToAddXshardTxListRequest = self.__getBranchToAddXshardTxListRequest(blockHash, xShardList)
            for branch, request in branchToAddXshardTxListRequest.items():
                branchToAddXshardTxListRequestList.setdefault(branch, []).append(request)

        rpcFutures = []
        for branch, requestList in branchToAddXshardTxListRequestList.items():
            if branch.value in self.shardStateMap:
                for request in requestList:
                    self.shardStateMap[branch.value].addCrossShardTxListByMinorBlockHash(
                        request.minorBlockHash, request.txList)

            batchRequest = BatchAddXshardTxListRequest(requestList)
            for slaveConn in self.shardToSlaves[branch.getShardId()]:
                future = slaveConn.writeRpcRequest(ClusterOp.BATCH_ADD_XSHARD_TX_LIST_REQUEST, batchRequest)
                rpcFutures.append(future)
        responses = await asyncio.gather(*rpcFutures)
        check(all([response.errorCode == 0 for _, response, _ in responses]))

    async def addBlock(self, block):
        ''' Returns true if block is successfully added. False on any error. '''
        branchValue = block.header.branch.value
        shardState = self.shardStateMap.get(branchValue, None)

        if not shardState:
            return False

        oldTip = shardState.tip()
        try:
            xShardList = shardState.addBlock(block)
        except Exception as e:
            Logger.errorException()
            return False

        # block has been added to local state and let's pass to peers
        try:
            if oldTip != shardState.tip():
                self.master.broadcastNewTip(block.header.branch)
        except Exception:
            Logger.warningEverySec("broadcast tip failure", 1)

        # block already existed in local shard state
        # but might not have been propagated to other shards and master
        # let's make sure all the shards and master got it before return
        if xShardList is None:
            future = self.addBlockFutures.get(block.header.getHash(), None)
            if future:
                Logger.info("[{}] {} is being added ... waiting for it to finish".format(
                    block.header.branch.getShardId(), block.header.height))
                await future
            return True

        self.addBlockFutures[block.header.getHash()] = self.loop.create_future()

        # Start mining new one before propagating inside cluster
        # The propagation should be done by the time the new block is mined
        self.minerMap[branchValue].mineNewBlockAsync()

        await self.broadcastXshardTxList(block, xShardList)
        await self.sendMinorBlockHeaderToMaster(
            block.header, len(block.txList), len(xShardList), shardState.getShardStats())

        self.addBlockFutures[block.header.getHash()].set_result(None)
        del self.addBlockFutures[block.header.getHash()]
        return True

    async def addBlockListForSync(self, blockList):
        ''' Add blocks in batch to reduce RPCs. Will NOT broadcast to peers.

        Returns true if blocks are successfully added. False on any error.
        This function only adds blocks to local and propagate xshard list to other shards.
        It does NOT notify master because the master should already have the minor header list,
        and will add them once this function returns successfully.
        '''
        if not blockList:
            return True

        branchValue = blockList[0].header.branch.value
        shardState = self.shardStateMap.get(branchValue, None)

        if not shardState:
            return False

        existingAddBlockFutures = []
        blockHashToXShardList = dict()
        for block in blockList:
            blockHash = block.header.getHash()
            try:
                xShardList = shardState.addBlock(block)
            except Exception as e:
                Logger.errorException()
                return False

            # block already existed in local shard state
            # but might not have been propagated to other shards and master
            # let's make sure all the shards and master got it before return
            if xShardList is None:
                future = self.addBlockFutures.get(blockHash, None)
                if future:
                    existingAddBlockFutures.append(future)
            else:
                blockHashToXShardList[blockHash] = xShardList
                self.addBlockFutures[blockHash] = self.loop.create_future()

        await self.batchBroadcastXshardTxList(blockHashToXShardList)

        for blockHash in blockHashToXShardList.keys():
            self.addBlockFutures[blockHash].set_result(None)
            del self.addBlockFutures[blockHash]

        await asyncio.gather(*existingAddBlockFutures)

        return True

    def addTxList(self, txList, shardConn=None):
        if not txList:
            return
        evmTx = txList[0].code.getEvmTransaction()
        evmTx.setShardSize(self.__getShardSize())
        branchValue = evmTx.fromShardId() | self.__getShardSize()
        validTxList = []
        for tx in txList:
            if self.addTx(tx):
                validTxList.append(tx)
        if not validTxList:
            return
        self.master.broadcastTxList(Branch(branchValue), validTxList, shardConn)

    def addTx(self, tx):
        evmTx = tx.code.getEvmTransaction()
        evmTx.setShardSize(self.__getShardSize())
        branchValue = evmTx.fromShardId() | self.__getShardSize()
        shardState = self.shardStateMap.get(branchValue, None)
        if not shardState:
            return False
        return shardState.addTx(tx)

    def executeTx(self, tx, fromAddress) -> Optional[bytes]:
        evmTx = tx.code.getEvmTransaction()
        evmTx.setShardSize(self.__getShardSize())
        branchValue = evmTx.fromShardId() | self.__getShardSize()
        shardState = self.shardStateMap.get(branchValue, None)
        if not shardState:
            return False
        return shardState.executeTx(tx, fromAddress)

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

    def getAccountData(self, address):
        results = []
        for branchValue, shardState in self.shardStateMap.items():
            results.append(AccountBranchData(
                branch=Branch(branchValue),
                transactionCount=shardState.getTransactionCount(address.recipient),
                balance=shardState.getBalance(address.recipient),
                isContract=len(shardState.getCode(address.recipient)) > 0,
            ))
        return results

    def getMinorBlockByHash(self, blockHash, branch):
        if branch.value not in self.shardStateMap:
            return None

        shardState = self.shardStateMap[branch.value]
        try:
            return shardState.db.getMinorBlockByHash(blockHash, False)
        except Exception:
            return None

    def getMinorBlockByHeight(self, height, branch):
        if branch.value not in self.shardStateMap:
            return None

        shardState = self.shardStateMap[branch.value]
        return shardState.db.getMinorBlockByHeight(height)

    def getTransactionByHash(self, txHash, branch):
        if branch.value not in self.shardStateMap:
            return None

        shardState = self.shardStateMap[branch.value]
        return shardState.getTransactionByHash(txHash)

    def getTransactionReceipt(self, txHash, branch) -> Optional[Tuple[MinorBlock, int, TransactionReceipt]]:
        if branch.value not in self.shardStateMap:
            return None

        shardState = self.shardStateMap[branch.value]
        return shardState.getTransactionReceipt(txHash)

    def getTransactionListByAddress(self, address, start, limit):
        branch = Branch.create(self.__getShardSize(), address.getShardId(self.__getShardSize()))
        if branch.value not in self.shardStateMap:
            return None
        shardState = self.shardStateMap[branch.value]
        return shardState.getTransactionListByAddress(address, start, limit)


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
    parser.add_argument(
        "--enable_transaction_history", default=False, type=bool)
    parser.add_argument("--in_memory_db", default=False)
    parser.add_argument("--clean", default=False)
    parser.add_argument("--db_path_root", default="./db", type=str)
    parser.add_argument("--log_level", default="info", type=str)
    args = parser.parse_args()

    set_logging_level(args.log_level)

    env = DEFAULT_ENV.copy()
    env.config.P2P_SERVER_PORT = args.server_port
    env.config.P2P_SEED_HOST = args.seed_host
    env.config.P2P_SEED_PORT = args.seed_port
    env.config.LOCAL_SERVER_PORT = args.local_port
    env.config.LOCAL_SERVER_ENABLE = args.enable_local_server
    env.config.ENABLE_TRANSACTION_HISTORY = args.enable_transaction_history

    env.clusterConfig.ID = bytes(args.node_id, "ascii")
    env.clusterConfig.NODE_PORT = args.node_port
    env.clusterConfig.SHARD_MASK_LIST = [ShardMask(args.shard_mask)]
    env.clusterConfig.DB_PATH_ROOT = None if args.in_memory_db else args.db_path_root
    env.clusterConfig.DB_CLEAN = args.clean

    return env


def main():
    env = parse_args()

    slaveServer = SlaveServer(env)
    slaveServer.startAndLoop()

    Logger.info("Slave server is shutdown")


if __name__ == '__main__':
    main()
