import argparse
import asyncio
import errno
import ipaddress
from collections import deque
from typing import Optional, Tuple

from quarkchain.cluster.miner import Miner
from quarkchain.cluster.p2p_commands import (
    CommandOp, OP_SERIALIZER_MAP, NewMinorBlockHeaderListCommand, GetMinorBlockListRequest, GetMinorBlockListResponse,
    GetMinorBlockHeaderListRequest, Direction, GetMinorBlockHeaderListResponse, NewTransactionListCommand
)
from quarkchain.cluster.protocol import (
    ClusterConnection, VirtualConnection, ClusterMetadata, ForwardingVirtualConnection,
    NULL_CONNECTION
)
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
)
from quarkchain.cluster.rpc import AddXshardTxListRequest, AddXshardTxListResponse
from quarkchain.cluster.rpc import ConnectToSlavesResponse, ClusterOp, CLUSTER_OP_SERIALIZER_MAP, Ping, Pong, \
    ExecuteTransactionResponse, GetTransactionReceiptResponse
from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tx_generator import TransactionGenerator
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Branch, ShardMask, Transaction
from quarkchain.core import (
    CrossShardTransactionList,
    MinorBlock,
    MinorBlockHeader,
    MinorBlockMeta,
    RootBlock,
    RootBlockHeader,
    TransactionReceipt,
)
from quarkchain.db import PersistentDb
from quarkchain.protocol import Connection
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
            await self.__run_sync()
        except Exception as e:
            Logger.logException()
            self.shardConn.close_with_error(str(e))

    async def __run_sync(self):
        if self.__has_block_hash(self.header.get_hash()):
            return

        # descending height
        blockHeaderChain = [self.header]

        # TODO: Stop if too many headers to revert
        while not self.__has_block_hash(blockHeaderChain[-1].hashPrevMinorBlock):
            blockHash = blockHeaderChain[-1].hashPrevMinorBlock
            height = blockHeaderChain[-1].height - 1

            if self.shardState.headerTip.height - height > self.maxStaleness:
                Logger.warning("[{}] abort syncing due to forking at very old block {} << {}".format(
                    self.header.branch.get_shard_id(), height, self.shardState.headerTip.height))
                return

            if not self.shardState.db.contain_root_block_by_hash(blockHeaderChain[-1].hashPrevRootBlock):
                return
            Logger.info("[{}] downloading headers from {} {}".format(
                self.shardState.branch.get_shard_id(), height, blockHash.hex()))
            blockHeaderList = await self.__download_block_headers(blockHash)
            Logger.info("[{}] downloaded {} headers from peer".format(
                self.shardState.branch.get_shard_id(), len(blockHeaderList)))
            if not self.__validate_block_headers(blockHeaderList):
                # TODO: tag bad peer
                return self.shardConn.close_with_error("Bad peer sending discontinuing block headers")
            for header in blockHeaderList:
                if self.__has_block_hash(header.get_hash()):
                    break
                blockHeaderChain.append(header)

        # ascending height
        blockHeaderChain.reverse()
        while len(blockHeaderChain) > 0:
            blockChain = await self.__download_blocks(blockHeaderChain[:100])
            Logger.info("[{}] downloaded {} blocks from peer".format(
                self.shardState.branch.get_shard_id(), len(blockChain)))
            check(len(blockChain) == len(blockHeaderChain[:100]))

            for block in blockChain:
                # Stop if the block depends on an unknown root block
                # TODO: move this check to early stage to avoid downloading unnecessary headers
                if not self.shardState.db.contain_root_block_by_hash(block.header.hashPrevRootBlock):
                    return
                await self.slaveServer.add_block(block)
                blockHeaderChain.pop(0)

    def __has_block_hash(self, blockHash):
        return self.shardState.db.contain_minor_block_by_hash(blockHash)

    def __validate_block_headers(self, blockHeaderList):
        # TODO: check difficulty and other stuff?
        for i in range(len(blockHeaderList) - 1):
            block, prev = blockHeaderList[i:i + 2]
            if block.height != prev.height + 1:
                return False
            if block.hashPrevMinorBlock != prev.get_hash():
                return False
        return True

    async def __download_block_headers(self, blockHash):
        request = GetMinorBlockHeaderListRequest(
            blockHash=blockHash,
            branch=self.shardState.branch,
            limit=100,
            direction=Direction.GENESIS,
        )
        op, resp, rpcId = await self.shardConn.write_rpc_request(
            CommandOp.GET_MINOR_BLOCK_HEADER_LIST_REQUEST, request)
        return resp.blockHeaderList

    async def __download_blocks(self, blockHeaderList):
        blockHashList = [b.get_hash() for b in blockHeaderList]
        op, resp, rpcId = await self.shardConn.write_rpc_request(
            CommandOp.GET_MINOR_BLOCK_LIST_REQUEST, GetMinorBlockListRequest(blockHashList))
        return resp.minorBlockList


class Synchronizer:
    ''' Buffer the headers received from peer and sync one by one '''

    def __init__(self):
        self.queue = deque()
        self.running = False

    def add_task(self, header, shardConn):
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

    def close_with_error(self, error):
        Logger.error("Closing shard connection with error {}".format(error))
        return super().close_with_error(error)

    async def handle_get_minor_block_header_list_request(self, request):
        if request.branch != self.shardState.branch:
            self.close_with_error("Wrong branch from peer")
        if request.limit <= 0:
            self.close_with_error("Bad limit")
        # TODO: support tip direction
        if request.direction != Direction.GENESIS:
            self.close_with_error("Bad direction")

        blockHash = request.blockHash
        headerList = []
        for i in range(request.limit):
            header = self.shardState.db.get_minor_block_header_by_hash(blockHash, consistencyCheck=False)
            headerList.append(header)
            if header.height == 0:
                break
            blockHash = header.hashPrevMinorBlock

        return GetMinorBlockHeaderListResponse(
            self.shardState.rootTip, self.shardState.headerTip, headerList)

    async def handle_get_minor_block_list_request(self, request):
        mBlockList = []
        for mBlockHash in request.minorBlockHashList:
            mBlock = self.shardState.db.get_minor_block_by_hash(mBlockHash, consistencyCheck=False)
            if mBlock is None:
                continue
            # TODO: Check list size to make sure the resp is smaller than limit
            mBlockList.append(mBlock)

        return GetMinorBlockListResponse(mBlockList)

    async def handle_new_minor_block_header_list_command(self, op, cmd, rpcId):
        # TODO: allow multiple headers if needed
        if len(cmd.minorBlockHeaderList) != 1:
            self.close_with_error("minor block header list must have only one header")
            return
        for mHeader in cmd.minorBlockHeaderList:
            Logger.info("[{}] received new header with height {}".format(
                mHeader.branch.get_shard_id(), mHeader.height))
            if mHeader.branch != self.shardState.branch:
                self.close_with_error("incorrect branch")
                return

        if self.bestRootBlockHeaderObserved:
            # check root header is not decreasing
            if cmd.rootBlockHeader.height < self.bestRootBlockHeaderObserved.height:
                return self.close_with_error("best observed root header height is decreasing {} < {}".format(
                    cmd.rootBlockHeader.height, self.bestRootBlockHeaderObserved.height))
            if cmd.rootBlockHeader.height == self.bestRootBlockHeaderObserved.height:
                if cmd.rootBlockHeader != self.bestRootBlockHeaderObserved:
                    return self.close_with_error("best observed root header changed with same height {}".format(
                        self.bestRootBlockHeaderObserved.height))

                # check minor header is not decreasing
                if mHeader.height < self.bestMinorBlockHeaderObserved.height:
                    return self.close_with_error("best observed minor header is decreasing {} < {}".format(
                        mHeader.height, self.bestMinorBlockHeaderObserved.height))

        self.bestRootBlockHeaderObserved = cmd.rootBlockHeader
        self.bestMinorBlockHeaderObserved = mHeader

        # Do not download if the new header is not higher than the current tip
        if self.shardState.headerTip.height >= mHeader.height:
            return

        self.synchronizer.add_task(mHeader, self)

    def broadcast_new_tip(self):
        if self.bestRootBlockHeaderObserved:
            if self.shardState.rootTip.height < self.bestRootBlockHeaderObserved.height:
                return
            if self.shardState.rootTip == self.bestRootBlockHeaderObserved:
                if self.shardState.headerTip.height < self.bestMinorBlockHeaderObserved.height:
                    return
                if self.shardState.headerTip == self.bestMinorBlockHeaderObserved:
                    return

        self.write_command(
            op=CommandOp.NEW_MINOR_BLOCK_HEADER_LIST,
            cmd=NewMinorBlockHeaderListCommand(self.shardState.rootTip, [self.shardState.headerTip]))

    async def handle_new_transaction_list_command(self, opCode, cmd, rpcId):
        self.slaveServer.add_tx_list(cmd.transactionList, self)

    def broadcast_tx_list(self, txList):
        self.write_command(
            op=CommandOp.NEW_TRANSACTION_LIST,
            cmd=NewTransactionListCommand(txList),
        )

    def get_metadata_to_write(self, metadata):
        ''' Override VirtualConnection.get_metadata_to_write()
        '''
        return ClusterMetadata(self.shardState.branch, self.clusterPeerId)


# P2P command definitions
OP_NONRPC_MAP = {
    CommandOp.NEW_MINOR_BLOCK_HEADER_LIST: ShardConnection.handle_new_minor_block_header_list_command,
    CommandOp.NEW_TRANSACTION_LIST: ShardConnection.handle_new_transaction_list_command,
}


OP_RPC_MAP = {
    CommandOp.GET_MINOR_BLOCK_HEADER_LIST_REQUEST:
        (CommandOp.GET_MINOR_BLOCK_HEADER_LIST_RESPONSE, ShardConnection.handle_get_minor_block_header_list_request),
    CommandOp.GET_MINOR_BLOCK_LIST_REQUEST:
        (CommandOp.GET_MINOR_BLOCK_LIST_RESPONSE, ShardConnection.handle_get_minor_block_list_request),
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

        asyncio.ensure_future(self.active_and_loop_forever())

        # clusterPeerId -> {branchValue -> ShardConn}
        self.vConnMap = dict()

    def get_connection_to_forward(self, metadata):
        ''' Override ProxyConnection.get_connection_to_forward()
        '''
        if metadata.clusterPeerId == 0:
            # Data from master
            return None

        if metadata.branch.value not in self.shardStateMap:
            self.close_with_error("incorrect forwarding branch")
            return

        connMap = self.vConnMap.get(metadata.clusterPeerId)
        if connMap is None:
            # Master can close the peer connection at any time
            # TODO: any way to avoid this race?
            Logger.warningEverySec("cannot find cluster peer id in vConnMap {}".format(metadata.clusterPeerId), 1)
            return NULL_CONNECTION

        return connMap[metadata.branch.value].get_forwarding_connection()

    def validate_connection(self, connection):
        return connection == NULL_CONNECTION or isinstance(connection, ForwardingVirtualConnection)

    def __get_shard_size(self):
        return self.env.config.SHARD_SIZE

    def close(self):
        for clusterPeerId, connMap in self.vConnMap.items():
            for branchValue, conn in connMap.items():
                conn.get_forwarding_connection().close()

        Logger.info("Lost connection with master")
        return super().close()

    def close_with_error(self, error):
        Logger.info("Closing connection with master: {}".format(error))
        return super().close_with_error(error)

    def close_connection(self, conn):
        ''' TODO: Notify master that the connection is closed by local.
        The master should close the peer connection, and notify the other slaves that a close happens
        More hint could be provided so that the master may blacklist the peer if it is mis-behaving
        '''
        pass

    # Cluster RPC handlers

    async def handle_ping(self, ping):
        self.slaveServer.init_shard_states(ping.rootTip)
        return Pong(self.slaveServer.id, self.slaveServer.shardMaskList)

    async def handle_connect_to_slaves_request(self, connectToSlavesRequest):
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
            await slave.wait_until_active()
            # Tell the remote slave who I am
            id, shardMaskList = await slave.send_ping()
            # Verify that remote slave indeed has the id and shard mask list advertised by the master
            if id != slave.id:
                resultList.append(bytes("id does not match. expect {} got {}".format(slave.id, id), "ascii"))
                continue
            if shardMaskList != slave.shardMaskList:
                resultList.append(bytes("shard mask list does not match. expect {} got {}".format(
                    slave.shardMaskList, shardMaskList), "ascii"))
                continue

            self.slaveServer.add_slave_connection(slave)
            resultList.append(bytes())
        return ConnectToSlavesResponse(resultList)

    async def handle_mine_request(self, request):
        if request.mining:
            self.slaveServer.start_mining(request.artificialTxConfig)
        else:
            self.slaveServer.stop_mining()
        return MineResponse(errorCode=0)

    async def handle_gen_tx_request(self, request):
        self.slaveServer.create_transactions(request.numTxPerShard, request.xShardPercent, request.tx)
        return GenTxResponse(errorCode=0)

    # Blockchain RPC handlers

    async def handle_add_root_block_request(self, req):
        # TODO: handle expectSwitch
        errorCode = 0
        switched = False
        for branchValue, shardState in self.shardStateMap.items():
            try:
                switched = shardState.add_root_block(req.rootBlock)
            except ValueError:
                Logger.logException()
                # TODO: May be enum or Unix errno?
                errorCode = errno.EBADMSG
                break

        return AddRootBlockResponse(errorCode, switched)

    async def handle_get_eco_info_list_request(self, req):
        ecoInfoList = []
        for branchValue, shardState in self.shardStateMap.items():
            ecoInfoList.append(EcoInfo(
                branch=Branch(branchValue),
                height=shardState.headerTip.height + 1,
                coinbaseAmount=shardState.get_next_block_coinbase_amount(),
                difficulty=shardState.get_next_block_difficulty(),
                unconfirmedHeadersCoinbaseAmount=shardState.get_unconfirmed_headers_coinbase_amount(),
            ))
        return GetEcoInfoListResponse(
            errorCode=0,
            ecoInfoList=ecoInfoList,
        )

    async def handle_get_next_block_to_mine_request(self, req):
        branchValue = req.branch.value
        if branchValue not in self.shardStateMap:
            return GetNextBlockToMineResponse(errorCode=errno.EBADMSG)

        block = self.shardStateMap[branchValue].create_block_to_mine(
            address=req.address,
        )
        response = GetNextBlockToMineResponse(
            errorCode=0,
            block=block,
        )
        return response

    async def handle_add_minor_block_request(self, req):
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

        if block.header.hashPrevMinorBlock != shardState.headerTip.get_hash():
            # Tip changed, don't bother creating a fork
            # TODO: push block candidate to miners than letting them pull
            Logger.info("[{}] dropped stale block {} mined locally".format(
                block.header.branch.get_shard_id(), block.header.height))
            return AddMinorBlockResponse(errorCode=0)

        success = await self.slaveServer.add_block(block)
        return AddMinorBlockResponse(
            errorCode=0 if success else errno.EFAULT,
        )

    async def handle_get_unconfirmed_header_list_request(self, req):
        headersInfoList = []
        for branchValue, shardState in self.shardStateMap.items():
            headersInfoList.append(HeadersInfo(
                branch=Branch(branchValue),
                headerList=shardState.get_unconfirmed_header_list(),
            ))
        return GetUnconfirmedHeadersResponse(
            errorCode=0,
            headersInfoList=headersInfoList,
        )

    async def handle_get_account_data_request(self, req):
        accountBranchDataList = self.slaveServer.get_account_data(req.address)
        return GetAccountDataResponse(
            errorCode=0,
            accountBranchDataList=accountBranchDataList,
        )

    async def handle_add_transaction(self, req):
        success = self.slaveServer.add_tx(req.tx)
        return AddTransactionResponse(
            errorCode=0 if success else 1,
        )

    async def handle_execute_transaction(self, req):
        res = self.slaveServer.execute_tx(req.tx, req.fromAddress)
        fail = res is None
        return ExecuteTransactionResponse(
            errorCode=int(fail),
            result=res if not fail else b''
        )

    async def handle_destroy_cluster_peer_connection_command(self, op, cmd, rpcId):
        if cmd.clusterPeerId not in self.vConnMap:
            Logger.error("cannot find cluster peer connection to destroy {}".format(cmd.clusterPeerId))
            return
        for branchValue, vConn in self.vConnMap[cmd.clusterPeerId].items():
            vConn.get_forwarding_connection().close()
        del self.vConnMap[cmd.clusterPeerId]

    async def handle_create_cluster_peer_connection_request(self, req):
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
            asyncio.ensure_future(conn.active_and_loop_forever())
            connMap[branchValue] = conn
            activeFutures.append(conn.activeFuture)
        # wait for all the connections to become active before return
        await asyncio.gather(*activeFutures)
        return CreateClusterPeerConnectionResponse(errorCode=0)

    def broadcast_new_tip(self, branch):
        for clusterPeerId, connMap in self.vConnMap.items():
            if branch.value not in connMap:
                Logger.error("Cannot find branch {} in conn {}".format(branch.value, clusterPeerId))
                continue

            connMap[branch.value].broadcast_new_tip()

    def broadcast_tx_list(self, branch, txList, shardConn=None):
        for clusterPeerId, connMap in self.vConnMap.items():
            if branch.value not in connMap:
                Logger.error("Cannot find branch {} in conn {}".format(branch.value, clusterPeerId))
                continue
            if shardConn == connMap[branch.value]:
                continue
            connMap[branch.value].broadcast_tx_list(txList)

    async def handle_get_minor_block_request(self, req):
        if req.minorBlockHash != bytes(32):
            block = self.slaveServer.get_minor_block_by_hash(req.minorBlockHash, req.branch)
        else:
            block = self.slaveServer.get_minor_block_by_height(req.height, req.branch)

        if not block:
            emptyBlock = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            return GetMinorBlockResponse(errorCode=1, minorBlock=emptyBlock)

        return GetMinorBlockResponse(errorCode=0, minorBlock=block)

    async def handle_get_transaction_request(self, req):
        minorBlock, i = self.slaveServer.get_transaction_by_hash(req.txHash, req.branch)
        if not minorBlock:
            emptyBlock = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            return GetTransactionResponse(errorCode=1, minorBlock=emptyBlock, index=0)

        return GetTransactionResponse(errorCode=0, minorBlock=minorBlock, index=i)

    async def handle_get_transaction_receipt_request(self, req):
        resp = self.slaveServer.get_transaction_receipt(req.txHash, req.branch)
        if not resp:
            emptyBlock = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            emptyReceipt = TransactionReceipt.create_empty_receipt()
            return GetTransactionReceiptResponse(
                errorCode=1, minorBlock=emptyBlock, index=0, receipt=emptyReceipt)
        minorBlock, i, receipt = resp
        return GetTransactionReceiptResponse(
            errorCode=0, minorBlock=minorBlock, index=i, receipt=receipt)

    async def handle_get_transaction_list_by_address_request(self, req):
        result = self.slaveServer.get_transaction_list_by_address(req.address, req.start, req.limit)
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

    async def handle_sync_minor_block_list_request(self, req):

        async def __download_blocks(blockHashList):
            op, resp, rpcId = await vConn.write_rpc_request(
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
                blockChain = await __download_blocks(blocksToDownload)
                Logger.info("[{}] sync request from master, downloaded {} blocks ({} - {})".format(
                    req.branch.get_shard_id(), len(blockChain),
                    blockChain[0].header.height, blockChain[-1].header.height))
                check(len(blockChain) == len(blocksToDownload))

                await self.slaveServer.add_block_list_for_sync(blockChain)
                blockHashList = blockHashList[BLOCK_BATCH_SIZE:]

        except Exception as e:
            Logger.errorException()
            return SyncMinorBlockListResponse(errorCode=1)

        return SyncMinorBlockListResponse(errorCode=0)


MASTER_OP_NONRPC_MAP = {
    ClusterOp.DESTROY_CLUSTER_PEER_CONNECTION_COMMAND: MasterConnection.handle_destroy_cluster_peer_connection_command,
}


MASTER_OP_RPC_MAP = {
    ClusterOp.PING:
        (ClusterOp.PONG, MasterConnection.handle_ping),
    ClusterOp.CONNECT_TO_SLAVES_REQUEST:
        (ClusterOp.CONNECT_TO_SLAVES_RESPONSE, MasterConnection.handle_connect_to_slaves_request),
    ClusterOp.MINE_REQUEST:
        (ClusterOp.MINE_RESPONSE, MasterConnection.handle_mine_request),
    ClusterOp.GEN_TX_REQUEST:
        (ClusterOp.GEN_TX_RESPONSE, MasterConnection.handle_gen_tx_request),
    ClusterOp.ADD_ROOT_BLOCK_REQUEST:
        (ClusterOp.ADD_ROOT_BLOCK_RESPONSE, MasterConnection.handle_add_root_block_request),
    ClusterOp.GET_ECO_INFO_LIST_REQUEST:
        (ClusterOp.GET_ECO_INFO_LIST_RESPONSE, MasterConnection.handle_get_eco_info_list_request),
    ClusterOp.GET_NEXT_BLOCK_TO_MINE_REQUEST:
        (ClusterOp.GET_NEXT_BLOCK_TO_MINE_RESPONSE, MasterConnection.handle_get_next_block_to_mine_request),
    ClusterOp.ADD_MINOR_BLOCK_REQUEST:
        (ClusterOp.ADD_MINOR_BLOCK_RESPONSE, MasterConnection.handle_add_minor_block_request),
    ClusterOp.GET_UNCONFIRMED_HEADERS_REQUEST:
        (ClusterOp.GET_UNCONFIRMED_HEADERS_RESPONSE, MasterConnection.handle_get_unconfirmed_header_list_request),
    ClusterOp.GET_ACCOUNT_DATA_REQUEST:
        (ClusterOp.GET_ACCOUNT_DATA_RESPONSE, MasterConnection.handle_get_account_data_request),
    ClusterOp.ADD_TRANSACTION_REQUEST:
        (ClusterOp.ADD_TRANSACTION_RESPONSE, MasterConnection.handle_add_transaction),
    ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST:
        (ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_RESPONSE, MasterConnection.handle_create_cluster_peer_connection_request),
    ClusterOp.GET_MINOR_BLOCK_REQUEST:
        (ClusterOp.GET_MINOR_BLOCK_RESPONSE, MasterConnection.handle_get_minor_block_request),
    ClusterOp.GET_TRANSACTION_REQUEST:
        (ClusterOp.GET_TRANSACTION_RESPONSE, MasterConnection.handle_get_transaction_request),
    ClusterOp.SYNC_MINOR_BLOCK_LIST_REQUEST:
        (ClusterOp.SYNC_MINOR_BLOCK_LIST_RESPONSE, MasterConnection.handle_sync_minor_block_list_request),
    ClusterOp.EXECUTE_TRANSACTION_REQUEST:
        (ClusterOp.EXECUTE_TRANSACTION_RESPONSE, MasterConnection.handle_execute_transaction),
    ClusterOp.GET_TRANSACTION_RECEIPT_REQUEST:
        (ClusterOp.GET_TRANSACTION_RECEIPT_RESPONSE, MasterConnection.handle_get_transaction_receipt_request),
    ClusterOp.GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST:
        (ClusterOp.GET_TRANSACTION_LIST_BY_ADDRESS_RESPONSE, MasterConnection.handle_get_transaction_list_by_address_request)
}


class SlaveConnection(Connection):

    def __init__(self, env, reader, writer, slaveServer, slaveId, shardMaskList, name=None):
        super().__init__(env, reader, writer, CLUSTER_OP_SERIALIZER_MAP, SLAVE_OP_NONRPC_MAP, SLAVE_OP_RPC_MAP, name=name)
        self.slaveServer = slaveServer
        self.id = slaveId
        self.shardMaskList = shardMaskList
        self.shardStateMap = self.slaveServer.shardStateMap

        asyncio.ensure_future(self.active_and_loop_forever())

    def __get_shard_size(self):
        return self.slaveServer.env.config.SHARD_SIZE

    def has_shard(self, shardId):
        for shardMask in self.shardMaskList:
            if shardMask.contain_shard_id(shardId):
                return True
        return False

    def close_with_error(self, error):
        Logger.info("Closing connection with slave {}".format(self.id))
        return super().close_with_error(error)

    async def send_ping(self):
        # TODO: Send real root tip and allow shards to confirm each other
        req = Ping(self.slaveServer.id, self.slaveServer.shardMaskList, RootBlock(RootBlockHeader()))
        op, resp, rpcId = await self.write_rpc_request(ClusterOp.PING, req)
        return (resp.id, resp.shardMaskList)

    # Cluster RPC handlers

    async def handle_ping(self, ping):
        if not self.id:
            self.id = ping.id
            self.shardMaskList = ping.shardMaskList
            self.slaveServer.add_slave_connection(self)
        if len(self.shardMaskList) == 0:
            return self.close_with_error("Empty shard mask list from slave {}".format(self.id))

        return Pong(self.slaveServer.id, self.slaveServer.shardMaskList)

    # Blockchain RPC handlers

    async def handle_add_xshard_tx_list_request(self, req):
        if req.branch.get_shard_size() != self.__get_shard_size():
            Logger.error(
                "add xshard tx list request shard size mismatch! "
                "Expect: {}, actual: {}".format(self.__get_shard_size(), req.branch.get_shard_size()))
            return AddXshardTxListResponse(errorCode=errno.ESRCH)

        if req.branch.value not in self.shardStateMap:
            Logger.error("cannot find shard id {} locally".format(req.branch.get_shard_id()))
            return AddXshardTxListResponse(errorCode=errno.ENOENT)

        self.shardStateMap[req.branch.value].add_cross_shard_tx_list_by_minor_block_hash(req.minorBlockHash, req.txList)
        return AddXshardTxListResponse(errorCode=0)

    async def handle_batch_add_xshard_tx_list_request(self, batchRequest):
        for request in batchRequest.addXshardTxListRequestList:
            response = await self.handle_add_xshard_tx_list_request(request)
            if response.errorCode != 0:
                return BatchAddXshardTxListResponse(errorCode=response.errorCode)
        return BatchAddXshardTxListResponse(errorCode=0)


SLAVE_OP_NONRPC_MAP = {}


SLAVE_OP_RPC_MAP = {
    ClusterOp.PING:
        (ClusterOp.PONG, SlaveConnection.handle_ping),
    ClusterOp.ADD_XSHARD_TX_LIST_REQUEST:
        (ClusterOp.ADD_XSHARD_TX_LIST_RESPONSE, SlaveConnection.handle_add_xshard_tx_list_request),
    ClusterOp.BATCH_ADD_XSHARD_TX_LIST_REQUEST:
        (ClusterOp.BATCH_ADD_XSHARD_TX_LIST_RESPONSE, SlaveConnection.handle_batch_add_xshard_tx_list_request)
}


class SlaveServer():
    """ Slave node in a cluster """

    def __init__(self, env, name="slave"):
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.id = self.env.clusterConfig.ID
        self.shardMaskList = self.env.clusterConfig.SHARD_MASK_LIST

        # shard id -> a list of slave running the shard
        self.shardToSlaves = [[] for i in range(self.__get_shard_size())]
        self.slaveConnections = set()
        self.slaveIds = set()

        self.master = None
        self.name = name

        self.artificialTxConfig = None
        self.txGenMap = dict()
        self.minerMap = dict()  # branchValue -> Miner
        self.shardStateMap = dict()  # branchValue -> ShardState
        self.__init_shards()
        self.shutdownInProgress = False
        self.slaveId = 0

        # block hash -> future (that will return when the block is fully propagated in the cluster)
        # the block that has been added locally but not have been fully propagated will have an entry here
        self.add_blockFutures = dict()

    def __init_shards(self):
        ''' branchValue -> ShardState mapping '''
        shardSize = self.__get_shard_size()
        branchValues = set()
        for shardMask in self.shardMaskList:
            for shardId in shardMask.iterate(shardSize):
                branchValue = shardId + shardSize
                branchValues.add(branchValue)

        for branchValue in branchValues:
            shardId = Branch(branchValue).get_shard_id()
            db = self.__init_shard_db(shardId)
            self.shardStateMap[branchValue] = ShardState(
                env=self.env,
                shardId=shardId,
                db=db,
            )
            self.__init_miner(branchValue)
            self.txGenMap[branchValue] = TransactionGenerator(Branch(branchValue), self)

    def __init_shard_db(self, shardId):
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

    def __init_miner(self, branchValue):
        minerAddress = self.env.config.TESTNET_MASTER_ACCOUNT.address_in_branch(Branch(branchValue))

        def __is_syncing():
            return any([vs[branchValue].synchronizer.running for vs in self.master.vConnMap.values()])

        async def __create_block():
            # hold off mining if the shard is syncing
            while __is_syncing():
                await asyncio.sleep(0.1)

            return self.shardStateMap[branchValue].create_block_to_mine(address=minerAddress)

        async def __add_block(block):
            # Do not add block if there is a sync in progress
            if __is_syncing():
                return
            # Do not add stale block
            if self.shardStateMap[block.header.branch.value].headerTip.height >= block.header.height:
                return
            await self.add_block(block)

        def __get_target_block_time():
            return self.artificialTxConfig.targetMinorBlockTime

        self.minerMap[branchValue] = Miner(
            __create_block,
            __add_block,
            __get_target_block_time,
        )

    def init_shard_states(self, rootTip):
        ''' Will be called when master connects to slaves '''
        for _, shardState in self.shardStateMap.items():
            shardState.init_from_root_block(rootTip)

    def start_mining(self, artificialTxConfig):
        self.artificialTxConfig = artificialTxConfig
        for branchValue, miner in self.minerMap.items():
            Logger.info("[{}] start mining with target minor block time {} seconds".format(
                Branch(branchValue).get_shard_id(),
                artificialTxConfig.targetMinorBlockTime,
            ))
            miner.enable()
            miner.mine_new_block_async();

    def create_transactions(self, numTxPerShard, xShardPercent, tx: Transaction):
        for generator in self.txGenMap.values():
            generator.generate(numTxPerShard, xShardPercent, tx)

    def stop_mining(self):
        for branchValue, miner in self.minerMap.items():
            Logger.info("[{}] stop mining".format(Branch(branchValue).get_shard_id()))
            miner.disable()

    def __get_shard_size(self):
        return self.env.config.SHARD_SIZE

    def add_slave_connection(self, slave):
        self.slaveIds.add(slave.id)
        self.slaveConnections.add(slave)
        for shardId in range(self.__get_shard_size()):
            if slave.has_shard(shardId):
                self.shardToSlaves[shardId].append(slave)

        # self.__logSummary()

    def __logSummary(self):
        for shardId, slaves in enumerate(self.shardToSlaves):
            Logger.info("[{}] is run by slave {}".format(shardId, [s.id for s in slaves]))

    async def __handle_master_connection_lost(self):
        check(self.master is not None)
        await self.waitUntilClose()

        if not self.shutdownInProgress:
            # TODO: May reconnect
            self.shutdown()

    async def __handle_new_connection(self, reader, writer):
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

    async def __start_server(self):
        ''' Run the server until shutdown is called '''
        self.server = await asyncio.start_server(
            self.__handle_new_connection, "0.0.0.0", self.env.clusterConfig.NODE_PORT, loop=self.loop)
        Logger.info("Listening on {} for intra-cluster RPC".format(
            self.server.sockets[0].getsockname()))

    def start(self):
        self.loop.create_task(self.__start_server())

    def start_and_loop(self):
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

    def get_shutdown_future(self):
        return self.server.wait_closed()

    # Blockchain functions

    async def send_minor_block_header_to_master(self, minorBlockHeader, txCount, xShardTxCount, shardStats):
        ''' Update master that a minor block has been appended successfully '''
        request = AddMinorBlockHeaderRequest(minorBlockHeader, txCount, xShardTxCount, shardStats)
        _, resp, _ = await self.master.write_rpc_request(ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST, request)
        check(resp.errorCode == 0)
        self.artificialTxConfig = resp.artificialTxConfig

    def __get_branch_to_add_xshard_tx_list_request(self, blockHash, xshardTxList):
        branchToAddXshardTxListRequest = dict()

        xshardMap = dict()
        for shardId in range(self.__get_shard_size()):
            xshardMap[shardId + self.__get_shard_size()] = []

        for xshardTx in xshardTxList:
            shardId = xshardTx.toAddress.get_shard_id(self.__get_shard_size())
            branchValue = Branch.create(self.__get_shard_size(), shardId).value
            xshardMap[branchValue].append(xshardTx)

        for branchValue, txList in xshardMap.items():
            crossShardTxList = CrossShardTransactionList(txList)

            branch = Branch(branchValue)
            request = AddXshardTxListRequest(branch, blockHash, crossShardTxList)
            branchToAddXshardTxListRequest[branch] = request

        return branchToAddXshardTxListRequest

    async def broadcast_xshard_tx_list(self, block, xshardTxList):
        ''' Broadcast x-shard transactions to their recipient shards '''

        blockHash = block.header.get_hash()
        branchToAddXshardTxListRequest = self.__get_branch_to_add_xshard_tx_list_request(blockHash, xshardTxList)
        rpcFutures = []
        for branch, request in branchToAddXshardTxListRequest.items():
            if branch.value in self.shardStateMap:
                self.shardStateMap[branch.value].add_cross_shard_tx_list_by_minor_block_hash(blockHash, request.txList)

            for slaveConn in self.shardToSlaves[branch.get_shard_id()]:
                future = slaveConn.write_rpc_request(ClusterOp.ADD_XSHARD_TX_LIST_REQUEST, request)
                rpcFutures.append(future)
        responses = await asyncio.gather(*rpcFutures)
        check(all([response.errorCode == 0 for _, response, _ in responses]))

    async def batch_broadcast_xshard_tx_list(self, blockHashToXShardList):
        branchToAddXshardTxListRequestList = dict()
        for blockHash, xShardList in blockHashToXShardList.items():
            branchToAddXshardTxListRequest = self.__get_branch_to_add_xshard_tx_list_request(blockHash, xShardList)
            for branch, request in branchToAddXshardTxListRequest.items():
                branchToAddXshardTxListRequestList.setdefault(branch, []).append(request)

        rpcFutures = []
        for branch, requestList in branchToAddXshardTxListRequestList.items():
            if branch.value in self.shardStateMap:
                for request in requestList:
                    self.shardStateMap[branch.value].add_cross_shard_tx_list_by_minor_block_hash(
                        request.minorBlockHash, request.txList)

            batchRequest = BatchAddXshardTxListRequest(requestList)
            for slaveConn in self.shardToSlaves[branch.get_shard_id()]:
                future = slaveConn.write_rpc_request(ClusterOp.BATCH_ADD_XSHARD_TX_LIST_REQUEST, batchRequest)
                rpcFutures.append(future)
        responses = await asyncio.gather(*rpcFutures)
        check(all([response.errorCode == 0 for _, response, _ in responses]))

    async def add_block(self, block):
        ''' Returns true if block is successfully added. False on any error. '''
        branchValue = block.header.branch.value
        shardState = self.shardStateMap.get(branchValue, None)

        if not shardState:
            return False

        oldTip = shardState.tip()
        try:
            xShardList = shardState.add_block(block)
        except Exception as e:
            Logger.errorException()
            return False

        # block has been added to local state and let's pass to peers
        try:
            if oldTip != shardState.tip():
                self.master.broadcast_new_tip(block.header.branch)
        except Exception:
            Logger.warningEverySec("broadcast tip failure", 1)

        # block already existed in local shard state
        # but might not have been propagated to other shards and master
        # let's make sure all the shards and master got it before return
        if xShardList is None:
            future = self.add_blockFutures.get(block.header.get_hash(), None)
            if future:
                Logger.info("[{}] {} is being added ... waiting for it to finish".format(
                    block.header.branch.get_shard_id(), block.header.height))
                await future
            return True

        self.add_blockFutures[block.header.get_hash()] = self.loop.create_future()

        # Start mining new one before propagating inside cluster
        # The propagation should be done by the time the new block is mined
        self.minerMap[branchValue].mine_new_block_async()

        await self.broadcast_xshard_tx_list(block, xShardList)
        await self.send_minor_block_header_to_master(
            block.header, len(block.txList), len(xShardList), shardState.get_shard_stats())

        self.add_blockFutures[block.header.get_hash()].set_result(None)
        del self.add_blockFutures[block.header.get_hash()]
        return True

    async def add_block_list_for_sync(self, blockList):
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
            blockHash = block.header.get_hash()
            try:
                xShardList = shardState.add_block(block)
            except Exception as e:
                Logger.errorException()
                return False

            # block already existed in local shard state
            # but might not have been propagated to other shards and master
            # let's make sure all the shards and master got it before return
            if xShardList is None:
                future = self.add_blockFutures.get(blockHash, None)
                if future:
                    existingAddBlockFutures.append(future)
            else:
                blockHashToXShardList[blockHash] = xShardList
                self.add_blockFutures[blockHash] = self.loop.create_future()

        await self.batch_broadcast_xshard_tx_list(blockHashToXShardList)

        for blockHash in blockHashToXShardList.keys():
            self.add_blockFutures[blockHash].set_result(None)
            del self.add_blockFutures[blockHash]

        await asyncio.gather(*existingAddBlockFutures)

        return True

    def add_tx_list(self, txList, shardConn=None):
        if not txList:
            return
        evmTx = txList[0].code.get_evm_transaction()
        evmTx.set_shard_size(self.__get_shard_size())
        branchValue = evmTx.from_shard_id() | self.__get_shard_size()
        validTxList = []
        for tx in txList:
            if self.add_tx(tx):
                validTxList.append(tx)
        if not validTxList:
            return
        self.master.broadcast_tx_list(Branch(branchValue), validTxList, shardConn)

    def add_tx(self, tx):
        evmTx = tx.code.get_evm_transaction()
        evmTx.set_shard_size(self.__get_shard_size())
        branchValue = evmTx.from_shard_id() | self.__get_shard_size()
        shardState = self.shardStateMap.get(branchValue, None)
        if not shardState:
            return False
        return shardState.add_tx(tx)

    def execute_tx(self, tx, fromAddress) -> Optional[bytes]:
        evmTx = tx.code.get_evm_transaction()
        evmTx.set_shard_size(self.__get_shard_size())
        branchValue = evmTx.from_shard_id() | self.__get_shard_size()
        shardState = self.shardStateMap.get(branchValue, None)
        if not shardState:
            return False
        return shardState.execute_tx(tx, fromAddress)

    def get_transaction_count(self, address):
        branch = Branch.create(self.__get_shard_size(), address.get_shard_id(self.__get_shard_size()))
        if branch.value not in self.shardStateMap:
            return None
        return self.shardStateMap[branch.value].get_transaction_count(address.recipient)

    def get_balance(self, address):
        branch = Branch.create(self.__get_shard_size(), address.get_shard_id(self.__get_shard_size()))
        if branch.value not in self.shardStateMap:
            return None
        return self.shardStateMap[branch.value].get_balance(address.recipient)

    def get_account_data(self, address):
        results = []
        for branchValue, shardState in self.shardStateMap.items():
            results.append(AccountBranchData(
                branch=Branch(branchValue),
                transactionCount=shardState.get_transaction_count(address.recipient),
                balance=shardState.get_balance(address.recipient),
                isContract=len(shardState.get_code(address.recipient)) > 0,
            ))
        return results

    def get_minor_block_by_hash(self, blockHash, branch):
        if branch.value not in self.shardStateMap:
            return None

        shardState = self.shardStateMap[branch.value]
        try:
            return shardState.db.get_minor_block_by_hash(blockHash, False)
        except Exception:
            return None

    def get_minor_block_by_height(self, height, branch):
        if branch.value not in self.shardStateMap:
            return None

        shardState = self.shardStateMap[branch.value]
        return shardState.db.get_minor_block_by_height(height)

    def get_transaction_by_hash(self, txHash, branch):
        if branch.value not in self.shardStateMap:
            return None

        shardState = self.shardStateMap[branch.value]
        return shardState.get_transaction_by_hash(txHash)

    def get_transaction_receipt(self, txHash, branch) -> Optional[Tuple[MinorBlock, int, TransactionReceipt]]:
        if branch.value not in self.shardStateMap:
            return None

        shardState = self.shardStateMap[branch.value]
        return shardState.get_transaction_receipt(txHash)

    def get_transaction_list_by_address(self, address, start, limit):
        branch = Branch.create(self.__get_shard_size(), address.get_shard_id(self.__get_shard_size()))
        if branch.value not in self.shardStateMap:
            return None
        shardState = self.shardStateMap[branch.value]
        return shardState.get_transaction_list_by_address(address, start, limit)


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
    slaveServer.start_and_loop()

    Logger.info("Slave server is shutdown")


if __name__ == '__main__':
    main()
