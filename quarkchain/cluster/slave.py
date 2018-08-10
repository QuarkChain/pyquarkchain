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

    def __init__(self, header, shard_conn):
        self.header = header
        self.shard_conn = shard_conn
        self.shard_state = shard_conn.shard_state
        self.slave_server = shard_conn.slave_server
        self.max_staleness = self.shard_state.env.config.MAX_STALE_MINOR_BLOCK_HEIGHT_DIFF

    async def sync(self):
        try:
            await self.__run_sync()
        except Exception as e:
            Logger.logException()
            self.shard_conn.close_with_error(str(e))

    async def __run_sync(self):
        if self.__has_block_hash(self.header.get_hash()):
            return

        # descending height
        block_header_chain = [self.header]

        # TODO: Stop if too many headers to revert
        while not self.__has_block_hash(block_header_chain[-1].hashPrevMinorBlock):
            block_hash = block_header_chain[-1].hashPrevMinorBlock
            height = block_header_chain[-1].height - 1

            if self.shard_state.header_tip.height - height > self.max_staleness:
                Logger.warning("[{}] abort syncing due to forking at very old block {} << {}".format(
                    self.header.branch.get_shard_id(), height, self.shard_state.header_tip.height))
                return

            if not self.shard_state.db.contain_root_block_by_hash(block_header_chain[-1].hashPrevRootBlock):
                return
            Logger.info("[{}] downloading headers from {} {}".format(
                self.shard_state.branch.get_shard_id(), height, block_hash.hex()))
            block_header_list = await self.__download_block_headers(block_hash)
            Logger.info("[{}] downloaded {} headers from peer".format(
                self.shard_state.branch.get_shard_id(), len(block_header_list)))
            if not self.__validate_block_headers(block_header_list):
                # TODO: tag bad peer
                return self.shard_conn.close_with_error("Bad peer sending discontinuing block headers")
            for header in block_header_list:
                if self.__has_block_hash(header.get_hash()):
                    break
                block_header_chain.append(header)

        # ascending height
        block_header_chain.reverse()
        while len(block_header_chain) > 0:
            block_chain = await self.__download_blocks(block_header_chain[:100])
            Logger.info("[{}] downloaded {} blocks from peer".format(
                self.shard_state.branch.get_shard_id(), len(block_chain)))
            check(len(block_chain) == len(block_header_chain[:100]))

            for block in block_chain:
                # Stop if the block depends on an unknown root block
                # TODO: move this check to early stage to avoid downloading unnecessary headers
                if not self.shard_state.db.contain_root_block_by_hash(block.header.hashPrevRootBlock):
                    return
                await self.slave_server.add_block(block)
                block_header_chain.pop(0)

    def __has_block_hash(self, block_hash):
        return self.shard_state.db.contain_minor_block_by_hash(block_hash)

    def __validate_block_headers(self, block_header_list):
        # TODO: check difficulty and other stuff?
        for i in range(len(block_header_list) - 1):
            block, prev = block_header_list[i:i + 2]
            if block.height != prev.height + 1:
                return False
            if block.hashPrevMinorBlock != prev.get_hash():
                return False
        return True

    async def __download_block_headers(self, block_hash):
        request = GetMinorBlockHeaderListRequest(
            blockHash=block_hash,
            branch=self.shard_state.branch,
            limit=100,
            direction=Direction.GENESIS,
        )
        op, resp, rpc_id = await self.shard_conn.write_rpc_request(
            CommandOp.GET_MINOR_BLOCK_HEADER_LIST_REQUEST, request)
        return resp.blockHeaderList

    async def __download_blocks(self, block_header_list):
        block_hash_list = [b.get_hash() for b in block_header_list]
        op, resp, rpc_id = await self.shard_conn.write_rpc_request(
            CommandOp.GET_MINOR_BLOCK_LIST_REQUEST, GetMinorBlockListRequest(block_hash_list))
        return resp.minorBlockList


class Synchronizer:
    ''' Buffer the headers received from peer and sync one by one '''

    def __init__(self):
        self.queue = deque()
        self.running = False

    def add_task(self, header, shard_conn):
        self.queue.append((header, shard_conn))
        if not self.running:
            self.running = True
            asyncio.ensure_future(self.__run())

    async def __run(self):
        while len(self.queue) > 0:
            header, shard_conn = self.queue.popleft()
            task = SyncTask(header, shard_conn)
            await task.sync()
        self.running = False


class ShardConnection(VirtualConnection):
    ''' A virtual connection between local shard and remote shard
    '''

    def __init__(self, master_conn, cluster_peer_id, shard_state, name=None):
        super().__init__(master_conn, OP_SERIALIZER_MAP, OP_NONRPC_MAP, OP_RPC_MAP, name=name)
        self.cluster_peer_id = cluster_peer_id
        self.shard_state = shard_state
        self.master_conn = master_conn
        self.slave_server = master_conn.slave_server
        self.synchronizer = Synchronizer()
        self.best_root_block_header_observed = None
        self.best_minor_block_header_observed = None

    def close_with_error(self, error):
        Logger.error("Closing shard connection with error {}".format(error))
        return super().close_with_error(error)

    async def handle_get_minor_block_header_list_request(self, request):
        if request.branch != self.shard_state.branch:
            self.close_with_error("Wrong branch from peer")
        if request.limit <= 0:
            self.close_with_error("Bad limit")
        # TODO: support tip direction
        if request.direction != Direction.GENESIS:
            self.close_with_error("Bad direction")

        block_hash = request.blockHash
        header_list = []
        for i in range(request.limit):
            header = self.shard_state.db.get_minor_block_header_by_hash(block_hash, consistency_check=False)
            header_list.append(header)
            if header.height == 0:
                break
            block_hash = header.hashPrevMinorBlock

        return GetMinorBlockHeaderListResponse(
            self.shard_state.root_tip, self.shard_state.header_tip, header_list)

    async def handle_get_minor_block_list_request(self, request):
        m_block_list = []
        for m_block_hash in request.minorBlockHashList:
            m_block = self.shard_state.db.get_minor_block_by_hash(m_block_hash, consistency_check=False)
            if m_block is None:
                continue
            # TODO: Check list size to make sure the resp is smaller than limit
            m_block_list.append(m_block)

        return GetMinorBlockListResponse(m_block_list)

    async def handle_new_minor_block_header_list_command(self, _op, cmd, _rpc_id):
        # TODO: allow multiple headers if needed
        if len(cmd.minorBlockHeaderList) != 1:
            self.close_with_error("minor block header list must have only one header")
            return
        for m_header in cmd.minorBlockHeaderList:
            Logger.info("[{}] received new header with height {}".format(
                m_header.branch.get_shard_id(), m_header.height))
            if m_header.branch != self.shard_state.branch:
                self.close_with_error("incorrect branch")
                return

        if self.best_root_block_header_observed:
            # check root header is not decreasing
            if cmd.rootBlockHeader.height < self.best_root_block_header_observed.height:
                return self.close_with_error("best observed root header height is decreasing {} < {}".format(
                    cmd.rootBlockHeader.height, self.best_root_block_header_observed.height))
            if cmd.rootBlockHeader.height == self.best_root_block_header_observed.height:
                if cmd.rootBlockHeader != self.best_root_block_header_observed:
                    return self.close_with_error("best observed root header changed with same height {}".format(
                        self.best_root_block_header_observed.height))

                # check minor header is not decreasing
                if m_header.height < self.best_minor_block_header_observed.height:
                    return self.close_with_error("best observed minor header is decreasing {} < {}".format(
                        m_header.height, self.best_minor_block_header_observed.height))

        self.best_root_block_header_observed = cmd.rootBlockHeader
        self.best_minor_block_header_observed = m_header

        # Do not download if the new header is not higher than the current tip
        if self.shard_state.header_tip.height >= m_header.height:
            return

        self.synchronizer.add_task(m_header, self)

    def broadcast_new_tip(self):
        if self.best_root_block_header_observed:
            if self.shard_state.root_tip.height < self.best_root_block_header_observed.height:
                return
            if self.shard_state.root_tip == self.best_root_block_header_observed:
                if self.shard_state.header_tip.height < self.best_minor_block_header_observed.height:
                    return
                if self.shard_state.header_tip == self.best_minor_block_header_observed:
                    return

        self.write_command(
            op=CommandOp.NEW_MINOR_BLOCK_HEADER_LIST,
            cmd=NewMinorBlockHeaderListCommand(self.shard_state.root_tip, [self.shard_state.header_tip]))

    async def handle_new_transaction_list_command(self, opCode, cmd, rpcId):
        self.slave_server.add_tx_list(cmd.transactionList, self)

    def broadcast_tx_list(self, txList):
        self.write_command(
            op=CommandOp.NEW_TRANSACTION_LIST,
            cmd=NewTransactionListCommand(txList),
        )

    def get_metadata_to_write(self, metadata):
        ''' Override VirtualConnection.get_metadata_to_write()
        '''
        return ClusterMetadata(self.shard_state.branch, self.cluster_peer_id)


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

    def __init__(self, env, reader, writer, slave_server, name=None):
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
        self.slave_server = slave_server
        self.shard_state_map = slave_server.shard_state_map

        asyncio.ensure_future(self.active_and_loop_forever())

        # clusterPeerId -> {branchValue -> ShardConn}
        self.v_conn_map = dict()

    def get_connection_to_forward(self, metadata):
        ''' Override ProxyConnection.get_connection_to_forward()
        '''
        if metadata.clusterPeerId == 0:
            # Data from master
            return None

        if metadata.branch.value not in self.shard_state_map:
            self.close_with_error("incorrect forwarding branch")
            return

        conn_map = self.v_conn_map.get(metadata.clusterPeerId)
        if conn_map is None:
            # Master can close the peer connection at any time
            # TODO: any way to avoid this race?
            Logger.warningEverySec("cannot find cluster peer id in vConnMap {}".format(metadata.clusterPeerId), 1)
            return NULL_CONNECTION

        return conn_map[metadata.branch.value].get_forwarding_connection()

    def validate_connection(self, connection):
        return connection == NULL_CONNECTION or isinstance(connection, ForwardingVirtualConnection)

    def __get_shard_size(self):
        return self.env.config.SHARD_SIZE

    def close(self):
        for clusterPeerId, connMap in self.v_conn_map.items():
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
        self.slave_server.init_shard_states(ping.rootTip)
        return Pong(self.slave_server.id, self.slave_server.shard_mask_list)

    async def handle_connect_to_slaves_request(self, connect_to_slave_request):
        """
        Master sends in the slave list. Let's connect to them.
        Skip self and slaves already connected.
        """
        result_list = []
        for slave_info in connect_to_slave_request.slaveInfoList:
            if slave_info.id == self.slave_server.id or slave_info.id in self.slave_server.slave_ids:
                result_list.append(bytes())
                continue

            ip = str(ipaddress.ip_address(slave_info.ip))
            port = slave_info.port
            try:
                reader, writer = await asyncio.open_connection(ip, port, loop=self.loop)
            except Exception as e:
                err_msg = "Failed to connect {}:{} with exception {}".format(ip, port, e)
                Logger.info(err_msg)
                result_list.append(bytes(err_msg, "ascii"))
                continue

            slave = SlaveConnection(self.env, reader, writer, self.slave_server, slave_info.id,
                                    slave_info.shardMaskList)
            await slave.wait_until_active()
            # Tell the remote slave who I am
            id, shard_mask_list = await slave.send_ping()
            # Verify that remote slave indeed has the id and shard mask list advertised by the master
            if id != slave.id:
                result_list.append(bytes("id does not match. expect {} got {}".format(slave.id, id), "ascii"))
                continue
            if shard_mask_list != slave.shard_mask_list:
                result_list.append(bytes("shard mask list does not match. expect {} got {}".format(
                    slave.shard_mask_list, shard_mask_list), "ascii"))
                continue

            self.slave_server.add_slave_connection(slave)
            result_list.append(bytes())
        return ConnectToSlavesResponse(result_list)

    async def handle_mine_request(self, request):
        if request.mining:
            self.slave_server.start_mining(request.artificialTxConfig)
        else:
            self.slave_server.stop_mining()
        return MineResponse(errorCode=0)

    async def handle_gen_tx_request(self, request):
        self.slave_server.create_transactions(request.numTxPerShard, request.xShardPercent, request.tx)
        return GenTxResponse(errorCode=0)

    # Blockchain RPC handlers

    async def handle_add_root_block_request(self, req):
        # TODO: handle expectSwitch
        error_code = 0
        switched = False
        for branchValue, shardState in self.shard_state_map.items():
            try:
                switched = shardState.add_root_block(req.rootBlock)
            except ValueError:
                Logger.logException()
                # TODO: May be enum or Unix errno?
                error_code = errno.EBADMSG
                break

        return AddRootBlockResponse(error_code, switched)

    async def handle_get_eco_info_list_request(self, _req):
        eco_info_list = []
        for branch_value, shard_state in self.shard_state_map.items():
            eco_info_list.append(EcoInfo(
                branch=Branch(branch_value),
                height=shard_state.header_tip.height + 1,
                coinbaseAmount=shard_state.get_next_block_coinbase_amount(),
                difficulty=shard_state.get_next_block_difficulty(),
                unconfirmedHeadersCoinbaseAmount=shard_state.get_unconfirmed_headers_coinbase_amount(),
            ))
        return GetEcoInfoListResponse(
            errorCode=0,
            ecoInfoList=eco_info_list,
        )

    async def handle_get_next_block_to_mine_request(self, req):
        branch_value = req.branch.value
        if branch_value not in self.shard_state_map:
            return GetNextBlockToMineResponse(errorCode=errno.EBADMSG)

        block = self.shard_state_map[branch_value].create_block_to_mine(
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
        branch_value = block.header.branch.value
        shard_state = self.slave_server.shard_state_map.get(branch_value, None)
        if not shard_state:
            return AddMinorBlockResponse(
                errorCode=errno.EBADMSG,
            )

        if block.header.hashPrevMinorBlock != shard_state.header_tip.get_hash():
            # Tip changed, don't bother creating a fork
            # TODO: push block candidate to miners than letting them pull
            Logger.info("[{}] dropped stale block {} mined locally".format(
                block.header.branch.get_shard_id(), block.header.height))
            return AddMinorBlockResponse(errorCode=0)

        success = await self.slave_server.add_block(block)
        return AddMinorBlockResponse(
            errorCode=0 if success else errno.EFAULT,
        )

    async def handle_get_unconfirmed_header_list_request(self, _req):
        headers_info_list = []
        for branchValue, shardState in self.shard_state_map.items():
            headers_info_list.append(HeadersInfo(
                branch=Branch(branchValue),
                headerList=shardState.get_unconfirmed_header_list(),
            ))
        return GetUnconfirmedHeadersResponse(
            errorCode=0,
            headersInfoList=headers_info_list,
        )

    async def handle_get_account_data_request(self, req):
        account_branch_data_list = self.slave_server.get_account_data(req.address)
        return GetAccountDataResponse(
            errorCode=0,
            accountBranchDataList=account_branch_data_list,
        )

    async def handle_add_transaction(self, req):
        success = self.slave_server.add_tx(req.tx)
        return AddTransactionResponse(
            errorCode=0 if success else 1,
        )

    async def handle_execute_transaction(self, req):
        res = self.slave_server.execute_tx(req.tx, req.fromAddress)
        fail = res is None
        return ExecuteTransactionResponse(
            errorCode=int(fail),
            result=res if not fail else b''
        )

    async def handle_destroy_cluster_peer_connection_command(self, op, cmd, rpcId):
        if cmd.clusterPeerId not in self.v_conn_map:
            Logger.error("cannot find cluster peer connection to destroy {}".format(cmd.clusterPeerId))
            return
        for branch_value, v_conn in self.v_conn_map[cmd.clusterPeerId].items():
            v_conn.get_forwarding_connection().close()
        del self.v_conn_map[cmd.clusterPeerId]

    async def handle_create_cluster_peer_connection_request(self, req):
        if req.clusterPeerId in self.v_conn_map:
            Logger.error("duplicated create cluster peer connection {}".format(req.clusterPeerId))
            return CreateClusterPeerConnectionResponse(errorCode=errno.ENOENT)

        conn_map = dict()
        self.v_conn_map[req.clusterPeerId] = conn_map
        active_futures = []
        for branch_value, shard_state in self.shard_state_map.items():
            conn = ShardConnection(master_conn=self, cluster_peer_id=req.clusterPeerId, shard_state=shard_state,
                                   name="{}_vconn_{}".format(self.name, req.clusterPeerId))
            asyncio.ensure_future(conn.active_and_loop_forever())
            conn_map[branch_value] = conn
            active_futures.append(conn.activeFuture)
        # wait for all the connections to become active before return
        await asyncio.gather(*active_futures)
        return CreateClusterPeerConnectionResponse(errorCode=0)

    def broadcast_new_tip(self, branch):
        for cluster_peer_id, conn_map in self.v_conn_map.items():
            if branch.value not in conn_map:
                Logger.error("Cannot find branch {} in conn {}".format(branch.value, cluster_peer_id))
                continue

            conn_map[branch.value].broadcast_new_tip()

    def broadcast_tx_list(self, branch, tx_list, shard_conn=None):
        for clusterPeerId, conn_map in self.v_conn_map.items():
            if branch.value not in conn_map:
                Logger.error("Cannot find branch {} in conn {}".format(branch.value, clusterPeerId))
                continue
            if shard_conn == conn_map[branch.value]:
                continue
            conn_map[branch.value].broadcast_tx_list(tx_list, )

    async def handle_get_minor_block_request(self, req):
        if req.minorBlockHash != bytes(32):
            block = self.slave_server.get_minor_block_by_hash(req.minorBlockHash, req.branch)
        else:
            block = self.slave_server.get_minor_block_by_height(req.height, req.branch)

        if not block:
            empty_block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            return GetMinorBlockResponse(errorCode=1, minorBlock=empty_block)

        return GetMinorBlockResponse(errorCode=0, minorBlock=block)

    async def handle_get_transaction_request(self, req):
        minor_block, i = self.slave_server.get_transaction_by_hash(req.txHash, req.branch)
        if not minor_block:
            empty_block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            return GetTransactionResponse(errorCode=1, minorBlock=empty_block, index=0)

        return GetTransactionResponse(errorCode=0, minorBlock=minor_block, index=i)

    async def handle_get_transaction_receipt_request(self, req):
        resp = self.slave_server.get_transaction_receipt(req.txHash, req.branch)
        if not resp:
            empty_block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            empty_receipt = TransactionReceipt.create_empty_receipt()
            return GetTransactionReceiptResponse(
                errorCode=1, minorBlock=empty_block, index=0, receipt=empty_receipt)
        minor_block, i, receipt = resp
        return GetTransactionReceiptResponse(
            errorCode=0, minorBlock=minor_block, index=i, receipt=receipt)

    async def handle_get_transaction_list_by_address_request(self, req):
        result = self.slave_server.get_transaction_list_by_address(req.address, req.start, req.limit)
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

        async def __download_blocks(block_hash_list):
            op, resp, rpcId = await v_conn.write_rpc_request(
                CommandOp.GET_MINOR_BLOCK_LIST_REQUEST, GetMinorBlockListRequest(block_hash_list))
            return resp.minorBlockList

        if req.clusterPeerId not in self.v_conn_map:
            return SyncMinorBlockListResponse(errorCode=errno.EBADMSG)
        if req.branch.value not in self.v_conn_map[req.clusterPeerId]:
            return SyncMinorBlockListResponse(errorCode=errno.EBADMSG)

        v_conn = self.v_conn_map[req.clusterPeerId][req.branch.value]

        BLOCK_BATCH_SIZE = 100
        try:
            block_hash_list = req.minorBlockHashList
            while len(block_hash_list) > 0:
                blocks_to_download = block_hash_list[:BLOCK_BATCH_SIZE]
                block_chain = await __download_blocks(blocks_to_download)
                Logger.info("[{}] sync request from master, downloaded {} blocks ({} - {})".format(
                    req.branch.get_shard_id(), len(block_chain),
                    block_chain[0].header.height, block_chain[-1].header.height))
                check(len(block_chain) == len(blocks_to_download))

                await self.slave_server.add_block_list_for_sync(block_chain)
                block_hash_list = block_hash_list[BLOCK_BATCH_SIZE:]

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

    def __init__(self, env, reader, writer, slave_server, slave_id, shard_mask_list, name=None):
        super().__init__(env, reader, writer, CLUSTER_OP_SERIALIZER_MAP, SLAVE_OP_NONRPC_MAP, SLAVE_OP_RPC_MAP, name=name)
        self.slave_server = slave_server
        self.id = slave_id
        self.shard_mask_list = shard_mask_list
        self.shard_state_map = self.slave_server.shard_state_map

        asyncio.ensure_future(self.active_and_loop_forever())

    def __get_shard_size(self):
        return self.slave_server.env.config.SHARD_SIZE

    def has_shard(self, shardId):
        for shardMask in self.shard_mask_list:
            if shardMask.contain_shard_id(shardId):
                return True
        return False

    def close_with_error(self, error):
        Logger.info("Closing connection with slave {}".format(self.id))
        return super().close_with_error(error)

    async def send_ping(self):
        # TODO: Send real root tip and allow shards to confirm each other
        req = Ping(self.slave_server.id, self.slave_server.shard_mask_list, RootBlock(RootBlockHeader()))
        op, resp, rpcId = await self.write_rpc_request(ClusterOp.PING, req)
        return (resp.id, resp.shardMaskList)

    # Cluster RPC handlers

    async def handle_ping(self, ping):
        if not self.id:
            self.id = ping.id
            self.shard_mask_list = ping.shardMaskList
            self.slave_server.add_slave_connection(self)
        if len(self.shard_mask_list) == 0:
            return self.close_with_error("Empty shard mask list from slave {}".format(self.id))

        return Pong(self.slave_server.id, self.slave_server.shard_mask_list)

    # Blockchain RPC handlers

    async def handle_add_xshard_tx_list_request(self, req):
        if req.branch.get_shard_size() != self.__get_shard_size():
            Logger.error(
                "add xshard tx list request shard size mismatch! "
                "Expect: {}, actual: {}".format(self.__get_shard_size(), req.branch.get_shard_size()))
            return AddXshardTxListResponse(errorCode=errno.ESRCH)

        if req.branch.value not in self.shard_state_map:
            Logger.error("cannot find shard id {} locally".format(req.branch.get_shard_id()))
            return AddXshardTxListResponse(errorCode=errno.ENOENT)

        self.shard_state_map[req.branch.value].add_cross_shard_tx_list_by_minor_block_hash(req.minorBlockHash, req.txList)
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
        self.shard_mask_list = self.env.clusterConfig.SHARD_MASK_LIST

        # shard id -> a list of slave running the shard
        self.shard_to_slaves = [[] for i in range(self.__get_shard_size())]
        self.slave_connections = set()
        self.slave_ids = set()

        self.master = None
        self.name = name

        self.artificial_tx_config = None
        self.tx_gen_map = dict()
        self.miner_map = dict()  # branchValue -> Miner
        self.shard_state_map = dict()  # branchValue -> ShardState
        self.__init_shards()
        self.shutdown_in_progress = False
        self.slave_id = 0

        # block hash -> future (that will return when the block is fully propagated in the cluster)
        # the block that has been added locally but not have been fully propagated will have an entry here
        self.add_blockFutures = dict()

    def __init_shards(self):
        ''' branchValue -> ShardState mapping '''
        shard_size = self.__get_shard_size()
        branch_values = set()
        for shardMask in self.shard_mask_list:
            for shard_id in shardMask.iterate(shard_size):
                branch_value = shard_id + shard_size
                branch_values.add(branch_value)

        for branch_value in branch_values:
            shard_id = Branch(branch_value).get_shard_id()
            db = self.__init_shard_db(shard_id)
            self.shard_state_map[branch_value] = ShardState(
                env=self.env,
                shard_id=shard_id,
                db=db,
            )
            self.__init_miner(branch_value)
            self.tx_gen_map[branch_value] = TransactionGenerator(Branch(branch_value), self)

    def __init_shard_db(self, shard_id):
        """
        Given a shardId (*not* full shard id), create a PersistentDB or use the env.db if
        DB_PATH_ROOT is not specified in the ClusterConfig.
        """
        if self.env.clusterConfig.DB_PATH_ROOT is None:
            return self.env.db

        db_path = "{path}/shard-{shardId}.db".format(
            path=self.env.clusterConfig.DB_PATH_ROOT,
            shardId=shard_id,
        )
        return PersistentDb(db_path, clean=self.env.clusterConfig.DB_CLEAN)

    def __init_miner(self, branch_value):
        miner_address = self.env.config.TESTNET_MASTER_ACCOUNT.address_in_branch(Branch(branch_value))

        def __is_syncing():
            return any([vs[branch_value].synchronizer.running for vs in self.master.v_conn_map.values()])

        async def __create_block():
            # hold off mining if the shard is syncing
            while __is_syncing():
                await asyncio.sleep(0.1)

            return self.shard_state_map[branch_value].create_block_to_mine(address=miner_address)

        async def __add_block(block):
            # Do not add block if there is a sync in progress
            if __is_syncing():
                return
            # Do not add stale block
            if self.shard_state_map[block.header.branch.value].header_tip.height >= block.header.height:
                return
            await self.add_block(block)

        def __get_target_block_time():
            return self.artificial_tx_config.targetMinorBlockTime

        self.miner_map[branch_value] = Miner(
            __create_block,
            __add_block,
            __get_target_block_time,
        )

    def init_shard_states(self, root_tip):
        ''' Will be called when master connects to slaves '''
        for _, shardState in self.shard_state_map.items():
            shardState.init_from_root_block(root_tip)

    def start_mining(self, artificial_tx_config):
        self.artificial_tx_config = artificial_tx_config
        for branch_value, miner in self.miner_map.items():
            Logger.info("[{}] start mining with target minor block time {} seconds".format(
                Branch(branch_value).get_shard_id(),
                artificial_tx_config.targetMinorBlockTime,
            ))
            miner.enable()
            miner.mine_new_block_async();

    def create_transactions(self, num_tx_per_shard, x_shard_percent, tx: Transaction):
        for generator in self.tx_gen_map.values():
            generator.generate(num_tx_per_shard, x_shard_percent, tx)

    def stop_mining(self):
        for branch_value, miner in self.miner_map.items():
            Logger.info("[{}] stop mining".format(Branch(branch_value).get_shard_id()))
            miner.disable()

    def __get_shard_size(self):
        return self.env.config.SHARD_SIZE

    def add_slave_connection(self, slave):
        self.slave_ids.add(slave.id)
        self.slave_connections.add(slave)
        for shard_id in range(self.__get_shard_size()):
            if slave.has_shard(shard_id):
                self.shard_to_slaves[shard_id].append(slave)

        # self.__logSummary()

    def __log_summary(self):
        for shard_id, slaves in enumerate(self.shard_to_slaves):
            Logger.info("[{}] is run by slave {}".format(shard_id, [s.id for s in slaves]))

    async def __handle_master_connection_lost(self):
        check(self.master is not None)
        await self.waitUntilClose()

        if not self.shutdown_in_progress:
            # TODO: May reconnect
            self.shutdown()

    async def __handle_new_connection(self, reader, writer):
        # The first connection should always come from master
        if not self.master:
            self.master = MasterConnection(self.env, reader, writer, self, name="{}_master".format(self.name))
            return

        self.slave_id += 1
        self.slave_connections.add(SlaveConnection(self.env, reader, writer, self, None, None,
                                                   name="{}_slave_{}".format(self.name, self.slave_id)))

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
        self.shutdown_in_progress = True
        if self.master is not None:
            self.master.close()
        for slave in self.slave_connections:
            slave.close()
        self.server.close()

    def get_shutdown_future(self):
        return self.server.wait_closed()

    # Blockchain functions

    async def send_minor_block_header_to_master(self, minor_block_header, tx_count, x_shard_tx_count, shard_stats):
        ''' Update master that a minor block has been appended successfully '''
        request = AddMinorBlockHeaderRequest(minor_block_header, tx_count, x_shard_tx_count, shard_stats)
        _, resp, _ = await self.master.write_rpc_request(ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST, request)
        check(resp.errorCode == 0)
        self.artificial_tx_config = resp.artificialTxConfig

    def __get_branch_to_add_xshard_tx_list_request(self, block_hash, xshard_tx_list):
        branch_to_add_xshard_tx_list_request = dict()

        xshard_map = dict()
        for shard_id in range(self.__get_shard_size()):
            xshard_map[shard_id + self.__get_shard_size()] = []

        for xshard_tx in xshard_tx_list:
            shard_id = xshard_tx.toAddress.get_shard_id(self.__get_shard_size())
            branch_value = Branch.create(self.__get_shard_size(), shard_id).value
            xshard_map[branch_value].append(xshard_tx)

        for branch_value, tx_list in xshard_map.items():
            cross_shard_tx_list = CrossShardTransactionList(tx_list)

            branch = Branch(branch_value)
            request = AddXshardTxListRequest(branch, block_hash, cross_shard_tx_list)
            branch_to_add_xshard_tx_list_request[branch] = request

        return branch_to_add_xshard_tx_list_request

    async def broadcast_xshard_tx_list(self, block, xshard_tx_list):
        ''' Broadcast x-shard transactions to their recipient shards '''

        block_hash = block.header.get_hash()
        branch_to_add_xshard_tx_list_request = self.__get_branch_to_add_xshard_tx_list_request(block_hash, xshard_tx_list)
        rpc_futures = []
        for branch, request in branch_to_add_xshard_tx_list_request.items():
            if branch.value in self.shard_state_map:
                self.shard_state_map[branch.value].add_cross_shard_tx_list_by_minor_block_hash(block_hash, request.txList)

            for slave_conn in self.shard_to_slaves[branch.get_shard_id()]:
                future = slave_conn.write_rpc_request(ClusterOp.ADD_XSHARD_TX_LIST_REQUEST, request)
                rpc_futures.append(future)
        responses = await asyncio.gather(*rpc_futures)
        check(all([response.errorCode == 0 for _, response, _ in responses]))

    async def batch_broadcast_xshard_tx_list(self, block_hash_to_xshard_list):
        branch_to_add_xshard_tx_list_request_list = dict()
        for block_hash, x_shard_list in block_hash_to_xshard_list.items():
            branch_to_add_xshard_tx_list_request = self.__get_branch_to_add_xshard_tx_list_request(block_hash, x_shard_list)
            for branch, request in branch_to_add_xshard_tx_list_request.items():
                branch_to_add_xshard_tx_list_request_list.setdefault(branch, []).append(request)

        rpc_futures = []
        for branch, request_list in branch_to_add_xshard_tx_list_request_list.items():
            if branch.value in self.shard_state_map:
                for request in request_list:
                    self.shard_state_map[branch.value].add_cross_shard_tx_list_by_minor_block_hash(
                        request.minorBlockHash, request.txList)

            batch_request = BatchAddXshardTxListRequest(request_list)
            for slaveConn in self.shard_to_slaves[branch.get_shard_id()]:
                future = slaveConn.write_rpc_request(ClusterOp.BATCH_ADD_XSHARD_TX_LIST_REQUEST, batch_request)
                rpc_futures.append(future)
        responses = await asyncio.gather(*rpc_futures)
        check(all([response.errorCode == 0 for _, response, _ in responses]))

    async def add_block(self, block):
        ''' Returns true if block is successfully added. False on any error. '''
        branch_value = block.header.branch.value
        shard_state = self.shard_state_map.get(branch_value, None)

        if not shard_state:
            return False

        old_tip = shard_state.tip()
        try:
            xshard_list = shard_state.add_block(block)
        except Exception as e:
            Logger.errorException()
            return False

        # block has been added to local state and let's pass to peers
        try:
            if old_tip != shard_state.tip():
                self.master.broadcast_new_tip(block.header.branch)
        except Exception:
            Logger.warningEverySec("broadcast tip failure", 1)

        # block already existed in local shard state
        # but might not have been propagated to other shards and master
        # let's make sure all the shards and master got it before return
        if xshard_list is None:
            future = self.add_blockFutures.get(block.header.get_hash(), None)
            if future:
                Logger.info("[{}] {} is being added ... waiting for it to finish".format(
                    block.header.branch.get_shard_id(), block.header.height))
                await future
            return True

        self.add_blockFutures[block.header.get_hash()] = self.loop.create_future()

        # Start mining new one before propagating inside cluster
        # The propagation should be done by the time the new block is mined
        self.miner_map[branch_value].mine_new_block_async()

        await self.broadcast_xshard_tx_list(block, xshard_list)
        await self.send_minor_block_header_to_master(
            block.header, len(block.txList), len(xshard_list), shard_state.get_shard_stats())

        self.add_blockFutures[block.header.get_hash()].set_result(None)
        del self.add_blockFutures[block.header.get_hash()]
        return True

    async def add_block_list_for_sync(self, block_list):
        ''' Add blocks in batch to reduce RPCs. Will NOT broadcast to peers.

        Returns true if blocks are successfully added. False on any error.
        This function only adds blocks to local and propagate xshard list to other shards.
        It does NOT notify master because the master should already have the minor header list,
        and will add them once this function returns successfully.
        '''
        if not block_list:
            return True

        branch_value = block_list[0].header.branch.value
        shard_state = self.shard_state_map.get(branch_value, None)

        if not shard_state:
            return False

        existing_add_block_futures = []
        block_hash_to_x_shard_list = dict()
        for block in block_list:
            block_hash = block.header.get_hash()
            try:
                xshard_list = shard_state.add_block(block)
            except Exception as e:
                Logger.errorException()
                return False

            # block already existed in local shard state
            # but might not have been propagated to other shards and master
            # let's make sure all the shards and master got it before return
            if xshard_list is None:
                future = self.add_blockFutures.get(block_hash, None)
                if future:
                    existing_add_block_futures.append(future)
            else:
                block_hash_to_x_shard_list[block_hash] = xshard_list
                self.add_blockFutures[block_hash] = self.loop.create_future()

        await self.batch_broadcast_xshard_tx_list(block_hash_to_x_shard_list)

        for block_hash in block_hash_to_x_shard_list.keys():
            self.add_blockFutures[block_hash].set_result(None)
            del self.add_blockFutures[block_hash]

        await asyncio.gather(*existing_add_block_futures)

        return True

    def add_tx_list(self, txList, shard_conn=None):
        if not txList:
            return
        evm_tx = txList[0].code.get_evm_transaction()
        evm_tx.set_shard_size(self.__get_shard_size())
        branch_value = evm_tx.from_shard_id() | self.__get_shard_size()
        valid_tx_list = []
        for tx in txList:
            if self.add_tx(tx):
                valid_tx_list.append(tx)
        if not valid_tx_list:
            return
        self.master.broadcast_tx_list(Branch(branch_value), valid_tx_list, shard_conn)

    def add_tx(self, tx):
        evm_tx = tx.code.get_evm_transaction()
        evm_tx.set_shard_size(self.__get_shard_size())
        branch_value = evm_tx.from_shard_id() | self.__get_shard_size()
        shard_state = self.shard_state_map.get(branch_value, None)
        if not shard_state:
            return False
        return shard_state.add_tx(tx)

    def execute_tx(self, tx, from_address) -> Optional[bytes]:
        evm_tx = tx.code.get_evm_transaction()
        evm_tx.set_shard_size(self.__get_shard_size())
        branch_value = evm_tx.from_shard_id() | self.__get_shard_size()
        shard_state = self.shard_state_map.get(branch_value, None)
        if not shard_state:
            return False
        return shard_state.execute_tx(tx, from_address)

    def get_transaction_count(self, address):
        branch = Branch.create(self.__get_shard_size(), address.get_shard_id(self.__get_shard_size()))
        if branch.value not in self.shard_state_map:
            return None
        return self.shard_state_map[branch.value].get_transaction_count(address.recipient)

    def get_balance(self, address):
        branch = Branch.create(self.__get_shard_size(), address.get_shard_id(self.__get_shard_size()))
        if branch.value not in self.shard_state_map:
            return None
        return self.shard_state_map[branch.value].get_balance(address.recipient)

    def get_account_data(self, address):
        results = []
        for branch_value, shard_state in self.shard_state_map.items():
            results.append(AccountBranchData(
                branch=Branch(branch_value),
                transactionCount=shard_state.get_transaction_count(address.recipient),
                balance=shard_state.get_balance(address.recipient),
                isContract=len(shard_state.get_code(address.recipient)) > 0,
            ))
        return results

    def get_minor_block_by_hash(self, blockHash, branch):
        if branch.value not in self.shard_state_map:
            return None

        shard_state = self.shard_state_map[branch.value]
        try:
            return shard_state.db.get_minor_block_by_hash(blockHash, False)
        except Exception:
            return None

    def get_minor_block_by_height(self, height, branch):
        if branch.value not in self.shard_state_map:
            return None

        shard_state = self.shard_state_map[branch.value]
        return shard_state.db.get_minor_block_by_height(height)

    def get_transaction_by_hash(self, tx_hash, branch):
        if branch.value not in self.shard_state_map:
            return None

        shard_state = self.shard_state_map[branch.value]
        return shard_state.get_transaction_by_hash(tx_hash)

    def get_transaction_receipt(self, tx_hash, branch) -> Optional[Tuple[MinorBlock, int, TransactionReceipt]]:
        if branch.value not in self.shard_state_map:
            return None

        shard_state = self.shard_state_map[branch.value]
        return shard_state.get_transaction_receipt(tx_hash)

    def get_transaction_list_by_address(self, address, start, limit):
        branch = Branch.create(self.__get_shard_size(), address.get_shard_id(self.__get_shard_size()))
        if branch.value not in self.shard_state_map:
            return None
        shard_state = self.shard_state_map[branch.value]
        return shard_state.get_transaction_list_by_address(address, start, limit)


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

    slave_server = SlaveServer(env)
    slave_server.start_and_loop()

    Logger.info("Slave server is shutdown")


if __name__ == '__main__':
    main()
