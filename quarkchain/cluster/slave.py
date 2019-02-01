import argparse
import asyncio
import errno
import os
from typing import Optional, Tuple, Dict, List, Union

from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.cluster.miner import MiningWork
from quarkchain.cluster.neighbor import is_neighbor
from quarkchain.cluster.p2p_commands import CommandOp, GetMinorBlockListRequest
from quarkchain.cluster.protocol import (
    ClusterConnection,
    ForwardingVirtualConnection,
    NULL_CONNECTION,
)
from quarkchain.cluster.rpc import (
    AddMinorBlockHeaderRequest,
    GetLogRequest,
    GetLogResponse,
    EstimateGasRequest,
    EstimateGasResponse,
    ExecuteTransactionRequest,
    GetStorageRequest,
    GetStorageResponse,
    GetCodeResponse,
    GetCodeRequest,
    GasPriceRequest,
    GasPriceResponse,
    GetAccountDataRequest,
    GetWorkRequest,
    GetWorkResponse,
    SubmitWorkRequest,
    SubmitWorkResponse,
)
from quarkchain.cluster.rpc import (
    AddRootBlockResponse,
    EcoInfo,
    GetEcoInfoListResponse,
    GetNextBlockToMineResponse,
    AddMinorBlockResponse,
    HeadersInfo,
    GetUnconfirmedHeadersResponse,
    GetAccountDataResponse,
    AddTransactionResponse,
    CreateClusterPeerConnectionResponse,
    SyncMinorBlockListResponse,
    GetMinorBlockResponse,
    GetTransactionResponse,
    AccountBranchData,
    TokenBalancePair,
    BatchAddXshardTxListRequest,
    BatchAddXshardTxListResponse,
    MineResponse,
    GenTxResponse,
    GetTransactionListByAddressResponse,
)
from quarkchain.cluster.rpc import AddXshardTxListRequest, AddXshardTxListResponse
from quarkchain.cluster.rpc import (
    ConnectToSlavesResponse,
    ClusterOp,
    CLUSTER_OP_SERIALIZER_MAP,
    Ping,
    Pong,
    ExecuteTransactionResponse,
    GetTransactionReceiptResponse,
    SlaveInfo,
)
from quarkchain.cluster.shard import Shard, PeerShardConnection
from quarkchain.core import Branch, Transaction, Address, Log
from quarkchain.core import (
    CrossShardTransactionList,
    MinorBlock,
    MinorBlockHeader,
    MinorBlockMeta,
    RootBlock,
    RootBlockHeader,
    TransactionReceipt,
)
from quarkchain.env import DEFAULT_ENV
from quarkchain.protocol import Connection
from quarkchain.utils import check, Logger


TIMEOUT = 10


class MasterConnection(ClusterConnection):
    def __init__(self, env, reader, writer, slave_server, name=None):
        super().__init__(
            env,
            reader,
            writer,
            CLUSTER_OP_SERIALIZER_MAP,
            MASTER_OP_NONRPC_MAP,
            MASTER_OP_RPC_MAP,
            name=name,
        )
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.slave_server = slave_server  # type: SlaveServer
        self.shards = slave_server.shards  # type: Dict[Branch, Shard]

        asyncio.ensure_future(self.active_and_loop_forever())

        # cluster_peer_id -> {branch_value -> shard_conn}
        self.v_conn_map = dict()

    def get_connection_to_forward(self, metadata):
        """ Override ProxyConnection.get_connection_to_forward()
        """
        if metadata.cluster_peer_id == 0:
            # RPC from master
            return None

        if (
            metadata.branch.get_full_shard_id()
            not in self.env.quark_chain_config.get_full_shard_ids()
        ):
            self.close_with_error(
                "incorrect forwarding branch {}".format(metadata.branch.to_str())
            )

        shard = self.shards.get(metadata.branch, None)
        if not shard:
            # shard has not been created yet
            return NULL_CONNECTION

        peer_shard_conn = shard.peers.get(metadata.cluster_peer_id, None)
        if peer_shard_conn is None:
            # Master can close the peer connection at any time
            # TODO: any way to avoid this race?
            Logger.warning_every_sec(
                "cannot find peer shard conn for cluster id {}".format(
                    metadata.cluster_peer_id
                ),
                1,
            )
            return NULL_CONNECTION

        return peer_shard_conn.get_forwarding_connection()

    def validate_connection(self, connection):
        return connection == NULL_CONNECTION or isinstance(
            connection, ForwardingVirtualConnection
        )

    def close(self):
        for shard in self.shards.values():
            for peer_shard_conn in shard.peers.values():
                peer_shard_conn.get_forwarding_connection().close()

        Logger.info("Lost connection with master. Shutting down slave ...")
        super().close()
        self.slave_server.shutdown()

    def close_with_error(self, error):
        Logger.info("Closing connection with master: {}".format(error))
        return super().close_with_error(error)

    def close_connection(self, conn):
        """ TODO: Notify master that the connection is closed by local.
        The master should close the peer connection, and notify the other slaves that a close happens
        More hint could be provided so that the master may blacklist the peer if it is mis-behaving
        """
        pass

    # Cluster RPC handlers

    async def handle_ping(self, ping):
        if ping.root_tip:
            await self.slave_server.create_shards(ping.root_tip)
        return Pong(self.slave_server.id, self.slave_server.chain_mask_list)

    async def handle_connect_to_slaves_request(self, connect_to_slave_request):
        """
        Master sends in the slave list. Let's connect to them.
        Skip self and slaves already connected.
        """
        futures = []
        for slave_info in connect_to_slave_request.slave_info_list:
            futures.append(
                self.slave_server.slave_connection_manager.connect_to_slave(slave_info)
            )
        result_str_list = await asyncio.gather(*futures)
        result_list = [bytes(result_str, "ascii") for result_str in result_str_list]
        return ConnectToSlavesResponse(result_list)

    async def handle_mine_request(self, request):
        if request.mining:
            self.slave_server.start_mining(request.artificial_tx_config)
        else:
            self.slave_server.stop_mining()
        return MineResponse(error_code=0)

    async def handle_gen_tx_request(self, request):
        self.slave_server.create_transactions(
            request.num_tx_per_shard, request.x_shard_percent, request.tx
        )
        return GenTxResponse(error_code=0)

    # Blockchain RPC handlers

    async def handle_add_root_block_request(self, req):
        # TODO: handle expect_switch
        error_code = 0
        switched = False
        for shard in self.shards.values():
            try:
                switched = await shard.add_root_block(req.root_block)
            except ValueError:
                Logger.log_exception()
                return AddRootBlockResponse(errno.EBADMSG, False)

        await self.slave_server.create_shards(req.root_block)

        return AddRootBlockResponse(error_code, switched)

    async def handle_get_eco_info_list_request(self, _req):
        eco_info_list = []
        for branch, shard in self.shards.items():
            if not shard.state.initialized:
                continue
            eco_info_list.append(
                EcoInfo(
                    branch=branch,
                    height=shard.state.header_tip.height + 1,
                    coinbase_amount=shard.state.get_next_block_coinbase_amount(),
                    difficulty=shard.state.get_next_block_difficulty(),
                    unconfirmed_headers_coinbase_amount=shard.state.get_unconfirmed_headers_coinbase_amount(),
                )
            )
        return GetEcoInfoListResponse(error_code=0, eco_info_list=eco_info_list)

    async def handle_get_next_block_to_mine_request(self, req):
        shard = self.shards.get(req.branch, None)
        check(shard is not None)
        block = shard.state.create_block_to_mine(address=req.address)
        response = GetNextBlockToMineResponse(error_code=0, block=block)
        return response

    async def handle_add_minor_block_request(self, req):
        """ For local miner to submit mined blocks through master """
        try:
            block = MinorBlock.deserialize(req.minor_block_data)
        except Exception:
            return AddMinorBlockResponse(error_code=errno.EBADMSG)
        shard = self.shards.get(block.header.branch, None)
        if not shard:
            return AddMinorBlockResponse(error_code=errno.EBADMSG)

        if block.header.hash_prev_minor_block != shard.state.header_tip.get_hash():
            # Tip changed, don't bother creating a fork
            Logger.info(
                "[{}] dropped stale block {} mined locally".format(
                    block.header.branch.to_str(), block.header.height
                )
            )
            return AddMinorBlockResponse(error_code=0)

        success = await shard.add_block(block)
        return AddMinorBlockResponse(error_code=0 if success else errno.EFAULT)

    async def handle_get_unconfirmed_header_list_request(self, _req):
        headers_info_list = []
        for branch, shard in self.shards.items():
            if not shard.state.initialized:
                continue
            headers_info_list.append(
                HeadersInfo(
                    branch=branch, header_list=shard.state.get_unconfirmed_header_list()
                )
            )
        return GetUnconfirmedHeadersResponse(
            error_code=0, headers_info_list=headers_info_list
        )

    async def handle_get_account_data_request(
        self, req: GetAccountDataRequest
    ) -> GetAccountDataResponse:
        account_branch_data_list = self.slave_server.get_account_data(
            req.address, req.block_height
        )
        return GetAccountDataResponse(
            error_code=0, account_branch_data_list=account_branch_data_list
        )

    async def handle_add_transaction(self, req):
        success = self.slave_server.add_tx(req.tx)
        return AddTransactionResponse(error_code=0 if success else 1)

    async def handle_execute_transaction(
        self, req: ExecuteTransactionRequest
    ) -> ExecuteTransactionResponse:
        res = self.slave_server.execute_tx(req.tx, req.from_address)
        fail = res is None
        return ExecuteTransactionResponse(
            error_code=int(fail), result=res if not fail else b""
        )

    async def handle_destroy_cluster_peer_connection_command(self, op, cmd, rpc_id):
        self.slave_server.remove_cluster_peer_id(cmd.cluster_peer_id)

        for shard in self.shards.values():
            peer_shard_conn = shard.peers.pop(cmd.cluster_peer_id, None)
            if peer_shard_conn:
                peer_shard_conn.get_forwarding_connection().close()

    async def handle_create_cluster_peer_connection_request(self, req):
        self.slave_server.add_cluster_peer_id(req.cluster_peer_id)

        shard_to_conn = dict()
        active_futures = []
        for shard in self.shards.values():
            if req.cluster_peer_id in shard.peers:
                Logger.error(
                    "duplicated create cluster peer connection {}".format(
                        req.cluster_peer_id
                    )
                )
                continue

            peer_shard_conn = PeerShardConnection(
                master_conn=self,
                cluster_peer_id=req.cluster_peer_id,
                shard=shard,
                name="{}_vconn_{}".format(self.name, req.cluster_peer_id),
            )
            asyncio.ensure_future(peer_shard_conn.active_and_loop_forever())
            active_futures.append(peer_shard_conn.active_future)
            shard_to_conn[shard] = peer_shard_conn

        # wait for all the connections to become active before return
        await asyncio.gather(*active_futures)

        # Make peer connection available to shard once they are active
        for shard, peer_shard_conn in shard_to_conn.items():
            shard.add_peer(peer_shard_conn)

        return CreateClusterPeerConnectionResponse(error_code=0)

    async def handle_get_minor_block_request(self, req):
        if req.minor_block_hash != bytes(32):
            block = self.slave_server.get_minor_block_by_hash(
                req.minor_block_hash, req.branch
            )
        else:
            block = self.slave_server.get_minor_block_by_height(req.height, req.branch)

        if not block:
            empty_block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            return GetMinorBlockResponse(error_code=1, minor_block=empty_block)

        return GetMinorBlockResponse(error_code=0, minor_block=block)

    async def handle_get_transaction_request(self, req):
        minor_block, i = self.slave_server.get_transaction_by_hash(
            req.tx_hash, req.branch
        )
        if not minor_block:
            empty_block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            return GetTransactionResponse(
                error_code=1, minor_block=empty_block, index=0
            )

        return GetTransactionResponse(error_code=0, minor_block=minor_block, index=i)

    async def handle_get_transaction_receipt_request(self, req):
        resp = self.slave_server.get_transaction_receipt(req.tx_hash, req.branch)
        if not resp:
            empty_block = MinorBlock(MinorBlockHeader(), MinorBlockMeta())
            empty_receipt = TransactionReceipt.create_empty_receipt()
            return GetTransactionReceiptResponse(
                error_code=1, minor_block=empty_block, index=0, receipt=empty_receipt
            )
        minor_block, i, receipt = resp
        return GetTransactionReceiptResponse(
            error_code=0, minor_block=minor_block, index=i, receipt=receipt
        )

    async def handle_get_transaction_list_by_address_request(self, req):
        result = self.slave_server.get_transaction_list_by_address(
            req.address, req.start, req.limit
        )
        if not result:
            return GetTransactionListByAddressResponse(
                error_code=1, tx_list=[], next=b""
            )
        return GetTransactionListByAddressResponse(
            error_code=0, tx_list=result[0], next=result[1]
        )

    async def handle_sync_minor_block_list_request(self, req):
        """ Raises on error"""

        async def __download_blocks(block_hash_list):
            op, resp, rpc_id = await peer_shard_conn.write_rpc_request(
                CommandOp.GET_MINOR_BLOCK_LIST_REQUEST,
                GetMinorBlockListRequest(block_hash_list),
            )
            return resp.minor_block_list

        shard = self.shards.get(req.branch, None)
        if not shard:
            return SyncMinorBlockListResponse(error_code=errno.EBADMSG)
        peer_shard_conn = shard.peers.get(req.cluster_peer_id, None)
        if not peer_shard_conn:
            return SyncMinorBlockListResponse(error_code=errno.EBADMSG)

        BLOCK_BATCH_SIZE = 100
        block_hash_list = req.minor_block_hash_list
        # empty
        if not block_hash_list:
            return SyncMinorBlockListResponse(error_code=0)

        try:
            while len(block_hash_list) > 0:
                blocks_to_download = block_hash_list[:BLOCK_BATCH_SIZE]
                try:
                    block_chain = await asyncio.wait_for(
                        __download_blocks(blocks_to_download), TIMEOUT
                    )
                except asyncio.TimeoutError as e:
                    Logger.info(
                        "[{}] sync request from master failed due to timeout".format(
                            req.branch.to_str()
                        )
                    )
                    raise e

                Logger.info(
                    "[{}] sync request from master, downloaded {} blocks ({} - {})".format(
                        req.branch.to_str(),
                        len(block_chain),
                        block_chain[0].header.height,
                        block_chain[-1].header.height,
                    )
                )

                # Step 1: Check if the len is correct
                if len(block_chain) != len(blocks_to_download):
                    raise RuntimeError(
                        "Failed to add minor blocks for syncing root block: "
                        + "length of downloaded block list is incorrect"
                    )

                # Step 2: Check if the blocks are valid
                add_block_success = await self.slave_server.add_block_list_for_sync(
                    block_chain
                )
                if not add_block_success:
                    raise RuntimeError(
                        "Failed to add minor blocks for syncing root block"
                    )
                block_hash_list = block_hash_list[BLOCK_BATCH_SIZE:]

            branch = block_chain[0].header.branch
            shard = self.slave_server.shards.get(branch, None)
            check(shard is not None)
            return SyncMinorBlockListResponse(
                error_code=0, shard_stats=shard.state.get_shard_stats()
            )
        except Exception:
            Logger.error_exception()
            return SyncMinorBlockListResponse(error_code=1)

    async def handle_get_logs(self, req: GetLogRequest) -> GetLogResponse:
        res = self.slave_server.get_logs(
            req.addresses, req.topics, req.start_block, req.end_block, req.branch
        )
        fail = res is None
        return GetLogResponse(
            error_code=int(fail),
            logs=res or [],  # `None` will be converted to empty list
        )

    async def handle_estimate_gas(self, req: EstimateGasRequest) -> EstimateGasResponse:
        res = self.slave_server.estimate_gas(req.tx, req.from_address)
        fail = res is None
        return EstimateGasResponse(error_code=int(fail), result=res or 0)

    async def handle_get_storage_at(self, req: GetStorageRequest) -> GetStorageResponse:
        res = self.slave_server.get_storage_at(req.address, req.key, req.block_height)
        fail = res is None
        return GetStorageResponse(error_code=int(fail), result=res or b"")

    async def handle_get_code(self, req: GetCodeRequest) -> GetCodeResponse:
        res = self.slave_server.get_code(req.address, req.block_height)
        fail = res is None
        return GetCodeResponse(error_code=int(fail), result=res or b"")

    async def handle_gas_price(self, req: GasPriceRequest) -> GasPriceResponse:
        res = self.slave_server.gas_price(req.branch)
        fail = res is None
        return GasPriceResponse(error_code=int(fail), result=res or 0)

    async def handle_get_work(self, req: GetWorkRequest) -> GetWorkResponse:
        res = await self.slave_server.get_work(req.branch)
        if not res:
            return GetWorkResponse(error_code=1)
        return GetWorkResponse(
            error_code=0,
            header_hash=res.hash,
            height=res.height,
            difficulty=res.difficulty,
        )

    async def handle_submit_work(self, req: SubmitWorkRequest) -> SubmitWorkResponse:
        res = await self.slave_server.submit_work(
            req.branch, req.header_hash, req.nonce, req.mixhash
        )
        if res is None:
            return SubmitWorkResponse(error_code=1, success=False)

        return SubmitWorkResponse(error_code=0, success=res)


MASTER_OP_NONRPC_MAP = {
    ClusterOp.DESTROY_CLUSTER_PEER_CONNECTION_COMMAND: MasterConnection.handle_destroy_cluster_peer_connection_command
}


MASTER_OP_RPC_MAP = {
    ClusterOp.PING: (ClusterOp.PONG, MasterConnection.handle_ping),
    ClusterOp.CONNECT_TO_SLAVES_REQUEST: (
        ClusterOp.CONNECT_TO_SLAVES_RESPONSE,
        MasterConnection.handle_connect_to_slaves_request,
    ),
    ClusterOp.MINE_REQUEST: (
        ClusterOp.MINE_RESPONSE,
        MasterConnection.handle_mine_request,
    ),
    ClusterOp.GEN_TX_REQUEST: (
        ClusterOp.GEN_TX_RESPONSE,
        MasterConnection.handle_gen_tx_request,
    ),
    ClusterOp.ADD_ROOT_BLOCK_REQUEST: (
        ClusterOp.ADD_ROOT_BLOCK_RESPONSE,
        MasterConnection.handle_add_root_block_request,
    ),
    ClusterOp.GET_ECO_INFO_LIST_REQUEST: (
        ClusterOp.GET_ECO_INFO_LIST_RESPONSE,
        MasterConnection.handle_get_eco_info_list_request,
    ),
    ClusterOp.GET_NEXT_BLOCK_TO_MINE_REQUEST: (
        ClusterOp.GET_NEXT_BLOCK_TO_MINE_RESPONSE,
        MasterConnection.handle_get_next_block_to_mine_request,
    ),
    ClusterOp.ADD_MINOR_BLOCK_REQUEST: (
        ClusterOp.ADD_MINOR_BLOCK_RESPONSE,
        MasterConnection.handle_add_minor_block_request,
    ),
    ClusterOp.GET_UNCONFIRMED_HEADERS_REQUEST: (
        ClusterOp.GET_UNCONFIRMED_HEADERS_RESPONSE,
        MasterConnection.handle_get_unconfirmed_header_list_request,
    ),
    ClusterOp.GET_ACCOUNT_DATA_REQUEST: (
        ClusterOp.GET_ACCOUNT_DATA_RESPONSE,
        MasterConnection.handle_get_account_data_request,
    ),
    ClusterOp.ADD_TRANSACTION_REQUEST: (
        ClusterOp.ADD_TRANSACTION_RESPONSE,
        MasterConnection.handle_add_transaction,
    ),
    ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST: (
        ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_RESPONSE,
        MasterConnection.handle_create_cluster_peer_connection_request,
    ),
    ClusterOp.GET_MINOR_BLOCK_REQUEST: (
        ClusterOp.GET_MINOR_BLOCK_RESPONSE,
        MasterConnection.handle_get_minor_block_request,
    ),
    ClusterOp.GET_TRANSACTION_REQUEST: (
        ClusterOp.GET_TRANSACTION_RESPONSE,
        MasterConnection.handle_get_transaction_request,
    ),
    ClusterOp.SYNC_MINOR_BLOCK_LIST_REQUEST: (
        ClusterOp.SYNC_MINOR_BLOCK_LIST_RESPONSE,
        MasterConnection.handle_sync_minor_block_list_request,
    ),
    ClusterOp.EXECUTE_TRANSACTION_REQUEST: (
        ClusterOp.EXECUTE_TRANSACTION_RESPONSE,
        MasterConnection.handle_execute_transaction,
    ),
    ClusterOp.GET_TRANSACTION_RECEIPT_REQUEST: (
        ClusterOp.GET_TRANSACTION_RECEIPT_RESPONSE,
        MasterConnection.handle_get_transaction_receipt_request,
    ),
    ClusterOp.GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST: (
        ClusterOp.GET_TRANSACTION_LIST_BY_ADDRESS_RESPONSE,
        MasterConnection.handle_get_transaction_list_by_address_request,
    ),
    ClusterOp.GET_LOG_REQUEST: (
        ClusterOp.GET_LOG_RESPONSE,
        MasterConnection.handle_get_logs,
    ),
    ClusterOp.ESTIMATE_GAS_REQUEST: (
        ClusterOp.ESTIMATE_GAS_RESPONSE,
        MasterConnection.handle_estimate_gas,
    ),
    ClusterOp.GET_STORAGE_REQUEST: (
        ClusterOp.GET_STORAGE_RESPONSE,
        MasterConnection.handle_get_storage_at,
    ),
    ClusterOp.GET_CODE_REQUEST: (
        ClusterOp.GET_CODE_RESPONSE,
        MasterConnection.handle_get_code,
    ),
    ClusterOp.GAS_PRICE_REQUEST: (
        ClusterOp.GAS_PRICE_RESPONSE,
        MasterConnection.handle_gas_price,
    ),
    ClusterOp.GET_WORK_REQUEST: (
        ClusterOp.GET_WORK_RESPONSE,
        MasterConnection.handle_get_work,
    ),
    ClusterOp.SUBMIT_WORK_REQUEST: (
        ClusterOp.SUBMIT_WORK_RESPONSE,
        MasterConnection.handle_submit_work,
    ),
}


class SlaveConnection(Connection):
    def __init__(
        self, env, reader, writer, slave_server, slave_id, chain_mask_list, name=None
    ):
        super().__init__(
            env,
            reader,
            writer,
            CLUSTER_OP_SERIALIZER_MAP,
            SLAVE_OP_NONRPC_MAP,
            SLAVE_OP_RPC_MAP,
            name=name,
        )
        self.slave_server = slave_server
        self.id = slave_id
        self.chain_mask_list = chain_mask_list
        self.shards = self.slave_server.shards

        self.ping_received_future = asyncio.get_event_loop().create_future()

        asyncio.ensure_future(self.active_and_loop_forever())

    async def wait_until_ping_received(self):
        await self.ping_received_future

    def has_shard(self, full_shard_id: int):
        for chain_mask in self.chain_mask_list:
            if chain_mask.contain_full_shard_id(full_shard_id):
                return True
        return False

    def close_with_error(self, error):
        Logger.info("Closing connection with slave {}".format(self.id))
        return super().close_with_error(error)

    async def send_ping(self):
        # TODO: Send real root tip and allow shards to confirm each other
        req = Ping(
            self.slave_server.id,
            self.slave_server.chain_mask_list,
            RootBlock(RootBlockHeader()),
        )
        op, resp, rpc_id = await self.write_rpc_request(ClusterOp.PING, req)
        return (resp.id, resp.chain_mask_list)

    # Cluster RPC handlers

    async def handle_ping(self, ping: Ping):
        if not self.id:
            self.id = ping.id
            self.chain_mask_list = ping.chain_mask_list

        if len(self.chain_mask_list) == 0:
            return self.close_with_error(
                "Empty shard mask list from slave {}".format(self.id)
            )

        self.ping_received_future.set_result(None)

        return Pong(self.slave_server.id, self.slave_server.chain_mask_list)

    # Blockchain RPC handlers

    async def handle_add_xshard_tx_list_request(self, req):
        if req.branch not in self.shards:
            Logger.error(
                "cannot find shard id {} locally".format(req.branch.get_full_shard_id())
            )
            return AddXshardTxListResponse(error_code=errno.ENOENT)

        self.shards[req.branch].state.add_cross_shard_tx_list_by_minor_block_hash(
            req.minor_block_hash, req.tx_list
        )
        return AddXshardTxListResponse(error_code=0)

    async def handle_batch_add_xshard_tx_list_request(self, batch_request):
        for request in batch_request.add_xshard_tx_list_request_list:
            response = await self.handle_add_xshard_tx_list_request(request)
            if response.error_code != 0:
                return BatchAddXshardTxListResponse(error_code=response.error_code)
        return BatchAddXshardTxListResponse(error_code=0)


SLAVE_OP_NONRPC_MAP = {}


SLAVE_OP_RPC_MAP = {
    ClusterOp.PING: (ClusterOp.PONG, SlaveConnection.handle_ping),
    ClusterOp.ADD_XSHARD_TX_LIST_REQUEST: (
        ClusterOp.ADD_XSHARD_TX_LIST_RESPONSE,
        SlaveConnection.handle_add_xshard_tx_list_request,
    ),
    ClusterOp.BATCH_ADD_XSHARD_TX_LIST_REQUEST: (
        ClusterOp.BATCH_ADD_XSHARD_TX_LIST_RESPONSE,
        SlaveConnection.handle_batch_add_xshard_tx_list_request,
    ),
}


class SlaveConnectionManager:
    """Manage a list of connections to other slaves"""

    def __init__(self, env, slave_server):
        self.env = env
        self.slave_server = slave_server
        self.full_shard_id_to_slaves = dict()  # full_shard_id -> list of slaves
        for full_shard_id in self.env.quark_chain_config.get_full_shard_ids():
            self.full_shard_id_to_slaves[full_shard_id] = []
        self.slave_connections = set()
        self.slave_ids = set()  # set(bytes)
        self.loop = asyncio.get_event_loop()

    def close_all(self):
        for conn in self.slave_connections:
            conn.close()

    def get_connections_by_full_shard_id(self, full_shard_id: int):
        return self.full_shard_id_to_slaves[full_shard_id]

    def _add_slave_connection(self, slave: SlaveConnection):
        self.slave_ids.add(slave.id)
        self.slave_connections.add(slave)
        for full_shard_id in self.env.quark_chain_config.get_full_shard_ids():
            if slave.has_shard(full_shard_id):
                self.full_shard_id_to_slaves[full_shard_id].append(slave)

    async def handle_new_connection(self, reader, writer):
        """ Handle incoming connection """
        # slave id and chain_mask_list will be set in handle_ping()
        slave_conn = SlaveConnection(
            self.env,
            reader,
            writer,
            self.slave_server,
            None,  # slave id
            None,  # chain_mask_list
        )
        await slave_conn.wait_until_ping_received()
        slave_conn.name = "{}<->{}".format(
            self.slave_server.id.decode("ascii"), slave_conn.id.decode("ascii")
        )
        self._add_slave_connection(slave_conn)

    async def connect_to_slave(self, slave_info: SlaveInfo) -> str:
        """ Create a connection to a slave server.
        Returns empty str on success otherwise return the error message."""
        if slave_info.id == self.slave_server.id or slave_info.id in self.slave_ids:
            return ""

        host = slave_info.host.decode("ascii")
        port = slave_info.port
        try:
            reader, writer = await asyncio.open_connection(host, port, loop=self.loop)
        except Exception as e:
            err_msg = "Failed to connect {}:{} with exception {}".format(host, port, e)
            Logger.info(err_msg)
            return err_msg

        conn_name = "{}<->{}".format(
            self.slave_server.id.decode("ascii"), slave_info.id.decode("ascii")
        )
        slave = SlaveConnection(
            self.env,
            reader,
            writer,
            self.slave_server,
            slave_info.id,
            slave_info.chain_mask_list,
            conn_name,
        )
        await slave.wait_until_active()
        # Tell the remote slave who I am
        id, chain_mask_list = await slave.send_ping()
        # Verify that remote slave indeed has the id and shard mask list advertised by the master
        if id != slave.id:
            return "id does not match. expect {} got {}".format(slave.id, id)
        if chain_mask_list != slave.chain_mask_list:
            return "shard mask list does not match. expect {} got {}".format(
                slave.chain_mask_list, chain_mask_list
            )

        self._add_slave_connection(slave)
        return ""


class SlaveServer:
    """ Slave node in a cluster """

    def __init__(self, env, name="slave"):
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.id = bytes(self.env.slave_config.ID, "ascii")
        self.chain_mask_list = self.env.slave_config.CHAIN_MASK_LIST

        # shard id -> a list of slave running the shard
        self.slave_connection_manager = SlaveConnectionManager(env, self)

        # A set of active cluster peer ids for building Shard.peers when creating new Shard.
        self.cluster_peer_ids = set()

        self.master = None
        self.name = name
        self.mining = False

        self.artificial_tx_config = None
        self.shards = dict()  # type: Dict[Branch, Shard]
        self.shutdown_future = self.loop.create_future()

        # block hash -> future (that will return when the block is fully propagated in the cluster)
        # the block that has been added locally but not have been fully propagated will have an entry here
        self.add_block_futures = dict()

    def __cover_shard_id(self, full_shard_id):
        """ Does the shard belong to this slave? """
        for chain_mask in self.chain_mask_list:
            if chain_mask.contain_full_shard_id(full_shard_id):
                return True
        return False

    def add_cluster_peer_id(self, cluster_peer_id):
        self.cluster_peer_ids.add(cluster_peer_id)

    def remove_cluster_peer_id(self, cluster_peer_id):
        if cluster_peer_id in self.cluster_peer_ids:
            self.cluster_peer_ids.remove(cluster_peer_id)

    async def create_shards(self, root_block: RootBlock):
        """ Create shards based on GENESIS config and root block height if they have
        not been created yet."""

        async def __init_shard(shard):
            await shard.init_from_root_block(root_block)
            await shard.create_peer_shard_connections(
                self.cluster_peer_ids, self.master
            )
            branch = Branch(shard.full_shard_id)
            self.shards[branch] = shard
            if self.mining:
                shard.miner.start()

        new_shards = []
        for (full_shard_id, shard_config) in self.env.quark_chain_config.shards.items():
            branch = Branch(full_shard_id)
            if branch in self.shards:
                continue
            if not self.__cover_shard_id(full_shard_id) or not shard_config.GENESIS:
                continue
            if root_block.header.height >= shard_config.GENESIS.ROOT_HEIGHT:
                new_shards.append(Shard(self.env, full_shard_id, self))

        await asyncio.gather(*[__init_shard(shard) for shard in new_shards])

    def start_mining(self, artificial_tx_config):
        self.artificial_tx_config = artificial_tx_config
        self.mining = True
        for branch, shard in self.shards.items():
            Logger.info(
                "[{}] start mining with target minor block time {} seconds".format(
                    branch.to_str(), artificial_tx_config.target_minor_block_time
                )
            )
            shard.miner.start()

    def create_transactions(self, num_tx_per_shard, x_shard_percent, tx: Transaction):
        for shard in self.shards.values():
            shard.tx_generator.generate(num_tx_per_shard, x_shard_percent, tx)

    def stop_mining(self):
        self.mining = False
        for branch, shard in self.shards.items():
            Logger.info("[{}] stop mining".format(branch.to_str()))
            shard.miner.disable()

    async def __handle_new_connection(self, reader, writer):
        # The first connection should always come from master
        if not self.master:
            self.master = MasterConnection(
                self.env, reader, writer, self, name="{}_master".format(self.name)
            )
            return
        await self.slave_connection_manager.handle_new_connection(reader, writer)

    async def __start_server(self):
        """ Run the server until shutdown is called """
        self.server = await asyncio.start_server(
            self.__handle_new_connection,
            "0.0.0.0",
            self.env.slave_config.PORT,
            loop=self.loop,
        )
        Logger.info(
            "Listening on {} for intra-cluster RPC".format(
                self.server.sockets[0].getsockname()
            )
        )

    def start(self):
        self.loop.create_task(self.__start_server())

    def do_loop(self):
        try:
            self.loop.run_until_complete(self.shutdown_future)
        except KeyboardInterrupt:
            pass

    def shutdown(self):
        if not self.shutdown_future.done():
            self.shutdown_future.set_result(None)

        self.slave_connection_manager.close_all()
        self.server.close()

    def get_shutdown_future(self):
        return self.shutdown_future

    # Cluster functions

    async def send_minor_block_header_to_master(
        self, minor_block_header, tx_count, x_shard_tx_count, shard_stats
    ):
        """ Update master that a minor block has been appended successfully """
        request = AddMinorBlockHeaderRequest(
            minor_block_header, tx_count, x_shard_tx_count, shard_stats
        )
        _, resp, _ = await self.master.write_rpc_request(
            ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST, request
        )
        check(resp.error_code == 0)
        self.artificial_tx_config = resp.artificial_tx_config

    def __get_branch_to_add_xshard_tx_list_request(
        self, block_hash, xshard_tx_list, prev_root_height
    ):
        xshard_map = dict()  # type: Dict[Branch, List[CrossShardTransactionDeposit]]

        # only broadcast to the shards that have been initialized
        initialized_full_shard_ids = self.env.quark_chain_config.get_initialized_full_shard_ids_before_root_height(
            prev_root_height
        )
        for full_shard_id in initialized_full_shard_ids:
            branch = Branch(full_shard_id)
            xshard_map[branch] = []

        for xshard_tx in xshard_tx_list:
            full_shard_id = self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                xshard_tx.to_address.full_shard_key
            )
            branch = Branch(full_shard_id)
            check(branch in xshard_map)
            xshard_map[branch].append(xshard_tx)

        branch_to_add_xshard_tx_list_request = (
            dict()
        )  # type: Dict[Branch, AddXshardTxListRequest]
        for branch, tx_list in xshard_map.items():
            cross_shard_tx_list = CrossShardTransactionList(tx_list)

            request = AddXshardTxListRequest(branch, block_hash, cross_shard_tx_list)
            branch_to_add_xshard_tx_list_request[branch] = request

        return branch_to_add_xshard_tx_list_request

    async def broadcast_xshard_tx_list(self, block, xshard_tx_list, prev_root_height):
        """ Broadcast x-shard transactions to their recipient shards """

        block_hash = block.header.get_hash()
        branch_to_add_xshard_tx_list_request = self.__get_branch_to_add_xshard_tx_list_request(
            block_hash, xshard_tx_list, prev_root_height
        )
        rpc_futures = []
        for branch, request in branch_to_add_xshard_tx_list_request.items():
            if branch == block.header.branch or not is_neighbor(
                block.header.branch,
                branch,
                len(
                    self.env.quark_chain_config.get_initialized_full_shard_ids_before_root_height(
                        prev_root_height
                    )
                ),
            ):
                check(
                    len(request.tx_list.tx_list) == 0,
                    "there shouldn't be xshard list for non-neighbor shard ({} -> {})".format(
                        block.header.branch.value, branch.value
                    ),
                )
                continue

            if branch in self.shards:
                self.shards[branch].state.add_cross_shard_tx_list_by_minor_block_hash(
                    block_hash, request.tx_list
                )

            for (
                slave_conn
            ) in self.slave_connection_manager.get_connections_by_full_shard_id(
                branch.get_full_shard_id()
            ):
                future = slave_conn.write_rpc_request(
                    ClusterOp.ADD_XSHARD_TX_LIST_REQUEST, request
                )
                rpc_futures.append(future)
        responses = await asyncio.gather(*rpc_futures)
        check(all([response.error_code == 0 for _, response, _ in responses]))

    async def batch_broadcast_xshard_tx_list(
        self,
        block_hash_to_xshard_list_and_prev_root_height: Dict[bytes, Tuple[List, int]],
        source_branch: Branch,
    ):
        branch_to_add_xshard_tx_list_request_list = dict()
        for (
            block_hash,
            x_shard_list_and_prev_root_height,
        ) in block_hash_to_xshard_list_and_prev_root_height.items():
            xshard_tx_list = x_shard_list_and_prev_root_height[0]
            prev_root_height = x_shard_list_and_prev_root_height[1]
            branch_to_add_xshard_tx_list_request = self.__get_branch_to_add_xshard_tx_list_request(
                block_hash, xshard_tx_list, prev_root_height
            )
            for branch, request in branch_to_add_xshard_tx_list_request.items():
                if branch == source_branch or not is_neighbor(
                    branch,
                    source_branch,
                    len(
                        self.env.quark_chain_config.get_initialized_full_shard_ids_before_root_height(
                            prev_root_height
                        )
                    ),
                ):
                    check(
                        len(request.tx_list.tx_list) == 0,
                        "there shouldn't be xshard list for non-neighbor shard ({} -> {})".format(
                            source_branch.value, branch.value
                        ),
                    )
                    continue

                branch_to_add_xshard_tx_list_request_list.setdefault(branch, []).append(
                    request
                )

        rpc_futures = []
        for branch, request_list in branch_to_add_xshard_tx_list_request_list.items():
            if branch in self.shards:
                for request in request_list:
                    self.shards[
                        branch
                    ].state.add_cross_shard_tx_list_by_minor_block_hash(
                        request.minor_block_hash, request.tx_list
                    )

            batch_request = BatchAddXshardTxListRequest(request_list)
            for (
                slave_conn
            ) in self.slave_connection_manager.get_connections_by_full_shard_id(
                branch.get_full_shard_id()
            ):
                future = slave_conn.write_rpc_request(
                    ClusterOp.BATCH_ADD_XSHARD_TX_LIST_REQUEST, batch_request
                )
                rpc_futures.append(future)
        responses = await asyncio.gather(*rpc_futures)
        check(all([response.error_code == 0 for _, response, _ in responses]))

    async def add_block_list_for_sync(self, block_list):
        """ Add blocks in batch to reduce RPCs. Will NOT broadcast to peers.
        Returns true if blocks are successfully added. False on any error.
        """
        if not block_list:
            return True
        branch = block_list[0].header.branch
        shard = self.shards.get(branch, None)
        check(shard is not None)
        return await shard.add_block_list_for_sync(block_list)

    def add_tx(self, tx: Transaction) -> bool:
        evm_tx = tx.code.get_evm_transaction()
        evm_tx.set_quark_chain_config(self.env.quark_chain_config)
        branch = Branch(evm_tx.from_full_shard_id)
        shard = self.shards.get(branch, None)
        if not shard:
            return False
        return shard.add_tx(tx)

    def execute_tx(self, tx, from_address) -> Optional[bytes]:
        evm_tx = tx.code.get_evm_transaction()
        evm_tx.set_quark_chain_config(self.env.quark_chain_config)
        branch = Branch(evm_tx.from_full_shard_id)
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.execute_tx(tx, from_address)

    def get_transaction_count(self, address):
        branch = Branch(
            self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                address.full_shard_key
            )
        )
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.get_transaction_count(address.recipient)

    def get_balances(self, address):
        branch = Branch(
            self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                address.full_shard_key
            )
        )
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.get_balances(address.recipient)

    def get_token_balance(self, address):
        branch = Branch(
            self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                address.full_shard_key
            )
        )
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.get_token_balance(address.recipient)

    def get_account_data(
        self, address: Address, block_height: Optional[int]
    ) -> List[AccountBranchData]:
        results = []
        for branch, shard in self.shards.items():
            balance_list = []
            token_balances = shard.state.get_balances(address.recipient, block_height)
            for k in sorted(
                token_balances
            ):  # keep token balance sorted to maintain deterministic serialization
                kv = TokenBalancePair(k, token_balances[k])
                balance_list.append(kv)
            results.append(
                AccountBranchData(
                    branch=branch,
                    transaction_count=shard.state.get_transaction_count(
                        address.recipient, block_height
                    ),
                    token_balances=balance_list,
                    is_contract=len(
                        shard.state.get_code(address.recipient, block_height)
                    )
                    > 0,
                )
            )
        return results

    def get_minor_block_by_hash(self, block_hash, branch: Branch):
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.db.get_minor_block_by_hash(block_hash, False)

    def get_minor_block_by_height(self, height, branch):
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.db.get_minor_block_by_height(height)

    def get_transaction_by_hash(self, tx_hash, branch):
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.get_transaction_by_hash(tx_hash)

    def get_transaction_receipt(
        self, tx_hash, branch
    ) -> Optional[Tuple[MinorBlock, int, TransactionReceipt]]:
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.get_transaction_receipt(tx_hash)

    def get_transaction_list_by_address(self, address, start, limit):
        branch = Branch(
            self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                address.full_shard_key
            )
        )
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.get_transaction_list_by_address(address, start, limit)

    def get_logs(
        self,
        addresses: List[Address],
        topics: List[Optional[Union[str, List[str]]]],
        start_block: int,
        end_block: int,
        branch: Branch,
    ) -> Optional[List[Log]]:
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.get_logs(addresses, topics, start_block, end_block)

    def estimate_gas(self, tx, from_address) -> Optional[int]:
        evm_tx = tx.code.get_evm_transaction()
        evm_tx.set_quark_chain_config(self.env.quark_chain_config)
        branch = Branch(evm_tx.from_full_shard_id)
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.estimate_gas(tx, from_address)

    def get_storage_at(
        self, address: Address, key: int, block_height: Optional[int]
    ) -> Optional[bytes]:
        branch = Branch(
            self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                address.full_shard_key
            )
        )
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.get_storage_at(address.recipient, key, block_height)

    def get_code(
        self, address: Address, block_height: Optional[int]
    ) -> Optional[bytes]:
        branch = Branch(
            self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
                address.full_shard_key
            )
        )
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.get_code(address.recipient, block_height)

    def gas_price(self, branch: Branch) -> Optional[int]:
        shard = self.shards.get(branch, None)
        if not shard:
            return None
        return shard.state.gas_price()

    async def get_work(self, branch: Branch) -> Optional[MiningWork]:
        if branch not in self.shards:
            return None
        try:
            shard = self.shards[branch]
            work, block = await shard.miner.get_work()
            if shard.state.shard_config.POSW_CONFIG.ENABLED:
                check(isinstance(block, MinorBlock))
                diff = shard.state.posw_diff_adjust(block)
                work = MiningWork(work.hash, work.height, diff)
            return work
        except Exception:
            Logger.log_exception()
            return None

    async def submit_work(
        self, branch: Branch, header_hash: bytes, nonce: int, mixhash: bytes
    ) -> Optional[bool]:
        try:
            return await self.shards[branch].miner.submit_work(
                header_hash, nonce, mixhash
            )
        except Exception:
            Logger.log_exception()
            return None


def parse_args():
    parser = argparse.ArgumentParser()
    ClusterConfig.attach_arguments(parser)
    # Unique Id identifying the node in the cluster
    parser.add_argument("--node_id", default="", type=str)
    args = parser.parse_args()

    env = DEFAULT_ENV.copy()
    env.cluster_config = ClusterConfig.create_from_args(args)
    env.slave_config = env.cluster_config.get_slave_config(args.node_id)

    return env


def main():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    env = parse_args()

    slave_server = SlaveServer(env)
    slave_server.start()
    slave_server.do_loop()

    Logger.info("Slave server is shutdown")


if __name__ == "__main__":
    main()
