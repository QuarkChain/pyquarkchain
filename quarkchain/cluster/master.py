import argparse
import asyncio
import ipaddress
import psutil
import random
import time
from collections import deque
from threading import Thread
from typing import Optional, List, Union, Dict, Tuple

from quarkchain.cluster.miner import Miner, MiningWork, validate_seal
from quarkchain.cluster.p2p_commands import (
    CommandOp,
    Direction,
    GetRootBlockHeaderListRequest,
    GetRootBlockListRequest,
)
from quarkchain.cluster.protocol import (
    ClusterMetadata,
    ClusterConnection,
    P2PConnection,
    ROOT_BRANCH,
    NULL_CONNECTION,
)
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.rpc import (
    AddMinorBlockHeaderResponse,
    GetEcoInfoListRequest,
    GetNextBlockToMineRequest,
    GetUnconfirmedHeadersRequest,
    GetAccountDataRequest,
    AddTransactionRequest,
    AddRootBlockRequest,
    AddMinorBlockRequest,
    CreateClusterPeerConnectionRequest,
    DestroyClusterPeerConnectionCommand,
    SyncMinorBlockListRequest,
    GetMinorBlockRequest,
    GetTransactionRequest,
    ArtificialTxConfig,
    MineRequest,
    GenTxRequest,
    GetLogResponse,
    GetLogRequest,
    ShardStats,
    EstimateGasRequest,
    GetStorageRequest,
    GetCodeRequest,
    GasPriceRequest,
    GetWorkRequest,
    GetWorkResponse,
    SubmitWorkRequest,
    SubmitWorkResponse,
)
from quarkchain.cluster.rpc import (
    ConnectToSlavesRequest,
    ClusterOp,
    CLUSTER_OP_SERIALIZER_MAP,
    ExecuteTransactionRequest,
    Ping,
    GetTransactionReceiptRequest,
    GetTransactionListByAddressRequest,
)
from quarkchain.cluster.simple_network import SimpleNetwork
from quarkchain.config import RootConfig
from quarkchain.env import DEFAULT_ENV
from quarkchain.core import (
    Branch,
    ShardMask,
    Log,
    Address,
    TransactionReceipt,
    MinorBlock,
)
from quarkchain.core import Transaction
from quarkchain.db import PersistentDb
from quarkchain.p2p.p2p_network import P2PNetwork, devp2p_app
from quarkchain.utils import Logger, check, time_ms
from quarkchain.cluster.cluster_config import ClusterConfig


class SyncTask:
    """ Given a header and a peer, the task will synchronize the local state
    including root chain and shards with the peer up to the height of the header.
    """

    def __init__(self, header, peer):
        self.header = header
        self.peer = peer
        self.master_server = peer.master_server
        self.root_state = peer.root_state
        self.max_staleness = (
            self.root_state.env.quark_chain_config.ROOT.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF
        )

    async def sync(self):
        try:
            await self.__run_sync()
        except Exception as e:
            Logger.log_exception()
            self.peer.close_with_error(str(e))

    async def __run_sync(self):
        """raise on any error so that sync() will close peer connection"""
        if self.__has_block_hash(self.header.get_hash()):
            return

        # descending height
        block_header_chain = [self.header]

        while not self.__has_block_hash(block_header_chain[-1].hash_prev_block):
            block_hash = block_header_chain[-1].hash_prev_block
            height = block_header_chain[-1].height - 1

            # abort if we have to download super old blocks
            if self.root_state.tip.height - height > self.max_staleness:
                Logger.warning(
                    "[R] abort syncing due to forking at super old block {} << {}".format(
                        height, self.root_state.tip.height
                    )
                )
                return

            Logger.info(
                "[R] downloading block header list from {} {}".format(
                    height, block_hash.hex()
                )
            )
            block_header_list = await self.__download_block_headers(block_hash)
            if not self.__validate_block_headers(block_header_list):
                # TODO: tag bad peer
                raise RuntimeError("Bad peer sending discontinuing block headers")
            for header in block_header_list:
                if self.__has_block_hash(header.get_hash()):
                    break
                block_header_chain.append(header)

        block_header_chain.reverse()

        Logger.info(
            "[R] going to download {} blocks ({} - {})".format(
                len(block_header_chain),
                block_header_chain[0].height,
                block_header_chain[-1].height,
            )
        )

        while len(block_header_chain) > 0:
            block_chain = await self.__download_blocks(block_header_chain[:100])
            Logger.info(
                "[R] downloaded {} blocks ({} - {}) from peer".format(
                    len(block_chain),
                    block_chain[0].header.height,
                    block_chain[-1].header.height,
                )
            )
            if len(block_chain) != len(block_header_chain[:100]):
                # TODO: tag bad peer
                raise RuntimeError("Bad peer missing blocks for headers they have")

            for block in block_chain:
                await self.__add_block(block)
                block_header_chain.pop(0)

    def __has_block_hash(self, block_hash):
        return self.root_state.contain_root_block_by_hash(block_hash)

    def __validate_block_headers(self, block_header_list):
        # TODO: check difficulty and other stuff?
        consensus_type = self.root_state.env.quark_chain_config.ROOT.CONSENSUS_TYPE
        for i in range(len(block_header_list) - 1):
            header, prev = block_header_list[i : i + 2]
            if header.height != prev.height + 1:
                return False
            if header.hash_prev_block != prev.get_hash():
                return False
            validate_seal(header, consensus_type)
        return True

    async def __download_block_headers(self, block_hash):
        request = GetRootBlockHeaderListRequest(
            block_hash=block_hash, limit=100, direction=Direction.GENESIS
        )
        op, resp, rpc_id = await self.peer.write_rpc_request(
            CommandOp.GET_ROOT_BLOCK_HEADER_LIST_REQUEST, request
        )
        return resp.block_header_list

    async def __download_blocks(self, block_header_list):
        block_hash_list = [b.get_hash() for b in block_header_list]
        op, resp, rpc_id = await self.peer.write_rpc_request(
            CommandOp.GET_ROOT_BLOCK_LIST_REQUEST,
            GetRootBlockListRequest(block_hash_list),
        )
        return resp.root_block_list

    async def __add_block(self, root_block):
        Logger.info(
            "[R] syncing root block {} {}".format(
                root_block.header.height, root_block.header.get_hash().hex()
            )
        )
        start = time.time()
        await self.__sync_minor_blocks(root_block.minor_block_header_list)
        await self.master_server.add_root_block(root_block)
        elapse = time.time() - start
        Logger.info(
            "[R] syncing root block {} {} took {:.2f} seconds".format(
                root_block.header.height, root_block.header.get_hash().hex(), elapse
            )
        )

    async def __sync_minor_blocks(self, minor_block_header_list):
        minor_block_download_map = dict()
        for m_block_header in minor_block_header_list:
            m_block_hash = m_block_header.get_hash()
            if not self.root_state.is_minor_block_validated(m_block_hash):
                minor_block_download_map.setdefault(m_block_header.branch, []).append(
                    m_block_hash
                )

        future_list = []
        for branch, m_block_hash_list in minor_block_download_map.items():
            slave_conn = self.master_server.get_slave_connection(branch=branch)
            future = slave_conn.write_rpc_request(
                op=ClusterOp.SYNC_MINOR_BLOCK_LIST_REQUEST,
                cmd=SyncMinorBlockListRequest(
                    m_block_hash_list, branch, self.peer.get_cluster_peer_id()
                ),
            )
            future_list.append(future)

        result_list = await asyncio.gather(*future_list)
        for result in result_list:
            if result is Exception:
                raise RuntimeError(
                    "Unable to download minor blocks from root block with exception {}".format(
                        result
                    )
                )
            _, result, _ = result
            if result.error_code != 0:
                raise RuntimeError("Unable to download minor blocks from root block")

        for m_header in minor_block_header_list:
            self.root_state.add_validated_minor_block_hash(m_header.get_hash())


class Synchronizer:
    """ Buffer the headers received from peer and sync one by one """

    def __init__(self):
        self.queue = deque()
        self.running = False

    def add_task(self, header, peer):
        self.queue.append((header, peer))
        Logger.info(
            "[R] added {} {} to sync queue (running={})".format(
                header.height, header.get_hash().hex(), self.running
            )
        )
        if not self.running:
            self.running = True
            asyncio.ensure_future(self.__run())

    async def __run(self):
        Logger.info("[R] synchronizer started!")
        while len(self.queue) > 0:
            header, peer = self.queue.popleft()
            task = SyncTask(header, peer)
            Logger.info(
                "[R] start sync task {} {}".format(
                    header.height, header.get_hash().hex()
                )
            )
            await task.sync()
            Logger.info(
                "[R] done sync task {} {}".format(
                    header.height, header.get_hash().hex()
                )
            )
        self.running = False
        Logger.info("[R] synchronizer finished!")


class SlaveConnection(ClusterConnection):
    OP_NONRPC_MAP = {}

    def __init__(
        self, env, reader, writer, master_server, slave_id, shard_mask_list, name=None
    ):
        super().__init__(
            env,
            reader,
            writer,
            CLUSTER_OP_SERIALIZER_MAP,
            self.OP_NONRPC_MAP,
            OP_RPC_MAP,
            name=name,
        )
        self.master_server = master_server
        self.id = slave_id
        self.shard_mask_list = shard_mask_list
        check(len(shard_mask_list) > 0)

        asyncio.ensure_future(self.active_and_loop_forever())

    def get_connection_to_forward(self, metadata):
        """ Override ProxyConnection.get_connection_to_forward()
        Forward traffic from slave to peer
        """
        if metadata.cluster_peer_id == 0:
            return None

        peer = self.master_server.get_peer(metadata.cluster_peer_id)
        if peer is None:
            return NULL_CONNECTION

        return peer

    def validate_connection(self, connection):
        return connection == NULL_CONNECTION or isinstance(connection, P2PConnection)

    def has_shard(self, shard_id):
        for shard_mask in self.shard_mask_list:
            if shard_mask.contain_shard_id(shard_id):
                return True
        return False

    def has_overlap(self, shard_mask):
        for local_shard_mask in self.shard_mask_list:
            if local_shard_mask.has_overlap(shard_mask):
                return True
        return False

    async def send_ping(self, initialize_shard_state=False):
        root_block = (
            self.master_server.root_state.get_tip_block()
            if initialize_shard_state
            else None
        )
        req = Ping("", [], root_block)
        op, resp, rpc_id = await self.write_rpc_request(
            op=ClusterOp.PING,
            cmd=req,
            metadata=ClusterMetadata(branch=ROOT_BRANCH, cluster_peer_id=0),
        )
        return (resp.id, resp.shard_mask_list)

    async def send_connect_to_slaves(self, slave_info_list):
        """ Make slave connect to other slaves.
        Returns True on success
        """
        req = ConnectToSlavesRequest(slave_info_list)
        op, resp, rpc_id = await self.write_rpc_request(
            ClusterOp.CONNECT_TO_SLAVES_REQUEST, req
        )
        check(len(resp.result_list) == len(slave_info_list))
        for i, result in enumerate(resp.result_list):
            if len(result) > 0:
                Logger.info(
                    "Slave {} failed to connect to {} with error {}".format(
                        self.id, slave_info_list[i].id, result
                    )
                )
                return False
        Logger.info("Slave {} connected to other slaves successfully".format(self.id))
        return True

    def close(self):
        Logger.info(
            "Lost connection with slave {}. Shutting down master ...".format(self.id)
        )
        super().close()
        self.master_server.shutdown()

    def close_with_error(self, error):
        Logger.info("Closing connection with slave {}".format(self.id))
        return super().close_with_error(error)

    async def add_transaction(self, tx):
        request = AddTransactionRequest(tx)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.ADD_TRANSACTION_REQUEST, request
        )
        return resp.error_code == 0

    async def execute_transaction(
        self, tx: Transaction, from_address, block_height: Optional[int]
    ):
        request = ExecuteTransactionRequest(tx, from_address, block_height)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.EXECUTE_TRANSACTION_REQUEST, request
        )
        return resp.result if resp.error_code == 0 else None

    async def get_minor_block_by_hash(self, block_hash, branch):
        request = GetMinorBlockRequest(branch, minor_block_hash=block_hash)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_MINOR_BLOCK_REQUEST, request
        )
        if resp.error_code != 0:
            return None
        return resp.minor_block

    async def get_minor_block_by_height(self, height, branch):
        request = GetMinorBlockRequest(branch, height=height)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_MINOR_BLOCK_REQUEST, request
        )
        if resp.error_code != 0:
            return None
        return resp.minor_block

    async def get_transaction_by_hash(self, tx_hash, branch):
        request = GetTransactionRequest(tx_hash, branch)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_TRANSACTION_REQUEST, request
        )
        if resp.error_code != 0:
            return None, None
        return resp.minor_block, resp.index

    async def get_transaction_receipt(self, tx_hash, branch):
        request = GetTransactionReceiptRequest(tx_hash, branch)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_TRANSACTION_RECEIPT_REQUEST, request
        )
        if resp.error_code != 0:
            return None
        return resp.minor_block, resp.index, resp.receipt

    async def get_transactions_by_address(self, address, start, limit):
        request = GetTransactionListByAddressRequest(address, start, limit)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_TRANSACTION_LIST_BY_ADDRESS_REQUEST, request
        )
        if resp.error_code != 0:
            return None
        return resp.tx_list, resp.next

    async def get_logs(
        self,
        branch: Branch,
        addresses: List[Address],
        topics: List[List[bytes]],
        start_block: int,
        end_block: int,
    ) -> Optional[List[Log]]:
        request = GetLogRequest(branch, addresses, topics, start_block, end_block)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_LOG_REQUEST, request
        )  # type: GetLogResponse
        return resp.logs if resp.error_code == 0 else None

    async def estimate_gas(
        self, tx: Transaction, from_address: Address
    ) -> Optional[int]:
        request = EstimateGasRequest(tx, from_address)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.ESTIMATE_GAS_REQUEST, request
        )
        return resp.result if resp.error_code == 0 else None

    async def get_storage_at(
        self, address: Address, key: int, block_height: Optional[int]
    ) -> Optional[bytes]:
        request = GetStorageRequest(address, key, block_height)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.GET_STORAGE_REQUEST, request
        )
        return resp.result if resp.error_code == 0 else None

    async def get_code(
        self, address: Address, block_height: Optional[int]
    ) -> Optional[bytes]:
        request = GetCodeRequest(address, block_height)
        _, resp, _ = await self.write_rpc_request(ClusterOp.GET_CODE_REQUEST, request)
        return resp.result if resp.error_code == 0 else None

    async def gas_price(self, branch: Branch) -> Optional[int]:
        request = GasPriceRequest(branch)
        _, resp, _ = await self.write_rpc_request(ClusterOp.GAS_PRICE_REQUEST, request)
        return resp.result if resp.error_code == 0 else None

    async def get_work(self, branch: Branch) -> Optional[MiningWork]:
        request = GetWorkRequest(branch)
        _, resp, _ = await self.write_rpc_request(ClusterOp.GET_WORK_REQUEST, request)
        get_work_resp = resp  # type: GetWorkResponse
        if get_work_resp.error_code != 0:
            return None
        return MiningWork(
            get_work_resp.header_hash, get_work_resp.height, get_work_resp.difficulty
        )

    async def submit_work(
        self, branch: Branch, header_hash: bytes, nonce: int, mixhash: bytes
    ) -> bool:
        request = SubmitWorkRequest(branch, header_hash, nonce, mixhash)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.SUBMIT_WORK_REQUEST, request
        )
        submit_work_resp = resp  # type: SubmitWorkResponse
        return submit_work_resp.error_code == 0 and submit_work_resp.success

    # RPC handlers

    async def handle_add_minor_block_header_request(self, req):
        self.master_server.root_state.add_validated_minor_block_hash(
            req.minor_block_header.get_hash()
        )
        self.master_server.update_shard_stats(req.shard_stats)
        self.master_server.update_tx_count_history(
            req.tx_count, req.x_shard_tx_count, req.minor_block_header.create_time
        )
        return AddMinorBlockHeaderResponse(
            error_code=0,
            artificial_tx_config=self.master_server.get_artificial_tx_config(),
        )


OP_RPC_MAP = {
    ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST: (
        ClusterOp.ADD_MINOR_BLOCK_HEADER_RESPONSE,
        SlaveConnection.handle_add_minor_block_header_request,
    )
}


class MasterServer:
    """ Master node in a cluster
    It does two things to initialize the cluster:
    1. Setup connection with all the slaves in ClusterConfig
    2. Make slaves connect to each other
    """

    def __init__(self, env, root_state, name="master"):
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.root_state = root_state
        self.network = None  # will be set by SimpleNetwork
        self.cluster_config = env.cluster_config

        # branch value -> a list of slave running the shard
        self.branch_to_slaves = dict()  # type: Dict[int, List[SlaveConnection]]
        self.slave_pool = set()

        self.cluster_active_future = self.loop.create_future()
        self.shutdown_future = self.loop.create_future()
        self.name = name

        self.artificial_tx_config = ArtificialTxConfig(
            target_root_block_time=self.env.quark_chain_config.ROOT.CONSENSUS_CONFIG.TARGET_BLOCK_TIME,
            target_minor_block_time=self.env.quark_chain_config.SHARD_LIST[
                0
            ].CONSENSUS_CONFIG.TARGET_BLOCK_TIME,
        )

        self.synchronizer = Synchronizer()

        self.branch_to_shard_stats = dict()  # type: Dict[int, ShardStats]
        # (epoch in minute, tx_count in the minute)
        self.tx_count_history = deque()

        self.__init_root_miner()

    def __init_root_miner(self):
        miner_address = self.env.quark_chain_config.testnet_master_address

        async def __create_block():
            while True:
                is_root, block = await self.get_next_block_to_mine(
                    address=miner_address, shard_mask_value=0, prefer_root=True
                )
                if is_root:
                    return block
                await asyncio.sleep(1)

        def __get_mining_params():
            return {
                "target_block_time": self.get_artificial_tx_config().target_root_block_time
            }

        root_config = self.env.quark_chain_config.ROOT  # type: RootConfig
        self.root_miner = Miner(
            root_config.CONSENSUS_TYPE,
            __create_block,
            self.add_root_block,
            __get_mining_params,
            remote=root_config.CONSENSUS_CONFIG.REMOTE_MINE,
        )

    def __get_shard_size(self):
        # TODO: replace it with dynamic size
        return self.env.quark_chain_config.SHARD_SIZE

    def get_shard_size(self):
        return self.__get_shard_size()

    def get_artificial_tx_config(self):
        return self.artificial_tx_config

    def __has_all_shards(self):
        """ Returns True if all the shards have been run by at least one node """
        return len(self.branch_to_slaves) == self.__get_shard_size() and all(
            [len(slaves) > 0 for _, slaves in self.branch_to_slaves.items()]
        )

    async def __connect(self, ip, port):
        """ Retries until success """
        Logger.info("Trying to connect {}:{}".format(ip, port))
        while True:
            try:
                reader, writer = await asyncio.open_connection(ip, port, loop=self.loop)
                break
            except Exception as e:
                Logger.info("Failed to connect {} {}: {}".format(ip, port, e))
                await asyncio.sleep(
                    self.env.cluster_config.MASTER.MASTER_TO_SLAVE_CONNECT_RETRY_DELAY
                )
        Logger.info("Connected to {}:{}".format(ip, port))
        return (reader, writer)

    async def __connect_to_slaves(self):
        """ Master connects to all the slaves """
        futures = []
        slaves = []
        for slave_info in self.cluster_config.get_slave_info_list():
            ip = str(ipaddress.ip_address(slave_info.ip))
            reader, writer = await self.__connect(ip, slave_info.port)

            slave = SlaveConnection(
                self.env,
                reader,
                writer,
                self,
                slave_info.id,
                slave_info.shard_mask_list,
                name="{}_slave_{}".format(self.name, slave_info.id),
            )
            await slave.wait_until_active()
            futures.append(slave.send_ping())
            slaves.append(slave)

        results = await asyncio.gather(*futures)

        for slave, result in zip(slaves, results):
            # Verify the slave does have the same id and shard mask list as the config file
            id, shard_mask_list = result
            if id != slave.id:
                Logger.error(
                    "Slave id does not match. expect {} got {}".format(slave.id, id)
                )
                self.shutdown()
            if shard_mask_list != slave.shard_mask_list:
                Logger.error(
                    "Slave {} shard mask list does not match. expect {} got {}".format(
                        slave.id, slave.shard_mask_list, shard_mask_list
                    )
                )
                self.shutdown()

            self.slave_pool.add(slave)
            for shard_id in range(self.__get_shard_size()):
                branch = Branch.create(self.__get_shard_size(), shard_id)
                if slave.has_shard(shard_id):
                    self.branch_to_slaves.setdefault(branch.value, []).append(slave)

    async def __setup_slave_to_slave_connections(self):
        """ Make slaves connect to other slaves.
        Retries until success.
        """
        for slave in self.slave_pool:
            await slave.wait_until_active()
            success = await slave.send_connect_to_slaves(
                self.cluster_config.get_slave_info_list()
            )
            if not success:
                self.shutdown()

    async def __init_shards(self):
        futures = []
        for slave in self.slave_pool:
            futures.append(slave.send_ping(initialize_shard_state=True))
        await asyncio.gather(*futures)

    async def __send_mining_config_to_slaves(self, mining):
        futures = []
        for slave in self.slave_pool:
            request = MineRequest(self.get_artificial_tx_config(), mining)
            futures.append(slave.write_rpc_request(ClusterOp.MINE_REQUEST, request))
        responses = await asyncio.gather(*futures)
        check(all([resp.error_code == 0 for _, resp, _ in responses]))

    async def start_mining(self):
        await self.__send_mining_config_to_slaves(True)
        self.root_miner.start()
        Logger.warning(
            "Mining started with root block time {} s, minor block time {} s".format(
                self.get_artificial_tx_config().target_root_block_time,
                self.get_artificial_tx_config().target_minor_block_time,
            )
        )

    async def stop_mining(self):
        await self.__send_mining_config_to_slaves(False)
        self.root_miner.disable()
        Logger.warning("Mining stopped")

    def get_slave_connection(self, branch):
        # TODO:  Support forwarding to multiple connections (for replication)
        check(len(self.branch_to_slaves[branch.value]) > 0)
        return self.branch_to_slaves[branch.value][0]

    def __log_summary(self):
        for branch_value, slaves in self.branch_to_slaves.items():
            Logger.info(
                "[{}] is run by slave {}".format(
                    Branch(branch_value).get_shard_id(), [s.id for s in slaves]
                )
            )

    async def __init_cluster(self):
        await self.__connect_to_slaves()
        self.__log_summary()
        if not self.__has_all_shards():
            Logger.error("Missing some shards. Check cluster config file!")
            return
        await self.__setup_slave_to_slave_connections()
        await self.__init_shards()

        self.cluster_active_future.set_result(None)

    def start(self):
        self.loop.create_task(self.__init_cluster())

    def start_and_loop(self):
        self.start()
        try:
            self.loop.run_until_complete(self.shutdown_future)
        except KeyboardInterrupt:
            pass

    def wait_until_cluster_active(self):
        # Wait until cluster is ready
        self.loop.run_until_complete(self.cluster_active_future)

    def shutdown(self):
        # TODO: May set exception and disconnect all slaves
        if not self.shutdown_future.done():
            self.shutdown_future.set_result(None)
        if not self.cluster_active_future.done():
            self.cluster_active_future.set_exception(
                RuntimeError("failed to start the cluster")
            )

    def get_shutdown_future(self):
        return self.shutdown_future

    async def __create_root_block_to_mine_or_fallback_to_minor_block(self, address):
        """ Try to create a root block to mine or fallback to create minor block if failed proof-of-progress """
        futures = []
        for slave in self.slave_pool:
            request = GetUnconfirmedHeadersRequest()
            futures.append(
                slave.write_rpc_request(
                    ClusterOp.GET_UNCONFIRMED_HEADERS_REQUEST, request
                )
            )
        responses = await asyncio.gather(*futures)

        # Slaves may run multiple copies of the same branch
        # branch_value -> HeaderList
        shard_id_to_header_list = dict()
        for response in responses:
            _, response, _ = response
            if response.error_code != 0:
                return (None, None)
            for headers_info in response.headers_info_list:
                if headers_info.branch.get_shard_size() != self.__get_shard_size():
                    Logger.error(
                        "Expect shard size {} got {}".format(
                            self.__get_shard_size(),
                            headers_info.branch.get_shard_size(),
                        )
                    )
                    return (None, None)

                height = 0
                for header in headers_info.header_list:
                    # check headers are ordered by height
                    check(height == 0 or height + 1 == header.height)
                    height = header.height

                    # Filter out the ones unknown to the master
                    if not self.root_state.is_minor_block_validated(header.get_hash()):
                        break
                    shard_id_to_header_list.setdefault(
                        headers_info.branch.get_shard_id(), []
                    ).append(header)

        header_list = []
        # check proof of progress
        shard_ids_to_check = self.env.quark_chain_config.get_initialized_shard_ids_before_root_height(
            self.root_state.tip.height + 1
        )
        for shard_id in shard_ids_to_check:
            headers = shard_id_to_header_list.get(shard_id, [])
            header_list.extend(headers)
            if len(headers) < self.env.quark_chain_config.PROOF_OF_PROGRESS_BLOCKS:
                # Fallback to create minor block
                block = await self.__get_minor_block_to_mine(
                    Branch.create(self.__get_shard_size(), shard_id), address
                )
                return (None, None) if not block else (False, block)

        return (True, self.root_state.create_block_to_mine(header_list, address))

    async def __get_minor_block_to_mine(self, branch, address):
        request = GetNextBlockToMineRequest(
            branch=branch,
            address=address.address_in_branch(branch),
            artificial_tx_config=self.get_artificial_tx_config(),
        )
        slave = self.get_slave_connection(branch)
        _, response, _ = await slave.write_rpc_request(
            ClusterOp.GET_NEXT_BLOCK_TO_MINE_REQUEST, request
        )
        return response.block if response.error_code == 0 else None

    async def get_next_block_to_mine(
        self, address, shard_mask_value=0, prefer_root=False, randomize_output=True
    ):
        """ Returns (is_root_block, block)

        shard_mask_value = 0 means considering root chain and all the shards
        """
        # Mining old blocks is useless
        if self.synchronizer.running:
            return None, None

        if prefer_root and shard_mask_value == 0:
            return await self.__create_root_block_to_mine_or_fallback_to_minor_block(
                address
            )

        shard_mask = None if shard_mask_value == 0 else ShardMask(shard_mask_value)
        futures = []

        # Collect EcoInfo from shards
        for slave in self.slave_pool:
            if shard_mask and not slave.has_overlap(shard_mask):
                continue
            request = GetEcoInfoListRequest()
            futures.append(
                slave.write_rpc_request(ClusterOp.GET_ECO_INFO_LIST_REQUEST, request)
            )
        responses = await asyncio.gather(*futures)

        # Slaves may run multiple copies of the same branch
        # We only need one EcoInfo per branch
        # branch_value -> EcoInfo
        branch_value_to_eco_info = dict()
        for response in responses:
            _, response, _ = response
            if response.error_code != 0:
                return None, None
            for eco_info in response.eco_info_list:
                branch_value_to_eco_info[eco_info.branch.value] = eco_info

        root_coinbase_amount = 0
        for branch_value, eco_info in branch_value_to_eco_info.items():
            root_coinbase_amount += eco_info.unconfirmed_headers_coinbase_amount
        root_coinbase_amount = root_coinbase_amount // 2

        branch_value_with_max_eco = 0 if shard_mask is None else None
        max_eco = root_coinbase_amount / self.root_state.get_next_block_difficulty()

        dup_eco_count = 1
        block_height = 0
        for branch_value, eco_info in branch_value_to_eco_info.items():
            if shard_mask and not shard_mask.contain_branch(Branch(branch_value)):
                continue
            # TODO: Obtain block reward and tx fee
            eco = eco_info.coinbase_amount / eco_info.difficulty
            if (
                branch_value_with_max_eco is None
                or eco > max_eco
                or (
                    eco == max_eco
                    and branch_value_with_max_eco > 0
                    and block_height > eco_info.height
                )
            ):
                branch_value_with_max_eco = branch_value
                max_eco = eco
                dup_eco_count = 1
                block_height = eco_info.height
            elif eco == max_eco and randomize_output:
                # The current block with max eco has smaller height, mine the block first
                # This should be only used during bootstrap.
                if branch_value_with_max_eco > 0 and block_height < eco_info.height:
                    continue
                dup_eco_count += 1
                if random.random() < 1 / dup_eco_count:
                    branch_value_with_max_eco = branch_value
                    max_eco = eco

        if branch_value_with_max_eco == 0:
            return await self.__create_root_block_to_mine_or_fallback_to_minor_block(
                address
            )

        block = await self.__get_minor_block_to_mine(
            Branch(branch_value_with_max_eco), address
        )
        return (None, None) if not block else (False, block)

    async def get_account_data(self, address: Address):
        """ Returns a dict where key is Branch and value is AccountBranchData """
        futures = []
        for slave in self.slave_pool:
            request = GetAccountDataRequest(address)
            futures.append(
                slave.write_rpc_request(ClusterOp.GET_ACCOUNT_DATA_REQUEST, request)
            )
        responses = await asyncio.gather(*futures)

        # Slaves may run multiple copies of the same branch
        # We only need one AccountBranchData per branch
        branch_to_account_branch_data = dict()
        for response in responses:
            _, response, _ = response
            check(response.error_code == 0)
            for account_branch_data in response.account_branch_data_list:
                branch_to_account_branch_data[
                    account_branch_data.branch
                ] = account_branch_data

        check(
            len(branch_to_account_branch_data)
            == len(self.env.quark_chain_config.get_genesis_shard_ids())
        )
        return branch_to_account_branch_data

    async def get_primary_account_data(
        self, address: Address, block_height: Optional[int] = None
    ):
        # TODO: Only query the shard who has the address
        shard_id = address.get_shard_id(self.__get_shard_size())
        branch = Branch.create(self.__get_shard_size(), shard_id)
        slaves = self.branch_to_slaves.get(branch.value, None)
        if not slaves:
            return None
        slave = slaves[0]
        request = GetAccountDataRequest(address, block_height)
        _, resp, _ = await slave.write_rpc_request(
            ClusterOp.GET_ACCOUNT_DATA_REQUEST, request
        )
        for account_branch_data in resp.account_branch_data_list:
            if account_branch_data.branch == branch:
                return account_branch_data
        return None

    async def add_transaction(self, tx, from_peer=None):
        """ Add transaction to the cluster and broadcast to peers """
        evm_tx = tx.code.get_evm_transaction()
        evm_tx.set_shard_size(self.__get_shard_size())
        branch = Branch.create(self.__get_shard_size(), evm_tx.from_shard_id())
        if branch.value not in self.branch_to_slaves:
            return False

        futures = []
        for slave in self.branch_to_slaves[branch.value]:
            futures.append(slave.add_transaction(tx))

        success = all(await asyncio.gather(*futures))
        if not success:
            return False

        if self.network is not None:
            for peer in self.network.iterate_peers():
                if peer == from_peer:
                    continue
                try:
                    peer.send_transaction(tx)
                except Exception:
                    Logger.log_exception()
        return True

    async def execute_transaction(
        self, tx: Transaction, from_address, block_height: Optional[int]
    ) -> Optional[bytes]:
        """ Execute transaction without persistence """
        evm_tx = tx.code.get_evm_transaction()
        evm_tx.set_shard_size(self.__get_shard_size())
        branch = Branch.create(self.__get_shard_size(), evm_tx.from_shard_id())
        if branch.value not in self.branch_to_slaves:
            return None

        futures = []
        for slave in self.branch_to_slaves[branch.value]:
            futures.append(slave.execute_transaction(tx, from_address, block_height))
        responses = await asyncio.gather(*futures)
        # failed response will return as None
        success = all(r is not None for r in responses) and len(set(responses)) == 1
        if not success:
            return None

        check(len(responses) >= 1)
        return responses[0]

    def handle_new_root_block_header(self, header, peer):
        self.synchronizer.add_task(header, peer)

    async def add_root_block(self, r_block):
        """ Add root block locally and broadcast root block to all shards and .
        All update root block should be done in serial to avoid inconsistent global root block state.
        """
        self.root_state.validate_block(r_block)  # throw exception if failed
        update_tip = False
        try:
            update_tip = self.root_state.add_block(r_block)
            success = True
        except ValueError:
            Logger.log_exception()
            success = False

        try:
            if update_tip and self.network is not None:
                for peer in self.network.iterate_peers():
                    peer.send_updated_tip()
        except Exception:
            pass

        if success:
            future_list = self.broadcast_rpc(
                op=ClusterOp.ADD_ROOT_BLOCK_REQUEST,
                req=AddRootBlockRequest(r_block, False),
            )
            result_list = await asyncio.gather(*future_list)
            check(all([resp.error_code == 0 for _, resp, _ in result_list]))

    async def add_raw_minor_block(self, branch, block_data):
        if branch.value not in self.branch_to_slaves:
            return False

        request = AddMinorBlockRequest(block_data)
        # TODO: support multiple slaves running the same shard
        _, resp, _ = await self.get_slave_connection(branch).write_rpc_request(
            ClusterOp.ADD_MINOR_BLOCK_REQUEST, request
        )
        return resp.error_code == 0

    async def add_root_block_from_miner(self, block):
        """ Should only be called by miner """
        # TODO: push candidate block to miner
        if block.header.hash_prev_block != self.root_state.tip.get_hash():
            Logger.info(
                "[R] dropped stale root block {} mined locally".format(
                    block.header.height
                )
            )
            return False
        await self.add_root_block(block)

    def broadcast_command(self, op, cmd):
        """ Broadcast command to all slaves.
        """
        for slave_conn in self.slave_pool:
            slave_conn.write_command(
                op=op, cmd=cmd, metadata=ClusterMetadata(ROOT_BRANCH, 0)
            )

    def broadcast_rpc(self, op, req):
        """ Broadcast RPC request to all slaves.
        """
        future_list = []
        for slave_conn in self.slave_pool:
            future_list.append(
                slave_conn.write_rpc_request(
                    op=op, cmd=req, metadata=ClusterMetadata(ROOT_BRANCH, 0)
                )
            )
        return future_list

    # ------------------------------ Cluster Peer Connection Management --------------
    def get_peer(self, cluster_peer_id):
        if self.network is None:
            return None
        return self.network.get_peer_by_cluster_peer_id(cluster_peer_id)

    async def create_peer_cluster_connections(self, cluster_peer_id):
        future_list = self.broadcast_rpc(
            op=ClusterOp.CREATE_CLUSTER_PEER_CONNECTION_REQUEST,
            req=CreateClusterPeerConnectionRequest(cluster_peer_id),
        )
        result_list = await asyncio.gather(*future_list)
        # TODO: Check result_list
        return

    def destroy_peer_cluster_connections(self, cluster_peer_id):
        # Broadcast connection lost to all slaves
        self.broadcast_command(
            op=ClusterOp.DESTROY_CLUSTER_PEER_CONNECTION_COMMAND,
            cmd=DestroyClusterPeerConnectionCommand(cluster_peer_id),
        )

    async def set_target_block_time(self, root_block_time, minor_block_time):
        root_block_time = (
            root_block_time
            if root_block_time
            else self.artificial_tx_config.target_root_block_time
        )
        minor_block_time = (
            minor_block_time
            if minor_block_time
            else self.artificial_tx_config.target_minor_block_time
        )
        self.artificial_tx_config = ArtificialTxConfig(
            target_root_block_time=root_block_time,
            target_minor_block_time=minor_block_time,
        )
        await self.start_mining()

    async def set_mining(self, mining):
        if mining:
            await self.start_mining()
        else:
            await self.stop_mining()

    async def create_transactions(
        self, num_tx_per_shard, xshard_percent, tx: Transaction
    ):
        """Create transactions and add to the network for load testing"""
        futures = []
        for slave in self.slave_pool:
            request = GenTxRequest(num_tx_per_shard, xshard_percent, tx)
            futures.append(slave.write_rpc_request(ClusterOp.GEN_TX_REQUEST, request))
        responses = await asyncio.gather(*futures)
        check(all([resp.error_code == 0 for _, resp, _ in responses]))

    def update_shard_stats(self, shard_state):
        self.branch_to_shard_stats[shard_state.branch.value] = shard_state

    def update_tx_count_history(self, tx_count, xshard_tx_count, timestamp):
        """ maintain a list of tuples of (epoch minute, tx count, xshard tx count) of 12 hours window
        Note that this is also counting transactions on forks and thus larger than if only couting the best chains. """
        minute = int(timestamp / 60) * 60
        if len(self.tx_count_history) == 0 or self.tx_count_history[-1][0] < minute:
            self.tx_count_history.append((minute, tx_count, xshard_tx_count))
        else:
            old = self.tx_count_history.pop()
            self.tx_count_history.append(
                (old[0], old[1] + tx_count, old[2] + xshard_tx_count)
            )

        while (
            len(self.tx_count_history) > 0
            and self.tx_count_history[0][0] < time.time() - 3600 * 12
        ):
            self.tx_count_history.popleft()

    async def get_stats(self):
        shards = [dict() for i in range(self.__get_shard_size())]
        for shard_stats in self.branch_to_shard_stats.values():
            shard_id = shard_stats.branch.get_shard_id()
            shards[shard_id]["height"] = shard_stats.height
            shards[shard_id]["timestamp"] = shard_stats.timestamp
            shards[shard_id]["txCount60s"] = shard_stats.tx_count60s
            shards[shard_id]["pendingTxCount"] = shard_stats.pending_tx_count
            shards[shard_id]["totalTxCount"] = shard_stats.total_tx_count
            shards[shard_id]["blockCount60s"] = shard_stats.block_count60s
            shards[shard_id]["staleBlockCount60s"] = shard_stats.stale_block_count60s
            shards[shard_id]["lastBlockTime"] = shard_stats.last_block_time

        tx_count60s = sum(
            [
                shard_stats.tx_count60s
                for shard_stats in self.branch_to_shard_stats.values()
            ]
        )
        block_count60s = sum(
            [
                shard_stats.block_count60s
                for shard_stats in self.branch_to_shard_stats.values()
            ]
        )
        pending_tx_count = sum(
            [
                shard_stats.pending_tx_count
                for shard_stats in self.branch_to_shard_stats.values()
            ]
        )
        stale_block_count60s = sum(
            [
                shard_stats.stale_block_count60s
                for shard_stats in self.branch_to_shard_stats.values()
            ]
        )
        total_tx_count = sum(
            [
                shard_stats.total_tx_count
                for shard_stats in self.branch_to_shard_stats.values()
            ]
        )

        root_last_block_time = 0
        if self.root_state.tip.height >= 3:
            prev = self.root_state.db.get_root_block_by_hash(
                self.root_state.tip.hash_prev_block
            )
            root_last_block_time = (
                self.root_state.tip.create_time - prev.header.create_time
            )

        tx_count_history = []
        for item in self.tx_count_history:
            tx_count_history.append(
                {"timestamp": item[0], "txCount": item[1], "xShardTxCount": item[2]}
            )

        return {
            "shardServerCount": len(self.slave_pool),
            "shardSize": self.__get_shard_size(),
            "rootHeight": self.root_state.tip.height,
            "rootTimestamp": self.root_state.tip.create_time,
            "rootLastBlockTime": root_last_block_time,
            "txCount60s": tx_count60s,
            "blockCount60s": block_count60s,
            "staleBlockCount60s": stale_block_count60s,
            "pendingTxCount": pending_tx_count,
            "totalTxCount": total_tx_count,
            "syncing": self.synchronizer.running,
            "mining": self.root_miner.is_enabled(),
            "shards": shards,
            "peers": [
                "{}:{}".format(peer.ip, peer.port)
                for _, peer in self.network.active_peer_pool.items()
            ],
            "minor_block_interval": self.get_artificial_tx_config().target_minor_block_time,
            "root_block_interval": self.get_artificial_tx_config().target_root_block_time,
            "cpus": psutil.cpu_percent(percpu=True),
            "txCountHistory": tx_count_history,
        }

    def is_syncing(self):
        return self.synchronizer.running

    def is_mining(self):
        return self.root_miner.is_enabled()

    async def get_minor_block_by_hash(self, block_hash, branch):
        if branch.value not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[branch.value][0]
        return await slave.get_minor_block_by_hash(block_hash, branch)

    async def get_minor_block_by_height(self, height: Optional[int], branch):
        if branch.value not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[branch.value][0]
        # use latest height if not specified
        height = (
            height
            if height is not None
            else self.branch_to_shard_stats[branch.value].height
        )
        return await slave.get_minor_block_by_height(height, branch)

    async def get_transaction_by_hash(self, tx_hash, branch):
        """ Returns (MinorBlock, i) where i is the index of the tx in the block tx_list """
        if branch.value not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[branch.value][0]
        return await slave.get_transaction_by_hash(tx_hash, branch)

    async def get_transaction_receipt(
        self, tx_hash, branch
    ) -> Optional[Tuple[MinorBlock, int, TransactionReceipt]]:
        if branch.value not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[branch.value][0]
        return await slave.get_transaction_receipt(tx_hash, branch)

    async def get_transactions_by_address(self, address, start, limit):
        branch = Branch.create(
            self.__get_shard_size(), address.get_shard_id(self.__get_shard_size())
        )
        slave = self.branch_to_slaves[branch.value][0]
        return await slave.get_transactions_by_address(address, start, limit)

    async def get_logs(
        self,
        addresses: List[Address],
        topics: List[List[bytes]],
        start_block: Union[int, str],
        end_block: Union[int, str],
        branch: Branch,
    ) -> Optional[List[Log]]:
        if branch.value not in self.branch_to_slaves:
            return None

        if start_block == "latest":
            start_block = self.branch_to_shard_stats[branch.value].height
        if end_block == "latest":
            end_block = self.branch_to_shard_stats[branch.value].height

        slave = self.branch_to_slaves[branch.value][0]
        return await slave.get_logs(branch, addresses, topics, start_block, end_block)

    async def estimate_gas(
        self, tx: Transaction, from_address: Address
    ) -> Optional[int]:
        evm_tx = tx.code.get_evm_transaction()
        evm_tx.set_shard_size(self.__get_shard_size())
        branch = Branch.create(self.__get_shard_size(), evm_tx.from_shard_id())
        if branch.value not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[branch.value][0]
        return await slave.estimate_gas(tx, from_address)

    async def get_storage_at(
        self, address: Address, key: int, block_height: Optional[int]
    ) -> Optional[bytes]:
        shard_size = self.__get_shard_size()
        shard_id = address.get_shard_id(shard_size)
        branch = Branch.create(shard_size, shard_id)
        if branch.value not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[branch.value][0]
        return await slave.get_storage_at(address, key, block_height)

    async def get_code(
        self, address: Address, block_height: Optional[int]
    ) -> Optional[bytes]:
        shard_size = self.__get_shard_size()
        shard_id = address.get_shard_id(shard_size)
        branch = Branch.create(shard_size, shard_id)
        if branch.value not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[branch.value][0]
        return await slave.get_code(address, block_height)

    async def gas_price(self, branch: Branch) -> Optional[int]:
        if branch.value not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[branch.value][0]
        return await slave.gas_price(branch)

    async def get_work(self, branch: Optional[Branch]) -> Optional[MiningWork]:
        if not branch:  # get root chain work
            return await self.root_miner.get_work()

        if branch.value not in self.branch_to_slaves:
            return None
        slave = self.branch_to_slaves[branch.value][0]
        return await slave.get_work(branch)

    async def submit_work(
        self, branch: Optional[Branch], header_hash: bytes, nonce: int, mixhash: bytes
    ) -> bool:
        if not branch:  # submit root chain work
            return await self.root_miner.submit_work(header_hash, nonce, mixhash)

        if branch.value not in self.branch_to_slaves:
            return False
        slave = self.branch_to_slaves[branch.value][0]
        return await slave.submit_work(branch, header_hash, nonce, mixhash)


def parse_args():
    parser = argparse.ArgumentParser()
    ClusterConfig.attach_arguments(parser)
    args = parser.parse_args()

    env = DEFAULT_ENV.copy()
    env.cluster_config = ClusterConfig.create_from_args(args)

    # initialize database
    if not env.cluster_config.use_mem_db():
        env.db = PersistentDb(
            "{path}/master.db".format(path=env.cluster_config.DB_PATH_ROOT),
            clean=env.cluster_config.CLEAN,
        )

    return env


def main():
    from quarkchain.cluster.jsonrpc import JSONRPCServer

    env = parse_args()
    root_state = RootState(env)

    master = MasterServer(env, root_state)
    master.start()
    master.wait_until_cluster_active()

    # kick off mining
    if env.cluster_config.MINE:
        asyncio.ensure_future(master.start_mining())

    network = (
        P2PNetwork(env, master)
        if env.cluster_config.use_p2p()
        else SimpleNetwork(env, master)
    )
    network.start()

    if env.cluster_config.use_p2p():
        thread = Thread(target=devp2p_app, args=[env, network], daemon=True)
        thread.start()

    public_json_rpc_server = JSONRPCServer.start_public_server(env, master)
    private_json_rpc_server = JSONRPCServer.start_private_server(env, master)

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(master.shutdown_future)
    except KeyboardInterrupt:
        pass

    public_json_rpc_server.shutdown()
    private_json_rpc_server.shutdown()

    Logger.info("Master server is shutdown")


if __name__ == "__main__":
    main()
