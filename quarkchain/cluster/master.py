import argparse
import asyncio
import os

import psutil
import time
from collections import deque
from typing import Optional, List, Union, Dict, Tuple

from quarkchain.cluster.guardian import Guardian
from quarkchain.cluster.miner import Miner, MiningWork, validate_seal
from quarkchain.cluster.p2p_commands import (
    CommandOp,
    Direction,
    GetRootBlockHeaderListRequest,
    GetRootBlockListRequest,
    GetRootBlockHeaderListWithSkipRequest,
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
    AddMinorBlockHeaderListResponse,
    RootBlockSychronizerStats,
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
    ChainMask,
    Log,
    Address,
    RootBlock,
    TransactionReceipt,
    TypedTransaction,
    MinorBlock,
)
from quarkchain.db import PersistentDb
from quarkchain.p2p.p2p_manager import P2PManager
from quarkchain.p2p.utils import RESERVED_CLUSTER_PEER_ID
from quarkchain.utils import Logger, check, time_ms
from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.constants import (
    SYNC_TIMEOUT,
    ROOT_BLOCK_BATCH_SIZE,
    ROOT_BLOCK_HEADER_LIST_LIMIT,
)


class SyncTask:
    """ Given a header and a peer, the task will synchronize the local state
    including root chain and shards with the peer up to the height of the header.
    """

    def __init__(self, header, peer, stats, root_block_header_list_limit):
        self.header = header
        self.peer = peer
        self.master_server = peer.master_server
        self.root_state = peer.root_state
        self.max_staleness = (
            self.root_state.env.quark_chain_config.ROOT.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF
        )
        self.stats = stats
        self.root_block_header_list_limit = root_block_header_list_limit
        check(root_block_header_list_limit >= 3)

    async def sync(self):
        try:
            await self.__run_sync()
        except Exception as e:
            Logger.log_exception()
            self.peer.close_with_error(str(e))

    async def __download_block_header_and_check(self, start, skip, limit):
        _, resp, _ = await self.peer.write_rpc_request(
            op=CommandOp.GET_ROOT_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST,
            cmd=GetRootBlockHeaderListWithSkipRequest.create_for_height(
                height=start, skip=skip, limit=limit, direction=Direction.TIP
            ),
        )

        self.stats.headers_downloaded += len(resp.block_header_list)

        if resp.root_tip.total_difficulty < self.header.total_difficulty:
            raise RuntimeError("Bad peer sending root block tip with lower TD")

        # new limit should equal to limit, but in case that remote has chain reorg,
        # the remote tip may has lower height and greater TD.
        new_limit = min(limit, len(range(start, resp.root_tip.height + 1, skip + 1)))
        if len(resp.block_header_list) != new_limit:
            # Something bad happens
            raise RuntimeError(
                "Bad peer sending incorrect number of root block headers"
            )

        return resp

    async def __find_ancestor(self):
        # Fast path
        if self.header.hash_prev_block == self.root_state.tip.get_hash():
            return self.root_state.tip

        # n-ary search
        start = max(self.root_state.tip.height - self.max_staleness, 0)
        end = min(self.root_state.tip.height, self.header.height)
        Logger.info("Finding root block ancestor from {} to {}...".format(start, end))
        best_ancestor = None

        while end >= start:
            self.stats.ancestor_lookup_requests += 1
            span = (end - start) // self.root_block_header_list_limit + 1
            resp = await self.__download_block_header_and_check(
                start, span - 1, len(range(start, end + 1, span))
            )

            if len(resp.block_header_list) == 0:
                # Remote chain re-org, may schedule re-sync
                raise RuntimeError(
                    "Remote chain reorg causing empty root block headers"
                )

            # Remote root block is reorg with new tip and new height (which may be lower than that of current)
            # Setup end as the new height
            if resp.root_tip != self.header:
                self.header = resp.root_tip
                end = min(resp.root_tip.height, end)

            prev_header = None
            for header in reversed(resp.block_header_list):
                # Check if header is correct
                if header.height < start or header.height > end:
                    raise RuntimeError(
                        "Bad peer returning root block height out of range"
                    )

                if prev_header is not None and header.height >= prev_header.height:
                    raise RuntimeError(
                        "Bad peer returning root block height must be ordered"
                    )
                prev_header = header

                if not self.__has_block_hash(header.get_hash()):
                    end = header.height - 1
                    continue

                if header.height == end:
                    return header

                start = header.height + 1
                best_ancestor = header
                check(end >= start)
                break

        # Return best ancenstor.  If no ancestor is found, return None.
        # Note that it is possible caused by remote root chain org.
        return best_ancestor

    async def __run_sync(self):
        """raise on any error so that sync() will close peer connection"""
        if self.header.total_difficulty <= self.root_state.tip.total_difficulty:
            return

        if self.__has_block_hash(self.header.get_hash()):
            return

        ancestor = await self.__find_ancestor()
        if ancestor is None:
            self.stats.ancestor_not_found_count += 1
            raise RuntimeError(
                "Cannot find common ancestor with max fork length {}".format(
                    self.max_staleness
                )
            )

        while self.header.height > ancestor.height:
            limit = min(
                self.header.height - ancestor.height, self.root_block_header_list_limit
            )
            resp = await self.__download_block_header_and_check(
                ancestor.height + 1, 0, limit
            )

            block_header_chain = resp.block_header_list
            if len(block_header_chain) == 0:
                Logger.info("Remote chain reorg causing empty root block headers")
                return

            # Remote root block is reorg with new tip and new height (which may be lower than that of current)
            if resp.root_tip != self.header:
                self.header = resp.root_tip

            if block_header_chain[0].hash_prev_block != ancestor.get_hash():
                # TODO: Remote chain may reorg, may retry the sync
                raise RuntimeError("Bad peer sending incorrect canonical headers")

            while len(block_header_chain) > 0:
                block_chain = await asyncio.wait_for(
                    self.__download_blocks(block_header_chain[:ROOT_BLOCK_BATCH_SIZE]),
                    SYNC_TIMEOUT,
                )
                Logger.info(
                    "[R] downloaded {} blocks ({} - {}) from peer".format(
                        len(block_chain),
                        block_chain[0].header.height,
                        block_chain[-1].header.height,
                    )
                )
                if len(block_chain) != len(block_header_chain[:ROOT_BLOCK_BATCH_SIZE]):
                    # TODO: tag bad peer
                    raise RuntimeError("Bad peer missing blocks for headers they have")

                for block in block_chain:
                    await self.__add_block(block)
                    ancestor = block_header_chain[0]
                    block_header_chain.pop(0)

    def __has_block_hash(self, block_hash):
        return self.root_state.db.contain_root_block_by_hash(block_hash)

    async def __download_blocks(self, block_header_list):
        block_hash_list = [b.get_hash() for b in block_header_list]
        op, resp, rpc_id = await self.peer.write_rpc_request(
            CommandOp.GET_ROOT_BLOCK_LIST_REQUEST,
            GetRootBlockListRequest(block_hash_list),
        )
        self.stats.blocks_downloaded += len(resp.root_block_list)
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
        self.stats.blocks_added += 1
        elapse = time.time() - start
        Logger.info(
            "[R] synced root block {} {} took {:.2f} seconds".format(
                root_block.header.height, root_block.header.get_hash().hex(), elapse
            )
        )

    async def __sync_minor_blocks(self, minor_block_header_list):
        minor_block_download_map = dict()
        for m_block_header in minor_block_header_list:
            m_block_hash = m_block_header.get_hash()
            if not self.root_state.db.contain_minor_block_by_hash(m_block_hash):
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
            if result.shard_stats:
                self.master_server.update_shard_stats(result.shard_stats)

        for m_header in minor_block_header_list:
            if not self.root_state.db.contain_minor_block_by_hash(m_header.get_hash()):
                raise RuntimeError(
                    "minor block {} from {} is still unavailable in master after root block sync".format(
                        m_header.get_hash().hex(), m_header.branch.to_str()
                    )
                )


class Synchronizer:
    """ Buffer the headers received from peer and sync one by one """

    def __init__(self):
        self.tasks = dict()
        self.running = False
        self.running_task = None
        self.stats = RootBlockSychronizerStats()
        self.root_block_header_list_limit = ROOT_BLOCK_HEADER_LIST_LIMIT

    def add_task(self, header, peer):
        if header.total_difficulty <= peer.root_state.tip.total_difficulty:
            return

        self.tasks[peer] = header
        Logger.info(
            "[R] added {} {} to sync queue (running={})".format(
                header.height, header.get_hash().hex(), self.running
            )
        )
        if not self.running:
            self.running = True
            asyncio.ensure_future(self.__run())

    def get_stats(self):
        def _task_to_dict(peer, header):
            return {
                "peerId": peer.id.hex(),
                "peerIp": str(peer.ip),
                "peerPort": peer.port,
                "rootHeight": header.height,
                "rootHash": header.get_hash().hex(),
            }

        return {
            "runningTask": _task_to_dict(self.running_task[1], self.running_task[0])
            if self.running_task
            else None,
            "queuedTasks": [
                _task_to_dict(peer, header) for peer, header in self.tasks.items()
            ],
        }

    def _pop_best_task(self):
        """ pop and return the task with heightest root """
        check(len(self.tasks) > 0)
        remove_list = []
        best_peer = None
        best_header = None
        for peer, header in self.tasks.items():
            if header.total_difficulty <= peer.root_state.tip.total_difficulty:
                remove_list.append(peer)
                continue

            if best_header is None or header.total_difficulty > best_header.total_difficulty:
                best_header = header
                best_peer = peer

        for peer in remove_list:
            del self.tasks[peer]
        if best_peer is not None:
            del self.tasks[best_peer]

        return best_header, best_peer

    async def __run(self):
        Logger.info("[R] synchronizer started!")
        while len(self.tasks) > 0:
            self.running_task = self._pop_best_task()
            header, peer = self.running_task
            if header is None:
                check(len(self.tasks) == 0)
                break
            task = SyncTask(header, peer, self.stats, self.root_block_header_list_limit)
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
        self.running_task = None
        Logger.info("[R] synchronizer finished!")


class SlaveConnection(ClusterConnection):
    OP_NONRPC_MAP = {}

    def __init__(
        self, env, reader, writer, master_server, slave_id, chain_mask_list, name=None
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
        self.chain_mask_list = chain_mask_list
        check(len(chain_mask_list) > 0)

        asyncio.ensure_future(self.active_and_loop_forever())

    def get_connection_to_forward(self, metadata):
        """ Override ProxyConnection.get_connection_to_forward()
        Forward traffic from slave to peer
        """
        if metadata.cluster_peer_id == RESERVED_CLUSTER_PEER_ID:
            return None

        peer = self.master_server.get_peer(metadata.cluster_peer_id)
        if peer is None:
            return NULL_CONNECTION

        return peer

    def validate_connection(self, connection):
        return connection == NULL_CONNECTION or isinstance(connection, P2PConnection)

    def has_shard(self, full_shard_id: int):
        for chain_mask in self.chain_mask_list:
            if chain_mask.contain_full_shard_id(full_shard_id):
                return True
        return False

    def has_overlap(self, chain_mask: ChainMask):
        for local_chain_mask in self.chain_mask_list:
            if local_chain_mask.has_overlap(chain_mask):
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
            metadata=ClusterMetadata(
                branch=ROOT_BRANCH, cluster_peer_id=RESERVED_CLUSTER_PEER_ID
            ),
        )
        return resp.id, resp.chain_mask_list

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
        self, tx: TypedTransaction, from_address, block_height: Optional[int]
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
        self, tx: TypedTransaction, from_address: Address
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
        self,
        branch: Branch,
        header_hash: bytes,
        nonce: int,
        mixhash: bytes,
        signature: Optional[bytes] = None,
    ) -> bool:
        request = SubmitWorkRequest(branch, header_hash, nonce, mixhash, signature)
        _, resp, _ = await self.write_rpc_request(
            ClusterOp.SUBMIT_WORK_REQUEST, request
        )
        submit_work_resp = resp  # type: SubmitWorkResponse
        return submit_work_resp.error_code == 0 and submit_work_resp.success

    # RPC handlers

    async def handle_add_minor_block_header_request(self, req):
        self.master_server.root_state.add_validated_minor_block_hash(
            req.minor_block_header.get_hash(), req.coinbase_amount_map.balance_map
        )
        self.master_server.update_shard_stats(req.shard_stats)
        self.master_server.update_tx_count_history(
            req.tx_count, req.x_shard_tx_count, req.minor_block_header.create_time
        )
        return AddMinorBlockHeaderResponse(
            error_code=0,
            artificial_tx_config=self.master_server.get_artificial_tx_config(),
        )

    async def handle_add_minor_block_header_list_request(self, req):
        check(len(req.minor_block_header_list) == len(req.coinbase_amount_map_list))
        for minor_block_header, coinbase_amount_map in zip(
            req.minor_block_header_list, req.coinbase_amount_map_list
        ):
            self.master_server.root_state.add_validated_minor_block_hash(
                minor_block_header.get_hash(), coinbase_amount_map.balance_map
            )
            Logger.info(
                "adding {} mblock to db".format(minor_block_header.get_hash().hex())
            )
        return AddMinorBlockHeaderListResponse(error_code=0)


OP_RPC_MAP = {
    ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST: (
        ClusterOp.ADD_MINOR_BLOCK_HEADER_RESPONSE,
        SlaveConnection.handle_add_minor_block_header_request,
    ),
    ClusterOp.ADD_MINOR_BLOCK_HEADER_LIST_REQUEST: (
        ClusterOp.ADD_MINOR_BLOCK_HEADER_LIST_RESPONSE,
        SlaveConnection.handle_add_minor_block_header_list_request,
    ),
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
        self.root_state = root_state  # type: RootState
        self.network = None  # will be set by network constructor
        self.cluster_config = env.cluster_config

        # branch value -> a list of slave running the shard
        self.branch_to_slaves = dict()  # type: Dict[int, List[SlaveConnection]]
        self.slave_pool = set()

        self.cluster_active_future = self.loop.create_future()
        self.shutdown_future = self.loop.create_future()
        self.name = name

        self.artificial_tx_config = ArtificialTxConfig(
            target_root_block_time=self.env.quark_chain_config.ROOT.CONSENSUS_CONFIG.TARGET_BLOCK_TIME,
            target_minor_block_time=next(
                iter(self.env.quark_chain_config.shards.values())
            ).CONSENSUS_CONFIG.TARGET_BLOCK_TIME,
        )

        self.synchronizer = Synchronizer()

        self.branch_to_shard_stats = dict()  # type: Dict[int, ShardStats]
        # (epoch in minute, tx_count in the minute)
        self.tx_count_history = deque()

        self.__init_root_miner()

    def __init_root_miner(self):
        miner_address = Address.create_from(
            self.env.quark_chain_config.ROOT.COINBASE_ADDRESS
        )

        async def __create_block(retry=True):
            while True:
                block = await self.__create_root_block_to_mine(address=miner_address)
                if block:
                    return block
                if not retry:
                    break
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
            guardian_private_key=self.env.quark_chain_config.guardian_private_key,
        )

    async def __rebroadcast_committing_root_block(self):
        committing_block_hash = self.root_state.get_committing_block_hash()
        if committing_block_hash:
            r_block = self.root_state.db.get_root_block_by_hash(
                committing_block_hash, consistency_check=False
            )
            # missing actual block, may have crashed before writing the block
            if not r_block:
                self.root_state.clear_committing_hash()
                return
            future_list = self.broadcast_rpc(
                op=ClusterOp.ADD_ROOT_BLOCK_REQUEST,
                req=AddRootBlockRequest(r_block, False),
            )
            result_list = await asyncio.gather(*future_list)
            check(all([resp.error_code == 0 for _, resp, _ in result_list]))
            self.root_state.clear_committing_hash()

    def get_artificial_tx_config(self):
        return self.artificial_tx_config

    def __has_all_shards(self):
        """ Returns True if all the shards have been run by at least one node """
        return len(self.branch_to_slaves) == len(
            self.env.quark_chain_config.get_full_shard_ids()
        ) and all([len(slaves) > 0 for _, slaves in self.branch_to_slaves.items()])

    async def __connect(self, host, port):
        """ Retries until success """
        Logger.info("Trying to connect {}:{}".format(host, port))
        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    host, port, loop=self.loop
                )
                break
            except Exception as e:
                Logger.info("Failed to connect {} {}: {}".format(host, port, e))
                await asyncio.sleep(
                    self.env.cluster_config.MASTER.MASTER_TO_SLAVE_CONNECT_RETRY_DELAY
                )
        Logger.info("Connected to {}:{}".format(host, port))
        return reader, writer

    async def __connect_to_slaves(self):
        """ Master connects to all the slaves """
        futures = []
        slaves = []
        for slave_info in self.cluster_config.get_slave_info_list():
            host = slave_info.host.decode("ascii")
            reader, writer = await self.__connect(host, slave_info.port)

            slave = SlaveConnection(
                self.env,
                reader,
                writer,
                self,
                slave_info.id,
                slave_info.chain_mask_list,
                name="{}_slave_{}".format(self.name, slave_info.id),
            )
            await slave.wait_until_active()
            futures.append(slave.send_ping())
            slaves.append(slave)

        results = await asyncio.gather(*futures)

        full_shard_ids = self.env.quark_chain_config.get_full_shard_ids()
        for slave, result in zip(slaves, results):
            # Verify the slave does have the same id and shard mask list as the config file
            id, chain_mask_list = result
            if id != slave.id:
                Logger.error(
                    "Slave id does not match. expect {} got {}".format(slave.id, id)
                )
                self.shutdown()
            if chain_mask_list != slave.chain_mask_list:
                Logger.error(
                    "Slave {} shard mask list does not match. expect {} got {}".format(
                        slave.id, slave.chain_mask_list, chain_mask_list
                    )
                )
                self.shutdown()

            self.slave_pool.add(slave)
            for full_shard_id in full_shard_ids:
                if slave.has_shard(full_shard_id):
                    self.branch_to_slaves.setdefault(full_shard_id, []).append(slave)

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
                    Branch(branch_value).to_str(), [s.id for s in slaves]
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
        await self.__rebroadcast_committing_root_block()

        self.cluster_active_future.set_result(None)

    def start(self):
        self.loop.create_task(self.__init_cluster())

    def do_loop(self):
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

    async def __create_root_block_to_mine(self, address) -> Optional[RootBlock]:
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
        full_shard_id_to_header_list = dict()
        for response in responses:
            _, response, _ = response
            if response.error_code != 0:
                return None
            for headers_info in response.headers_info_list:
                height = 0
                for header in headers_info.header_list:
                    # check headers are ordered by height
                    check(height == 0 or height + 1 == header.height)
                    height = header.height

                    # Filter out the ones unknown to the master
                    if not self.root_state.db.contain_minor_block_by_hash(
                        header.get_hash()
                    ):
                        break
                    full_shard_id_to_header_list.setdefault(
                        headers_info.branch.get_full_shard_id(), []
                    ).append(header)

        header_list = []
        full_shard_ids_to_check = self.env.quark_chain_config.get_initialized_full_shard_ids_before_root_height(
            self.root_state.tip.height + 1
        )
        for full_shard_id in full_shard_ids_to_check:
            headers = full_shard_id_to_header_list.get(full_shard_id, [])
            header_list.extend(headers)

        return self.root_state.create_block_to_mine(header_list, address)

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
        self, address, branch_value: Optional[int]
    ) -> Optional[Union[RootBlock, MinorBlock]]:
        """Return root block is branch value provided is None."""
        # Mining old blocks is useless
        if self.synchronizer.running:
            return None

        if branch_value is None:
            root = await self.__create_root_block_to_mine(address)
            return root or None

        block = await self.__get_minor_block_to_mine(Branch(branch_value), address)
        return block or None

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
            == len(self.env.quark_chain_config.get_full_shard_ids())
        )
        return branch_to_account_branch_data

    async def get_primary_account_data(
        self, address: Address, block_height: Optional[int] = None
    ):
        # TODO: Only query the shard who has the address
        full_shard_id = self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
            address.full_shard_key
        )
        slaves = self.branch_to_slaves.get(full_shard_id, None)
        if not slaves:
            return None
        slave = slaves[0]
        request = GetAccountDataRequest(address, block_height)
        _, resp, _ = await slave.write_rpc_request(
            ClusterOp.GET_ACCOUNT_DATA_REQUEST, request
        )
        for account_branch_data in resp.account_branch_data_list:
            if account_branch_data.branch.value == full_shard_id:
                return account_branch_data
        return None

    async def add_transaction(self, tx: TypedTransaction, from_peer=None):
        """ Add transaction to the cluster and broadcast to peers """
        evm_tx = tx.tx.to_evm_tx()
        evm_tx.set_quark_chain_config(self.env.quark_chain_config)
        branch = Branch(evm_tx.from_full_shard_id)
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
        self, tx: TypedTransaction, from_address, block_height: Optional[int]
    ) -> Optional[bytes]:
        """ Execute transaction without persistence """
        evm_tx = tx.tx.to_evm_tx()
        evm_tx.set_quark_chain_config(self.env.quark_chain_config)
        branch = Branch(evm_tx.from_full_shard_id)
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
        # use write-ahead log so if crashed the root block can be re-broadcasted
        self.root_state.write_committing_hash(r_block.header.get_hash())

        try:
            update_tip = self.root_state.add_block(r_block)
        except ValueError as e:
            Logger.log_exception()
            raise e

        try:
            if update_tip and self.network is not None:
                for peer in self.network.iterate_peers():
                    peer.send_updated_tip()
        except Exception:
            pass

        future_list = self.broadcast_rpc(
            op=ClusterOp.ADD_ROOT_BLOCK_REQUEST, req=AddRootBlockRequest(r_block, False)
        )
        result_list = await asyncio.gather(*future_list)
        check(all([resp.error_code == 0 for _, resp, _ in result_list]))
        self.root_state.clear_committing_hash()

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
        self, num_tx_per_shard, xshard_percent, tx: TypedTransaction
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

    def get_block_count(self):
        header = self.root_state.tip
        shard_r_c = self.root_state.db.get_block_count(header.height)
        return {"rootHeight": header.height, "shardRC": shard_r_c}

    async def get_stats(self):
        shards = []
        for shard_stats in self.branch_to_shard_stats.values():
            shard = dict()
            shard["fullShardId"] = shard_stats.branch.get_full_shard_id()
            shard["chainId"] = shard_stats.branch.get_chain_id()
            shard["shardId"] = shard_stats.branch.get_shard_id()
            shard["height"] = shard_stats.height
            shard["difficulty"] = shard_stats.difficulty
            shard["coinbaseAddress"] = "0x" + shard_stats.coinbase_address.to_hex()
            shard["timestamp"] = shard_stats.timestamp
            shard["txCount60s"] = shard_stats.tx_count60s
            shard["pendingTxCount"] = shard_stats.pending_tx_count
            shard["totalTxCount"] = shard_stats.total_tx_count
            shard["blockCount60s"] = shard_stats.block_count60s
            shard["staleBlockCount60s"] = shard_stats.stale_block_count60s
            shard["lastBlockTime"] = shard_stats.last_block_time
            shards.append(shard)
        shards.sort(key=lambda x: x["fullShardId"])

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
            prev = self.root_state.db.get_root_block_header_by_hash(
                self.root_state.tip.hash_prev_block
            )
            root_last_block_time = self.root_state.tip.create_time - prev.create_time

        tx_count_history = []
        for item in self.tx_count_history:
            tx_count_history.append(
                {"timestamp": item[0], "txCount": item[1], "xShardTxCount": item[2]}
            )

        return {
            "networkId": self.env.quark_chain_config.NETWORK_ID,
            "chainSize": self.env.quark_chain_config.CHAIN_SIZE,
            "shardServerCount": len(self.slave_pool),
            "rootHeight": self.root_state.tip.height,
            "rootDifficulty": self.root_state.tip.difficulty,
            "rootCoinbaseAddress": "0x" + self.root_state.tip.coinbase_address.to_hex(),
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
        full_shard_id = self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
            address.full_shard_key
        )
        slave = self.branch_to_slaves[full_shard_id][0]
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
        self, tx: TypedTransaction, from_address: Address
    ) -> Optional[int]:
        evm_tx = tx.tx.to_evm_tx()
        evm_tx.set_quark_chain_config(self.env.quark_chain_config)
        branch = Branch(evm_tx.from_full_shard_id)
        if branch.value not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[branch.value][0]
        return await slave.estimate_gas(tx, from_address)

    async def get_storage_at(
        self, address: Address, key: int, block_height: Optional[int]
    ) -> Optional[bytes]:
        full_shard_id = self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
            address.full_shard_key
        )
        if full_shard_id not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[full_shard_id][0]
        return await slave.get_storage_at(address, key, block_height)

    async def get_code(
        self, address: Address, block_height: Optional[int]
    ) -> Optional[bytes]:
        full_shard_id = self.env.quark_chain_config.get_full_shard_id_by_full_shard_key(
            address.full_shard_key
        )
        if full_shard_id not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[full_shard_id][0]
        return await slave.get_code(address, block_height)

    async def gas_price(self, branch: Branch) -> Optional[int]:
        if branch.value not in self.branch_to_slaves:
            return None

        slave = self.branch_to_slaves[branch.value][0]
        return await slave.gas_price(branch)

    async def get_work(self, branch: Optional[Branch]) -> Optional[MiningWork]:
        if not branch:  # get root chain work
            work, _ = await self.root_miner.get_work()
            return work

        if branch.value not in self.branch_to_slaves:
            return None
        slave = self.branch_to_slaves[branch.value][0]
        return await slave.get_work(branch)

    async def submit_work(
        self,
        branch: Optional[Branch],
        header_hash: bytes,
        nonce: int,
        mixhash: bytes,
        signature: Optional[bytes] = None,
    ) -> bool:
        if not branch:  # submit root chain work
            return await self.root_miner.submit_work(
                header_hash, nonce, mixhash, signature
            )

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

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    env = parse_args()
    root_state = RootState(env)

    master = MasterServer(env, root_state)
    master.start()
    master.wait_until_cluster_active()

    # kick off simulated mining if enabled
    if env.cluster_config.START_SIMULATED_MINING:
        asyncio.ensure_future(master.start_mining())

    loop = asyncio.get_event_loop()

    if env.cluster_config.use_p2p():
        network = P2PManager(env, master, loop)
    else:
        network = SimpleNetwork(env, master, loop)
    network.start()

    public_json_rpc_server = JSONRPCServer.start_public_server(env, master)
    private_json_rpc_server = JSONRPCServer.start_private_server(env, master)

    master.do_loop()

    public_json_rpc_server.shutdown()
    private_json_rpc_server.shutdown()

    Logger.info("Master server is shutdown")


if __name__ == "__main__":
    main()
