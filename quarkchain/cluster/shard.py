import asyncio
from collections import deque
from typing import List, Optional, Callable

from quarkchain.cluster.miner import Miner, validate_seal
from quarkchain.cluster.p2p_commands import (
    OP_SERIALIZER_MAP,
    CommandOp,
    Direction,
    GetMinorBlockHeaderListRequest,
    GetMinorBlockHeaderListResponse,
    GetMinorBlockListRequest,
    GetMinorBlockListResponse,
    NewBlockMinorCommand,
    NewMinorBlockHeaderListCommand,
    NewTransactionListCommand,
)
from quarkchain.cluster.protocol import ClusterMetadata, VirtualConnection
from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tx_generator import TransactionGenerator
from quarkchain.config import ShardConfig, ConsensusType
from quarkchain.core import (
    Address,
    Branch,
    MinorBlockHeader,
    RootBlock,
    TypedTransaction,
)
from quarkchain.constants import (
    ALLOWED_FUTURE_BLOCKS_TIME_BROADCAST,
    NEW_TRANSACTION_LIST_LIMIT,
    MINOR_BLOCK_BATCH_SIZE,
    MINOR_BLOCK_HEADER_LIST_LIMIT,
    SYNC_TIMEOUT,
    BLOCK_UNCOMMITTED,
    BLOCK_COMMITTING,
    BLOCK_COMMITTED,
)
from quarkchain.db import InMemoryDb, PersistentDb
from quarkchain.utils import Logger, check, time_ms
from quarkchain.p2p.utils import RESERVED_CLUSTER_PEER_ID


class PeerShardConnection(VirtualConnection):
    """ A virtual connection between local shard and remote shard
    """

    def __init__(self, master_conn, cluster_peer_id, shard, name=None):
        super().__init__(
            master_conn, OP_SERIALIZER_MAP, OP_NONRPC_MAP, OP_RPC_MAP, name=name
        )
        self.cluster_peer_id = cluster_peer_id
        self.shard = shard
        self.shard_state = shard.state
        self.best_root_block_header_observed = None
        self.best_minor_block_header_observed = None

    def get_metadata_to_write(self, metadata):
        """ Override VirtualConnection.get_metadata_to_write()
        """
        if self.cluster_peer_id == RESERVED_CLUSTER_PEER_ID:
            self.close_with_error(
                "PeerShardConnection: remote is using reserved cluster peer id which is prohibited"
            )
        return ClusterMetadata(self.shard_state.branch, self.cluster_peer_id)

    def close_with_error(self, error):
        Logger.error("Closing shard connection with error {}".format(error))
        return super().close_with_error(error)

    ################### Outgoing requests ################

    def send_new_block(self, block):
        # TODO do not send seen blocks with this peer, optional
        self.write_command(
            op=CommandOp.NEW_BLOCK_MINOR, cmd=NewBlockMinorCommand(block)
        )

    def broadcast_new_tip(self):
        if self.best_root_block_header_observed:
            if (
                self.shard_state.root_tip.total_difficulty
                < self.best_root_block_header_observed.total_difficulty
            ):
                return
            if self.shard_state.root_tip == self.best_root_block_header_observed:
                if (
                    self.shard_state.header_tip.height
                    < self.best_minor_block_header_observed.height
                ):
                    return
                if self.shard_state.header_tip == self.best_minor_block_header_observed:
                    return

        self.write_command(
            op=CommandOp.NEW_MINOR_BLOCK_HEADER_LIST,
            cmd=NewMinorBlockHeaderListCommand(
                self.shard_state.root_tip, [self.shard_state.header_tip]
            ),
        )

    def broadcast_tx_list(self, tx_list):
        self.write_command(
            op=CommandOp.NEW_TRANSACTION_LIST, cmd=NewTransactionListCommand(tx_list)
        )

    ################## RPC handlers ###################

    async def handle_get_minor_block_header_list_request(self, request):
        if request.branch != self.shard_state.branch:
            self.close_with_error("Wrong branch from peer")
        if request.limit <= 0 or request.limit > 2 * MINOR_BLOCK_HEADER_LIST_LIMIT:
            self.close_with_error("Bad limit")
        # TODO: support tip direction
        if request.direction != Direction.GENESIS:
            self.close_with_error("Bad direction")

        block_hash = request.block_hash
        header_list = []
        for i in range(request.limit):
            header = self.shard_state.db.get_minor_block_header_by_hash(block_hash)
            header_list.append(header)
            if header.height == 0:
                break
            block_hash = header.hash_prev_minor_block

        return GetMinorBlockHeaderListResponse(
            self.shard_state.root_tip, self.shard_state.header_tip, header_list
        )

    async def handle_get_minor_block_header_list_with_skip_request(self, request):
        if request.branch != self.shard_state.branch:
            self.close_with_error("Wrong branch from peer")
        if request.limit <= 0 or request.limit > 2 * MINOR_BLOCK_HEADER_LIST_LIMIT:
            self.close_with_error("Bad limit")
        if request.type != 0 and request.type != 1:
            self.close_with_error("Bad type value")

        if request.type == 1:
            block_height = request.get_height()
        else:
            block_hash = request.get_hash()
            block_header = self.shard_state.db.get_minor_block_header_by_hash(
                block_hash
            )
            if block_header is None:
                return GetMinorBlockHeaderListResponse(
                    self.shard_state.root_tip, self.shard_state.header_tip, []
                )

            # Check if it is canonical chain
            block_height = block_header.height
            if (
                self.shard_state.db.get_minor_block_header_by_height(block_height)
                != block_header
            ):
                return GetMinorBlockHeaderListResponse(
                    self.shard_state.root_tip, self.shard_state.header_tip, []
                )

        header_list = []
        while (
            len(header_list) < request.limit
            and block_height >= 0
            and block_height <= self.shard_state.header_tip.height
        ):
            block_header = self.shard_state.db.get_minor_block_header_by_height(
                block_height
            )
            if block_header is None:
                break
            header_list.append(block_header)
            if request.direction == Direction.GENESIS:
                block_height -= request.skip + 1
            else:
                block_height += request.skip + 1

        return GetMinorBlockHeaderListResponse(
            self.shard_state.root_tip, self.shard_state.header_tip, header_list
        )

    async def handle_get_minor_block_list_request(self, request):
        if len(request.minor_block_hash_list) > 2 * MINOR_BLOCK_BATCH_SIZE:
            self.close_with_error("Bad number of minor blocks requested")
        m_block_list = []
        for m_block_hash in request.minor_block_hash_list:
            m_block = self.shard_state.db.get_minor_block_by_hash(m_block_hash)
            if m_block is None:
                continue
            # TODO: Check list size to make sure the resp is smaller than limit
            m_block_list.append(m_block)

        return GetMinorBlockListResponse(m_block_list)

    async def handle_new_block_minor_command(self, _op, cmd, _rpc_id):
        self.best_minor_block_header_observed = cmd.block.header
        await self.shard.handle_new_block(cmd.block)

    async def handle_new_minor_block_header_list_command(self, _op, cmd, _rpc_id):
        # TODO: allow multiple headers if needed
        if len(cmd.minor_block_header_list) != 1:
            self.close_with_error("minor block header list must have only one header")
            return
        for m_header in cmd.minor_block_header_list:
            if m_header.branch != self.shard_state.branch:
                self.close_with_error("incorrect branch")
                return

        if self.best_root_block_header_observed:
            # check root header is not decreasing
            if (
                cmd.root_block_header.total_difficulty
                < self.best_root_block_header_observed.total_difficulty
            ):
                return self.close_with_error(
                    "best observed root header total_difficulty is decreasing {} < {}".format(
                        cmd.root_block_header.total_difficulty,
                        self.best_root_block_header_observed.total_difficulty,
                    )
                )
            if (
                cmd.root_block_header.total_difficulty
                == self.best_root_block_header_observed.total_difficulty
            ):
                if cmd.root_block_header != self.best_root_block_header_observed:
                    return self.close_with_error(
                        "best observed root header changed with same total_difficulty {}".format(
                            self.best_root_block_header_observed.total_difficulty
                        )
                    )

                # check minor header is not decreasing
                if m_header.height < self.best_minor_block_header_observed.height:
                    return self.close_with_error(
                        "best observed minor header is decreasing {} < {}".format(
                            m_header.height,
                            self.best_minor_block_header_observed.height,
                        )
                    )

        self.best_root_block_header_observed = cmd.root_block_header
        self.best_minor_block_header_observed = m_header

        # Do not download if the new header is not higher than the current tip
        if self.shard_state.header_tip.height >= m_header.height:
            return

        # Do not download if the prev root block is not synced
        rblock_header = self.shard_state.get_root_block_header_by_hash(m_header.hash_prev_root_block)
        if (rblock_header is None):
            return

        # Do not download if the new header's confirmed root is lower then current root tip last header's confirmed root
        # This means the minor block's root is a fork, which will be handled by master sync
        confirmed_tip = self.shard_state.confirmed_header_tip
        confirmed_root_header = None if confirmed_tip is None else self.shard_state.get_root_block_header_by_hash(confirmed_tip.hash_prev_root_block)
        if confirmed_root_header is not None and confirmed_root_header.height > rblock_header.height:
            return

        Logger.info_every_sec(
            "[{}] received new tip with height {}".format(
                m_header.branch.to_str(), m_header.height
            ),
            5,
        )
        self.shard.synchronizer.add_task(m_header, self)

    async def handle_new_transaction_list_command(self, op_code, cmd, rpc_id):
        if len(cmd.transaction_list) > NEW_TRANSACTION_LIST_LIMIT:
            self.close_with_error("Too many transactions in one command")
        self.shard.add_tx_list(cmd.transaction_list, self)


# P2P command definitions
OP_NONRPC_MAP = {
    CommandOp.NEW_MINOR_BLOCK_HEADER_LIST: PeerShardConnection.handle_new_minor_block_header_list_command,
    CommandOp.NEW_TRANSACTION_LIST: PeerShardConnection.handle_new_transaction_list_command,
    CommandOp.NEW_BLOCK_MINOR: PeerShardConnection.handle_new_block_minor_command,
}


OP_RPC_MAP = {
    CommandOp.GET_MINOR_BLOCK_HEADER_LIST_REQUEST: (
        CommandOp.GET_MINOR_BLOCK_HEADER_LIST_RESPONSE,
        PeerShardConnection.handle_get_minor_block_header_list_request,
    ),
    CommandOp.GET_MINOR_BLOCK_LIST_REQUEST: (
        CommandOp.GET_MINOR_BLOCK_LIST_RESPONSE,
        PeerShardConnection.handle_get_minor_block_list_request,
    ),
    CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_REQUEST: (
        CommandOp.GET_MINOR_BLOCK_HEADER_LIST_WITH_SKIP_RESPONSE,
        PeerShardConnection.handle_get_minor_block_header_list_with_skip_request,
    ),
}


class SyncTask:
    """ Given a header and a shard connection, the synchronizer will synchronize
    the shard state with the peer shard up to the height of the header.
    """

    def __init__(self, header: MinorBlockHeader, shard_conn: PeerShardConnection):
        self.header = header
        self.shard_conn = shard_conn
        self.shard_state = shard_conn.shard_state  # type: ShardState
        self.shard = shard_conn.shard

        full_shard_id = self.header.branch.get_full_shard_id()
        shard_config = self.shard_state.env.quark_chain_config.shards[full_shard_id]
        self.max_staleness = shard_config.max_stale_minor_block_height_diff

    async def sync(self, notify_sync: Callable):
        try:
            await self.__run_sync(notify_sync)
        except Exception as e:
            Logger.log_exception()
            self.shard_conn.close_with_error(str(e))

    async def __run_sync(self, notify_sync: Callable):
        if self.__has_block_hash(self.header.get_hash()):
            return

        # descending height
        block_header_chain = [self.header]

        while not self.__has_block_hash(block_header_chain[-1].hash_prev_minor_block):
            block_hash = block_header_chain[-1].hash_prev_minor_block
            height = block_header_chain[-1].height - 1

            if self.shard_state.header_tip.height - height > self.max_staleness:
                Logger.warning(
                    "[{}] abort syncing due to forking at very old block {} << {}".format(
                        self.header.branch.to_str(),
                        height,
                        self.shard_state.header_tip.height,
                    )
                )
                return

            if not self.shard_state.db.contain_root_block_by_hash(
                block_header_chain[-1].hash_prev_root_block
            ):
                return
            Logger.info(
                "[{}] downloading headers from {} {}".format(
                    self.shard_state.branch.to_str(), height, block_hash.hex()
                )
            )
            block_header_list = await asyncio.wait_for(
                self.__download_block_headers(block_hash), SYNC_TIMEOUT
            )
            Logger.info(
                "[{}] downloaded {} headers from peer".format(
                    self.shard_state.branch.to_str(), len(block_header_list)
                )
            )
            if not self.__validate_block_headers(block_header_list):
                # TODO: tag bad peer
                return self.shard_conn.close_with_error(
                    "Bad peer sending discontinuing block headers"
                )
            for header in block_header_list:
                if self.__has_block_hash(header.get_hash()):
                    break
                block_header_chain.append(header)

        # ascending height
        block_header_chain.reverse()
        while len(block_header_chain) > 0:
            block_chain = await asyncio.wait_for(
                self.__download_blocks(block_header_chain[:MINOR_BLOCK_BATCH_SIZE]),
                SYNC_TIMEOUT,
            )
            Logger.info(
                "[{}] downloaded {} blocks from peer".format(
                    self.shard_state.branch.to_str(), len(block_chain)
                )
            )
            if len(block_chain) != len(block_header_chain[:MINOR_BLOCK_BATCH_SIZE]):
                # TODO: tag bad peer
                return self.shard_conn.close_with_error(
                    "Bad peer sending less than requested blocks"
                )

            counter = 0
            for block in block_chain:
                # Stop if the block depends on an unknown root block
                # TODO: move this check to early stage to avoid downloading unnecessary headers
                if not self.shard_state.db.contain_root_block_by_hash(
                    block.header.hash_prev_root_block
                ):
                    return
                await self.shard.add_block(block)
                if counter % 100 == 0:
                    sync_data = (block.header.height, block_header_chain[-1])
                    asyncio.ensure_future(notify_sync(sync_data))
                    counter = 0
                counter += 1
                block_header_chain.pop(0)

    def __has_block_hash(self, block_hash):
        return self.shard_state.db.contain_minor_block_by_hash(block_hash)

    def __validate_block_headers(self, block_header_list: List[MinorBlockHeader]):
        for i in range(len(block_header_list) - 1):
            header, prev = block_header_list[i : i + 2]  # type: MinorBlockHeader
            if header.height != prev.height + 1:
                return False
            if header.hash_prev_minor_block != prev.get_hash():
                return False
            try:
                # Note that PoSW may lower diff, so checks here are necessary but not sufficient
                # More checks happen during block addition
                shard_config = self.shard.env.quark_chain_config.shards[
                    header.branch.get_full_shard_id()
                ]
                consensus_type = shard_config.CONSENSUS_TYPE
                diff = header.difficulty
                if shard_config.POSW_CONFIG.ENABLED:
                    diff //= shard_config.POSW_CONFIG.get_diff_divider(header.create_time)
                validate_seal(
                    header,
                    consensus_type,
                    adjusted_diff=diff,
                    qkchash_with_rotation_stats=consensus_type
                    == ConsensusType.POW_QKCHASH
                    and self.shard.state._qkchashx_enabled(header),
                )
            except Exception as e:
                Logger.warning(
                    "[{}] got block with bad seal in sync: {}".format(
                        header.branch.to_str(), str(e)
                    )
                )
                return False
        return True

    async def __download_block_headers(self, block_hash):
        request = GetMinorBlockHeaderListRequest(
            block_hash=block_hash,
            branch=self.shard_state.branch,
            limit=MINOR_BLOCK_HEADER_LIST_LIMIT,
            direction=Direction.GENESIS,
        )
        op, resp, rpc_id = await self.shard_conn.write_rpc_request(
            CommandOp.GET_MINOR_BLOCK_HEADER_LIST_REQUEST, request
        )
        return resp.block_header_list

    async def __download_blocks(self, block_header_list):
        block_hash_list = [b.get_hash() for b in block_header_list]
        op, resp, rpc_id = await self.shard_conn.write_rpc_request(
            CommandOp.GET_MINOR_BLOCK_LIST_REQUEST,
            GetMinorBlockListRequest(block_hash_list),
        )
        return resp.minor_block_list


class Synchronizer:
    """ Buffer the headers received from peer and sync one by one """

    def __init__(
        self,
        notify_sync: Callable[[bool, int, int, int], None],
        header_tip_getter: Callable[[], MinorBlockHeader],
    ):
        self.queue = deque()
        self.running = False
        self.notify_sync = notify_sync
        self.header_tip_getter = header_tip_getter
        self.counter = 0

    def add_task(self, header, shard_conn):
        self.queue.append((header, shard_conn))
        if not self.running:
            self.running = True
            asyncio.ensure_future(self.__run())
            if self.counter % 10 == 0:
                self.__call_notify_sync()
                self.counter = 0
            self.counter += 1

    async def __run(self):
        while len(self.queue) > 0:
            header, shard_conn = self.queue.popleft()
            task = SyncTask(header, shard_conn)
            await task.sync(self.notify_sync)
        self.running = False
        if self.counter % 10 == 1:
            self.__call_notify_sync()

    def __call_notify_sync(self):
        sync_data = (
            (self.header_tip_getter().height, max(h.height for h, _ in self.queue))
            if len(self.queue) > 0
            else None
        )
        asyncio.ensure_future(self.notify_sync(sync_data))


class Shard:
    def __init__(self, env, full_shard_id, slave):
        self.env = env
        self.full_shard_id = full_shard_id
        self.slave = slave

        self.state = ShardState(env, full_shard_id, self.__init_shard_db())

        self.loop = asyncio.get_event_loop()
        self.synchronizer = Synchronizer(
            self.state.subscription_manager.notify_sync, lambda: self.state.header_tip
        )

        self.peers = dict()  # cluster_peer_id -> PeerShardConnection

        # block hash -> future (that will return when the block is fully propagated in the cluster)
        # the block that has been added locally but not have been fully propagated will have an entry here
        self.add_block_futures = dict()

        self.tx_generator = TransactionGenerator(self.env.quark_chain_config, self)

        self.__init_miner()

    def __init_shard_db(self):
        """
        Create a PersistentDB or use the env.db if DB_PATH_ROOT is not specified in the ClusterConfig.
        """
        if self.env.cluster_config.use_mem_db():
            return InMemoryDb()

        db_path = "{path}/shard-{shard_id}.db".format(
            path=self.env.cluster_config.DB_PATH_ROOT, shard_id=self.full_shard_id
        )
        return PersistentDb(db_path, clean=self.env.cluster_config.CLEAN)

    def __init_miner(self):
        async def __create_block(coinbase_addr: Address, retry=True):
            # hold off mining if the shard is syncing
            while self.synchronizer.running or not self.state.initialized:
                if not retry:
                    break
                await asyncio.sleep(0.1)

            if coinbase_addr.is_empty():  # devnet or wrong config
                coinbase_addr.full_shard_key = self.full_shard_id
            return self.state.create_block_to_mine(address=coinbase_addr)

        async def __add_block(block):
            # do not add block if there is a sync in progress
            if self.synchronizer.running:
                return
            # do not add stale block
            if self.state.header_tip.height >= block.header.height:
                return
            await self.handle_new_block(block)

        def __get_mining_param():
            return {
                "target_block_time": self.slave.artificial_tx_config.target_minor_block_time
            }

        shard_config = self.env.quark_chain_config.shards[
            self.full_shard_id
        ]  # type: ShardConfig
        self.miner = Miner(
            shard_config.CONSENSUS_TYPE,
            __create_block,
            __add_block,
            __get_mining_param,
            lambda: self.state.header_tip,
            remote=shard_config.CONSENSUS_CONFIG.REMOTE_MINE,
        )

    @property
    def genesis_root_height(self):
        return self.env.quark_chain_config.get_genesis_root_height(self.full_shard_id)

    def add_peer(self, peer: PeerShardConnection):
        self.peers[peer.cluster_peer_id] = peer
        Logger.info(
            "[{}] connected to peer {}".format(
                Branch(self.full_shard_id).to_str(), peer.cluster_peer_id
            )
        )

    async def create_peer_shard_connections(self, cluster_peer_ids, master_conn):
        conns = []
        for cluster_peer_id in cluster_peer_ids:
            peer_shard_conn = PeerShardConnection(
                master_conn=master_conn,
                cluster_peer_id=cluster_peer_id,
                shard=self,
                name="{}_vconn_{}".format(master_conn.name, cluster_peer_id),
            )
            asyncio.ensure_future(peer_shard_conn.active_and_loop_forever())
            conns.append(peer_shard_conn)
        await asyncio.gather(*[conn.active_future for conn in conns])
        for conn in conns:
            self.add_peer(conn)

    async def __init_genesis_state(self, root_block: RootBlock):
        block, coinbase_amount_map = self.state.init_genesis_state(root_block)
        xshard_list = []
        await self.slave.broadcast_xshard_tx_list(
            block, xshard_list, root_block.header.height
        )
        await self.slave.send_minor_block_header_to_master(
            block.header,
            len(block.tx_list),
            len(xshard_list),
            coinbase_amount_map,
            self.state.get_shard_stats(),
        )

    async def init_from_root_block(self, root_block: RootBlock):
        """ Either recover state from local db or create genesis state based on config"""
        if root_block.header.height > self.genesis_root_height:
            return self.state.init_from_root_block(root_block)

        if root_block.header.height == self.genesis_root_height:
            await self.__init_genesis_state(root_block)

    async def add_root_block(self, root_block: RootBlock):
        if root_block.header.height > self.genesis_root_height:
            return self.state.add_root_block(root_block)

        # this happens when there is a root chain fork
        if root_block.header.height == self.genesis_root_height:
            await self.__init_genesis_state(root_block)

    def broadcast_new_block(self, block):
        for cluster_peer_id, peer in self.peers.items():
            peer.send_new_block(block)

    def broadcast_new_tip(self):
        for cluster_peer_id, peer in self.peers.items():
            peer.broadcast_new_tip()

    def broadcast_tx_list(self, tx_list, source_peer=None):
        for cluster_peer_id, peer in self.peers.items():
            if source_peer == peer:
                continue
            peer.broadcast_tx_list(tx_list)

    async def handle_new_block(self, block):
        """
        This is a fast path for block propagation. The block is broadcasted to peers before being added to local state.
        0. if local shard is syncing, doesn't make sense to add, skip
        1. if block parent is not in local state/new block pool, discard (TODO: is this necessary?)
        2. if already in cache or in local state/new block pool, pass
        3. validate: check time, difficulty, POW
        4. add it to new minor block broadcast cache
        5. broadcast to all peers (minus peer that sent it, optional)
        6. add_block() to local state (then remove from cache)
           also, broadcast tip if tip is updated (so that peers can sync if they missed blocks, or are new)
        """
        if self.synchronizer.running:
            # TODO optional: queue the block if it came from broadcast to so that once sync is over,
            # catch up immediately
            return

        if block.header.get_hash() in self.state.new_block_header_pool:
            return
        if self.state.db.contain_minor_block_by_hash(block.header.get_hash()):
            return

        prev_hash, prev_header = block.header.hash_prev_minor_block, None
        if prev_hash in self.state.new_block_header_pool:
            prev_header = self.state.new_block_header_pool[prev_hash]
        else:
            prev_header = self.state.db.get_minor_block_header_by_hash(prev_hash)
        if prev_header is None:  # Missing prev
            return

        # Sanity check on timestamp and block height
        if (
            block.header.create_time
            > time_ms() // 1000 + ALLOWED_FUTURE_BLOCKS_TIME_BROADCAST
        ):
            return
        # Ignore old blocks
        if (
            self.state.header_tip
            and self.state.header_tip.height - block.header.height
            > self.state.shard_config.max_stale_minor_block_height_diff
        ):
            return

        # There is a race that the root block may not be processed at the moment.
        # Ignore it if its root block is not found.
        # Otherwise, validate_block() will fail and we will disconnect the peer.
        rblock_header = self.state.get_root_block_header_by_hash(block.header.hash_prev_root_block)
        if (rblock_header is None):
            return

        # Do not download if the new header's confirmed root is lower then current root tip last header's confirmed root
        # This means the minor block's root is a fork, which will be handled by master sync
        confirmed_tip = self.state.confirmed_header_tip
        confirmed_root_header = None if confirmed_tip is None else self.state.get_root_block_header_by_hash(confirmed_tip.hash_prev_root_block)
        if confirmed_root_header is not None and confirmed_root_header.height > rblock_header.height:
            return

        try:
            self.state.validate_block(block)
        except Exception as e:
            Logger.warning(
                "[{}] got bad block in handle_new_block: {}".format(
                    block.header.branch.to_str(), str(e)
                )
            )
            raise e

        self.state.new_block_header_pool[block.header.get_hash()] = block.header

        Logger.info(
            "[{}/{}] got new block with height {}".format(
                block.header.branch.get_chain_id(),
                block.header.branch.get_shard_id(),
                block.header.height,
            )
        )

        self.broadcast_new_block(block)
        await self.add_block(block)

    def __get_block_commit_status_by_hash(self, block_hash):
        # If the block is committed, it means
        # - All neighbor shards/slaves receives x-shard tx list
        # - The block header is sent to master
        # then return immediately
        if self.state.is_committed_by_hash(block_hash):
            return BLOCK_COMMITTED, None

        # Check if the block is being propagating to other slaves and the master
        # Let's make sure all the shards and master got it before committing it
        future = self.add_block_futures.get(block_hash)
        if future is not None:
            return BLOCK_COMMITTING, future

        return BLOCK_UNCOMMITTED, None

    async def add_block(self, block):
        """ Returns true if block is successfully added. False on any error.
        called by 1. local miner (will not run if syncing) 2. SyncTask
        """

        block_hash = block.header.get_hash()
        commit_status, future = self.__get_block_commit_status_by_hash(block_hash)
        if commit_status == BLOCK_COMMITTED:
            return True
        elif commit_status == BLOCK_COMMITTING:
            Logger.info(
                "[{}] {} is being added ... waiting for it to finish".format(
                    block.header.branch.to_str(), block.header.height
                )
            )
            await future
            return True

        check(commit_status == BLOCK_UNCOMMITTED)
        # Validate and add the block
        old_tip = self.state.header_tip
        try:
            xshard_list, coinbase_amount_map = self.state.add_block(block, force=True)
        except Exception as e:
            Logger.error_exception()
            return False

        # only remove from pool if the block successfully added to state,
        # this may cache failed blocks but prevents them being broadcasted more than needed
        # TODO add ttl to blocks in new_block_header_pool
        self.state.new_block_header_pool.pop(block_hash, None)
        # block has been added to local state, broadcast tip so that peers can sync if needed
        try:
            if old_tip != self.state.header_tip:
                self.broadcast_new_tip()
        except Exception:
            Logger.warning_every_sec("broadcast tip failure", 1)

        # Add the block in future and wait
        self.add_block_futures[block_hash] = self.loop.create_future()

        prev_root_height = self.state.db.get_root_block_header_by_hash(
            block.header.hash_prev_root_block
        ).height
        await self.slave.broadcast_xshard_tx_list(block, xshard_list, prev_root_height)
        await self.slave.send_minor_block_header_to_master(
            block.header,
            len(block.tx_list),
            len(xshard_list),
            coinbase_amount_map,
            self.state.get_shard_stats(),
        )

        # Commit the block
        self.state.commit_by_hash(block_hash)
        Logger.debug("committed mblock {}".format(block_hash.hex()))

        # Notify the rest
        self.add_block_futures[block_hash].set_result(None)
        del self.add_block_futures[block_hash]
        return True

    def check_minor_block_by_header(self, header):
        """ Raise exception of the block is invalid
        """
        block = self.state.get_block_by_hash(header.get_hash())
        if block is None:
            raise RuntimeError("block {} cannot be found".format(header.get_hash()))
        if header.height == 0:
            return
        self.state.add_block(block, force=True, write_db=False, skip_if_too_old=False)

    async def add_block_list_for_sync(self, block_list):
        """ Add blocks in batch to reduce RPCs. Will NOT broadcast to peers.

        Returns true if blocks are successfully added. False on any error.
        Additionally, returns list of coinbase_amount_map for each block
        This function only adds blocks to local and propagate xshard list to other shards.
        It does NOT notify master because the master should already have the minor header list,
        and will add them once this function returns successfully.
        """
        coinbase_amount_list = []
        if not block_list:
            return True, coinbase_amount_list

        existing_add_block_futures = []
        block_hash_to_x_shard_list = dict()
        uncommitted_block_header_list = []
        uncommitted_coinbase_amount_map_list = []
        for block in block_list:
            check(block.header.branch.get_full_shard_id() == self.full_shard_id)

            block_hash = block.header.get_hash()
            # adding the block header one assuming the block will be validated.
            coinbase_amount_list.append(block.header.coinbase_amount_map)

            commit_status, future = self.__get_block_commit_status_by_hash(block_hash)
            if commit_status == BLOCK_COMMITTED:
                # Skip processing the block if it is already committed
                Logger.warning(
                    "minor block to sync {} is already committed".format(
                        block_hash.hex()
                    )
                )
                continue
            elif commit_status == BLOCK_COMMITTING:
                # Check if the block is being propagating to other slaves and the master
                # Let's make sure all the shards and master got it before committing it
                Logger.info(
                    "[{}] {} is being added ... waiting for it to finish".format(
                        block.header.branch.to_str(), block.header.height
                    )
                )
                existing_add_block_futures.append(future)
                continue

            check(commit_status == BLOCK_UNCOMMITTED)
            # Validate and add the block
            try:
                xshard_list, coinbase_amount_map = self.state.add_block(
                    block, skip_if_too_old=False, force=True
                )
            except Exception as e:
                Logger.error_exception()
                return False, None

            prev_root_height = self.state.db.get_root_block_header_by_hash(
                block.header.hash_prev_root_block
            ).height
            block_hash_to_x_shard_list[block_hash] = (xshard_list, prev_root_height)
            self.add_block_futures[block_hash] = self.loop.create_future()
            uncommitted_block_header_list.append(block.header)
            uncommitted_coinbase_amount_map_list.append(
                block.header.coinbase_amount_map
            )

        await self.slave.batch_broadcast_xshard_tx_list(
            block_hash_to_x_shard_list, block_list[0].header.branch
        )
        check(
            len(uncommitted_coinbase_amount_map_list)
            == len(uncommitted_block_header_list)
        )
        await self.slave.send_minor_block_header_list_to_master(
            uncommitted_block_header_list, uncommitted_coinbase_amount_map_list
        )

        # Commit all blocks and notify all rest add block operations
        for block_header in uncommitted_block_header_list:
            block_hash = block_header.get_hash()
            self.state.commit_by_hash(block_hash)
            Logger.debug("committed mblock {}".format(block_hash.hex()))

            self.add_block_futures[block_hash].set_result(None)
            del self.add_block_futures[block_hash]

        # Wait for the other add block operations
        await asyncio.gather(*existing_add_block_futures)

        return True, coinbase_amount_list

    def add_tx_list(self, tx_list, source_peer=None):
        if not tx_list:
            return
        valid_tx_list = []
        for tx in tx_list:
            if self.add_tx(tx):
                valid_tx_list.append(tx)
        if not valid_tx_list:
            return
        self.broadcast_tx_list(valid_tx_list, source_peer)

    def add_tx(self, tx: TypedTransaction):
        return self.state.add_tx(tx)
