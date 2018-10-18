import asyncio
from collections import deque

from quarkchain.cluster.p2p_commands import (
    CommandOp,
    OP_SERIALIZER_MAP,
    NewMinorBlockHeaderListCommand,
    GetMinorBlockListRequest,
    GetMinorBlockListResponse,
    GetMinorBlockHeaderListRequest,
    Direction,
    GetMinorBlockHeaderListResponse,
    NewTransactionListCommand,
    NewBlockMinorCommand,
)
from quarkchain.cluster.miner import Miner, validate_seal
from quarkchain.cluster.tx_generator import TransactionGenerator
from quarkchain.cluster.protocol import VirtualConnection, ClusterMetadata
from quarkchain.cluster.shard_state import ShardState
from quarkchain.config import ShardConfig
from quarkchain.core import RootBlock, MinorBlock, MinorBlockHeader, Branch, Transaction
from quarkchain.utils import Logger, check, time_ms
from quarkchain.db import InMemoryDb, PersistentDb


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
                self.shard_state.root_tip.height
                < self.best_root_block_header_observed.height
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
        if request.limit <= 0:
            self.close_with_error("Bad limit")
        # TODO: support tip direction
        if request.direction != Direction.GENESIS:
            self.close_with_error("Bad direction")

        block_hash = request.block_hash
        header_list = []
        for i in range(request.limit):
            header = self.shard_state.db.get_minor_block_header_by_hash(
                block_hash, consistency_check=False
            )
            header_list.append(header)
            if header.height == 0:
                break
            block_hash = header.hash_prev_minor_block

        return GetMinorBlockHeaderListResponse(
            self.shard_state.root_tip, self.shard_state.header_tip, header_list
        )

    async def handle_get_minor_block_list_request(self, request):
        m_block_list = []
        for m_block_hash in request.minor_block_hash_list:
            m_block = self.shard_state.db.get_minor_block_by_hash(
                m_block_hash, consistency_check=False
            )
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
            Logger.info(
                "[{}] received new header with height {}".format(
                    m_header.branch.get_shard_id(), m_header.height
                )
            )
            if m_header.branch != self.shard_state.branch:
                self.close_with_error("incorrect branch")
                return

        if self.best_root_block_header_observed:
            # check root header is not decreasing
            if (
                cmd.root_block_header.height
                < self.best_root_block_header_observed.height
            ):
                return self.close_with_error(
                    "best observed root header height is decreasing {} < {}".format(
                        cmd.root_block_header.height,
                        self.best_root_block_header_observed.height,
                    )
                )
            if (
                cmd.root_block_header.height
                == self.best_root_block_header_observed.height
            ):
                if cmd.root_block_header != self.best_root_block_header_observed:
                    return self.close_with_error(
                        "best observed root header changed with same height {}".format(
                            self.best_root_block_header_observed.height
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

        self.shard.synchronizer.add_task(m_header, self)

    async def handle_new_transaction_list_command(self, op_code, cmd, rpc_id):
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
}


class SyncTask:
    """ Given a header and a shard connection, the synchronizer will synchronize
    the shard state with the peer shard up to the height of the header.
    """

    def __init__(self, header: MinorBlockHeader, shard_conn: PeerShardConnection):
        self.header = header
        self.shard_conn = shard_conn
        self.shard_state = shard_conn.shard_state
        self.shard = shard_conn.shard

        shard_id = self.header.branch.get_shard_id()
        shard_config = self.shard_state.env.quark_chain_config.SHARD_LIST[shard_id]
        self.max_staleness = shard_config.max_stale_minor_block_height_diff

    async def sync(self):
        try:
            await self.__run_sync()
        except Exception as e:
            Logger.log_exception()
            self.shard_conn.close_with_error(str(e))

    async def __run_sync(self):
        if self.__has_block_hash(self.header.get_hash()):
            return

        # descending height
        block_header_chain = [self.header]

        # TODO: Stop if too many headers to revert
        while not self.__has_block_hash(block_header_chain[-1].hash_prev_minor_block):
            block_hash = block_header_chain[-1].hash_prev_minor_block
            height = block_header_chain[-1].height - 1

            if self.shard_state.header_tip.height - height > self.max_staleness:
                Logger.warning(
                    "[{}] abort syncing due to forking at very old block {} << {}".format(
                        self.header.branch.get_shard_id(),
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
                    self.shard_state.branch.get_shard_id(), height, block_hash.hex()
                )
            )
            block_header_list = await self.__download_block_headers(block_hash)
            Logger.info(
                "[{}] downloaded {} headers from peer".format(
                    self.shard_state.branch.get_shard_id(), len(block_header_list)
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
            block_chain = await self.__download_blocks(block_header_chain[:100])
            Logger.info(
                "[{}] downloaded {} blocks from peer".format(
                    self.shard_state.branch.get_shard_id(), len(block_chain)
                )
            )
            check(len(block_chain) == len(block_header_chain[:100]))

            for block in block_chain:
                # Stop if the block depends on an unknown root block
                # TODO: move this check to early stage to avoid downloading unnecessary headers
                if not self.shard_state.db.contain_root_block_by_hash(
                    block.header.hash_prev_root_block
                ):
                    return
                await self.shard.add_block(block)
                block_header_chain.pop(0)

    def __has_block_hash(self, block_hash):
        return self.shard_state.db.contain_minor_block_by_hash(block_hash)

    def __validate_block_headers(self, block_header_list):
        for i in range(len(block_header_list) - 1):
            header, prev = block_header_list[i : i + 2]
            if header.height != prev.height + 1:
                return False
            if header.hash_prev_minor_block != prev.get_hash():
                return False
            shard_id = header.branch.get_shard_id()
            consensus_type = self.shard.env.quark_chain_config.SHARD_LIST[
                shard_id
            ].CONSENSUS_TYPE
            validate_seal(header, consensus_type)
        return True

    async def __download_block_headers(self, block_hash):
        request = GetMinorBlockHeaderListRequest(
            block_hash=block_hash,
            branch=self.shard_state.branch,
            limit=100,
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


class Shard:
    def __init__(self, env, shard_id, slave):
        self.env = env
        self.shard_id = shard_id
        self.slave = slave

        self.state = ShardState(env, shard_id, self.__init_shard_db())

        self.loop = asyncio.get_event_loop()
        self.synchronizer = Synchronizer()

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
            path=self.env.cluster_config.DB_PATH_ROOT, shard_id=self.shard_id
        )
        return PersistentDb(db_path, clean=self.env.cluster_config.CLEAN)

    def __init_miner(self):
        miner_address = self.env.quark_chain_config.testnet_master_address.address_in_branch(
            Branch.create(self.__get_shard_size(), self.shard_id)
        )

        async def __create_block():
            # hold off mining if the shard is syncing
            while self.synchronizer.running or not self.state.initialized:
                await asyncio.sleep(0.1)

            return self.state.create_block_to_mine(address=miner_address)

        async def __add_block(block):
            # Do not add block if there is a sync in progress
            if self.synchronizer.running:
                return
            # Do not add stale block
            if self.state.header_tip.height >= block.header.height:
                return
            await self.handle_new_block(block)

        def __get_mining_param():
            return {
                "target_block_time": self.slave.artificial_tx_config.target_minor_block_time
            }

        shard_config = self.env.quark_chain_config.SHARD_LIST[
            self.shard_id
        ]  # type: ShardConfig
        self.miner = Miner(
            shard_config.CONSENSUS_TYPE,
            __create_block,
            __add_block,
            __get_mining_param,
            remote=shard_config.CONSENSUS_CONFIG.REMOTE_MINE,
        )

    def __get_shard_size(self):
        return self.env.quark_chain_config.SHARD_SIZE

    @property
    def genesis_root_height(self):
        return self.env.quark_chain_config.get_genesis_root_height(self.shard_id)

    def add_peer(self, peer: PeerShardConnection):
        self.peers[peer.cluster_peer_id] = peer

    async def __init_genesis_state(self, root_block: RootBlock):
        block = self.state.init_genesis_state(root_block)
        xshard_list = []
        await self.slave.broadcast_xshard_tx_list(
            block, xshard_list, root_block.header.height
        )
        await self.slave.send_minor_block_header_to_master(
            block.header,
            len(block.tx_list),
            len(xshard_list),
            self.state.get_shard_stats(),
        )

    async def init_from_root_block(self, root_block: RootBlock):
        """ Either recover state from local db or create genesis state based on config"""
        if root_block.header.height > self.genesis_root_height:
            return self.state.init_from_root_block(root_block)

        if root_block.header.height == self.genesis_root_height:
            await self.__init_genesis_state(root_block)

    async def add_root_block(self, root_block: RootBlock):
        check(root_block.header.height >= self.genesis_root_height)

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
        0. if local shard is syncing, doesn't make sense to add, skip
        1. if block parent is not in local state/new block pool, discard
        2. if already in cache or in local state/new block pool, pass
        3. validate: check time, difficulty, POW
        4. add it to new minor block broadcast cache
        5. broadcast to all peers (minus peer that sent it, optional)
        6. add_block() to local state (then remove from cache)
             also, broadcast tip if tip is updated (so that peers can sync if they missed blocks, or are new)
        """
        if self.synchronizer.running:
            # TODO optinal: queue the block if it came from broadcast to so that once sync is over, catch up immediately
            return

        if block.header.get_hash() in self.state.new_block_pool:
            return
        if self.state.db.contain_minor_block_by_hash(block.header.get_hash()):
            return

        if not self.state.db.contain_minor_block_by_hash(
            block.header.hash_prev_minor_block
        ):
            if block.header.hash_prev_minor_block not in self.state.new_block_pool:
                return

        shard_id = block.header.branch.get_shard_id()
        consensus_type = self.env.quark_chain_config.SHARD_LIST[shard_id].CONSENSUS_TYPE
        try:
            validate_seal(block.header, consensus_type)
        except Exception as e:
            Logger.warning("[{}] Got block with bad seal: {}".format(shard_id, str(e)))
            return

        if block.header.create_time > time_ms() // 1000 + 30:
            return

        self.state.new_block_pool[block.header.get_hash()] = block

        self.broadcast_new_block(block)
        await self.add_block(block)

    async def add_block(self, block):
        """ Returns true if block is successfully added. False on any error.
        called by 1. local miner (will not run if syncing) 2. SyncTask
        """
        old_tip = self.state.header_tip
        try:
            xshard_list = self.state.add_block(block)
        except Exception as e:
            Logger.error_exception()
            return False

        # only remove from pool if the block successfully added to state,
        #   this may cache failed blocks but prevents them being broadcasted more than needed
        # TODO add ttl to blocks in new_block_pool
        self.state.new_block_pool.pop(block.header.get_hash(), None)
        # block has been added to local state, broadcast tip so that peers can sync if needed
        try:
            if old_tip != self.state.header_tip:
                self.broadcast_new_tip()
        except Exception:
            Logger.warning_every_sec("broadcast tip failure", 1)

        # block already existed in local shard state
        # but might not have been propagated to other shards and master
        # let's make sure all the shards and master got it before return
        if xshard_list is None:
            future = self.add_block_futures.get(block.header.get_hash(), None)
            if future:
                Logger.info(
                    "[{}] {} is being added ... waiting for it to finish".format(
                        block.header.branch.get_shard_id(), block.header.height
                    )
                )
                await future
            return True

        self.add_block_futures[block.header.get_hash()] = self.loop.create_future()

        prev_root_height = self.state.db.get_root_block_by_hash(
            block.header.hash_prev_root_block
        ).header.height
        await self.slave.broadcast_xshard_tx_list(block, xshard_list, prev_root_height)
        await self.slave.send_minor_block_header_to_master(
            block.header,
            len(block.tx_list),
            len(xshard_list),
            self.state.get_shard_stats(),
        )

        self.add_block_futures[block.header.get_hash()].set_result(None)
        del self.add_block_futures[block.header.get_hash()]
        return True

    async def add_block_list_for_sync(self, block_list):
        """ Add blocks in batch to reduce RPCs. Will NOT broadcast to peers.

        Returns true if blocks are successfully added. False on any error.
        This function only adds blocks to local and propagate xshard list to other shards.
        It does NOT notify master because the master should already have the minor header list,
        and will add them once this function returns successfully.
        """
        if not block_list:
            return True

        existing_add_block_futures = []
        block_hash_to_x_shard_list = dict()
        for block in block_list:
            check(block.header.branch.get_shard_id() == self.shard_id)

            block_hash = block.header.get_hash()
            try:
                xshard_list = self.state.add_block(block)
            except Exception as e:
                Logger.error_exception()
                return False

            # block already existed in local shard state
            # but might not have been propagated to other shards and master
            # let's make sure all the shards and master got it before return
            if xshard_list is None:
                future = self.add_block_futures.get(block_hash, None)
                if future:
                    existing_add_block_futures.append(future)
            else:
                prev_root_height = self.state.db.get_root_block_by_hash(
                    block.header.hash_prev_root_block
                ).header.height
                block_hash_to_x_shard_list[block_hash] = (xshard_list, prev_root_height)
                self.add_block_futures[block_hash] = self.loop.create_future()

        await self.slave.batch_broadcast_xshard_tx_list(
            block_hash_to_x_shard_list, block_list[0].header.branch
        )

        for block_hash in block_hash_to_x_shard_list.keys():
            self.add_block_futures[block_hash].set_result(None)
            del self.add_block_futures[block_hash]

        await asyncio.gather(*existing_add_block_futures)

        return True

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

    def add_tx(self, tx: Transaction):
        return self.state.add_tx(tx)
