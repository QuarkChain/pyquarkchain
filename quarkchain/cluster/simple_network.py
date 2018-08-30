import asyncio
import ipaddress
import socket

from quarkchain.cluster.p2p_commands import CommandOp, OP_SERIALIZER_MAP
from quarkchain.cluster.p2p_commands import (
    HelloCommand,
    GetPeerListRequest,
    GetPeerListResponse,
    PeerInfo,
)
from quarkchain.cluster.p2p_commands import (
    NewMinorBlockHeaderListCommand,
    GetRootBlockHeaderListResponse,
    Direction,
)
from quarkchain.cluster.p2p_commands import (
    NewTransactionListCommand,
    GetRootBlockListResponse,
)
from quarkchain.cluster.protocol import P2PConnection, ROOT_SHARD_ID
from quarkchain.core import random_bytes
from quarkchain.protocol import ConnectionState
from quarkchain.utils import Logger


class Peer(P2PConnection):
    """Endpoint for communication with other clusters

    Note a Peer object exists in both parties of communication.
    """

    def __init__(
        self, env, reader, writer, network, master_server, cluster_peer_id, name=None
    ):
        if name is None:
            name = "{}_peer_{}".format(master_server.name, cluster_peer_id)
        super().__init__(
            env, reader, writer, OP_SERIALIZER_MAP, OP_NONRPC_MAP, OP_RPC_MAP
        )
        self.network = network
        self.master_server = master_server
        self.root_state = master_server.root_state

        # The following fields should be set once active
        self.id = None
        self.shard_mask_list = None
        self.best_root_block_header_observed = None
        self.cluster_peer_id = cluster_peer_id

    def send_hello(self):
        cmd = HelloCommand(
            version=self.env.quark_chain_config.P2P_PROTOCOL_VERSION,
            network_id=self.env.quark_chain_config.NETWORK_ID,
            peer_id=self.network.self_id,
            peer_ip=int(self.network.ip),
            peer_port=self.network.port,
            shard_mask_list=[],
            root_block_header=self.root_state.tip,
        )
        # Send hello request
        self.write_command(CommandOp.HELLO, cmd)

    async def start(self, is_server=False):
        """
        race condition may arise when two peers connecting each other at the same time
        to resolve: 1. acquire asyncio lock (what if the corotine holding the lock failed?)
        2. disconnect whenever duplicates are detected, right after await (what if both connections are disconnected?)
        3. only initiate connection from one side, eg. from smaller of ip_port; in SimpleNetwork, from new nodes only
        3 is the way to go
        """
        op, cmd, rpc_id = await self.read_command()
        if op is None:
            assert self.state == ConnectionState.CLOSED
            Logger.info("Failed to read command, peer may have closed connection")
            return "Failed to read command"

        if op != CommandOp.HELLO:
            return self.close_with_error("Hello must be the first command")

        if cmd.version != self.env.quark_chain_config.P2P_PROTOCOL_VERSION:
            return self.close_with_error("incompatible protocol version")

        if cmd.network_id != self.env.quark_chain_config.NETWORK_ID:
            return self.close_with_error("incompatible network id")

        self.id = cmd.peer_id
        self.shard_mask_list = cmd.shard_mask_list
        self.ip = ipaddress.ip_address(cmd.peer_ip)
        self.port = cmd.peer_port

        Logger.info(
            "Got HELLO from peer {} ({}:{})".format(self.id.hex(), self.ip, self.port)
        )

        # Validate best root and minor blocks from peer
        # TODO: validate hash and difficulty through a helper function
        if (
            cmd.root_block_header.shard_info.get_shard_size()
            != self.env.quark_chain_config.SHARD_SIZE
        ):
            return self.close_with_error(
                "Shard size from root block header does not match local"
            )

        self.best_root_block_header_observed = cmd.root_block_header

        if self.id == self.network.self_id:
            # connect to itself, stop it
            return self.close_with_error("Cannot connect to itself")

        if self.id in self.network.active_peer_pool:
            return self.close_with_error(
                "Peer {} already connected".format(self.id.hex())
            )

        # Send hello back
        if is_server:
            self.send_hello()

        await self.master_server.create_peer_cluster_connections(self.cluster_peer_id)
        Logger.info(
            "Established virtual shard connections with peer {}".format(self.id.hex())
        )

        asyncio.ensure_future(self.active_and_loop_forever())
        await self.wait_until_active()

        # Only make the peer connection avaialbe after exchanging HELLO and creating virtual shard connections
        self.network.active_peer_pool[self.id] = self
        self.network.cluster_peer_pool[self.cluster_peer_id] = self
        Logger.info("Peer {} added to active peer pool".format(self.id.hex()))

        self.master_server.handle_new_root_block_header(
            self.best_root_block_header_observed, self
        )
        return None

    def close(self):
        if self.state == ConnectionState.ACTIVE:
            assert self.id is not None
            if self.id in self.network.active_peer_pool:
                del self.network.active_peer_pool[self.id]
            if self.cluster_peer_id in self.network.cluster_peer_pool:
                del self.network.cluster_peer_pool[self.cluster_peer_id]
            Logger.info(
                "Peer {} disconnected, remaining {}".format(
                    self.id.hex(), len(self.network.active_peer_pool)
                )
            )
            self.master_server.destroy_peer_cluster_connections(self.cluster_peer_id)

        super().close()

    def close_dead_peer(self):
        assert self.id is not None
        if self.id in self.network.active_peer_pool:
            del self.network.active_peer_pool[self.id]
        if self.cluster_peer_id in self.network.cluster_peer_pool:
            del self.network.cluster_peer_pool[self.cluster_peer_id]
        Logger.info(
            "Peer {} ({}:{}) disconnected, remaining {}".format(
                self.id.hex(), self.ip, self.port, len(self.network.active_peer_pool)
            )
        )
        self.master_server.destroy_peer_cluster_connections(self.cluster_peer_id)
        super().close()

    def close_with_error(self, error):
        Logger.info(
            "Closing peer %s with the following reason: %s"
            % (self.id.hex() if self.id is not None else "unknown", error)
        )
        return super().close_with_error(error)

    async def handle_error(self, op, cmd, rpc_id):
        self.close_with_error("Unexpected op {}".format(op))

    async def handle_get_peer_list_request(self, request):
        resp = GetPeerListResponse()
        for peer_id, peer in self.network.active_peer_pool.items():
            if peer == self:
                continue
            resp.peer_info_list.append(PeerInfo(int(peer.ip), peer.port))
            if len(resp.peer_info_list) >= request.max_peers:
                break
        return resp

    # ------------------------ Operations for forwarding ---------------------
    def get_cluster_peer_id(self):
        """ Override P2PConnection.get_cluster_peer_id()
        """
        return self.cluster_peer_id

    def get_connection_to_forward(self, metadata):
        """ Override P2PConnection.get_connection_to_forward()
        """
        if metadata.branch.value == ROOT_SHARD_ID:
            return None

        return self.master_server.get_slave_connection(metadata.branch)

    # ----------------------- RPC handlers ---------------------------------

    async def handle_new_minor_block_header_list(self, op, cmd, rpc_id):
        if len(cmd.minor_block_header_list) != 0:
            return self.close_with_error("minor block header list must be empty")

        if cmd.root_block_header.height < self.best_root_block_header_observed.height:
            return self.close_with_error(
                "root block height is decreasing {} < {}".format(
                    cmd.root_block_header.height,
                    self.best_root_block_header_observed.height,
                )
            )
        if cmd.root_block_header.height == self.best_root_block_header_observed.height:
            if cmd.root_block_header != self.best_root_block_header_observed:
                return self.close_with_error(
                    "root block header changed with same height {}".format(
                        self.best_root_block_header_observed.height
                    )
                )

        self.best_root_block_header_observed = cmd.root_block_header
        self.master_server.handle_new_root_block_header(cmd.root_block_header, self)

    async def handle_new_transaction_list(self, op, cmd, rpc_id):
        for tx in cmd.transaction_list:
            Logger.debug(
                "Received tx {} from peer {}".format(tx.get_hash().hex(), self.id.hex())
            )
            await self.master_server.add_transaction(tx, self)

    async def handle_get_root_block_header_list_request(self, request):
        if request.limit <= 0:
            self.close_with_error("Bad limit")
        # TODO: support tip direction
        if request.direction != Direction.GENESIS:
            self.close_with_error("Bad direction")

        block_hash = request.block_hash
        header_list = []
        for i in range(request.limit):
            header = self.root_state.db.get_root_block_header_by_hash(
                block_hash, consistency_check=False
            )
            header_list.append(header)
            if header.height == 0:
                break
            block_hash = header.hash_prev_block
        return GetRootBlockHeaderListResponse(self.root_state.tip, header_list)

    async def handle_get_root_block_list_request(self, request):
        r_block_list = []
        for h in request.root_block_hash_list:
            r_block = self.root_state.db.get_root_block_by_hash(
                h, consistency_check=False
            )
            if r_block is None:
                continue
            r_block_list.append(r_block)
        return GetRootBlockListResponse(r_block_list)

    def send_updated_tip(self):
        if self.root_state.tip.height <= self.best_root_block_header_observed.height:
            return

        self.write_command(
            op=CommandOp.NEW_MINOR_BLOCK_HEADER_LIST,
            cmd=NewMinorBlockHeaderListCommand(self.root_state.tip, []),
        )

    def send_transaction(self, tx):
        self.write_command(
            op=CommandOp.NEW_TRANSACTION_LIST, cmd=NewTransactionListCommand([tx])
        )


# Only for non-RPC (fire-and-forget) and RPC request commands
OP_NONRPC_MAP = {
    CommandOp.HELLO: Peer.handle_error,
    CommandOp.NEW_MINOR_BLOCK_HEADER_LIST: Peer.handle_new_minor_block_header_list,
    CommandOp.NEW_TRANSACTION_LIST: Peer.handle_new_transaction_list,
}

# For RPC request commands
OP_RPC_MAP = {
    CommandOp.GET_PEER_LIST_REQUEST: (
        CommandOp.GET_PEER_LIST_RESPONSE,
        Peer.handle_get_peer_list_request,
    ),
    CommandOp.GET_ROOT_BLOCK_HEADER_LIST_REQUEST: (
        CommandOp.GET_ROOT_BLOCK_HEADER_LIST_RESPONSE,
        Peer.handle_get_root_block_header_list_request,
    ),
    CommandOp.GET_ROOT_BLOCK_LIST_REQUEST: (
        CommandOp.GET_ROOT_BLOCK_LIST_RESPONSE,
        Peer.handle_get_root_block_list_request,
    ),
}


class SimpleNetwork:
    """Fully connected P2P network for inter-cluster communication
    """

    def __init__(self, env, master_server):
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.active_peer_pool = dict()  # peer id => peer
        self.self_id = random_bytes(32)
        self.master_server = master_server
        master_server.network = self
        self.ip = ipaddress.ip_address(socket.gethostbyname(socket.gethostname()))
        self.port = self.env.cluster_config.P2P_PORT
        # Internal peer id in the cluster, mainly for connection management
        # 0 is reserved for master
        self.next_cluster_peer_id = 0
        self.cluster_peer_pool = dict()  # cluster peer id => peer

    async def new_peer(self, client_reader, client_writer):
        peer = Peer(
            self.env,
            client_reader,
            client_writer,
            self,
            self.master_server,
            self.__get_next_cluster_peer_id(),
        )
        await peer.start(is_server=True)

    async def connect(self, ip, port):
        Logger.info("connecting {} {}".format(ip, port))
        try:
            reader, writer = await asyncio.open_connection(ip, port, loop=self.loop)
        except Exception as e:
            Logger.info("failed to connect {} {}: {}".format(ip, port, e))
            return None
        peer = Peer(
            self.env,
            reader,
            writer,
            self,
            self.master_server,
            self.__get_next_cluster_peer_id(),
        )
        peer.send_hello()
        result = await peer.start(is_server=False)
        if result is not None:
            return None
        return peer

    async def connect_seed(self, ip, port):
        peer = await self.connect(ip, port)
        if peer is None:
            # Fail to connect
            return

        # Make sure the peer is ready for incoming messages
        await peer.wait_until_active()
        try:
            op, resp, rpc_id = await peer.write_rpc_request(
                CommandOp.GET_PEER_LIST_REQUEST, GetPeerListRequest(10)
            )
        except Exception as e:
            Logger.log_exception()
            return

        Logger.info("connecting {} peers ...".format(len(resp.peer_info_list)))
        for peer_info in resp.peer_info_list:
            asyncio.ensure_future(
                self.connect(str(ipaddress.ip_address(peer_info.ip)), peer_info.port)
            )

        # TODO: Sync with total diff

    def iterate_peers(self):
        return self.cluster_peer_pool.values()

    def shutdown_peers(self):
        active_peer_pool = self.active_peer_pool
        self.active_peer_pool = dict()
        for peer_id, peer in active_peer_pool.items():
            peer.close()

    def start_server(self):
        coro = asyncio.start_server(self.new_peer, "0.0.0.0", self.port, loop=self.loop)
        self.server = self.loop.run_until_complete(coro)
        Logger.info("Self id {}".format(self.self_id.hex()))
        Logger.info(
            "Listening on {} for p2p".format(self.server.sockets[0].getsockname())
        )

    def shutdown(self):
        self.shutdown_peers()
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())

    def start(self):
        self.start_server()

        self.loop.create_task(
            self.connect_seed(
                self.env.cluster_config.SIMPLE_NETWORK.BOOTSTRAP_HOST,
                self.env.cluster_config.SIMPLE_NETWORK.BOOTSTRAP_PORT,
            )
        )

    # ------------------------------- Cluster Peer Management --------------------------------
    def __get_next_cluster_peer_id(self):
        self.next_cluster_peer_id = self.next_cluster_peer_id + 1
        return self.next_cluster_peer_id

    def get_peer_by_cluster_peer_id(self, cluster_peer_id):
        return self.cluster_peer_pool.get(cluster_peer_id)
