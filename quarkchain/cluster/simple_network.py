import asyncio
import ipaddress
import socket

from quarkchain.core import random_bytes
from quarkchain.protocol import ConnectionState
from quarkchain.cluster.protocol import (
    P2PConnection, ROOT_SHARD_ID,
)
from quarkchain.cluster.p2p_commands import CommandOp, OP_SERIALIZER_MAP
from quarkchain.cluster.p2p_commands import HelloCommand, GetPeerListRequest, GetPeerListResponse, PeerInfo
from quarkchain.cluster.p2p_commands import NewTransactionListCommand, GetRootBlockListResponse
from quarkchain.cluster.p2p_commands import NewMinorBlockHeaderListCommand, GetRootBlockHeaderListResponse, Direction
from quarkchain.utils import Logger


class Peer(P2PConnection):
    """Endpoint for communication with other clusters

    Note a Peer object exists in both parties of communication.
    """

    def __init__(self, env, reader, writer, network, masterServer, clusterPeerId, name=None):
        if name is None:
            name = "{}_peer_{}".format(masterServer.name, clusterPeerId)
        super().__init__(env, reader, writer, OP_SERIALIZER_MAP, OP_NONRPC_MAP, OP_RPC_MAP, name=name)
        self.network = network
        self.masterServer = masterServer
        self.rootState = masterServer.rootState

        # The following fields should be set once active
        self.id = None
        self.shardMaskList = None
        self.bestRootBlockHeaderObserved = None
        self.clusterPeerId = clusterPeerId

    def sendHello(self):
        cmd = HelloCommand(version=self.env.config.P2P_PROTOCOL_VERSION,
                           networkId=self.env.config.NETWORK_ID,
                           peerId=self.network.selfId,
                           peerIp=int(self.network.ip),
                           peerPort=self.network.port,
                           shardMaskList=[],
                           rootBlockHeader=self.rootState.tip)
        # Send hello request
        self.writeCommand(CommandOp.HELLO, cmd)

    async def start(self, isServer=False):
        '''
        race condition may arise when two peers connecting each other at the same time
        to resolve: 1. acquire asyncio lock (what if the corotine holding the lock failed?)
        2. disconnect whenever duplicates are detected, right after await (what if both connections are disconnected?)
        3. only initiate connection from one side, eg. from smaller of ip_port; in SimpleNetwork, from new nodes only
        3 is the way to go
        '''
        op, cmd, rpcId = await self.readCommand()
        if op is None:
            assert(self.state == ConnectionState.CLOSED)
            Logger.info("Failed to read command, peer may have closed connection")
            return "Failed to read command"

        if op != CommandOp.HELLO:
            return self.closeWithError("Hello must be the first command")

        if cmd.version != self.env.config.P2P_PROTOCOL_VERSION:
            return self.closeWithError("incompatible protocol version")

        if cmd.networkId != self.env.config.NETWORK_ID:
            return self.closeWithError("incompatible network id")

        self.id = cmd.peerId
        self.shardMaskList = cmd.shardMaskList
        self.ip = ipaddress.ip_address(cmd.peerIp)
        self.port = cmd.peerPort

        Logger.info("Got HELLO from peer {} ({}:{})".format(self.id.hex(), self.ip, self.port))

        # Validate best root and minor blocks from peer
        # TODO: validate hash and difficulty through a helper function
        if cmd.rootBlockHeader.shardInfo.getShardSize() != self.env.config.SHARD_SIZE:
            return self.closeWithError(
                "Shard size from root block header does not match local")

        self.bestRootBlockHeaderObserved = cmd.rootBlockHeader

        if self.id == self.network.selfId:
            # connect to itself, stop it
            return self.closeWithError("Cannot connect to itself")

        if self.id in self.network.activePeerPool:
            return self.closeWithError("Peer {} already connected".format(self.id.hex()))

        # Send hello back
        if isServer:
            self.sendHello()

        await self.masterServer.createPeerClusterConnections(self.clusterPeerId)
        Logger.info("Established virtual shard connections with peer {}".format(self.id.hex()))

        asyncio.ensure_future(self.activeAndLoopForever())
        await self.waitUntilActive()

        # Only make the peer connection avaialbe after exchanging HELLO and creating virtual shard connections
        self.network.activePeerPool[self.id] = self
        self.network.clusterPeerPool[self.clusterPeerId] = self
        Logger.info("Peer {} added to active peer pool".format(self.id.hex()))

        self.masterServer.handleNewRootBlockHeader(self.bestRootBlockHeaderObserved, self)
        return None

    def close(self):
        if self.state == ConnectionState.ACTIVE:
            assert(self.id is not None)
            if self.id in self.network.activePeerPool:
                del self.network.activePeerPool[self.id]
            if self.clusterPeerId in self.network.clusterPeerPool:
                del self.network.clusterPeerPool[self.clusterPeerId]
            Logger.info("Peer {} disconnected, remaining {}".format(
                self.id.hex(), len(self.network.activePeerPool)))
            self.masterServer.destroyPeerClusterConnections(self.clusterPeerId)

        super().close()

    def closeDeadPeer(self):
        assert(self.id is not None)
        if self.id in self.network.activePeerPool:
            del self.network.activePeerPool[self.id]
        if self.clusterPeerId in self.network.clusterPeerPool:
            del self.network.clusterPeerPool[self.clusterPeerId]
        Logger.info("Peer {} ({}:{}) disconnected, remaining {}".format(
            self.id.hex(), self.ip, self.port, len(self.network.activePeerPool)))
        self.masterServer.destroyPeerClusterConnections(self.clusterPeerId)
        super().close()

    def closeWithError(self, error):
        Logger.info(
            "Closing peer %s with the following reason: %s" %
            (self.id.hex() if self.id is not None else "unknown", error))
        return super().closeWithError(error)

    async def handleError(self, op, cmd, rpcId):
        self.closeWithError("Unexpected op {}".format(op))

    async def handleGetPeerListRequest(self, request):
        resp = GetPeerListResponse()
        for peerId, peer in self.network.activePeerPool.items():
            if peer == self:
                continue
            resp.peerInfoList.append(PeerInfo(int(peer.ip), peer.port))
            if len(resp.peerInfoList) >= request.maxPeers:
                break
        return resp

    # ------------------------ Operations for forwarding ---------------------
    def getClusterPeerId(self):
        ''' Override P2PConnection.getClusterPeerId()
        '''
        return self.clusterPeerId

    def getConnectionToForward(self, metadata):
        ''' Override P2PConnection.getConnectionToForward()
        '''
        if metadata.branch.value == ROOT_SHARD_ID:
            return None

        return self.masterServer.getSlaveConnection(metadata.branch)

    # ----------------------- RPC handlers ---------------------------------

    async def handleNewMinorBlockHeaderList(self, op, cmd, rpcId):
        if len(cmd.minorBlockHeaderList) != 0:
            return self.closeWithError("minor block header list must be empty")

        if cmd.rootBlockHeader.height < self.bestRootBlockHeaderObserved.height:
            return self.closeWithError("root block height is decreasing {} < {}".format(
                cmd.rootBlockHeader.height, self.bestRootBlockHeaderObserved.height))
        if cmd.rootBlockHeader.height == self.bestRootBlockHeaderObserved.height:
            if cmd.rootBlockHeader != self.bestRootBlockHeaderObserved:
                return self.closeWithError("root block header changed with same height {}".format(
                    self.bestRootBlockHeaderObserved.height))

        self.bestRootBlockHeaderObserved = cmd.rootBlockHeader
        self.masterServer.handleNewRootBlockHeader(cmd.rootBlockHeader, self)

    async def handleNewTransactionList(self, op, cmd, rpcId):
        for tx in cmd.transactionList:
            Logger.debug("Received tx {} from peer {}".format(tx.getHash().hex(), self.id.hex()))
            await self.masterServer.addTransaction(tx, self)

    async def handleGetRootBlockHeaderListRequest(self, request):
        if request.limit <= 0:
            self.closeWithError("Bad limit")
        # TODO: support tip direction
        if request.direction != Direction.GENESIS:
            self.closeWithError("Bad direction")

        blockHash = request.blockHash
        headerList = []
        for i in range(request.limit):
            header = self.rootState.db.getRootBlockHeaderByHash(blockHash, consistencyCheck=False)
            headerList.append(header)
            if header.height == 0:
                break
            blockHash = header.hashPrevBlock
        return GetRootBlockHeaderListResponse(self.rootState.tip, headerList)

    async def handleGetRootBlockListRequest(self, request):
        rBlockList = []
        for h in request.rootBlockHashList:
            rBlock = self.rootState.db.getRootBlockByHash(h, consistencyCheck=False)
            if rBlock is None:
                continue
            rBlockList.append(rBlock)
        return GetRootBlockListResponse(rBlockList)

    def sendUpdatedTip(self):
        if self.rootState.tip.height <= self.bestRootBlockHeaderObserved.height:
            return

        self.writeCommand(
            op=CommandOp.NEW_MINOR_BLOCK_HEADER_LIST,
            cmd=NewMinorBlockHeaderListCommand(self.rootState.tip, []))

    def sendTransaction(self, tx):
        self.writeCommand(
            op=CommandOp.NEW_TRANSACTION_LIST,
            cmd=NewTransactionListCommand([tx]))


# Only for non-RPC (fire-and-forget) and RPC request commands
OP_NONRPC_MAP = {
    CommandOp.HELLO: Peer.handleError,
    CommandOp.NEW_MINOR_BLOCK_HEADER_LIST: Peer.handleNewMinorBlockHeaderList,
    CommandOp.NEW_TRANSACTION_LIST: Peer.handleNewTransactionList,
}

# For RPC request commands
OP_RPC_MAP = {
    CommandOp.GET_PEER_LIST_REQUEST:
        (CommandOp.GET_PEER_LIST_RESPONSE, Peer.handleGetPeerListRequest),
    CommandOp.GET_ROOT_BLOCK_HEADER_LIST_REQUEST:
        (CommandOp.GET_ROOT_BLOCK_HEADER_LIST_RESPONSE, Peer.handleGetRootBlockHeaderListRequest),
    CommandOp.GET_ROOT_BLOCK_LIST_REQUEST:
        (CommandOp.GET_ROOT_BLOCK_LIST_RESPONSE, Peer.handleGetRootBlockListRequest),
}


class SimpleNetwork:
    """Fully connected P2P network for inter-cluster communication
    """

    def __init__(self, env, masterServer):
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.activePeerPool = dict()    # peer id => peer
        self.selfId = random_bytes(32)
        self.masterServer = masterServer
        masterServer.network = self
        self.ip = ipaddress.ip_address(
            socket.gethostbyname(socket.gethostname()))
        self.port = self.env.config.P2P_SERVER_PORT
        self.localPort = self.env.config.LOCAL_SERVER_PORT
        # Internal peer id in the cluster, mainly for connection management
        # 0 is reserved for master
        self.nextClusterPeerId = 0
        self.clusterPeerPool = dict()   # cluster peer id => peer

    async def newPeer(self, client_reader, client_writer):
        peer = Peer(
            self.env,
            client_reader,
            client_writer,
            self,
            self.masterServer,
            self.__getNextClusterPeerId())
        await peer.start(isServer=True)

    async def connect(self, ip, port):
        Logger.info("connecting {} {}".format(ip, port))
        try:
            reader, writer = await asyncio.open_connection(ip, port, loop=self.loop)
        except Exception as e:
            Logger.info("failed to connect {} {}: {}".format(ip, port, e))
            return None
        peer = Peer(self.env, reader, writer, self, self.masterServer, self.__getNextClusterPeerId())
        peer.sendHello()
        result = await peer.start(isServer=False)
        if result is not None:
            return None
        return peer

    async def connectSeed(self, ip, port):
        peer = await self.connect(ip, port)
        if peer is None:
            # Fail to connect
            return

        # Make sure the peer is ready for incoming messages
        await peer.waitUntilActive()
        try:
            op, resp, rpcId = await peer.writeRpcRequest(
                CommandOp.GET_PEER_LIST_REQUEST, GetPeerListRequest(10))
        except Exception as e:
            Logger.logException()
            return

        Logger.info("connecting {} peers ...".format(len(resp.peerInfoList)))
        for peerInfo in resp.peerInfoList:
            asyncio.ensure_future(self.connect(
                str(ipaddress.ip_address(peerInfo.ip)), peerInfo.port))

        # TODO: Sync with total diff

    def iteratePeers(self):
        return self.clusterPeerPool.values()

    def shutdownPeers(self):
        activePeerPool = self.activePeerPool
        self.activePeerPool = dict()
        for peerId, peer in activePeerPool.items():
            peer.close()

    def startServer(self):
        coro = asyncio.start_server(
            self.newPeer, "0.0.0.0", self.port, loop=self.loop)
        self.server = self.loop.run_until_complete(coro)
        Logger.info("Self id {}".format(self.selfId.hex()))
        Logger.info("Listening on {} for p2p".format(
            self.server.sockets[0].getsockname()))

    def shutdown(self):
        self.shutdownPeers()
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())

    def start(self):
        self.startServer()

        if self.env.config.LOCAL_SERVER_ENABLE:
            coro = asyncio.start_server(
                self.newLocalClient, "0.0.0.0", self.localPort, loop=self.loop)
            self.local_server = self.loop.run_until_complete(coro)
            Logger.info("Listening on {} for local".format(
                self.local_server.sockets[0].getsockname()))

        self.loop.create_task(
            self.connectSeed(self.env.config.P2P_SEED_HOST, self.env.config.P2P_SEED_PORT))

    # ------------------------------- Cluster Peer Management --------------------------------
    def __getNextClusterPeerId(self):
        self.nextClusterPeerId = self.nextClusterPeerId + 1
        return self.nextClusterPeerId

    def getPeerByClusterPeerId(self, clusterPeerId):
        return self.clusterPeerPool.get(clusterPeerId)
