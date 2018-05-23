import argparse
import asyncio
import ipaddress
import socket

from quarkchain.core import random_bytes
from quarkchain.config import DEFAULT_ENV
from quarkchain.protocol import ConnectionState
from quarkchain.cluster.protocol import (
    P2PConnection, ROOT_SHARD_ID,
)
from quarkchain.db import PersistentDb
from quarkchain.cluster.p2p_commands import CommandOp, OP_SERIALIZER_MAP
from quarkchain.cluster.p2p_commands import HelloCommand, GetPeerListRequest, GetPeerListResponse, PeerInfo
from quarkchain.cluster.p2p_commands import GetRootBlockListResponse
from quarkchain.cluster.p2p_commands import NewMinorBlockHeaderListCommand, GetRootBlockHeaderListResponse, Direction
from quarkchain.utils import set_logging_level, Logger


class Peer(P2PConnection):

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
        op, cmd, rpcId = await self.readCommand()
        if op is None:
            assert(self.state == ConnectionState.CLOSED)
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

    async def handleGetRootBlockHeaderListRequest(self, request):
        if request.limit <= 0:
            self.closeWithError("Bad limit")
        # TODO: support tip direction
        if request.direction != Direction.GENESIS:
            self.closeWithError("Bad direction")

        blockHash = request.blockHash
        headerList = []
        for i in range(request.limit):
            header = self.rootState.db.getRootBlockHeaderByHash(blockHash)
            headerList.append(header)
            if header.height == 0:
                break
            blockHash = header.hashPrevBlock
        return GetRootBlockHeaderListResponse(self.rootState.tip, headerList)

    async def handleGetRootBlockListRequest(self, request):
        rBlockList = []
        for h in request.rootBlockHashList:
            rBlock = self.rootState.getRootBlockByHash(h)
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


# Only for non-RPC (fire-and-forget) and RPC request commands
OP_NONRPC_MAP = {
    CommandOp.HELLO: Peer.handleError,
    CommandOp.NEW_MINOR_BLOCK_HEADER_LIST: Peer.handleNewMinorBlockHeaderList,
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

    def __broadcastCommand(self, op, cmd, sourcePeerId=None):
        data = cmd.serialize()
        for peerId, peer in self.activePeerPool.items():
            if peerId == sourcePeerId:
                continue
            peer.writeRawCommand(op, data)

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

    def startAndLoop(self):
        self.start()

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass

        self.shutdown()
        self.loop.close()
        Logger.info("Server is shutdown")

    # ------------------------------- Cluster Peer Management --------------------------------
    def __getNextClusterPeerId(self):
        self.nextClusterPeerId = self.nextClusterPeerId + 1
        return self.nextClusterPeerId

    def getPeerByClusterPeerId(self, clusterPeerId):
        return self.clusterPeerPool.get(clusterPeerId)


def parse_args():
    parser = argparse.ArgumentParser()
    # P2P port
    parser.add_argument(
        "--server_port", default=DEFAULT_ENV.config.P2P_SERVER_PORT, type=int)
    # Local port for JSON-RPC, wallet, etc
    parser.add_argument(
        "--enable_local_server", default=False, type=bool)
    # Seed host which provides the list of available peers
    parser.add_argument(
        "--seed_host", default=DEFAULT_ENV.config.P2P_SEED_HOST, type=str)
    parser.add_argument(
        "--seed_port", default=DEFAULT_ENV.config.P2P_SEED_PORT, type=int)
    parser.add_argument("--in_memory_db", default=False)
    parser.add_argument("--db_path", default="./db", type=str)
    parser.add_argument("--log_level", default="info", type=str)
    args = parser.parse_args()

    set_logging_level(args.log_level)

    env = DEFAULT_ENV.copy()
    env.config.P2P_SERVER_PORT = args.server_port
    env.config.P2P_SEED_HOST = args.seed_host
    env.config.P2P_SEED_PORT = args.seed_port
    if not args.in_memory_db:
        env.db = PersistentDb(path=args.db_path, clean=True)

    return env
