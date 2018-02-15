
import asyncio
import argparse
import ipaddress
import socket
from quarkchain.core import Transaction, MinorBlockHeader
from quarkchain.core import RootBlock
from quarkchain.core import Serializable, RootBlockHeader, PreprendedSizeListSerializer
from quarkchain.core import uint16, uint32, uint128, hash256
from quarkchain.core import random_bytes
from quarkchain.config import DEFAULT_ENV
from quarkchain.chain import QuarkChainState
from enum import Enum

SEED_HOST = ("localhost", 38291)


class HelloCommand(Serializable):
    FIELDS = [
        ("version", uint32),
        ("networkId", uint32),
        ("peerId", hash256),
        ("peerIp", uint128),
        ("peerPort", uint16),
        ("shardMaskList", PreprendedSizeListSerializer(
            4, uint32)),  # TODO create shard mask object
        ("rootBlockHeader", RootBlockHeader)
    ]

    def __init__(self,
                 version=0,
                 networkId=0,
                 peerId=bytes(32),
                 peerIp=int(ipaddress.ip_address("127.0.0.1")),
                 peerPort=38291,
                 shardMaskList=[],
                 rootBlockHeader=RootBlockHeader()):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)


class NewMinorBlockHeaderListCommand(Serializable):
    FIELDS = [
        ("rootBlockHeader", RootBlockHeader),
        ("minorBlockHeaderList", PreprendedSizeListSerializer(4, MinorBlockHeader)),
    ]

    def __init__(self, rootBlockHeader, minorBlockHeaderList):
        self.rootBlockHeader = rootBlockHeader
        self.minorBlockHeaderList = minorBlockHeaderList


class NewTransactionListCommand(Serializable):
    FIELDS = [
        ("transactionList", PreprendedSizeListSerializer(4, Transaction))
    ]

    def __init__(self, transactionList=[]):
        self.transactionList = transactionList


class GetRootBlockListRequest(Serializable):
    FIELDS = [
        ("rootBlockHashList", PreprendedSizeListSerializer(4, hash256))
    ]

    def __init__(self, rootBlockHashList=[]):
        self.rootBlockHashList = rootBlockHashList


class GetRootBlockListResponse(Serializable):
    FIELDS = [
        ("rootBlockList", PreprendedSizeListSerializer(4, RootBlock))
    ]

    def __init__(self, rootBlockList=[]):
        self.rootBlockList = rootBlockList


class PeerState(Enum):
    CONNECTING = 0   # conneting before hello
    DOWNLOADING = 1  # hello is down and the peer is downloading
    ACTIVE = 2       # the peer is active, be able to receive relay
    CLOSED = 3       # the peer connection is closed


class CommandOp():
    HELLO = 0
    NEW_MINOR_BLOCK_HEADER_LIST = 1
    NEW_TRANSACTION_LIST = 2
    GET_ROOT_BLOCK_LIST_REQUEST = 3
    GET_ROOT_BLOCK_LIST_RESPONSE = 4


OP_SERIALIZER_LIST = {
    CommandOp.HELLO: HelloCommand,
    CommandOp.NEW_MINOR_BLOCK_HEADER_LIST: NewMinorBlockHeaderListCommand,
    CommandOp.NEW_TRANSACTION_LIST: NewTransactionListCommand,
    CommandOp.GET_ROOT_BLOCK_LIST_REQUEST: GetRootBlockListRequest,
    CommandOp.GET_ROOT_BLOCK_LIST_RESPONSE: GetRootBlockListResponse,
}


class Peer:

    def __init__(self, env, reader, writer, network):
        self.env = env
        self.reader = reader
        self.writer = writer
        self.network = network
        self.state = PeerState.CONNECTING
        self.id = None
        self.shardMaskList = []
        self.rpcId = 0  # 0 is broadcast
        self.rpcFutureMap = dict()
        # Most recently received rpc id
        self.peerRpcId = -1

    async def readCommand(self):
        opBytes = await self.reader.read(1)
        if len(opBytes) == 0:
            self.close()
            return (None, None, None)

        op = opBytes[0]
        if op not in OP_SERIALIZER_LIST:
            self.closeWithError("Unsupported op {}".format(op))
            return (None, None, None)

        ser = OP_SERIALIZER_LIST[op]
        sizeBytes = await self.reader.read(4)
        size = int.from_bytes(sizeBytes, byteorder="big")
        if size > self.env.config.P2P_COMMAND_SIZE_LIMIT:
            self.closeWithError("command package exceed limit")
            return (None, None, None)

        rpcIdBytes = await self.reader.read(8)
        rpcId = int.from_bytes(rpcIdBytes, byteorder="big")

        cmdBytes = await self.reader.read(size)
        try:
            cmd = ser.deserialize(cmdBytes)
        except Exception as e:
            self.closeWithError("%s" % e)
            return (None, None, None)

        if rpcId <= self.peerRpcId:
            self.closeWithError("incorrect rpc id sequence")
            return (None, None, None)

        self.peerRpcId = max(rpcId, self.peerRpcId)
        return (op, cmd, rpcId)

    def writeCommand(self, op, cmd, rpcId=0):
        data = cmd.serialize()
        ba = bytearray()
        ba.append(op)
        ba.extend(len(data).to_bytes(4, byteorder="big"))
        ba.extend(rpcId.to_bytes(8, byteorder="big"))
        ba.extend(data)
        self.writer.write(ba)

    def writeRpcRequest(self, op, cmd):
        rpcFuture = asyncio.Future()

        if self.state != PeerState.ACTIVE:
            rpcFuture.set_exception(RuntimeError("Peer connection is not active"))
            return rpcFuture

        self.rpcId += 1
        rpcId = self.rpcId
        self.rpcFutureMap[rpcId] = rpcFuture
        self.writeCommand(op, cmd, rpcId)
        return rpcFuture

    def writeRpcResponse(self, op, cmd, rpcId):
        self.writeCommand(op, cmd, rpcId)

    def sendHello(self):
        cmd = HelloCommand(version=self.env.config.P2P_PROTOCOL_VERSION,
                           networkId=self.env.config.NETWORK_ID,
                           peerId=self.network.selfId,
                           peerIp=int(self.network.ip),
                           peerPort=self.network.port,
                           shardMaskList=[],
                           rootBlockHeader=RootBlockHeader())
        # Send hello request
        self.writeCommand(CommandOp.HELLO, cmd)

    async def start(self, isServer=False):
        op, cmd, rpcId = await self.readCommand()
        if op is None:
            assert(self.state == PeerState.CLOSED)
            return

        if op != CommandOp.HELLO:
            return self.closeWithError("Hello must be the first command")

        if cmd.version != self.env.config.P2P_PROTOCOL_VERSION:
            return self.closeWithError("incompatible protocol version")

        if cmd.networkId != self.env.config.NETWORK_ID:
            return self.closeWithError("incompatible network id")

        self.id = cmd.peerId
        self.shardMaskList = cmd.shardMaskList
        # TODO handle root block header
        if self.id == self.network.selfId:
            # connect to itself, stop it
            self.close()
            return None

        self.network.activePeerPool[self.id] = self
        print("Peer {} connected".format(self.id.hex()))

        # Send hello back
        if isServer:
            self.sendHello()

        self.state = PeerState.ACTIVE
        asyncio.ensure_future(self.loopForever())

    async def loopForever(self):
        while self.state == PeerState.ACTIVE:
            try:
                op, cmd, rpcId = await self.readCommand()
            except Exception as e:
                self.closeWithError("Error when reading {}".format(e))
                break
            if op is None:
                break
            if op in OP_HANDLER_LIST:
                handler = OP_HANDLER_LIST[op]
                asyncio.ensure_future(handler(self, op, cmd, rpcId))
            else:
                if rpcId not in self.rpcFutureMap:
                    self.closeWithError("Unexpected rpc response %d" % rpcId)
                    break
                future = self.rpcFutureMap[rpcId]
                del self.rpcFutureMap[rpcId]
                future.set_result((op, cmd, rpcId))
        assert(self.state == PeerState.CLOSED)

        # Abort all in-flight RPCs
        for rpcId, future in self.rpcFutureMap.items():
            future.set_exception(RuntimeError("Connection abort"))
        self.rpcFutureMap.clear()

    def close(self):
        if self.id is not None:
            print("Peer {} disconnected".format(self.id.hex()))
        self.writer.close()
        if self.state == PeerState.ACTIVE:
            assert(self.id is not None)
            if self.id in self.network.activePeerPool:
                del self.network.activePeerPool[self.id]
        self.state = PeerState.CLOSED

    def closeWithError(self, error):
        print("Closing peer %s with the following reason: %s" %
              (self.id.hex(), error))
        self.close()
        return None

    async def handleError(self, op, cmd, rpcId):
        self.closeWithError("Unexpected op {}".format(op))

    async def handleGetRootBlockListRequest(self, op, cmd, rpcId):
        cmd = GetRootBlockListResponse()
        self.writeRpcResponse(CommandOp.GET_ROOT_BLOCK_LIST_RESPONSE, cmd, rpcId)


# Only for non-RPC (fire-and-forget) and RPC request commands
OP_HANDLER_LIST = {
    CommandOp.HELLO: Peer.handleError,
    CommandOp.GET_ROOT_BLOCK_LIST_REQUEST: Peer.handleGetRootBlockListRequest
}


class SimpleNetwork:

    def __init__(self, env, qcState):
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.activePeerPool = dict()    # peer id => peer
        self.selfId = random_bytes(32)
        self.qcState = qcState
        self.ip = ipaddress.ip_address(socket.gethostbyname(socket.gethostname()))
        self.port = self.env.config.P2P_SERVER_PORT

    async def newClient(self, client_reader, client_writer):
        peer = Peer(self.env, client_reader, client_writer, self)
        await peer.start(isServer=True)

    async def connect(self, ip, port):
        print("connecting {} {}".format(ip, port))
        reader, writer = await asyncio.open_connection(ip, port, loop=self.loop)
        peer = Peer(self.env, reader, writer, self)
        peer.sendHello()
        await peer.start(isServer=False)

    def shutdownPeers(self):
        activePeerPool = self.activePeerPool
        self.activePeerPool = dict()
        for peerId, peer in activePeerPool.items():
            peer.close()

    def start(self):
        coro = asyncio.start_server(
            self.newClient, "localhost", self.port, loop=self.loop)
        self.server = self.loop.run_until_complete(coro)

        print("Self id {}".format(self.selfId.hex()))
        print("Litsening on {}".format(self.server.sockets[0].getsockname()))

        self.loop.create_task(self.connect(SEED_HOST[0], SEED_HOST[1]))

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass

        self.shutdownPeers()
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())
        self.loop.close()
        print("Server is shutdown")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server_port", default=DEFAULT_ENV.config.P2P_SERVER_PORT, type=int)
    args = parser.parse_args()

    env = DEFAULT_ENV.copy()
    env.config.P2P_SERVER_PORT = args.server_port
    return env


def main():
    env = parse_args()
    env.NETWORK_ID = 1  # testnet

    qcState = QuarkChainState(env)
    network = SimpleNetwork(env, qcState)
    network.start()


if __name__ == '__main__':
    main()
