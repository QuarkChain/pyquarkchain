
import asyncio
from quarkchain.core import Serializable, uint32, hash256, RootBlockHeader, PreprendedSizeListSerializer
from quarkchain.core import random_bytes
from quarkchain.config import DEFAULT_ENV
from enum import Enum
import argparse

SEED_HOST = ("localhost", 38291)


class HelloCommand(Serializable):
    FIELDS = [
        ("version", uint32),
        ("peerId", hash256),
        ("shardMaskList", PreprendedSizeListSerializer(
            4, uint32)),  # TODO create shard mask object
        ("rootBlockHeader", RootBlockHeader)
    ]

    def __init__(self, version=0, peerId=bytes(32), shardMaskList=[], rootBlockHeader=RootBlockHeader()):
        self.version = version
        self.peerId = peerId
        self.shardMaskList = shardMaskList
        self.rootBlockHeader = rootBlockHeader


class PeerState(Enum):
    CONNECTING = 0   # conneting before hello
    DOWNLOADING = 1  # hello is down and the peer is downloading
    ACTIVE = 2       # the peer is active, be able to receive relay
    CLOSED = 3       # the peer connection is closed


class CommandOp():
    HELLO = 0


OP_SERIALIZER_LIST = {
    CommandOp.HELLO: HelloCommand
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

    async def readCommand(self):
        opBytes = await self.reader.read(1)
        if len(opBytes) == 0:
            self.close()
            return (None, None)

        op = opBytes[0]
        if op not in OP_SERIALIZER_LIST:
            return self.closeWithError("Unsupported op {}".format(op))

        ser = OP_SERIALIZER_LIST[op]
        sizeBytes = await self.reader.read(4)
        size = int.from_bytes(sizeBytes, byteorder="big")
        if size > self.env.config.P2P_COMMAND_SIZE_LIMIT:
            return self.closeWithError("command package exceed limit")

        cmdBytes = await self.reader.read(size)
        try:
            cmd = ser.deserialize(cmdBytes)
        except Exception as e:
            return self.closeWithError("%s" % e)

        return (op, cmd)

    def writeCommand(self, op, cmd):
        data = cmd.serialize()
        self.writer.write(bytes([op]))
        self.writer.write(len(data).to_bytes(4, byteorder="big"))
        self.writer.write(data)

    def sendHello(self):
        cmd = HelloCommand(version=self.env.config.P2P_PROTOCOL_VERSION,
                           peerId=self.network.selfId,
                           shardMaskList=[],
                           rootBlockHeader=RootBlockHeader())
        # Send hello request
        self.writeCommand(CommandOp.HELLO, cmd)

    async def handleError(self, op, cmd):
        self.closeWithError("Unexpected op {}".format(op))
        return "Unexpected op"

    async def start(self, isServer=False):
        op, cmd = await self.readCommand()
        if op is None:
            assert(self.state == PeerState.CLOSED)
            return

        if op != CommandOp.HELLO:
            return self.closeWithError("Hello must be the first command")

        if cmd.version != self.env.config.P2P_PROTOCOL_VERSION:
            return self.closeWithError("incompatible protocol version")

        self.id = cmd.peerId
        self.shardMaskList = cmd.shardMaskList
        # TODO handle root block header
        if self.id == self.network.selfId:
            # connect to itself, stop it
            self.close()
            return

        self.network.activePeerPool[self.id] = self
        print("Peer {} connected".format(self.id.hex()))

        # Send hello back
        if isServer:
            self.sendHello()

        while True:
            op, cmd = await self.readCommand()
            if op is None:
                break
            handler = OP_HANDLER_LIST[op]
            if await handler(self, op, cmd) is not None:
                assert(self.state == PeerState.CLOSED)
                break
        assert(self.state == PeerState.CLOSED)

    def close(self):
        if self.id is not None:
            print("Peer {} disconnected".format(self.id.hex()))
        self.writer.close()
        if self.state == PeerState.ACTIVE:
            assert(self.id is not None)
            del self.network.activePeerPool[self.id]
        self.state = PeerState.CLOSED

    def closeWithError(self, error):
        print("Closing peer %s with the following reason: %s" % (self.id.hex(), error))
        self.close()
        return None


OP_HANDLER_LIST = {
    CommandOp.HELLO: Peer.handleError
}


class SimpleNetwork:

    def __init__(self, env):
        self.loop = asyncio.get_event_loop()
        self.env = env
        self.activePeerPool = dict()    # peer id => peer
        self.selfId = random_bytes(32)
        pass

    async def newClient(self, client_reader, client_writer):
        peer = Peer(self.env, client_reader, client_writer, self)
        await peer.start(isServer=True)

    async def connect(self, ip, port):
        print("connecting {} {}".format(ip, port))
        reader, writer = await asyncio.open_connection(ip, port, loop=self.loop)
        peer = Peer(self.env, reader, writer, self)
        peer.sendHello()
        await peer.start(isServer=False)

    def start(self):
        coro = asyncio.start_server(
            self.newClient, "localhost", self.env.config.P2P_SERVER_PORT, loop=self.loop)
        self.server = self.loop.run_until_complete(coro)

        print("Self id {}".format(self.selfId.hex()))
        print("Listening on {}".format(self.server.sockets[0].getsockname()))

        self.loop.create_task(self.connect(SEED_HOST[0], SEED_HOST[1]))

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass

        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())
        self.loop.close()
        print("Server is shutdown")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server_port", default=DEFAULT_ENV.config.P2P_SERVER_PORT)
    args = parser.parse_args()

    env = DEFAULT_ENV.copy()
    env.config.P2P_SERVER_PORT = args.server_port
    return env


def main():
    env = parse_args()
    env.NETWORK_ID = 1  # testnet

    network = SimpleNetwork(DEFAULT_ENV)
    network.start()


if __name__ == '__main__':
    main()
