import asyncio
from quarkchain.commands import *
from quarkchain.p2pinterface_pb2 import QuarkMessage
from quarkchain.utils import Logger

'''
P2PPeer build on P2PNetwork, it only needs to know peer id which normally is
peer's truncated public key. Sending message to peer is by calling P2PNetwork with
peer id, network will do routing. Receiving message is via a asyncio queue.
P2PNetwork will deliver received message to the queue.
'''


class P2PPeer:
    def __init__(self, env, network, peerId, p2pNetworkSender, loop):
        self.loop = loop
        self.env = env
        self.network = network
        self.peerId = peerId
        self.p2pNetworkSender = p2pNetworkSender
        self.msgQueue = asyncio.Queue(loop=loop)
        self.helloSent = False

    # Called by network.
    async def networkCallback(self, msg):
        Logger.debug('P2PPeer put in msgqueue')
        await self.msgQueue.put(msg)

    def __serialize(self, op, cmd, rpcId=0) -> bytearray:
        data = cmd.serialize()
        ba = bytearray()
        ba.append(op)
        ba.extend(len(data).to_bytes(4, byteorder="big"))
        ba.extend(rpcId.to_bytes(8, byteorder="big"))
        ba.extend(data)
        return ba

    def __deserialize(self, payload: bytes):
        op = payload[0]
        if op not in OP_SERIALIZER_MAP:
            Logger.error("Unsupported op {}".format(op))
            return (None, None, None)

        ser = OP_SERIALIZER_MAP[op]
        sizeBytes = payload[1:5]
        size = int.from_bytes(sizeBytes, byteorder="big")
        if size > self.env.config.P2P_COMMAND_SIZE_LIMIT:
            Logger.error("command package exceed limit")
            return (None, None, None)

        rpcIdBytes = payload[5:5+8]
        rpcId = int.from_bytes(rpcIdBytes, byteorder="big")

        cmdBytes = payload[13:13+size]
        try:
            cmd = ser.deserialize(cmdBytes)
        except Exception as e:
            Logger.error("exception:{}".format(e))
            return (None, None, None)
        return (op, cmd, rpcId)

    # Send P2P message using p2pNetworkSender
    def __p2pSender(self, msg: bytes, isBcast: bool):
        quarkMessage = QuarkMessage()
        quarkMessage.is_broadcast = isBcast
        quarkMessage.peer_id = self.peerId
        quarkMessage.type = QuarkMessage.OTHER
        quarkMessage.payload = msg
        self.p2pNetworkSender(quarkMessage.SerializeToString())

    # Send to Peer op message.
    def p2pSend(self, op, cmd, isBcast: bool, rpcId=0):
        cmdBlob = self.__serialize(op, cmd, rpcId)
        self.__p2pSender(bytes(cmdBlob), isBcast)

    # Loop for Peer to process received messages from remote.
    async def recvMsg(self):
        Logger.info('P2PPeer start recvMsg...')
        while True:
            msg = await self.msgQueue.get()
            assert isinstance(msg, QuarkMessage)
            Logger.info('P2PPeer.recvMsg got {}'.format(msg.__str__()))
            if msg.type == QuarkMessage.P2PHELLO:
                if not self.helloSent:
                    # We need say HELLO to exchange chains.
                    cmd = HelloCommand(version=self.env.config.P2P_PROTOCOL_VERSION,
                                       networkId=self.env.config.NETWORK_ID,
                                       peerId=self.network.selfId[:32],
                                       peerIp=0,
                                       peerPort=0,
                                       shardMaskList=[],
                                       rootBlockHeader=RootBlockHeader())
                    cmdBlob = self.__serialize(CommandOp.HELLO, cmd)
                    self.__p2pSender(bytes(cmdBlob), False)
                    self.helloSent = True
                    # No need to make periodic hello, as P2P network already do it.
                Logger.info('Receive peer hello {}'.format(cmd))

            if msg.type == QuarkMessage.OTHER:
                # We got something from peer.
                op, cmd, rpcId = self.__deserialize(msg.payload)
                Logger.info('Receive {}, {}, {} TODO push state machine'.format(op, cmd, rpcId))
