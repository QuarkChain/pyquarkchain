# A server that provide P2P service over devp2p protocol.format
# Most of quarkchain core is developed with asyncio, devp2p is written in gevent.
# They are not compatible especially each has its own event loop.
# One possible solution is to implement future waiting on gevent and register in
# asyncio eventloop and run gevent loop in another thread.
# But its painful to implement and maintain. So the solution here is to create
# a process and use socket to exchange messages between these two components.

from devp2p import app_helper
from devp2p.app import BaseApp
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService
from devp2p.service import WiredService
from devp2p.utils import colors, COLOR_END
from gevent.server import StreamServer
from multiprocessing import Process
from quarkchain.chain import QuarkChainState
from quarkchain.commands import NewMinorBlockHeaderListCommand, NewTransaction, NewTransactionListCommand
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import random_bytes
from quarkchain.core import RootBlock, RootBlockHeader
from quarkchain.core import Serializable, PreprendedSizeListSerializer, PreprendedSizeBytesSerializer
from quarkchain.core import Transaction, MinorBlockHeader, MinorBlock
from quarkchain.core import uint16, uint32, uint128, hash256, uint8, boolean
from quarkchain.db import PersistentDb
from quarkchain.local import LocalServer
from quarkchain.p2p_peer import P2PPeer
from quarkchain.p2pinterface_pb2 import QuarkMessage
from quarkchain.protocol import Connection, ConnectionState
from quarkchain.simple_network import Peer, CommandOp, GetPeerListRequest
from quarkchain.utils import check, Logger
from quarkchain.utils import set_logging_level
from rlp.utils import encode_hex, decode_hex, is_integer
import argparse
import asyncio
import ethereum.slogging as slogging
import ipaddress
import random
import rlp
import socket
import sys
slogging.configure(config_string=':info,p2p.protocol:debug,p2p.peer:info')


class Quark(rlp.Serializable):

    "Blob with the quarkchain information"
    fields = [
        ('quark', rlp.sedes.binary)
    ]

    def __init__(self, quark=b''):
        assert isinstance(quark, bytes)
        super(Quark, self).__init__(quark)

    @property
    def hash(self):
        return sha3(rlp.encode(self))

    def __repr__(self):
        try:
            return '<{}:quark={}>'.format(self.__class__.__name__,
                                          encode_hex(self.hash)[:4])
        except:
            return '<{}>'.format(self.__class__.__name__)


class QuarkChainProtocol(BaseProtocol):
    protocol_id = 6
    network_id = 0
    max_cmd_id = 1  # Actually max id is 0, but 0 is the special value.
    name = b'quarkchain'
    version = 1

    def __init__(self, peer, service):
        # required by P2PProtocol
        self.config = peer.config
        BaseProtocol.__init__(self, peer, service)

    class quark(BaseProtocol.command):

        """
        message sending a quark
        """
        cmd_id = 0

        structure = [
            ('quark', rlp.sedes.binary)
        ]

        def receive(self, proto, data):
            Logger.debug('receive_quark peer={} data={}'.format(proto.peer, data))
            quarkMessage = data['quark']
            proto.service.proxySocketSend(len(quarkMessage).to_bytes(4, 'big') + quarkMessage)

# P2P network side gevent domain.


class P2PProxyService(WiredService):
    name = 'P2PProxyService'
    default_config = dict()

    pubkeyProtoDict = dict()
    wire_protocol = QuarkChainProtocol

    def __init__(self, app):
        self.config = app.config
        self.address = privtopub_raw(decode_hex(
            self.config['node']['privkey_hex']))
        super(P2PProxyService, self).__init__(app)
        Logger.info('P2PProxyService init-ed. id:{}'.format(self.address))

    # This server is for quarkchain to p2p netowrk stack communication.
    def startIPCServer(self):
        self.proxyServer = StreamServer(
            ('127.0.0.1', self.config['internal_ipc_port']), self.proxySocketRecv)
        self.log('{} start listening on {}'.format(self.name, self.config['internal_ipc_port']))
        self.proxyServer.start()  # start accepting new connections

    # Receive on the socket and send any received message to remote.
    def proxySocketRecv(self, sock, address):
        self.proxySocket = sock
        Logger.info('Receive sock {} started'.format(sock))
        while True:
            lenBytes = sock.recv(4)
            if lenBytes == '':
                break
            msgLen = int.from_bytes(lenBytes, 'big')
            msgBuf = sock.recv(msgLen)
            if msgBuf != b'':
                # TODO remove deserialize, no need.
                quarkMessage = QuarkMessage()
                quarkMessage.ParseFromString(msgBuf)
                Logger.debug('proxySocket receive {} bytes msg {}'.format(msgLen, quarkMessage))
                if quarkMessage.is_broadcast:
                    self.bcastQuark(msgBuf)
                else:
                    self.sendP2PQuark(quarkMessage)
            else:
                break
        self.log('close server side socket')
        sock.close()
        self.proxySocket = None

    # quarkMessage is msgLen + serialized proto.
    def proxySocketSend(self, quarkMessage: bytes):
        # unsafe.
        if self.proxySocket:
            self.proxySocket.send(quarkMessage)

    def start(self):
        Logger.info('P2PProxyService start')
        self.startIPCServer()
        super(P2PProxyService, self).start()

    def log(self, text, **kargs):
        node_num = self.config['node_num']
        base_port = self.config['base_port']
        msg = ' '.join([
            colors[node_num % len(colors)],
            "NODE{}".format(base_port),
            text,
            (' %r' % kargs if kargs else ''),
            COLOR_END])
        Logger.info(msg)

    def broadcast(self, obj, origin=None):
        self.log('broadcasting', obj=obj)
        bcast = self.app.services.peermanager.broadcast
        bcast(QuarkChainProtocol, 'quark', args=(obj,),
              exclude_peers=[origin.peer] if origin else [])

    def on_wire_protocol_stop(self, proto):
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_wire_protocol_stop', proto=proto)

    # Each peer connection will call this with proto.peer contains peer address.
    def on_wire_protocol_start(self, proto):
        self.log('----------------------------------')
        self.log('on_wire_protocol_start', proto=proto)
        assert isinstance(proto, self.wire_protocol)
        # register callbacks
        proto.receive_quark_callbacks.append(self.onReceiveQuark)
        # Get remoteKey, convert bytes to string.
        remotePubKey = proto.peer.remote_pubkey.decode('latin-1')
        self.pubkeyProtoDict[remotePubKey] = proto
        # Notify QC core, we had a Peer connection
        msg = QuarkMessage()
        msg.is_broadcast = False
        msg.peer_id = remotePubKey  # Can we just use name?
        self.log('peer ', id=remotePubKey)
        msg.type = QuarkMessage.P2PHELLO
        protoStr = msg.SerializeToString()
        payload = len(protoStr).to_bytes(4, 'big') + protoStr
        self.proxySocketSend(payload)

    def onReceiveQuark(self, proto, quark):
        assert isinstance(quark, Quark)
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_receive quark', quark=quark, proto=proto)
        self.proxySocketSend(quark)

    def bcastQuark(self, quarkPayload):
        self.log('----------------------------------')
        self.log('sending quark', quark=quarkPayload)
        self.broadcast(quarkPayload)

    def sendP2PQuark(self, quarkMessage):
        self.log('----------------------------------')
        Logger.info('peer_id:{}'.format(quarkMessage.peer_id.encode('latin-1')))
        proto = self.pubkeyProtoDict.get(quarkMessage.peer_id)
        if proto != None:
            self.log('sending quark', quark=quarkMessage)
            proto.send_quark(quarkMessage.SerializeToString())
        else:
            self.log('Failed to find peer {}'.format(quarkMessage.peer_id))


class ProxyServerApp(BaseApp):
    client_name = 'proxyserverapp'
    version = '0.0'
    client_version = '{}{}{}'.format(version, sys.platform,
                                     'py%d.%d.%d' % sys.version_info[:3])
    client_version_string = '{}/v{}'.format(client_name, client_version)
    default_config = dict(BaseApp.default_config)
    default_config['client_version_string'] = client_version_string
    default_config['post_app_start_callback'] = None
    default_config['internal_ipc_port'] = random.randint(10000, 14000)


# QC Core side asyncio domain.
class P2PNetwork:

    def __init__(self, env, qcState, p2pApp):
        self.loop = asyncio.get_event_loop()
        self.loop.set_debug(True)
        self.env = env
        self.activePeerPool = dict()    # peer id => peer
        # TODO: Add an attribute for p2pApp to get self id.
        self.selfId = p2pApp.services.P2PProxyService.address
        self.qcState = qcState
        self.ip = ipaddress.ip_address(
            socket.gethostbyname(socket.gethostname()))
        self.port = self.env.config.P2P_SERVER_PORT
        self.localPort = self.env.config.LOCAL_SERVER_PORT
        self.p2pApp = p2pApp
        self.config = p2pApp.config
        self.p2pReader = None
        self.p2pWriter = None
        self.activePeerPool = dict()
        self.syncing = False

    # msgHandler must take a message as paramter.
    def registerP2PCallbacks(self, msgHandler):
        if msgHandler not in self.p2pReadCallbacks:
            # The order is the priority.
            self.p2pReadCallbacks.append(msgHandler)

    # All QC Core messages are received from here.
    async def handlRecvP2P(self):
        while True:
            lenStr = await self.p2pReader.read(4)
            if lenStr == '':
                break
            msgLen = int.from_bytes(lenStr, 'big')
            msgBuf = await self.p2pReader.read(msgLen)
            if msgBuf == '':
                Logger.info('StreamReader closed')
                return
            Logger.info('msgbuf={}'.format(msgBuf))
            quarkMessage = QuarkMessage()
            quarkMessage.ParseFromString(msgBuf)
            Logger.debug('QC Core Receive message {}'.format(quarkMessage))
            peer = self.activePeerPool.get(quarkMessage.peer_id)
            if peer is None:
                peer = P2PPeer(self.env, self, quarkMessage.peer_id, self.writeToP2P, self.loop)
                self.activePeerPool[quarkMessage.peer_id] = peer
                asyncio.ensure_future(peer.recvMsg(), loop=self.loop)

            await peer.networkCallback(quarkMessage)

    def writeToP2P(self, msg: bytes):
        proxyMsg = len(msg).to_bytes(4, 'big') + msg
        Logger.info('QC Core sending to P2P {}'.format(proxyMsg))
        self.p2pWriter.write(proxyMsg)

    async def newLocalClient(self, reader, writer):
        localServer = LocalServer(self.env, reader, writer, self)
        await localServer.start()

    def start(self):
        if self.env.config.ENABLE_P2P:
            coro = asyncio.open_connection('127.0.0.1', self.config['internal_ipc_port'],
                                           loop=self.loop)
            self.p2pReader, self.p2pWriter = self.loop.run_until_complete(coro)
            Logger.info('QC Core connected with P2PProxyService')
            asyncio.ensure_future(self.handlRecvP2P(), loop=self.loop)

        if self.env.config.LOCAL_SERVER_ENABLE:
            coro = asyncio.start_server(
                self.newLocalClient, "0.0.0.0", self.localPort, loop=self.loop)
            self.local_server = self.loop.run_until_complete(coro)
            Logger.info("Listening on {} for local".format(
                self.local_server.sockets[0].getsockname()))

        try:
            self.loop.run_forever()
        except KeyboardInterrupt:
            pass
        self.loop.close()
        Logger.info("Server is shutdown")

    def __broadcastCommand(self, op, cmd, sourcePeerId=None):
        for peerId, peer in self.activePeerPool.items():
            if peerId == sourcePeerId:
                continue
            Logger.info('bcast to one peer is enough')
            peer.p2pSend(op, cmd, True)
            break

    # The same interfaces as simple network.
    def broadcastBlockHeaders(self, rHeader, mHeaderList=[]):
        cmd = NewMinorBlockHeaderListCommand(rHeader, mHeaderList)
        self.__broadcastCommand(CommandOp.NEW_MINOR_BLOCK_HEADER_LIST, cmd)

    def broadcastTransaction(self, shardId, tx, sourcePeerId=None):
        cmd = NewTransactionListCommand([NewTransaction(shardId, tx)])
        self.__broadcastCommand(CommandOp.NEW_TRANSACTION_LIST, cmd, sourcePeerId)

    def isSyncing(self):
        return self.syncing

    def __sync(self):
        # TODO Add syncing.
        pass


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server_port", default=DEFAULT_ENV.config.P2P_SERVER_PORT, type=int)
    # Local port for JSON-RPC, wallet, etc
    parser.add_argument(
        "--enable_local_server", default=False, type=bool)
    parser.add_argument(
        "--local_port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    parser.add_argument("--in_memory_db", default=False)
    parser.add_argument("--db_path", default="./db", type=str)
    parser.add_argument("--log_level", default="info", type=str)
    parser.add_argument("--enable_p2p", default=True, type=bool)
    parser.add_argument("--i_am_seed", default=False, type=bool)
    parser.add_argument("--node_name", default='N/A', type=str)
    args = parser.parse_args()

    set_logging_level(args.log_level)

    env = DEFAULT_ENV.copy()
    env.config.P2P_SERVER_PORT = args.server_port
    env.config.LOCAL_SERVER_PORT = args.local_port
    env.config.LOCAL_SERVER_ENABLE = args.enable_local_server
    env.config.ENABLE_P2P = args.enable_p2p
    env.config.IS_SEED = args.i_am_seed
    env.config.node_name = args.node_name
    if not args.in_memory_db:
        env.db = PersistentDb(path=args.db_path, clean=True)

    return env


def runner(env, app):
    qcState = QuarkChainState(env)
    network = P2PNetwork(env, qcState, app)
    network.start()


def main():
    env = parse_args()
    env.NETWORK_ID = 1  # testnet

    if env.config.ENABLE_P2P:
        if env.config.IS_SEED:
            # Seed 0 will be seed node.
            seed = 0
        else:
            seed = random.randint(1, 10000)
        app = app_helper.setup_apps(ProxyServerApp, P2PProxyService, num_nodes=1,
                                    seed=seed, random_port=False if seed == 0 else True)
        proc = Process(target=runner, args=(env, app[0]))
        proc.start()
        app_helper.serve_until_stopped(app)
        proc.terminate()


if __name__ == '__main__':
    main()
