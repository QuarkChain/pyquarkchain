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
from quarkchain.utils import check, Logger
from rlp.utils import encode_hex, decode_hex, is_integer
import random
import rlp
import sys


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
        message sending a token and a nonce
        """
        cmd_id = 0

        structure = [
            ('quark', Quark)
        ]


class P2PProxyService(WiredService):
    name = 'P2PProxyService'
    default_config = dict()

    wire_protocol = QuarkChainProtocol

    def __init__(self, app):
        Logger.info('P2PProxyService init')
        self.config = app.config
        self.address = privtopub_raw(decode_hex(
            self.config['node']['privkey_hex']))
        super(P2PProxyService, self).__init__(app)

    # This server is for quarkchain to p2p netowrk stack communication.
    def startIPCServer(self):
        self.proxyServer = StreamServer(
            ('127.0.0.1', 1234), self.proxySocketRecv)
        print('{} start listening on 1234'.format(self.name))
        self.proxyServer.start()  # start accepting new connections

    # Receive on the socket and send any received message to remote.
    def proxySocketRecv(self, sock, address):
        self.proxySocket = sock
        while True:
            line = sock.recv(64000)
            self.log('handle receive {}'.format(line))
            if line != b'':
                self.send_quark(line)
            else:
                break
        print('close server side socket')
        sock.close()
        self.proxySocket = None

    def proxySocketSend(self, quark):
        # unsafe.
        if self.proxySocket:
            self.proxySocket.send(quark)

    def start(self):
        Logger.info('P2PProxyService start')
        self.startIPCServer()
        super(P2PProxyService, self).start()

    def log(self, text, **kargs):
        node_num = self.config['node_num']
        msg = ' '.join([
            colors[node_num % len(colors)],
            "NODE%d" % node_num,
            text,
            (' %r' % kargs if kargs else ''),
            COLOR_END])
        Logger.info(msg)

    def broadcast(self, obj, origin=None):
        fmap = {Quark: 'quark'}
        self.log('broadcasting', obj=obj)
        bcast = self.app.services.peermanager.broadcast
        bcast(QuarkChainProtocol, fmap[type(obj)], args=(obj,),
              exclude_peers=[origin.peer] if origin else [])

    def on_wire_protocol_stop(self, proto):
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_wire_protocol_stop', proto=proto)

    # application logic

    def on_wire_protocol_start(self, proto):
        self.log('----------------------------------')
        self.log('on_wire_protocol_start', proto=proto,
                 peers=self.app.services.peermanager.peers)
        assert isinstance(proto, self.wire_protocol)
        # register callbacks
        proto.receive_quark_callbacks.append(self.on_receive_token)

    def on_receive_token(self, proto, quark):
        assert isinstance(quark, Quark)
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_receive quark', quark=quark, proto=proto)
        self.proxySocketSend(quark)

    def send_quark(self, quark):
        self.log('----------------------------------')
        self.log('sending quark', quark=quark)
        self.broadcast(quark)


class ProxyServerApp(BaseApp):
    client_name = 'proxyserverapp'
    version = '0.0'
    client_version = '{}{}{}'.format(version, sys.platform,
                                     'py%d.%d.%d' % sys.version_info[:3])
    client_version_string = '{}/v{}'.format(client_name, client_version)
    default_config = dict(BaseApp.default_config)
    default_config['client_version_string'] = client_version_string
    default_config['post_app_start_callback'] = None


if __name__ == '__main__':
    app_helper.run(ProxyServerApp, P2PProxyService, num_nodes=1)
