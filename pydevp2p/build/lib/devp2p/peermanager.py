import random
import gevent
import socket
import atexit
import time
import re
from gevent.server import StreamServer
from gevent.socket import create_connection, timeout
from .service import WiredService
from .protocol import BaseProtocol
from .p2p_protocol import P2PProtocol
from .upnp import add_portmap, remove_portmap
from devp2p import kademlia
from .peer import Peer
from devp2p import crypto
from devp2p import utils
from rlp.utils import decode_hex

from devp2p import slogging
log = slogging.get_logger('p2p.peermgr')


def on_peer_exit(peer):
    peer.stop()


class PeerManager(WiredService):

    """
    todo:
        connects new peers if there are too few
        selects peers based on a DHT
        keeps track of peer reputation
        saves/loads peers (rather discovery buckets) to disc

    connection strategy
        for service which requires peers
            while num peers > min_num_peers:
                    gen random id
                    resolve closest node address
                    [ideally know their services]
                    connect closest node
    """
    name = 'peermanager'
    required_services = []
    wire_protocol = P2PProtocol
    nat_upnp = None
    default_config = dict(p2p=dict(bootstrap_nodes=[],
                                   min_peers=5,
                                   max_peers=10,
                                   listen_port=30303,
                                   listen_host='0.0.0.0'),
                          log_disconnects=False,
                          node=dict(privkey_hex=''))

    connect_timeout = 2.
    connect_loop_delay = 0.1
    discovery_delay = 0.5

    def __init__(self, app):
        log.info('PeerManager init')
        WiredService.__init__(self, app)
        self.peers = []
        self.errors = PeerErrors() if self.config['log_disconnects'] else PeerErrorsBase()

        # setup nodeid based on privkey
        if 'id' not in self.config['p2p']:
            self.config['node']['id'] = crypto.privtopub(
                decode_hex(self.config['node']['privkey_hex']))

        self.listen_addr = (self.config['p2p']['listen_host'], self.config['p2p']['listen_port'])
        self.server = StreamServer(self.listen_addr, handle=self._on_new_connection)

    def on_hello_received(self, proto, version, client_version_string, capabilities,
                          listen_port, remote_pubkey):
        log.debug('hello_received', peer=proto.peer, num_peers=len(self.peers))
        if len(self.peers) > self.config['p2p']['max_peers']:
            log.debug('too many peers', max=self.config['p2p']['max_peers'])
            proto.send_disconnect(proto.disconnect.reason.too_many_peers)
            return False
        if remote_pubkey in [p.remote_pubkey for p in self.peers if p != proto.peer]:
            log.debug('connected to that node already')
            proto.send_disconnect(proto.disconnect.reason.useless_peer)
            return False

        return True

    @property
    def wired_services(self):
        return [s for s in self.app.services.values() if isinstance(s, WiredService)]

    def broadcast(self, protocol, command_name, args=[], kargs={},
                  num_peers=None, exclude_peers=[]):
        log.debug('broadcasting', protcol=protocol, command=command_name,
                  num_peers=num_peers, exclude_peers=exclude_peers)
        assert num_peers is None or num_peers > 0
        peers_with_proto = [p for p in self.peers
                            if protocol in p.protocols and p not in exclude_peers]

        if not peers_with_proto:
            log.debug('no peers with proto found', protos=[p.protocols for p in self.peers])
        num_peers = num_peers or len(peers_with_proto)
        for peer in random.sample(peers_with_proto, min(num_peers, len(peers_with_proto))):
            log.debug('broadcasting to', proto=peer.protocols[protocol])
            func = getattr(peer.protocols[protocol], 'send_' + command_name)
            func(*args, **kargs)
            # sequential uploads
            # wait until the message is out, before initiating next
            peer.safe_to_read.wait()
            log.debug('broadcasting done', ts=time.time())

    def _start_peer(self, connection, address, remote_pubkey=None):
        # create peer
        peer = Peer(self, connection, remote_pubkey=remote_pubkey)
        peer.link(on_peer_exit)
        log.debug('created new peer', peer=peer, fno=connection.fileno())
        self.peers.append(peer)

        # loop
        peer.start()
        log.debug('peer started', peer=peer, fno=connection.fileno())
        assert not connection.closed
        return peer

    def connect(self, address, remote_pubkey):
        log.debug('connecting', address=address)
        """
        gevent.socket.create_connection(address, timeout=Timeout, source_address=None)
        Connect to address (a 2-tuple (host, port)) and return the socket object.
        Passing the optional timeout parameter will set the timeout
        getdefaulttimeout() is default
        """
        try:
            connection = create_connection(address, timeout=self.connect_timeout)
        except socket.timeout:
            log.debug('connection timeout', address=address, timeout=self.connect_timeout)
            self.errors.add(address, 'connection timeout')
            return False
        except socket.error as e:
            log.debug('connection error', errno=e.errno, reason=e.strerror)
            self.errors.add(address, 'connection error')
            return False
        log.debug('connecting to', connection=connection)
        self._start_peer(connection, address, remote_pubkey)
        return True

    def _bootstrap(self, bootstrap_nodes=[]):
        for uri in bootstrap_nodes:
            ip, port, pubkey = utils.host_port_pubkey_from_uri(uri)
            log.info('connecting bootstrap server', uri=uri)
            try:
                self.connect((ip, port), pubkey)
            except socket.error:
                log.warn('connecting bootstrap server failed')

    def start(self):
        log.info('starting peermanager')
        # try upnp nat
        self.nat_upnp = add_portmap(
            self.config['p2p']['listen_port'],
            'TCP',
            'Ethereum DEVP2P Peermanager'
        )
        # start a listening server
        log.info('starting listener', addr=self.listen_addr)
        self.server.set_handle(self._on_new_connection)
        self.server.start()
        super(PeerManager, self).start()
        gevent.spawn_later(0.001, self._bootstrap, self.config['p2p']['bootstrap_nodes'])
        gevent.spawn_later(1, self._discovery_loop)

    def _on_new_connection(self, connection, address):
        log.debug('incoming connection', connection=connection)
        peer = self._start_peer(connection, address)
        # Explicit join is required in gevent >= 1.1.
        # See: https://github.com/gevent/gevent/issues/594
        # and http://www.gevent.org/whatsnew_1_1.html#compatibility
        peer.join()

    def num_peers(self):
        ps = [p for p in self.peers if p]
        aps = [p for p in ps if not p.is_stopped]
        if len(ps) != len(aps):
            log.warn('stopped peers in peers list', inlist=len(ps), active=len(aps))
        return len(aps)

    def remote_pubkeys(self):
        return [p.remote_pubkey for p in self.peers]

    def _discovery_loop(self):
        log.info('waiting for bootstrap')
        gevent.sleep(self.discovery_delay)
        while not self.is_stopped:
            try:
                num_peers, min_peers = self.num_peers(), self.config['p2p']['min_peers']
                kademlia_proto = self.app.services.discovery.protocol.kademlia
                if num_peers < min_peers:
                    log.debug('missing peers', num_peers=num_peers,
                              min_peers=min_peers, known=len(kademlia_proto.routing))
                    nodeid = kademlia.random_nodeid()
                    kademlia_proto.find_node(nodeid)  # fixme, should be a task
                    gevent.sleep(self.discovery_delay)  # wait for results
                    neighbours = kademlia_proto.routing.neighbours(nodeid, 2)
                    if not neighbours:
                        gevent.sleep(self.connect_loop_delay)
                        continue
                    node = random.choice(neighbours)
                    if node.pubkey in self.remote_pubkeys():
                        gevent.sleep(self.discovery_delay)
                        continue
                    log.debug('connecting random', node=node)
                    local_pubkey = crypto.privtopub(decode_hex(self.config['node']['privkey_hex']))
                    if node.pubkey == local_pubkey:
                        continue
                    if node.pubkey in [p.remote_pubkey for p in self.peers]:
                        continue
                    self.connect((node.address.ip, node.address.tcp_port), node.pubkey)
            except AttributeError:
                # TODO: Is this the correct thing to do here?
                log.error("Discovery service not available.")
                break
            except Exception as e:
                log.error("discovery failed", error=e, num_peers=num_peers, min_peers=min_peers)
            gevent.sleep(self.connect_loop_delay)

        evt = gevent.event.Event()
        evt.wait()

    def stop(self):
        log.info('stopping peermanager')
        remove_portmap(self.nat_upnp, self.config['p2p']['listen_port'], 'TCP')
        self.server.stop()
        for peer in self.peers:
            peer.stop()
        super(PeerManager, self).stop()


class PeerErrorsBase(object):
    def add(self, address, error, client_version=''):
        pass


class PeerErrors(PeerErrorsBase):
    def __init__(self):
        self.errors = dict()  # node: ['error',]
        self.client_versions = dict()  # address: client_version

        def report():
            for k, v in self.errors.items():
                print(k, self.client_versions.get(k, ''))
                for e in v:
                    print('\t', e)

        atexit.register(report)

    def add(self, address, error, client_version=''):
        self.errors.setdefault(address, []).append(error)
        if client_version:
            self.client_versions[address] = client_version
