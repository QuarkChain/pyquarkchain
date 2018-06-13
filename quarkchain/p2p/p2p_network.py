import argparse
import copy
import sys
import gevent
import asyncio
import ipaddress
import socket

from devp2p import peermanager
from devp2p.app import BaseApp
from devp2p.discovery import NodeDiscovery
from devp2p.protocol import BaseProtocol
from devp2p.service import BaseService, WiredService
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.utils import host_port_pubkey_to_uri, update_config_with_defaults
from devp2p import app_helper
from rlp.utils import decode_hex, encode_hex

from quarkchain.core import random_bytes
from quarkchain.cluster.protocol import (
    P2PConnection, ROOT_SHARD_ID,
)
from quarkchain.cluster.p2p_commands import CommandOp
from quarkchain.cluster.p2p_commands import GetPeerListRequest
from quarkchain.utils import Logger
from quarkchain.cluster.simple_network import Peer

try:
    import ethereum.slogging as slogging
    slogging.configure(config_string=':info,p2p.protocol:info,p2p.peer:info')
except:
    import devp2p.slogging as slogging
log = slogging.get_logger('poc_app')

"""
Spawns 1 app connecting to bootstrap node.
use poc_network.py for multiple apps in different processes.
"""


class ExampleProtocol(BaseProtocol):
    protocol_id = 1
    network_id = 0
    max_cmd_id = 1  # Actually max id is 0, but 0 is the special value.
    name = b'example'
    version = 1

    def __init__(self, peer, service):
        # required by P2PProtocol
        self.config = peer.config
        BaseProtocol.__init__(self, peer, service)


class ExampleService(WiredService):

    # required by BaseService
    name = 'exampleservice'
    default_config = dict(example=dict(num_participants=1))

    # required by WiredService
    wire_protocol = ExampleProtocol  # create for each peer

    def __init__(self, app):
        log.info('ExampleService init')
        self.config = app.config
        self.address = privtopub_raw(decode_hex(
            self.config['node']['privkey_hex']))
        super(ExampleService, self).__init__(app)

    def on_wire_protocol_stop(self, proto):
        log.info(
            'NODE{} on_wire_protocol_stop'.format(self.config['node_num']),
            proto=proto)
        self.show_peers()

    def on_wire_protocol_start(self, proto):
        log.info(
            'NODE{} on_wire_protocol_start'.format(self.config['node_num']),
            proto=proto)
        self.show_peers()

    def show_peers(self):
        log.warning("I am {} I have {} peers: {}".format(
            self.app.config['client_version_string'],
            self.app.services.peermanager.num_peers(),
            list(map(lambda p: p.remote_client_version, self.app.services.peermanager.peers))
        ))

    def start(self):
        log.info('ExampleService start')
        super(ExampleService, self).start()


class ExampleApp(BaseApp):
    client_name = 'exampleapp'
    version = '0.1'
    client_version = '%s/%s/%s' % (version, sys.platform,
                                   'py%d.%d.%d' % sys.version_info[:3])
    client_version_string = '%s/v%s' % (client_name, client_version)
    default_config = dict(BaseApp.default_config)
    default_config['client_version_string'] = client_version_string
    default_config['post_app_start_callback'] = None


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap_host", default='0.0.0.0', type=str)
    parser.add_argument(
        "--bootstrap_port", default=29000, type=int)
    # p2p port for this node
    parser.add_argument(
        "--node_port", default=29000, type=int)
    parser.add_argument(
        "--node_num", default=0, type=int)
    parser.add_argument(
        "--min_peers", default=2, type=int)
    parser.add_argument(
        "--max_peers", default=10, type=int)
    seed = 0

    args = parser.parse_args()

    gevent.get_hub().SYSTEM_ERROR = BaseException

    # get bootstrap node (node0) enode
    bootstrap_node_privkey = sha3(
        '{}:udp:{}'.format(0, 0).encode('utf-8'))
    bootstrap_node_pubkey = privtopub_raw(bootstrap_node_privkey)
    enode = host_port_pubkey_to_uri(
        args.bootstrap_host, args.bootstrap_port, bootstrap_node_pubkey)

    services = [NodeDiscovery, peermanager.PeerManager, ExampleService]

    # prepare config
    base_config = dict()
    for s in services:
        update_config_with_defaults(base_config, s.default_config)

    base_config['discovery']['bootstrap_nodes'] = [enode]
    base_config['seed'] = seed
    base_config['base_port'] = args.node_port
    base_config['min_peers'] = args.min_peers
    base_config['max_peers'] = args.max_peers
    log.info('run:', base_config=base_config)

    min_peers = base_config['min_peers']
    max_peers = base_config['max_peers']

    assert min_peers <= max_peers
    config = copy.deepcopy(base_config)
    config['node_num'] = args.node_num

    # create this node priv_key
    config['node']['privkey_hex'] = encode_hex(sha3(
        '{}:udp:{}'.format(seed, args.node_num).encode('utf-8')))
    # set ports based on node
    config['discovery']['listen_port'] = args.node_port
    config['p2p']['listen_port'] = args.node_port
    config['p2p']['min_peers'] = min(10, min_peers)
    config['p2p']['max_peers'] = max_peers
    config['client_version_string'] = 'NODE{}'.format(args.node_num)

    app = ExampleApp(config)
    log.info('create_app', config=app.config)
    # register services
    for service in services:
        assert issubclass(service, BaseService)
        if service.name not in app.config['deactivated_services']:
            assert service.name not in app.services
            service.register_with_app(app)
            assert hasattr(app.services, service.name)

    app_helper.serve_until_stopped([app])


class P2PNetwork:
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

if __name__ == '__main__':
    main()
