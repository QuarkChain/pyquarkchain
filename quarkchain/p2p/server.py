# TODO move this out of poc
from quarkchain.p2p.poc.trinity_server import BaseServer

from eth_keys import keys

from quarkchain.cluster.simple_network import Peer, AbstractNetwork
from quarkchain.p2p import ecies
from quarkchain.p2p.cancel_token.token import CancelToken
from quarkchain.p2p.peer import BasePeer, BasePeerContext, BasePeerPool, BasePeerFactory
from quarkchain.p2p.protocol import Command, _DecodedMsgType
from quarkchain.p2p.protocol import Protocol


class QuarkProtocol(Protocol):
    name = "quarkchain"
    version = 1


class QuarkPeer(BasePeer):
    _supported_sub_protocols = [QuarkProtocol]
    sub_proto = None  # : QuarkProtocol
    peer_idle_timeout = None  # do not timeout for connected peers

    async def send_sub_proto_handshake(self) -> None:
        pass

    async def process_sub_proto_handshake(
        self, cmd: Command, msg: _DecodedMsgType
    ) -> None:
        pass

    async def do_sub_proto_handshake(self) -> None:
        pass


class QuarkContext(BasePeerContext):
    quarkchain = "quarkchain"  # : str


class QuarkPeerFactory(BasePeerFactory):
    peer_class = QuarkPeer
    context = None  # : QuarkContext


class QuarkPeerPool(BasePeerPool):
    peer_factory_class = QuarkPeerFactory
    context = None  # : QuarkContext


class QuarkServer(BaseServer):
    """
    a server using QuarkPeerPool
    """

    def _make_peer_pool(self):
        return QuarkPeerPool(
            privkey=self.privkey,
            context=QuarkContext(),
            listen_port=self.port,
            token=self.cancel_token,
        )

    def _make_syncer(self):
        return


class P2PManager(AbstractNetwork):
    def __init__(self, env, master_server, loop):
        self.loop = loop
        self.env = env
        self.master_server = master_server
        master_server.network = self  # cannot say this is a good design

        self.cancel_token = CancelToken("p2pserver")
        if env.cluster_config.P2P.BOOT_NODES:
            bootstrap_nodes = env.cluster_config.P2P.BOOT_NODES.split(",")
        else:
            bootstrap_nodes = []
        if env.cluster_config.P2P.PRIV_KEY:
            privkey = keys.PrivateKey(bytes.fromhex(env.cluster_config.P2P.PRIV_KEY))
        else:
            privkey = ecies.generate_privkey()

        self.server = QuarkServer(
            privkey=privkey,
            port=env.cluster_config.P2P_PORT,
            network_id=env.cluster_config.P2P.NETWORK_ID,
            bootstrap_nodes=tuple(
                [kademlia.Node.from_uri(enode) for enode in bootstrap_nodes]
            ),
            token=self.cancel_token,
            upnp=env.cluster_config.P2P.UPNP,
        )

    def start(self) -> None:
        self.loop.create_task(self.server.run())

    def iterate_peers(self):
        pass

    def get_peer_by_cluster_peer_id(self):
        pass
