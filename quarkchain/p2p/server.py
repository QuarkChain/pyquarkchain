from quarkchain.cluster.simple_network import Peer, AbstractNetwork

class P2PManager(AbstractNetwork):
    def __init__(self, env, master_server, loop):
        self.loop = loop
        self.env = env
        self.master_server = master_server
        master_server.network = self

    def start(self) -> None:
        pass

    def iterate_peers(self):
        pass

    def get_peer_by_cluster_peer_id(self):
        pass
