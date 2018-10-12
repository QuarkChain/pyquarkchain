import argparse
import copy
import sys
from devp2p import peermanager
from devp2p.app import BaseApp
from devp2p.discovery import NodeDiscovery
from devp2p.protocol import BaseProtocol
from devp2p.service import BaseService, WiredService
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.utils import (
    colors,
    COLOR_END,
    host_port_pubkey_to_uri,
    update_config_with_defaults,
)
from devp2p import app_helper
import rlp
from quarkchain.rlp.utils import encode_hex, decode_hex, is_integer
import gevent

try:
    import quarkchain.evm.slogging as slogging

    slogging.configure(config_string=":info,p2p.protocol:info,p2p.peer:info")
except:
    import devp2p.slogging as slogging
log = slogging.get_logger("poc_app")

"""
Spawns 1 app connecting to bootstrap node.
use poc_network.py for multiple apps in different processes.
"""


class ExampleProtocol(BaseProtocol):
    protocol_id = 1
    network_id = 0
    max_cmd_id = 1  # Actually max id is 0, but 0 is the special value.
    name = b"example"
    version = 1

    def __init__(self, peer, service):
        # required by P2PProtocol
        self.config = peer.config
        BaseProtocol.__init__(self, peer, service)


class ExampleService(WiredService):

    # required by BaseService
    name = "exampleservice"
    default_config = dict(example=dict(num_participants=1))

    # required by WiredService
    wire_protocol = ExampleProtocol  # create for each peer

    def __init__(self, app):
        log.info("ExampleService init")
        self.config = app.config
        self.address = privtopub_raw(decode_hex(self.config["node"]["privkey_hex"]))
        super(ExampleService, self).__init__(app)

    def on_wire_protocol_stop(self, proto):
        log.info(
            "NODE{} on_wire_protocol_stop".format(self.config["node_num"]), proto=proto
        )
        self.show_peers()

    def on_wire_protocol_start(self, proto):
        log.info(
            "NODE{} on_wire_protocol_start".format(self.config["node_num"]), proto=proto
        )
        self.show_peers()

    def show_peers(self):
        log.warning(
            "I am {} I have {} peers: {}".format(
                self.app.config["client_version_string"],
                self.app.services.peermanager.num_peers(),
                list(
                    map(
                        lambda p: p.remote_client_version,
                        self.app.services.peermanager.peers,
                    )
                ),
            )
        )

    def start(self):
        log.info("ExampleService start")
        super(ExampleService, self).start()


class ExampleApp(BaseApp):
    client_name = "exampleapp"
    version = "0.1"
    client_version = "%s/%s/%s" % (
        version,
        sys.platform,
        "py%d.%d.%d" % sys.version_info[:3],
    )
    client_version_string = "%s/v%s" % (client_name, client_version)
    default_config = dict(BaseApp.default_config)
    default_config["client_version_string"] = client_version_string.encode('utf-8')
    default_config["post_app_start_callback"] = None


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap_host", default="0.0.0.0", type=str)
    parser.add_argument("--bootstrap_port", default=29000, type=int)
    # p2p port for this node
    parser.add_argument("--node_port", default=29000, type=int)
    parser.add_argument("--node_num", default=0, type=int)
    parser.add_argument("--min_peers", default=2, type=int)
    parser.add_argument("--max_peers", default=10, type=int)
    seed = 0

    args = parser.parse_args()

    gevent.get_hub().SYSTEM_ERROR = BaseException

    # get bootstrap node (node0) enode
    bootstrap_node_privkey = sha3("{}:udp:{}".format(0, 0).encode("utf-8"))
    bootstrap_node_pubkey = privtopub_raw(bootstrap_node_privkey)
    enode = host_port_pubkey_to_uri(
        args.bootstrap_host, args.bootstrap_port, bootstrap_node_pubkey
    )

    services = [NodeDiscovery, peermanager.PeerManager, ExampleService]

    # prepare config
    base_config = dict()
    for s in services:
        update_config_with_defaults(base_config, s.default_config)

    base_config["discovery"]["bootstrap_nodes"] = [enode]
    base_config["seed"] = seed
    base_config["base_port"] = args.node_port
    base_config["min_peers"] = args.min_peers
    base_config["max_peers"] = args.max_peers
    log.info("run:", base_config=base_config)

    min_peers = base_config["min_peers"]
    max_peers = base_config["max_peers"]

    assert min_peers <= max_peers
    config = copy.deepcopy(base_config)
    config["node_num"] = args.node_num

    # create this node priv_key
    config["node"]["privkey_hex"] = encode_hex(
        sha3("{}:udp:{}".format(seed, args.node_num).encode("utf-8"))
    )
    # set ports based on node
    config["discovery"]["listen_port"] = args.node_port
    config["p2p"]["listen_port"] = args.node_port
    config["p2p"]["min_peers"] = min(10, min_peers)
    config["p2p"]["max_peers"] = max_peers
    config["client_version_string"] = "NODE{}".format(args.node_num).encode('utf-8')

    app = ExampleApp(config)
    log.info("create_app", config=app.config)
    # register services
    for service in services:
        assert issubclass(service, BaseService)
        if service.name not in app.config["deactivated_services"]:
            assert service.name not in app.services
            service.register_with_app(app)
            assert hasattr(app.services, service.name)

    app_helper.serve_until_stopped([app])


if __name__ == "__main__":
    main()
