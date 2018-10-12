import sys
from devp2p.app import BaseApp
from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.utils import colors, COLOR_END
from devp2p import app_helper
import rlp
from quarkchain.rlp.utils import encode_hex, decode_hex, is_integer
import gevent

try:
    import quarkchain.evm.slogging as slogging

    slogging.configure(config_string=":info,p2p.protocol:info,p2p.peer:info")
except:
    import devp2p.slogging as slogging
log = slogging.get_logger("my_app")

"""
based on devp2p/examples/full_app.py
Spawns 10 apps (gevent) with discovery service on different ports,
and print out their peers periodically.
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

    def show_peers(self):
        while True:
            gevent.sleep(10)
            log.warning(
                "I am {} I have {} peers: {}".format(
                    self.app.config["discovery"]["listen_port"],
                    self.app.services.peermanager.num_peers(),
                    list(map(lambda p: p.ip_port, self.app.services.peermanager.peers)),
                )
            )

    def start(self):
        log.info("ExampleService start")
        super(ExampleService, self).start()
        gevent.spawn(self.show_peers)


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


if __name__ == "__main__":
    apps = app_helper.setup_apps(
        ExampleApp,
        ExampleService,
        num_nodes=10,
        seed=0,
        min_peers=2,
        max_peers=10,
        random_port=False,
    )
    app_helper.serve_until_stopped(apps)
