"""
Example runs:

# run bootnode, this will fire up both UDP(discovery) and TCP(P2P) server
# note the default private key is correct key for bootnode
python trinity_node.py --logging_level=debug

# run a different node on a new port, note we need to leave private key empty to automatically generate a new one
python trinity_node.py --privkey="" --listen_port=29001 --logging_level=debug
"""
import argparse
import asyncio
from eth_keys import keys
import signal

from quarkchain.p2p import ecies
from quarkchain.p2p import kademlia
from quarkchain.p2p.cancel_token.token import CancelToken
from quarkchain.p2p.poc.trinity_server import BaseServer
from quarkchain.p2p.tools.paragon import ParagonContext, ParagonPeer, ParagonPeerPool

from quarkchain.utils import Logger

NETWORK_ID = 999


class ParagonServer(BaseServer):
    def _make_peer_pool(self):
        return ParagonPeerPool(
            privkey=self.privkey,
            context=ParagonContext(),
            listen_port=self.port,
            token=self.cancel_token,
        )

    def _make_syncer(self):
        return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootnode",
        default="enode://c571e0db93d17cc405cb57640826b70588a6a28785f38b21be471c609ca12fcb06cb306ac44872908f5bed99046031a5af82072d484e3ef9029560c1707193a0@127.0.0.1:29000",
        type=str,
    )
    parser.add_argument(
        "--privkey",
        default="31552f186bf90908ce386fb547dd0410bf443309125cc43fd0ffd642959bf6d9",
        help="hex string of private key; if empty, will be auto-generated",
        type=str,
    )
    parser.add_argument(
        "--listen_port",
        default=29000,
        help="port for discovery UDP and P2P TCP connection",
        type=int,
    )
    parser.add_argument("--max_peers", default=10, type=int)
    parser.add_argument("--logging_level", default="info", type=str)
    parser.add_argument(
        "--upnp",
        default=False,
        action="store_true",
        help="if set, will automatically set up port-fowarding if upnp devices that support port forwarding can be found",
    )
    args = parser.parse_args()

    Logger.set_logging_level(args.logging_level)

    if args.privkey:
        privkey = keys.PrivateKey(bytes.fromhex(args.privkey))
    else:
        privkey = ecies.generate_privkey()

    cancel_token = CancelToken("server")
    server = ParagonServer(
        privkey=privkey,
        port=args.listen_port,
        network_id=NETWORK_ID,
        bootstrap_nodes=tuple([kademlia.Node.from_uri(args.bootnode)]),
        token=cancel_token,
        upnp=args.upnp,
    )

    loop = asyncio.get_event_loop()
    # loop.set_debug(True)

    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, cancel_token.trigger)

    loop.run_until_complete(server.run())
    loop.run_until_complete(server.cancel())
    loop.close()


if __name__ == "__main__":
    main()
