import argparse
import asyncio
from eth_keys import keys
import signal

from quarkchain.p2p.cancel_token.token import CancelToken
from quarkchain.p2p.poc.trinity_server import BaseServer
from quarkchain.p2p.tools.paragon import ParagonContext, ParagonPeer, ParagonPeerPool
from quarkchain.p2p import ecies

from quarkchain.utils import Logger

NETWORK_ID = 999


class ParagonServer(BaseServer):
    def _make_peer_pool(self):
        return ParagonPeerPool(
            privkey=self.privkey, context=ParagonContext(), token=self.cancel_token
        )

    def _make_syncer(self):
        return


def main():
    Logger.set_logging_level("info")

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--privkey",
        default="",
        help="hex string of private key; if empty, will be auto-generated",
        type=str,
    )
    parser.add_argument(
        "--listen_port",
        default=29000,
        help="port for discovery UDP and P2P TCP connection",
        type=int,
    )

    args = parser.parse_args()

    if args.privkey:
        privkey = keys.PrivateKey(bytes.fromhex(args.privkey))
    else:
        privkey = ecies.generate_privkey()

    cancel_token = CancelToken("server")
    server = ParagonServer(
        privkey=privkey,
        port=args.listen_port,
        network_id=NETWORK_ID,
        token=cancel_token,
    )

    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, cancel_token.trigger)

    loop.run_until_complete(server.run())
    loop.run_until_complete(server.cancel())
    loop.close()


if __name__ == "__main__":
    main()
