"""
trinity_discovery.py - runs a single discover service that connects to the specified bootnode on startup
Example runs:
# this is the correct private key for the default bootnode, discovery will figure out that it is bootnode itself and won't bond at all ("Failed to bond with bootstrap nodes" message is expected)
python p2p/poc/p2p_app.py --privkey=31552f186bf90908ce386fb547dd0410bf443309125cc43fd0ffd642959bf6d9
# after bootnode is up, running a new node will populate discovery table of both nodes
python trinity_discovery.py --listen_port=29001
"""
import argparse
import asyncio
import signal

from eth_keys import keys

from quarkchain.utils import Logger
from quarkchain.p2p import ecies
from quarkchain.p2p import kademlia
from quarkchain.p2p.cancel_token.token import CancelToken, OperationCancelled
from quarkchain.p2p.discovery import DiscoveryProtocol


def main():
    Logger.set_logging_level("debug")
    loop = asyncio.get_event_loop()
    loop.set_debug(True)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootnode",
        default="enode://c571e0db93d17cc405cb57640826b70588a6a28785f38b21be471c609ca12fcb06cb306ac44872908f5bed99046031a5af82072d484e3ef9029560c1707193a0@127.0.0.1:29000",
        type=str,
    )
    parser.add_argument("--listen_host", default="127.0.0.1", type=str)
    parser.add_argument(
        "--listen_port",
        default=29000,
        help="port for discovery UDP and P2P TCP connection",
        type=int,
    )
    parser.add_argument("--max_peers", default=10, type=int)
    # private key of the bootnode above is 31552f186bf90908ce386fb547dd0410bf443309125cc43fd0ffd642959bf6d9
    parser.add_argument(
        "--privkey",
        default="",
        help="hex string of private key; if empty, will be auto-generated",
        type=str,
    )

    args = parser.parse_args()

    if args.privkey:
        privkey = keys.PrivateKey(bytes.fromhex(args.privkey))
    else:
        privkey = ecies.generate_privkey()
    addr = kademlia.Address(args.listen_host, args.listen_port, args.listen_port)
    bootstrap_nodes = tuple([kademlia.Node.from_uri(args.bootnode)])

    cancel_token = CancelToken("discovery")
    discovery = DiscoveryProtocol(privkey, addr, bootstrap_nodes, cancel_token)

    async def run() -> None:
        await loop.create_datagram_endpoint(
            lambda: discovery, local_addr=("0.0.0.0", args.listen_port)
        )
        try:
            await discovery.bootstrap()
            while True:
                Logger.info("Routing table size={}".format(len(discovery.routing)))
                await cancel_token.cancellable_wait(asyncio.sleep(5))
        except OperationCancelled:
            pass
        finally:
            await discovery.stop()

    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, cancel_token.trigger)

    loop.run_until_complete(run())
    loop.close()


if __name__ == "__main__":
    main()
