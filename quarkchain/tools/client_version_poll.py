import argparse
import asyncio
import rlp
from typing import Any, cast, Dict

from quarkchain.p2p import auth
from quarkchain.p2p import ecies
from quarkchain.p2p.exceptions import HandshakeFailure, HandshakeDisconnectedFailure
from quarkchain.p2p.kademlia import Node
from quarkchain.p2p.p2p_manager import QuarkServer
from quarkchain.p2p.p2p_proto import Disconnect, DisconnectReason, Hello
from quarkchain.p2p.peer import PeerConnection


def get_quark_peer_factory():
    privkey = ecies.generate_privkey()
    server = QuarkServer(privkey=privkey, port=38291, network_id=0)
    return server.peer_pool.get_peer_factory()


async def handshake_for_version(remote: Node, factory):
    """Perform the auth and P2P handshakes (without sub-protocol handshake) with the given remote.
    Disconnect after initial hello message exchange, and return version id
    """
    try:
        (
            aes_secret,
            mac_secret,
            egress_mac,
            ingress_mac,
            reader,
            writer,
        ) = await auth.handshake(remote, factory.privkey, factory.cancel_token)
    except (ConnectionRefusedError, OSError) as e:
        raise UnreachablePeer() from e
    connection = PeerConnection(
        reader=reader,
        writer=writer,
        aes_secret=aes_secret,
        mac_secret=mac_secret,
        egress_mac=egress_mac,
        ingress_mac=ingress_mac,
    )
    peer = factory.create_peer(remote=remote, connection=connection, inbound=False)
    # see await peer.do_p2p_handshake()
    peer.base_protocol.send_handshake()
    try:
        cmd, msg = await peer.read_msg(timeout=peer.conn_idle_timeout)
    except rlp.DecodingError:
        raise HandshakeFailure("Got invalid rlp data during handshake")
    except MalformedMessage as e:
        raise HandshakeFailure("Got malformed message during handshake") from e
    if isinstance(cmd, Disconnect):
        msg = cast(Dict[str, Any], msg)
        raise HandshakeDisconnectedFailure(
            "disconnected before completing sub-proto handshake: {}".format(
                msg["reason_name"]
            )
        )
    msg = cast(Dict[str, Any], msg)
    if not isinstance(cmd, Hello):
        await peer.disconnect(DisconnectReason.bad_protocol)
        raise HandshakeFailure(
            "Expected a Hello msg, got {}, disconnecting".format(cmd)
        )
    return msg["client_version_string"]


# to test, run local quarkchain cluster with:
#  python cluster.py --p2p --p2p_port=38291 --privkey=9e88b123b2200d6d78bf288a2dd7e3b2f31c77c3b119f8222d5a2d510b4c8d94


async def main():
    parser = argparse.ArgumentParser()
    # do not use "localhost", use the private ip if you run this from EC2
    parser.add_argument(
        "--remote",
        default="enode://28698cd33c5c78514ce1d8a7228e0071f341d75509dc48f12e26f9e22584740a5b6bf8a447eab8679e8744d283dd4173ddbdc52f44a7cb5ff508ecbd04b500f0@127.0.0.1:38291",
        type=str,
    )

    args = parser.parse_args()
    factory = get_quark_peer_factory()
    remote = Node.from_uri(args.remote)
    version = await handshake_for_version(remote, factory)
    print(version)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
