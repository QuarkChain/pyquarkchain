import asyncio
import os

from abc import abstractmethod
from typing import Sequence, Tuple

from eth_keys import datatypes
from eth_utils import big_endian_to_int

from quarkchain.p2p.auth import decode_authentication, HandshakeResponder
from quarkchain.p2p.cancel_token.token import CancelToken, OperationCancelled
from quarkchain.p2p.constants import (
    ENCRYPTED_AUTH_MSG_LEN,
    DEFAULT_MAX_PEERS,
    HASH_LEN,
    REPLY_TIMEOUT,
)
from quarkchain.p2p.discovery import (
    DiscoveryByTopicProtocol,
    DiscoveryProtocol,
    DiscoveryService,
    PreferredNodeDiscoveryProtocol,
)
from quarkchain.p2p.exceptions import (
    DecryptionError,
    HandshakeFailure,
    PeerConnectionLost,
    HandshakeDisconnectedFailure,
)
from quarkchain.p2p.p2p_proto import DisconnectReason
from quarkchain.p2p.peer import BasePeer, PeerConnection
from quarkchain.p2p.service import BaseService
from quarkchain.p2p.kademlia import Address, Node
from quarkchain.p2p.nat import UPnPService

from quarkchain.utils import Logger


NO_SAME_IP = False


class BaseServer(BaseService):
    """Server listening for incoming connections"""

    _tcp_listener = None
    peer_pool = None

    def __init__(
        self,
        privkey: datatypes.PrivateKey,
        port: int,
        network_id: int,
        max_peers: int = DEFAULT_MAX_PEERS,
        bootstrap_nodes: Tuple[Node, ...] = None,
        preferred_nodes: Sequence[Node] = [],
        use_discv5: bool = False,
        token: CancelToken = None,
        upnp: bool = False,
        allow_dial_in_ratio: float = 1.0,
    ) -> None:
        super().__init__(token)
        self.privkey = privkey
        self.port = port
        self.network_id = network_id
        self.max_peers = max_peers
        self.bootstrap_nodes = bootstrap_nodes
        self.preferred_nodes = preferred_nodes
        self.use_discv5 = use_discv5
        self.upnp_service = None
        if upnp:
            self.upnp_service = UPnPService(port, token=self.cancel_token)
        self.allow_dial_in_ratio = allow_dial_in_ratio
        self.peer_pool = self._make_peer_pool()

        if not bootstrap_nodes:
            self.logger.warning("Running with no bootstrap nodes")

    @abstractmethod
    def _make_peer_pool(self):
        pass

    @abstractmethod
    def _make_syncer(self) -> BaseService:
        pass

    async def _start_tcp_listener(self) -> None:
        # TODO: Support IPv6 addresses as well.
        self._tcp_listener = await asyncio.start_server(
            self.receive_handshake, host="0.0.0.0", port=self.port
        )

    async def _close_tcp_listener(self) -> None:
        if self._tcp_listener:
            self._tcp_listener.close()
            await self._tcp_listener.wait_closed()

    async def _run(self) -> None:
        self.logger.info("Running server...")
        mapped_external_ip = None
        if self.upnp_service:
            mapped_external_ip = await self.upnp_service.add_nat_portmap()
        external_ip = mapped_external_ip or "0.0.0.0"
        await self._start_tcp_listener()
        self.logger.info(
            "this server: enode://%s@%s:%s",
            self.privkey.public_key.to_hex()[2:],
            external_ip,
            self.port,
        )
        self.logger.info("network: %s", self.network_id)
        self.logger.info("peers: max_peers=%s", self.max_peers)
        addr = Address(external_ip, self.port, self.port)
        if self.use_discv5:
            topic = self._get_discv5_topic()
            self.logger.info(
                "Using experimental v5 (topic) discovery mechanism; topic: %s", topic
            )
            discovery_proto = DiscoveryByTopicProtocol(
                topic, self.privkey, addr, self.bootstrap_nodes, self.cancel_token
            )
        else:
            discovery_proto = PreferredNodeDiscoveryProtocol(
                self.privkey,
                addr,
                self.bootstrap_nodes,
                self.preferred_nodes,
                self.cancel_token,
            )
        self.discovery = DiscoveryService(
            discovery_proto, self.peer_pool, self.port, token=self.cancel_token
        )
        self.run_daemon(self.peer_pool)
        self.run_daemon(self.discovery)
        if self.upnp_service:
            # UPNP service is still experimental and not essential, so we don't use run_daemon() for
            # it as that means if it crashes we'd be terminated as well.
            self.run_child_service(self.upnp_service)
        self.syncer = self._make_syncer()
        await self.cancel_token.wait()

    async def _cleanup(self) -> None:
        self.logger.info("Closing server...")
        await self._close_tcp_listener()

    def _get_discv5_topic(self) -> bytes:
        # TODO set up topic correctly
        return b"\x00"

    async def receive_handshake(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        ip, socket, *_ = writer.get_extra_info("peername")
        remote_address = Address(ip, socket)
        if self.peer_pool.chk_dialin_blacklist(remote_address):
            Logger.info_every_n(
                "{} has been blacklisted, refusing connection".format(remote_address),
                100,
            )
            reader.feed_eof()
            writer.close()
        expected_exceptions = (
            TimeoutError,
            PeerConnectionLost,
            HandshakeFailure,
            asyncio.IncompleteReadError,
            HandshakeDisconnectedFailure,
        )
        try:
            await self._receive_handshake(reader, writer)
        except expected_exceptions as e:
            self.logger.debug("Could not complete handshake: %s", e)
            Logger.error_every_n("Could not complete handshake: {}".format(e), 100)
            reader.feed_eof()
            writer.close()
        except OperationCancelled:
            self.logger.error("OperationCancelled")
            reader.feed_eof()
            writer.close()
        except Exception as e:
            self.logger.exception("Unexpected error handling handshake")
            reader.feed_eof()
            writer.close()

    async def _receive_handshake(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        msg = await self.wait(
            reader.read(ENCRYPTED_AUTH_MSG_LEN), timeout=REPLY_TIMEOUT
        )
        ip, socket, *_ = writer.get_extra_info("peername")
        remote_address = Address(ip, socket)
        self.logger.debug("Receiving handshake from %s", remote_address)
        got_eip8 = False
        try:
            ephem_pubkey, initiator_nonce, initiator_pubkey = decode_authentication(
                msg, self.privkey
            )
        except DecryptionError:
            # Try to decode as EIP8
            got_eip8 = True
            msg_size = big_endian_to_int(msg[:2])
            remaining_bytes = msg_size - ENCRYPTED_AUTH_MSG_LEN + 2
            msg += await self.wait(reader.read(remaining_bytes), timeout=REPLY_TIMEOUT)
            try:
                ephem_pubkey, initiator_nonce, initiator_pubkey = decode_authentication(
                    msg, self.privkey
                )
            except DecryptionError as e:
                self.logger.debug("Failed to decrypt handshake: %s", e)
                return

        initiator_remote = Node(initiator_pubkey, remote_address)
        responder = HandshakeResponder(
            initiator_remote, self.privkey, got_eip8, self.cancel_token
        )

        responder_nonce = os.urandom(HASH_LEN)
        auth_ack_msg = responder.create_auth_ack_message(responder_nonce)
        auth_ack_ciphertext = responder.encrypt_auth_ack_message(auth_ack_msg)

        # Use the `writer` to send the reply to the remote
        writer.write(auth_ack_ciphertext)
        await self.wait(writer.drain())

        # Call `HandshakeResponder.derive_shared_secrets()` and use return values to create `Peer`
        aes_secret, mac_secret, egress_mac, ingress_mac = responder.derive_secrets(
            initiator_nonce=initiator_nonce,
            responder_nonce=responder_nonce,
            remote_ephemeral_pubkey=ephem_pubkey,
            auth_init_ciphertext=msg,
            auth_ack_ciphertext=auth_ack_ciphertext,
        )
        connection = PeerConnection(
            reader=reader,
            writer=writer,
            aes_secret=aes_secret,
            mac_secret=mac_secret,
            egress_mac=egress_mac,
            ingress_mac=ingress_mac,
        )

        # Create and register peer in peer_pool
        peer = self.peer_pool.get_peer_factory().create_peer(
            remote=initiator_remote, connection=connection, inbound=True
        )

        if (
            peer.remote in self.peer_pool.connected_nodes
            or peer.remote.pubkey in self.peer_pool.dialedout_pubkeys
        ):
            self.logger.debug("already connected or dialed, disconnecting...")
            await peer.disconnect(DisconnectReason.already_connected)
            return

        if self.peer_pool.is_full:
            await peer.disconnect(DisconnectReason.too_many_peers)
            return
        elif NO_SAME_IP and not self.peer_pool.is_valid_connection_candidate(
            peer.remote
        ):
            await peer.disconnect(DisconnectReason.useless_peer)
            return

        total_peers = len(self.peer_pool)
        inbound_peer_count = len(
            [peer for peer in self.peer_pool.connected_nodes.values() if peer.inbound]
        )
        if (
            total_peers > 1
            and inbound_peer_count / total_peers > self.allow_dial_in_ratio
        ):
            # make sure to have at least (1-allow_dial_in_ratio) outbound connections out of total connections
            await peer.disconnect(DisconnectReason.too_many_peers)
        else:
            # We use self.wait() here as a workaround for
            # https://github.com/ethereum/py-evm/issues/670.
            await self.wait(self.do_handshake(peer))

    async def do_handshake(self, peer: BasePeer) -> None:
        await peer.do_p2p_handshake()
        await peer.do_sub_proto_handshake()
        await self.peer_pool.start_peer(peer)
