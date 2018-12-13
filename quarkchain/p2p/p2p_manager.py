import ipaddress
import socket
from cryptography.hazmat.primitives.constant_time import bytes_eq
from eth_keys import keys
from typing import Tuple, Dict

from quarkchain.utils import Logger
from quarkchain.cluster.simple_network import Peer, AbstractNetwork
from quarkchain.protocol import AbstractConnection, Connection, ConnectionState
from quarkchain.cluster.p2p_commands import CommandOp
from quarkchain.p2p import ecies
from quarkchain.p2p.cancel_token.token import CancelToken
from quarkchain.p2p.kademlia import Node
from quarkchain.p2p.p2p_server import BaseServer
from quarkchain.p2p.peer import BasePeer, BasePeerContext, BasePeerPool, BasePeerFactory
from quarkchain.p2p.protocol import Command, _DecodedMsgType, NULL_BYTE, Protocol
from .constants import HEADER_LEN, MAC_LEN
from quarkchain.p2p.exceptions import (
    DecryptionError,
    PeerConnectionLost,
    HandshakeFailure,
)
from quarkchain.p2p.utils import sxor


class QuarkProtocol(Protocol):
    name = "quarkchain"
    # TODO use self.env.quark_chain_config.P2P_PROTOCOL_VERSION
    version = 1


def encode_bytes(data: bytes) -> Tuple[bytes, bytes]:
    """
    a modified version of rlpx framed protocol
    returns header, body
    header = len(body) as 4-byte big-endian integer, pad to 16 bytes
    body = data
    """
    frame_size = len(data)
    if frame_size.bit_length() > 32:
        raise ValueError("Frame size has to fit in a 4-byte integer")
    header = len(data).to_bytes(4, byteorder="big").ljust(HEADER_LEN, NULL_BYTE)
    return header, data


class QuarkPeer(BasePeer):
    """
    keep secure handshake of BasePeer, but override all wire protocols.
    specifically, _run() does not call read_msg() which interpret bytes as RLPx;
    instead, we use QuarkChain-specific wire protocol
    """

    env = None

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
        """ overrides BasePeer.do_sub_proto_handshake()
        """
        self.secure_peer = SecurePeer(self)
        Logger.info("starting peer hello exchange")
        start_state = await self.secure_peer.start()
        if start_state:
            # returns None if successful
            raise HandshakeFailure(
                "hello message exchange failed: {}".format(start_state)
            )

    def decrypt_raw_bytes(self, data: bytes, size: int) -> bytes:
        """
        same as decrypt_body() but no roundup
        """
        if len(data) < size + MAC_LEN:
            raise ValueError(
                "Insufficient body length; Got {}, wanted {} + {}".format(
                    len(data), size, MAC_LEN
                )
            )

        frame_ciphertext = data[:size]
        frame_mac = data[size : size + MAC_LEN]

        self.ingress_mac.update(frame_ciphertext)
        fmac_seed = self.ingress_mac.digest()[:MAC_LEN]
        self.ingress_mac.update(sxor(self.mac_enc(fmac_seed), fmac_seed))
        expected_frame_mac = self.ingress_mac.digest()[:MAC_LEN]
        if not bytes_eq(expected_frame_mac, frame_mac):
            raise DecryptionError(
                "Invalid frame mac: expected {}, got {}".format(
                    expected_frame_mac, frame_mac
                )
            )
        return self.aes_dec.update(frame_ciphertext)[:size]

    def send_raw_bytes(self, data: bytes) -> None:
        header, body = encode_bytes(data)
        self.writer.write(self.encrypt(header, body))

    async def read_raw_bytes(self, timeout) -> bytes:
        header_data = await self.read(HEADER_LEN + MAC_LEN, timeout=timeout)
        header = self.decrypt_header(header_data)
        frame_size = int.from_bytes(header[:4], byteorder="big")
        if frame_size > self.env.quark_chain_config.P2P_COMMAND_SIZE_LIMIT:
            raise RuntimeError("{} command package exceed limit".format(self))
        frame_data = await self.read(frame_size + MAC_LEN, timeout=timeout)
        msg = self.decrypt_raw_bytes(frame_data, frame_size)
        return msg

    async def _run(self) -> None:
        """
        overrides BasePeer._run()
        forwards decrypted messages to QuarkChain Peer
        """
        self.run_child_service(self.boot_manager)
        self.secure_peer.add_sync_task()
        if self.secure_peer.state == ConnectionState.CONNECTING:
            self.secure_peer.state = ConnectionState.ACTIVE
            self.secure_peer.active_future.set_result(None)
        try:
            while self.is_operational:
                metadata, raw_data = await self.secure_peer.read_metadata_and_raw_data()
                self.run_task(
                    self.secure_peer.secure_handle_metadata_and_raw_data(
                        metadata, raw_data
                    )
                )
        except (PeerConnectionLost, TimeoutError) as err:
            self.logger.debug(
                "%s stopped responding (%r), disconnecting", self.remote, err
            )
        except DecryptionError as err:
            self.logger.warning(
                "Unable to decrypt message from %s, disconnecting: %r", self.remote, err
            )
        except Exception as e:
            self.logger.error("Unknown exception from %s, message: %r", self.remote, e)
            Logger.error_exception()
        self.secure_peer.abort_in_flight_rpcs()
        self.secure_peer.close()


class SecurePeer(Peer):
    """
    keep all wire-level functions (especially for proxy/forwarding-related transports),
    but delegate all StreamReader/Writer operations to QuarkPeer
    ** overrides **
    Peer.start
    Peer.close
    Peer.handle_get_peer_list_request
    Connection.read_metadata_and_raw_data
    Connection.write_raw_data
    ** unchanged **
    Peer.close_with_error
    Peer.get_cluster_peer_id
    Peer.get_connection_to_forward
    Peer.handle_error
    Peer.handle_get_root_block_header_list_request
    Peer.handle_get_root_block_list_request
    Peer.handle_new_minor_block_header_list
    Peer.handle_new_transaction_list
    Peer.send_hello
    Peer.send_transaction
    Peer.send_updated_tip
    AbstractConnection.__handle_request
    AbstractConnection.__handle_rpc_request
    AbstractConnection.__parse_command
    AbstractConnection.__write_rpc_response
    AbstractConnection.read_command
    AbstractConnection.validate_and_update_peer_rpc_id
    AbstractConnection.write_command
    AbstractConnection.write_raw_command
    AbstractConnection.write_rpc_request
    P2PConnection.get_metadata_to_forward
    P2PConnection.validate_connection
    ProxyConnection.handle_metadata_and_raw_data
    ** unused **
    Peer.close_dead_peer
    AbstractConnection.__get_next_connection_id
    AbstractConnection.__internal_handle_metadata_and_raw_data
    Connection.__read_fully
    AbstractConnection.active_and_loop_forever
    ProxyConnection.close_connection
    AbstractConnection.is_active
    AbstractConnection.is_closed
    AbstractConnection.loop_once
    AbstractConnection.wait_until_active
    AbstractConnection.wait_until_closed
    """

    # Singletons will be set as class variable
    env = None
    network = None
    master_server = None

    def __init__(self, quark_peer: QuarkPeer):
        cluster_peer_id = quark_peer.remote.id % 2 ** 64
        super().__init__(
            env=self.env,
            reader=None,
            writer=None,
            network=self.network,
            master_server=self.master_server,
            cluster_peer_id=cluster_peer_id,
            name=repr(quark_peer),
        )
        self.quark_peer = quark_peer

    async def start(self) -> str:
        """ Override Peer.start()
        exchange hello command, establish cluster connections in master
        """
        self.send_hello()

        op, cmd, rpc_id = await self.read_command()
        if op is None:
            assert self.state == ConnectionState.CLOSED
            Logger.info("Failed to read command, peer may have closed connection")
            return "Failed to read command"

        if op != CommandOp.HELLO:
            return self.close_with_error("Hello must be the first command")

        if cmd.version != self.env.quark_chain_config.P2P_PROTOCOL_VERSION:
            return self.close_with_error("incompatible protocol version")

        if cmd.network_id != self.env.quark_chain_config.NETWORK_ID:
            return self.close_with_error("incompatible network id")

        self.id = cmd.peer_id
        self.shard_mask_list = cmd.shard_mask_list
        # ip is from peer.remote, there may be 2 cases:
        #  1. dialed-out: ip is from discovery service;
        #  2. dialed-in: ip is from writer.get_extra_info("peername")
        self.ip = ipaddress.ip_address(self.quark_peer.remote.address.ip)
        # port is what peer claim to be using
        self.port = cmd.peer_port

        Logger.info(
            "Got HELLO from peer {} ({}:{})".format(self.quark_peer, self.ip, self.port)
        )

        if (
            cmd.root_block_header.shard_info.get_shard_size()
            != self.env.quark_chain_config.SHARD_SIZE
        ):
            return self.close_with_error(
                "Shard size from root block header does not match local"
            )

        self.best_root_block_header_observed = cmd.root_block_header

        await self.master_server.create_peer_cluster_connections(self.cluster_peer_id)
        Logger.info(
            "Established virtual shard connections with peer {}".format(self.id.hex())
        )

    def add_sync_task(self):
        self.master_server.handle_new_root_block_header(
            self.best_root_block_header_observed, self
        )

    def abort_in_flight_rpcs(self):
        for rpc_id, future in self.rpc_future_map.items():
            future.set_exception(RuntimeError("{}: connection abort".format(self.name)))
        AbstractConnection.aborted_rpc_count += len(self.rpc_future_map)
        self.rpc_future_map.clear()

    def write_raw_data(self, metadata, raw_data):
        """ Override Connection.write_raw_data()
        """
        # NOTE QuarkChain serialization returns bytearray
        self.quark_peer.send_raw_bytes(bytes(metadata.serialize() + raw_data))

    async def read_metadata_and_raw_data(self):
        """ Override Connection.read_metadata_and_raw_data()
        """
        data = await self.quark_peer.read_raw_bytes(timeout=None)
        metadata_bytes = data[: self.metadata_class.get_byte_size()]
        metadata = self.metadata_class.deserialize(metadata_bytes)
        return metadata, data[self.metadata_class.get_byte_size() :]

    def close(self):
        if self.state == ConnectionState.ACTIVE:
            Logger.info(
                "destroying proxy slave connections for {}".format(
                    self.quark_peer.remote
                )
            )
            self.master_server.destroy_peer_cluster_connections(self.cluster_peer_id)
        super(Connection, self).close()
        self.quark_peer.close()

    async def handle_get_peer_list_request(self, request):
        """ shall not handle this request for a real p2p network
        """
        pass

    async def secure_handle_metadata_and_raw_data(self, metadata, raw_data):
        """ same as __internal_handle_metadata_and_raw_data but callable
        """
        try:
            await self.handle_metadata_and_raw_data(metadata, raw_data)
        except Exception as e:
            Logger.log_exception()
            self.close_with_error(
                "{}: error processing request: {}".format(self.name, e)
            )


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
    """
    a network based on QuarkServer, need the following members for peer conn to work:
    network.self_id
    network.ip
    network.port
    """

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

        if env.cluster_config.P2P.BOOT_NODES:
            preferred_nodes = env.cluster_config.P2P.PREFERRED_NODES.split(",")
        else:
            preferred_nodes = []

        self.server = QuarkServer(
            privkey=privkey,
            port=env.cluster_config.P2P_PORT,
            network_id=env.quark_chain_config.NETWORK_ID,
            bootstrap_nodes=tuple([Node.from_uri(enode) for enode in bootstrap_nodes]),
            preferred_nodes=[Node.from_uri(enode) for enode in preferred_nodes],
            token=self.cancel_token,
            max_peers=env.cluster_config.P2P.MAX_PEERS,
            upnp=env.cluster_config.P2P.UPNP,
            allow_dial_in_ratio=env.cluster_config.P2P.ALLOW_DIAL_IN_RATIO,
        )

        QuarkPeer.env = env
        SecurePeer.env = env
        SecurePeer.network = self
        SecurePeer.master_server = master_server

        # used in HelloCommand.peer_id which is hash256
        self.self_id = privkey.public_key.to_bytes()[:32]
        self.ip = ipaddress.ip_address(socket.gethostbyname(socket.gethostname()))
        self.port = env.cluster_config.P2P_PORT

    def start(self) -> None:
        self.loop.create_task(self.server.run())

    def iterate_peers(self):
        return [p.secure_peer for p in self.server.peer_pool.connected_nodes.values()]

    @property
    def active_peer_pool(self) -> Dict[bytes, Peer]:
        """ for jrpc and stat reporting
        """
        return {
            p.secure_peer.id: p.secure_peer
            for p in self.server.peer_pool.connected_nodes.values()
        }

    def get_peer_by_cluster_peer_id(self, cluster_peer_id):
        quark_peer = self.server.peer_pool.cluster_peer_map.get(cluster_peer_id)
        if quark_peer:
            return quark_peer.secure_peer
        return None
