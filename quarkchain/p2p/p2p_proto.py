import enum
import sys
from typing import cast, Dict

from toolz import assoc

import rlp
from rlp import sedes

from quarkchain.p2p.exceptions import MalformedMessage
from quarkchain.p2p.protocol import Command, Protocol, _DecodedMsgType


class Hello(Command):
    _cmd_id = 0
    decode_strict = False
    structure = [
        ("version", sedes.big_endian_int),
        ("client_version_string", sedes.text),
        (
            "capabilities",
            sedes.CountableList(sedes.List([sedes.text, sedes.big_endian_int])),
        ),
        ("listen_port", sedes.big_endian_int),
        ("remote_pubkey", sedes.binary),
    ]


@enum.unique
class DisconnectReason(enum.Enum):
    """More details at https://github.com/ethereum/wiki/wiki/%C3%90%CE%9EVp2p-Wire-Protocol#p2p"""

    disconnect_requested = 0
    tcp_sub_system_error = 1
    bad_protocol = 2
    useless_peer = 3
    too_many_peers = 4
    already_connected = 5
    incompatible_p2p_version = 6
    null_node_identity_received = 7
    client_quitting = 8
    unexpected_identity = 9
    connected_to_self = 10
    timeout = 11
    subprotocol_error = 16


class Disconnect(Command):
    _cmd_id = 1
    structure = [("reason", sedes.big_endian_int)]

    def get_reason_name(self, reason_id: int) -> str:
        try:
            return DisconnectReason(reason_id).name
        except ValueError:
            return "unknown reason"

    def decode(self, data: bytes) -> _DecodedMsgType:
        try:
            raw_decoded = cast(Dict[str, int], super().decode(data))
        except rlp.exceptions.ListDeserializationError:
            self.logger.warning("Malformed Disconnect message: %s", data)
            raise MalformedMessage("Malformed Disconnect message: {}".format(data))
        return assoc(
            raw_decoded, "reason_name", self.get_reason_name(raw_decoded["reason"])
        )


class Ping(Command):
    _cmd_id = 2


class Pong(Command):
    _cmd_id = 3


def construct_quark_chain_client_identifier() -> str:
    __version__ = 2.0
    """
    Constructs the client identifier string

    e.g. 'QuarkChain/v1.2.3/darwin-amd64/python3.6.5'
    """
    return "QuarkChain/{0}/{platform}/{imp.name}{v.major}.{v.minor}.{v.micro}".format(
        __version__,
        platform=sys.platform,
        v=sys.version_info,
        # mypy Doesn't recognize the `sys` module as having an `implementation` attribute.
        imp=sys.implementation,  # type: ignore
    )


class P2PProtocol(Protocol):
    name = "p2p"
    version = 4
    _commands = [Hello, Ping, Pong, Disconnect]
    cmd_length = 16

    def __init__(self, peer) -> None:
        # For the base protocol the cmd_id_offset is always 0.
        super().__init__(peer, cmd_id_offset=0)

    def send_handshake(self) -> None:
        data = dict(
            version=self.version,
            client_version_string=construct_quark_chain_client_identifier(),
            capabilities=self.peer.capabilities,
            listen_port=self.peer.listen_port,
            remote_pubkey=self.peer.privkey.public_key.to_bytes(),
        )
        header, body = Hello(self.cmd_id_offset).encode(data)
        self.send(header, body)

    def send_disconnect(self, reason: DisconnectReason) -> None:
        header, body = Disconnect(self.cmd_id_offset).encode(dict(reason=reason))
        self.send(header, body)

    def send_pong(self) -> None:
        header, body = Pong(self.cmd_id_offset).encode({})
        self.send(header, body)
