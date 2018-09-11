from enum import IntEnum

from quarkchain.core import Branch, uint8, uint16, uint32, uint128, hash256, Transaction
from quarkchain.core import RootBlockHeader, MinorBlockHeader, RootBlock, MinorBlock
from quarkchain.core import Serializable, PrependedSizeListSerializer


class HelloCommand(Serializable):
    FIELDS = [
        ("version", uint32),
        ("network_id", uint32),
        ("peer_id", hash256),
        ("peer_ip", uint128),
        ("peer_port", uint16),
        (
            "shard_mask_list",
            PrependedSizeListSerializer(4, uint32),
        ),  # TODO create shard mask object
        ("root_block_header", RootBlockHeader),
    ]

    def __init__(
        self,
        version,
        network_id,
        peer_id,
        peer_ip,
        peer_port,
        shard_mask_list,
        root_block_header,
    ):
        fields = {k: v for k, v in locals().items() if k != "self"}
        super(type(self), self).__init__(**fields)


class GetPeerListRequest(Serializable):
    FIELDS = [("max_peers", uint32)]

    def __init__(self, max_peers):
        self.max_peers = max_peers


class PeerInfo(Serializable):
    FIELDS = [("ip", uint128), ("port", uint16)]

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port


class GetPeerListResponse(Serializable):
    FIELDS = [("peer_info_list", PrependedSizeListSerializer(4, PeerInfo))]

    def __init__(self, peer_info_list=None):
        self.peer_info_list = peer_info_list if peer_info_list is not None else []


class NewMinorBlockHeaderListCommand(Serializable):
    """RPC to inform peers about new root or minor blocks with the following constraints
    - If the RPC is sent to root, then the minor block header list must be empty.
    - If the RPC is sent to a shard, then all minor block headers must be in the shard.
    """

    FIELDS = [
        ("root_block_header", RootBlockHeader),
        ("minor_block_header_list", PrependedSizeListSerializer(4, MinorBlockHeader)),
    ]

    def __init__(self, root_block_header, minor_block_header_list):
        self.root_block_header = root_block_header
        self.minor_block_header_list = minor_block_header_list


class NewTransactionListCommand(Serializable):
    """ Broadcast transactions """

    FIELDS = [("transaction_list", PrependedSizeListSerializer(4, Transaction))]

    def __init__(self, transaction_list=None):
        self.transaction_list = transaction_list if transaction_list is not None else []


class Direction(IntEnum):
    GENESIS = 0
    TIP = 1


class GetRootBlockHeaderListRequest(Serializable):
    """ Obtain block hashs in the active chain.
    """

    FIELDS = [
        ("block_hash", hash256),
        ("limit", uint32),
        ("direction", uint8),  # 0 to genesis, 1 to tip
    ]

    def __init__(self, block_hash, limit, direction):
        self.block_hash = block_hash
        self.limit = limit
        self.direction = direction


class GetRootBlockHeaderListResponse(Serializable):
    FIELDS = [
        ("root_tip", RootBlockHeader),
        ("block_header_list", PrependedSizeListSerializer(4, RootBlockHeader)),
    ]

    def __init__(self, root_tip, block_header_list):
        self.root_tip = root_tip
        self.block_header_list = block_header_list


class GetRootBlockListRequest(Serializable):
    """ RPC to get a root block list.  The RPC should be only fired by root chain
    """

    FIELDS = [("root_block_hash_list", PrependedSizeListSerializer(4, hash256))]

    def __init__(self, root_block_hash_list=None):
        self.root_block_hash_list = (
            root_block_hash_list if root_block_hash_list is not None else []
        )


class GetRootBlockListResponse(Serializable):
    FIELDS = [("root_block_list", PrependedSizeListSerializer(4, RootBlock))]

    def __init__(self, root_block_list=None):
        self.root_block_list = root_block_list if root_block_list is not None else []


class GetMinorBlockListRequest(Serializable):
    """ RPCP to get a minor block list.  The RPC should be only fired by a shard, and
    all minor blocks should be from the same shard.
    """

    FIELDS = [("minor_block_hash_list", PrependedSizeListSerializer(4, hash256))]

    def __init__(self, minor_block_hash_list=None):
        self.minor_block_hash_list = (
            minor_block_hash_list if minor_block_hash_list is not None else []
        )


class GetMinorBlockListResponse(Serializable):
    FIELDS = [("minor_block_list", PrependedSizeListSerializer(4, MinorBlock))]

    def __init__(self, minor_block_list=None):
        self.minor_block_list = minor_block_list if minor_block_list is not None else []


class GetMinorBlockHeaderListRequest(Serializable):
    """ Obtain block hashs in the active chain.
    """

    FIELDS = [
        ("block_hash", hash256),
        ("branch", Branch),
        ("limit", uint32),
        ("direction", uint8),  # 0 to genesis, 1 to tip
    ]

    def __init__(self, block_hash, branch, limit, direction):
        self.block_hash = block_hash
        self.branch = branch
        self.limit = limit
        self.direction = direction


class GetMinorBlockHeaderListResponse(Serializable):
    FIELDS = [
        ("root_tip", RootBlockHeader),
        ("shard_tip", MinorBlockHeader),
        ("block_header_list", PrependedSizeListSerializer(4, MinorBlockHeader)),
    ]

    def __init__(self, root_tip, shard_tip, block_header_list):
        self.root_tip = root_tip
        self.shard_tip = shard_tip
        self.block_header_list = block_header_list


class NewBlockMinorCommand(Serializable):
    FIELDS = [("block", MinorBlock)]

    def __init__(self, block):
        self.block = block


class CommandOp:
    HELLO = 0
    NEW_MINOR_BLOCK_HEADER_LIST = 1
    NEW_TRANSACTION_LIST = 2
    GET_PEER_LIST_REQUEST = 3
    GET_PEER_LIST_RESPONSE = 4
    GET_ROOT_BLOCK_HEADER_LIST_REQUEST = 5
    GET_ROOT_BLOCK_HEADER_LIST_RESPONSE = 6
    GET_ROOT_BLOCK_LIST_REQUEST = 7
    GET_ROOT_BLOCK_LIST_RESPONSE = 8
    GET_MINOR_BLOCK_LIST_REQUEST = 9
    GET_MINOR_BLOCK_LIST_RESPONSE = 10
    GET_MINOR_BLOCK_HEADER_LIST_REQUEST = 11
    GET_MINOR_BLOCK_HEADER_LIST_RESPONSE = 12
    NEW_BLOCK_MINOR = 13


OP_SERIALIZER_MAP = {
    CommandOp.HELLO: HelloCommand,
    CommandOp.NEW_MINOR_BLOCK_HEADER_LIST: NewMinorBlockHeaderListCommand,
    CommandOp.NEW_TRANSACTION_LIST: NewTransactionListCommand,
    CommandOp.GET_PEER_LIST_REQUEST: GetPeerListRequest,
    CommandOp.GET_PEER_LIST_RESPONSE: GetPeerListResponse,
    CommandOp.GET_ROOT_BLOCK_HEADER_LIST_REQUEST: GetRootBlockHeaderListRequest,
    CommandOp.GET_ROOT_BLOCK_HEADER_LIST_RESPONSE: GetRootBlockHeaderListResponse,
    CommandOp.GET_ROOT_BLOCK_LIST_REQUEST: GetRootBlockListRequest,
    CommandOp.GET_ROOT_BLOCK_LIST_RESPONSE: GetRootBlockListResponse,
    CommandOp.GET_MINOR_BLOCK_LIST_REQUEST: GetMinorBlockListRequest,
    CommandOp.GET_MINOR_BLOCK_LIST_RESPONSE: GetMinorBlockListResponse,
    CommandOp.GET_MINOR_BLOCK_HEADER_LIST_REQUEST: GetMinorBlockHeaderListRequest,
    CommandOp.GET_MINOR_BLOCK_HEADER_LIST_RESPONSE: GetMinorBlockHeaderListResponse,
    CommandOp.NEW_BLOCK_MINOR: NewBlockMinorCommand,
}
