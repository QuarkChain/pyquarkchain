from enum import IntEnum
from quarkchain.core import Transaction, MinorBlockHeader, MinorBlock
from quarkchain.core import RootBlock, RootBlockHeader
from quarkchain.core import Serializable, PrependedSizeListSerializer, PrependedSizeBytesSerializer
from quarkchain.core import uint16, uint32, uint128, hash256, uint8, boolean


class HelloCommand(Serializable):
    FIELDS = [
        ("version", uint32),
        ("networkId", uint32),
        ("peerId", hash256),
        ("peerIp", uint128),
        ("peerPort", uint16),
        ("shardMaskList", PrependedSizeListSerializer(
            4, uint32)),  # TODO create shard mask object
        ("rootBlockHeader", RootBlockHeader),
        ("minorBlockHeaderList", PrependedSizeListSerializer(4, MinorBlockHeader)),
    ]

    def __init__(self,
                 version,
                 networkId,
                 peerId,
                 peerIp,
                 peerPort,
                 shardMaskList,
                 rootBlockHeader,
                 minorBlockHeaderList):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)


class NewMinorBlockHeaderListCommand(Serializable):
    '''RPC to inform peers about new root or minor blocks
    '''
    FIELDS = [
        ("rootBlockHeader", RootBlockHeader),
        ("minorBlockHeaderList", PrependedSizeListSerializer(4, MinorBlockHeader)),
    ]

    def __init__(self, rootBlockHeader, minorBlockHeaderList):
        self.rootBlockHeader = rootBlockHeader
        self.minorBlockHeaderList = minorBlockHeaderList


class NewTransaction(Serializable):
    FIELDS = [
        ("shardId", uint32),
        ("transaction", Transaction),
    ]

    def __init__(self, shardId, transaction):
        """ Negative shardId indicates unknown shard (not support yet)
        """
        self.shardId = shardId
        self.transaction = transaction


class NewTransactionListCommand(Serializable):
    FIELDS = [
        ("transactionList", PrependedSizeListSerializer(4, NewTransaction))
    ]

    def __init__(self, transactionList=None):
        self.transactionList = transactionList if transactionList is not None else []


class GetRootBlockListRequest(Serializable):
    FIELDS = [
        ("rootBlockHashList", PrependedSizeListSerializer(4, hash256))
    ]

    def __init__(self, rootBlockHashList=None):
        self.rootBlockHashList = rootBlockHashList if rootBlockHashList is not None else []


class GetRootBlockListResponse(Serializable):
    FIELDS = [
        ("rootBlockList", PrependedSizeListSerializer(4, RootBlock))
    ]

    def __init__(self, rootBlockList=None):
        self.rootBlockList = rootBlockList if rootBlockList is not None else []


class GetMinorBlockListRequest(Serializable):
    FIELDS = [
        ("minorBlockHashList", PrependedSizeListSerializer(4, hash256))
    ]

    def __init__(self, minorBlockHashList=None):
        self.minorBlockHashList = minorBlockHashList if minorBlockHashList is not None else []


class GetMinorBlockListResponse(Serializable):
    FIELDS = [
        ("minorBlockList", PrependedSizeListSerializer(4, MinorBlock))
    ]

    def __init__(self, minorBlockList=None):
        self.minorBlockList = minorBlockList if minorBlockList is not None else []


class Direction(IntEnum):
    GENESIS = 0
    TIP = 1


class GetBlockHeaderListRequest(Serializable):
    """ Obtain block hashs in the active chain.
    """
    FIELDS = [
        ("blockHash", hash256),
        ("isRoot", boolean),
        ("shardId", uint32),
        ("maxBlocks", uint32),
        ("direction", uint8),       # 0 to genesis, 1 to tip
    ]

    def __init__(self, blockHash, isRoot, shardId, maxBlocks, direction):
        self.blockHash = blockHash
        self.isRoot = isRoot
        self.shardId = shardId
        self.maxBlocks = maxBlocks
        self.direction = direction


class GetBlockHeaderListResponse(Serializable):
    FIELDS = [
        ("rootTip", RootBlockHeader),
        ("shardTip", MinorBlockHeader),
        ("blockHeaderList", PrependedSizeListSerializer(4, PrependedSizeBytesSerializer(4)))
    ]

    def __init__(self, rootTip, shardTip, blockHeaderList):
        self.rootTip = rootTip
        self.shardTip = shardTip
        self.blockHeaderList = blockHeaderList


class GetPeerListRequest(Serializable):
    FIELDS = [
        ("maxPeers", uint32),
    ]

    def __init__(self, maxPeers):
        self.maxPeers = maxPeers


class PeerInfo(Serializable):
    FIELDS = [
        ("ip", uint128),
        ("port", uint16),
    ]

    def __init__(self, ip, port):
        self.ip = ip
        self.port = port


class GetPeerListResponse(Serializable):
    FIELDS = [
        ("peerInfoList", PrependedSizeListSerializer(4, PeerInfo))
    ]

    def __init__(self, peerInfoList=None):
        self.peerInfoList = peerInfoList if peerInfoList is not None else []


class CommandOp():
    HELLO = 0
    NEW_MINOR_BLOCK_HEADER_LIST = 1
    NEW_TRANSACTION_LIST = 2
    GET_ROOT_BLOCK_LIST_REQUEST = 3
    GET_ROOT_BLOCK_LIST_RESPONSE = 4
    GET_PEER_LIST_REQUEST = 5
    GET_PEER_LIST_RESPONSE = 6
    GET_MINOR_BLOCK_LIST_REQUEST = 7
    GET_MINOR_BLOCK_LIST_RESPONSE = 8
    GET_BLOCK_HEADER_LIST_REQUEST = 9
    GET_BLOCK_HEADER_LIST_RESPONSE = 10


OP_SERIALIZER_MAP = {
    CommandOp.HELLO: HelloCommand,
    CommandOp.NEW_MINOR_BLOCK_HEADER_LIST: NewMinorBlockHeaderListCommand,
    CommandOp.NEW_TRANSACTION_LIST: NewTransactionListCommand,
    CommandOp.GET_ROOT_BLOCK_LIST_REQUEST: GetRootBlockListRequest,
    CommandOp.GET_ROOT_BLOCK_LIST_RESPONSE: GetRootBlockListResponse,
    CommandOp.GET_PEER_LIST_REQUEST: GetPeerListRequest,
    CommandOp.GET_PEER_LIST_RESPONSE: GetPeerListResponse,
    CommandOp.GET_MINOR_BLOCK_LIST_REQUEST: GetMinorBlockListRequest,
    CommandOp.GET_MINOR_BLOCK_LIST_RESPONSE: GetMinorBlockListResponse,
    CommandOp.GET_BLOCK_HEADER_LIST_REQUEST: GetBlockHeaderListRequest,
    CommandOp.GET_BLOCK_HEADER_LIST_RESPONSE: GetBlockHeaderListResponse,
}
