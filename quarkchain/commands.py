from quarkchain.core import Transaction, MinorBlockHeader, MinorBlock
from quarkchain.core import RootBlock, RootBlockHeader
from quarkchain.core import Serializable, PreprendedSizeListSerializer, PreprendedSizeBytesSerializer
from quarkchain.core import uint16, uint32, uint128, hash256, uint8, boolean
from quarkchain.core import random_bytes
import ipaddress


class HelloCommand(Serializable):
    FIELDS = [
        ("version", uint32),
        ("networkId", uint32),
        ("peerId", hash256),
        ("peerIp", uint128),
        ("peerPort", uint16),
        ("shardMaskList", PreprendedSizeListSerializer(
            4, uint32)),  # TODO create shard mask object
        ("rootBlockHeader", RootBlockHeader)
    ]

    def __init__(self,
                 version=0,
                 networkId=0,
                 peerId=bytes(32),
                 peerIp=int(ipaddress.ip_address("127.0.0.1")),
                 peerPort=38291,
                 shardMaskList=None,
                 rootBlockHeader=None):
        shardMaskList = shardMaskList if shardMaskList is not None else []
        rootBlockHeader = rootBlockHeader if rootBlockHeader is not None else []
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)


class NewMinorBlockHeaderListCommand(Serializable):
    FIELDS = [
        ("rootBlockHeader", RootBlockHeader),
        ("minorBlockHeaderList", PreprendedSizeListSerializer(4, MinorBlockHeader)),
    ]

    def __init__(self, rootBlockHeader, minorBlockHeaderList):
        self.rootBlockHeader = rootBlockHeader
        self.minorBlockHeaderList = minorBlockHeaderList


class NewTransactionListCommand(Serializable):
    FIELDS = [
        ("transactionList", PreprendedSizeListSerializer(4, Transaction))
    ]

    def __init__(self, transactionList=None):
        self.transactionList = transactionList if transactionList is not None else []


class GetRootBlockListRequest(Serializable):
    FIELDS = [
        ("rootBlockHashList", PreprendedSizeListSerializer(4, hash256))
    ]

    def __init__(self, rootBlockHashList=None):
        self.rootBlockHashList = rootBlockHashList if rootBlockHashList is not None else []


class GetRootBlockListResponse(Serializable):
    FIELDS = [
        ("rootBlockList", PreprendedSizeListSerializer(4, RootBlock))
    ]

    def __init__(self, rootBlockList=None):
        self.rootBlockList = rootBlockList if rootBlockList is not None else []


class GetMinorBlockListRequest(Serializable):
    FIELDS = [
        ("minorBlockHashList", PreprendedSizeListSerializer(4, hash256))
    ]

    def __init__(self, minorBlockHashList=None):
        self.minorBlockHashList = minorBlockHashList if minorBlockHashList is not None else []


class GetMinorBlockListResponse(Serializable):
    FIELDS = [
        ("minorBlockList", PreprendedSizeListSerializer(4, MinorBlock))
    ]

    def __init__(self, minorBlockList=None):
        self.minorBlockList = minorBlockList if minorBlockList is not None else []


class GetBlockHeaderListRequest(Serializable):
    FIELDS = [
        ("blockHash", hash256),
        ("maxBlocks", uint32),
        ("direction", uint8),       # 0 to genesis, 1 to tip
    ]

    def __init__(self, blockHash, maxBlocks, direction):
        self.blockHash = blockHash
        self.maxBlocks = maxBlocks
        self.direction = direction


class GetBlockHeaderListResponse(Serializable):
    FIELDS = [
        ("isRoot", boolean),
        ("blockHeaderList", PreprendedSizeListSerializer(4, PreprendedSizeBytesSerializer))
    ]

    def __init__(self, isRoot, blockHeaderList):
        self.isRoot = isRoot
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
        ("peerInfoList", PreprendedSizeListSerializer(4, PeerInfo))
    ]

    def __init__(self, peerInfoList=None):
        self.peerInfoList = peerInfoList if peerInfoList is not None else []


class NewBlockCommand(Serializable):
    FIELDS = [
        ("isRootBlock", boolean),
        ("blockData", PreprendedSizeBytesSerializer(4))
    ]

    def __init__(self, isRootBlock, blockData):
        self.isRootBlock = isRootBlock
        self.blockData = blockData


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
    NEW_BLOCK_COMMAND = 11


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
    CommandOp.NEW_BLOCK_COMMAND: NewBlockCommand,
}
