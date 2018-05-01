from quarkchain.cluster.core import RootBlockHeader, MinorBlockHeader, RootBlock, MinorBlock
from quarkchain.core import Serializable, PreprendedSizeListSerializer
from quarkchain.core import uint16, uint32, uint128, hash256


class HelloCommand(Serializable):
    FIELDS = [
        ("version", uint32),
        ("networkId", uint32),
        ("peerId", hash256),
        ("peerIp", uint128),
        ("peerPort", uint16),
        ("shardMaskList", PreprendedSizeListSerializer(
            4, uint32)),  # TODO create shard mask object
        ("rootBlockHeader", RootBlockHeader),
    ]

    def __init__(self,
                 version,
                 networkId,
                 peerId,
                 peerIp,
                 peerPort,
                 shardMaskList,
                 rootBlockHeader):
        fields = {k: v for k, v in locals().items() if k != 'self'}
        super(type(self), self).__init__(**fields)


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


class NewMinorBlockHeaderListCommand(Serializable):
    '''RPC to inform peers about new root or minor blocks with the following constraints
    - If the RPC is sent to root, then the minor block header list must be empty.
    - If the RPC is sent to a shard, then all minor block headers must be in the shard.
    '''
    FIELDS = [
        ("rootBlockHeader", RootBlockHeader),
        ("minorBlockHeaderList", PreprendedSizeListSerializer(4, MinorBlockHeader)),
    ]

    def __init__(self, rootBlockHeader, minorBlockHeaderList):
        self.rootBlockHeader = rootBlockHeader
        self.minorBlockHeaderList = minorBlockHeaderList


class GetRootBlockListRequest(Serializable):
    ''' RPC to get a root block list.  The RPC should be only fired by root chain
    '''
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
    ''' RPCP to get a minor block list.  The RPC should be only fired by a shard, and
    all minor blocks should be from the same shard.
    '''
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
    CommandOp.GET_ROOT_BLOCK_LIST_REQUEST: GetRootBlockListRequest,
    CommandOp.GET_ROOT_BLOCK_LIST_RESPONSE: GetRootBlockListResponse,
    CommandOp.GET_PEER_LIST_REQUEST: GetPeerListRequest,
    CommandOp.GET_PEER_LIST_RESPONSE: GetPeerListResponse,
    CommandOp.GET_MINOR_BLOCK_LIST_REQUEST: GetMinorBlockListRequest,
    CommandOp.GET_MINOR_BLOCK_LIST_RESPONSE: GetMinorBlockListResponse,
}
