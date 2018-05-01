from quarkchain.core import hash256, uint16, uint32, uint64, uint128, uint256, boolean
from quarkchain.core import (
    PreprendedSizeBytesSerializer, PreprendedSizeListSerializer, Serializable, Branch, ShardMask
)

from quarkchain.cluster.core import MinorBlock, MinorBlockHeader, RootBlock, CrossShardTransactionList


# RPCs to initialize a cluster

class Ping(Serializable):
    FIELDS = [
        ("id", PreprendedSizeBytesSerializer(4)),
        ("shardMaskList", PreprendedSizeListSerializer(4, ShardMask)),
    ]

    def __init__(self, id, shardMaskList):
        """ Empty shardMaskList means root """
        if isinstance(id, bytes):
            self.id = id
        else:
            self.id = bytes(id, "ascii")
        self.shardMaskList = shardMaskList


class Pong(Serializable):
    FIELDS = [
        ("id", PreprendedSizeBytesSerializer(4)),
        ("shardMaskList", PreprendedSizeListSerializer(4, ShardMask)),
    ]

    def __init__(self, id, shardMaskList):
        """ Empty slaveId and shardMaskList means root """
        if isinstance(id, bytes):
            self.id = id
        else:
            self.id = bytes(id, "ascii")
        self.shardMaskList = shardMaskList


class SlaveInfo(Serializable):
    FIELDS = [
        ("id", PreprendedSizeBytesSerializer(4)),
        ("ip", uint128),
        ("port", uint16),
        ("shardMaskList", PreprendedSizeListSerializer(4, ShardMask)),
    ]

    def __init__(self, id, ip, port, shardMaskList):
        if isinstance(id, bytes):
            self.id = id
        else:
            self.id = bytes(id, "ascii")
        self.ip = ip
        self.port = port
        self.shardMaskList = shardMaskList


class ConnectToSlavesRequest(Serializable):
    ''' Master instructs a slave to connect to other slaves '''
    FIELDS = [
        ("slaveInfoList", PreprendedSizeListSerializer(4, SlaveInfo)),
    ]

    def __init__(self, slaveInfoList):
        self.slaveInfoList = slaveInfoList


class ConnectToSlavesResponse(Serializable):
    ''' resultList must have the same size as salveInfoList in the request.
    Empty result means success otherwise it would a serialized error message.
    '''
    FIELDS = [
        ("resultList", PreprendedSizeListSerializer(4, PreprendedSizeBytesSerializer(4))),
    ]

    def __init__(self, resultList):
        self.resultList = resultList


# RPCs to update blockchains

# master -> slave

class AddRootBlockRequest(Serializable):
    ''' Add root block to each slave
    '''
    FIELDS = [
        ("rootBlock", RootBlock),
        ("expectSwitch", boolean),
    ]

    def __init__(self, rootBlock, expectSwitch):
        self.rootBlock = rootBlock
        self.expectSwitch = expectSwitch


class AddRootBlockResponse(Serializable):
    FIELDS = [
        ("errorCode", uint32),
        ("switched", boolean),
    ]

    def __init__(self, errorCode, switched):
        self.errorCode = errorCode
        self.switched = switched


class EcoInfo(Serializable):
    ''' Necessary information for master to decide the best block to mine '''
    FIELDS = [
        ("branch", Branch),
        ("height", uint64),
        ("coinbaseAmount", uint256),
        ("difficulty", uint64),
    ]

    def __init__(self, branch, height, coinbaseAmount, difficulty):
        self.branch = branch
        self.height = height
        self.coinbaseAmount = coinbaseAmount
        self.difficulty = difficulty


class GetEcoInfoListRequest(Serializable):
    FIELDS = []

    def __init__(self):
        pass


class GetEcoInfoListResponse(Serializable):
    FIELDS = [
        ("errorCode", uint32),
        ("ecoInfoList", PreprendedSizeListSerializer(4, EcoInfo)),
    ]

    def __init__(self, errorCode, ecoInfoList):
        self.errorCode = errorCode
        self.ecoInfoList = ecoInfoList


class GetNextBlockToMineRequest(Serializable):
    FIELDS = [
        ("branch", Branch),
    ]

    def __init__(self, branch):
        self.branch = branch


class GetNextBlockToMineResponse(Serializable):
    FIELDS = [
        ("errorCode", uint32),
        ("block", MinorBlock),
    ]

    def __init__(self, errorCode, block):
        self.errorCode = errorCode
        self.block = block


# slave -> master

class AddMinorBlockHeaderRequest(Serializable):
    FIELDS = [
        ("minorBlockHeader", MinorBlockHeader),
    ]

    def __init__(self, minorBlockHeader):
        self.minorBlockHeader = minorBlockHeader


class AddMinorBlockHeaderResponse(Serializable):
    FIELDS = [
        ("errorCode", uint32),
    ]

    def __init__(self, errorCode):
        self.errorCode = errorCode


# slave -> slave

class AddXshardTxListRequest(Serializable):
    FIELDS = [
        ("branch", Branch),
        ("minorBlockHash", hash256),
        ("txList", CrossShardTransactionList),
    ]

    def __init__(self, branch, txList):
        self.branch = branch
        self.txList = txList


class AddXshardTxListResponse(Serializable):
    FIELDS = [
        ("errorCode", uint32)
    ]

    def __init__(self, errorCode):
        self.errorCode = errorCode


class ClusterOp():
    PING = 1
    PONG = 2
    CONNECT_TO_SLAVES_REQUEST = 3
    CONNECT_TO_SLAVES_RESPONSE = 4
    ADD_ROOT_BLOCK_REQUEST = 5
    ADD_ROOT_BLOCK_RESPONSE = 6
    GET_ECO_INFO_LIST_REQUEST = 7
    GET_ECO_INFO_LIST_RESPONSE = 8
    GET_NEXT_BLOCK_TO_MINE_REQUEST = 9
    GET_NEXT_BLOCK_TO_MINE_RESPONSE = 10
    ADD_MINOR_BLOCK_HEADER_REQUEST = 11
    ADD_MINOR_BLOCK_HEADER_RESPONSE = 12
    ADD_XSHARD_TX_LIST_REQUEST = 13
    ADD_XSHARD_TX_LIST_RESPONSE = 14


CLUSTER_OP_SERIALIZER_MAP = {
    ClusterOp.PING: Ping,
    ClusterOp.PONG: Pong,
    ClusterOp.CONNECT_TO_SLAVES_REQUEST: ConnectToSlavesRequest,
    ClusterOp.CONNECT_TO_SLAVES_RESPONSE: ConnectToSlavesResponse,
    ClusterOp.ADD_ROOT_BLOCK_REQUEST: AddRootBlockRequest,
    ClusterOp.ADD_ROOT_BLOCK_RESPONSE: AddRootBlockResponse,
    ClusterOp.GET_ECO_INFO_LIST_REQUEST: GetEcoInfoListRequest,
    ClusterOp.GET_ECO_INFO_LIST_RESPONSE: GetEcoInfoListResponse,
    ClusterOp.GET_NEXT_BLOCK_TO_MINE_REQUEST: GetNextBlockToMineRequest,
    ClusterOp.GET_NEXT_BLOCK_TO_MINE_RESPONSE: GetNextBlockToMineResponse,
    ClusterOp.ADD_MINOR_BLOCK_HEADER_REQUEST: AddMinorBlockHeaderRequest,
    ClusterOp.ADD_MINOR_BLOCK_HEADER_RESPONSE: AddMinorBlockHeaderResponse,
    ClusterOp.ADD_XSHARD_TX_LIST_REQUEST: AddXshardTxListRequest,
    ClusterOp.ADD_XSHARD_TX_LIST_RESPONSE: AddXshardTxListResponse,
}
