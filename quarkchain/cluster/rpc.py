from quarkchain.core import hash256, uint16, uint32, uint128, boolean
from quarkchain.core import (
    PreprendedSizeBytesSerializer, PreprendedSizeListSerializer, Serializable, Branch
)

from quarkchin.cluster.core import RootBlock, CrossShardTransactionList


# RPCs to initialize a cluster

class Ping(Serializable):
    FIELDS = [
        ("id", PreprendedSizeBytesSerializer(4)),
        ("shardMaskList", PreprendedSizeListSerializer(4, uint32)),
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
        ("shardMaskList", PreprendedSizeListSerializer(4, uint32)),
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
        ("shardMaskList", PreprendedSizeListSerializer(4, uint32)),
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

# slave -> master

# TODO: AddMinorBlockHeader


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
    ADD_XSHARD_TX_LIST_REQUEST = 7
    ADD_XSHARD_TX_LIST_RESPONSE = 8


CLUSTER_OP_SERIALIZER_MAP = {
    ClusterOp.PING: Ping,
    ClusterOp.PONG: Pong,
    ClusterOp.CONNECT_TO_SLAVES_REQUEST: ConnectToSlavesRequest,
    ClusterOp.CONNECT_TO_SLAVES_RESPONSE: ConnectToSlavesResponse,
    ClusterOp.ADD_ROOT_BLOCK_REQUEST: AddRootBlockRequest,
    ClusterOp.ADD_ROOT_BLOCK_RESPONSE: AddRootBlockResponse,
    ClusterOp.ADD_XSHARD_TX_LIST_REQUEST: AddXshardTxListRequest,
    ClusterOp.ADD_XSHARD_TX_LIST_RESPONSE: AddXshardTxListResponse,
}
