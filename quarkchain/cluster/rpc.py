from quarkchain.core import (
    PreprendedSizeBytesSerializer, PreprendedSizeListSerializer, Serializable, uint16, uint32, uint128
)


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


class ClusterOp():
    PING = 1
    PONG = 2
    CONNECT_TO_SLAVES_REQUEST = 3
    CONNECT_TO_SLAVES_RESPONSE = 4


CLUSTER_OP_SERIALIZER_MAP = {
    ClusterOp.PING: Ping,
    ClusterOp.PONG: Pong,
    ClusterOp.CONNECT_TO_SLAVES_REQUEST: ConnectToSlavesRequest,
    ClusterOp.CONNECT_TO_SLAVES_RESPONSE: ConnectToSlavesResponse,
}