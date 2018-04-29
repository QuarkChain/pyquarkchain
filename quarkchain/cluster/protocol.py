from quarkchain.core import hash256, Branch
from quarkchain.protocol import Connection
from quarkchain.protocol import Metadata
from quarkchain.utils import check

ROOT_SHARD_ID = 0


class ProxyConnection(Connection):
    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop=None, metadataClass=None):
        super().__init__(env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop, metadataClass)

    def getConnectionToForward(self, metadata):
        ''' Returns the Connection object to forward a request for metadata.
        Returns None if the request should not be forwarded for metadata.

        Subclass should override this if the branch is on another node and this node
        will forward the request without deserialize the request.

        metadata is a uint32 having shard size and shard id encoded as
            shardSize | shardId
        See the definition for Branch in core.py.
        0 represents the root chain.

        For example:
        Assuming requests are sent to shards and master does the forwarding.

        1. slave -> master -> peer
            For requests coming from the slaves inside the cluster, this should return
            a connection to the peer

        2. peer -> master -> slave
            For requests coming from other peers, this should return a connection
            to the slave running the shard
        '''
        return None

    def validateConnection(self, connection):
        ''' Subclass can override this to validate the connection '''
        return True

    def getMetadataToForward(self, metadata):
        return metadata

    async def handleMetadataAndRawData(self, metadata, rawData):
        forwardConn = self.getConnectionToForward(metadata)
        if forwardConn:
            check(self.validateConnection(forwardConn))
            return forwardConn.writeRawData(self.getMetadataToForward(metadata), rawData)
        await super().handleMetadataAndRawData(metadata, rawData)


class P2PMetadata(Metadata):
    ''' Metadata for P2P (cluster to cluster) connections'''
    FIELDS = [
        ("branch", Branch)
    ]

    def __init__(self, branch=None):
        self.branch = branch if branch else Branch(ROOT_SHARD_ID)

    @staticmethod
    def getByteSize():
        return 4


class ClusterMetadata(Metadata):
    ''' Metadata for intra-cluster (master and slave) connections '''
    FIELDS = [
        ("branch", Branch),
        ("peerId", hash256)
    ]

    def __init__(self, branch=None, peerId=b"\x00" * 32):
        self.branch = branch if branch else Branch(ROOT_SHARD_ID)
        self.peerId = peerId

    @staticmethod
    def getByteSize():
        return 36


class P2PConnection(ProxyConnection):
    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop=None):
        super().__init__(env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop, P2PMetadata)

    def getPeerId(self):
        ''' To be implemented by subclass '''
        return b"\x00" * 32

    def getConnectionToForward(self, metadata):
        ''' To be implemented by subclass '''
        return None

    def getMetadataToForward(self, metadata):
        return ClusterMetadata(metadata.branch, self.getPeerId())

    def validateConnection(self, connection):
        return isinstance(connection, ClusterConnection)


class ClusterConnection(ProxyConnection):
    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop=None):
        super().__init__(env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop, ClusterMetadata)
        self.peerRpcIds = dict()

    def getConnectionToForward(self, metadata):
        ''' To be implemented by subclass '''
        return None

    def getMetadataToForward(self, metadata):
        return P2PMetadata(metadata.branch)

    def validateConnection(self, connection):
        return isinstance(connection, P2PConnection)

    def validateAndUpdatePeerRpcId(self, metadata, rpcId):
        if rpcId <= self.peerRpcIds.setdefault(metadata.peerId, 0):
            raise RuntimeError("incorrect rpc request id sequence")
        self.peerRpcIds[metadata.peerId] = rpcId
