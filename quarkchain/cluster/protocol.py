import asyncio
from collections import deque

from quarkchain.core import uint64, Branch
from quarkchain.protocol import Connection, AbstractConnection
from quarkchain.protocol import Metadata
from quarkchain.utils import check

ROOT_SHARD_ID = 0
ROOT_BRANCH = Branch(ROOT_SHARD_ID)


class ProxyConnection(Connection):
    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop=None, metadataClass=None, name=None):
        super().__init__(
            env,
            reader,
            writer,
            opSerMap,
            opNonRpcMap,
            opRpcMap,
            loop=loop,
            metadataClass=metadataClass,
            name=name)

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

    def closeConnection(self, conn):
        pass

    async def handleMetadataAndRawData(self, metadata, rawData):
        forwardConn = self.getConnectionToForward(metadata)
        if forwardConn:
            check(self.validateConnection(forwardConn))
            return forwardConn.writeRawData(self.getMetadataToForward(metadata), rawData)
        await super().handleMetadataAndRawData(metadata, rawData)


class ForwardingVirtualConnection():
    ''' A forwarding virtual connection only forward to to a virtual connection
    No need to inherit AbstractConnection since it does not maintain RPC state.
    '''
    def __init__(self, vConn):
        self.vConn = vConn

    def writeRawData(self, metadata, rawData):
        self.vConn.readDeque.append((metadata, rawData))
        if not self.vConn.readEvent.is_set():
            self.vConn.readEvent.set()

    def close(self):
        # Write EOF
        self.writeRawData(None, None)


class VirtualConnection(AbstractConnection):

    def __init__(self, proxyConn, opSerMap, opNonRpcMap, opRpcMap, loop=None, metadataClass=Metadata, name=None):
        super().__init__(opSerMap, opNonRpcMap, opRpcMap, loop, metadataClass, name=name)
        self.readDeque = deque()
        self.readEvent = asyncio.Event()
        self.proxyConn = proxyConn
        self.forwardConn = ForwardingVirtualConnection(self)

    async def readMetadataAndRawData(self):
        ''' Override AbstractConnection.readMetadataAndRawData()
        '''
        while len(self.readDeque) == 0:
            self.readEvent.clear()
            await self.readEvent.wait()

        metadata, rawDataWithoutSize = self.readDeque.popleft()
        return metadata, rawDataWithoutSize

    def writeRawData(self, metadata, rawData):
        self.proxyConn.writeRawData(
            self.getMetadataToWrite(metadata),
            rawData)

    def getForwardingConnection(self):
        return self.forwardConn

    def getMetadataToWrite(self, metadata):
        ''' Metadata when a forwarding conn write back to the proxy connection
        '''
        raise NotImplementedError()


class NullConnection(AbstractConnection):

    def __init__(self):
        super().__init__(dict(), dict(), dict(), None, Metadata, name="NULL_CONNECTION")

    def writeRawData(self, metadata, rawData):
        pass


NULL_CONNECTION = NullConnection()


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
        ("clusterPeerId", uint64)
    ]

    def __init__(self, branch=None, clusterPeerId=0):
        self.branch = branch if branch else Branch(ROOT_SHARD_ID)
        self.clusterPeerId = clusterPeerId

    @staticmethod
    def getByteSize():
        return 12


class P2PConnection(ProxyConnection):
    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop=None, name=None):
        super().__init__(env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop, P2PMetadata, name=name)

    def getClusterPeerId(self):
        ''' To be implemented by subclass '''
        raise NotImplementedError()

    def getConnectionToForward(self, metadata):
        ''' To be implemented by subclass '''
        raise NotImplementedError()

    def getMetadataToForward(self, metadata):
        return ClusterMetadata(metadata.branch, self.getClusterPeerId())

    def validateConnection(self, connection):
        return isinstance(connection, ClusterConnection)


class ClusterConnection(ProxyConnection):
    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop=None, name=None):
        super().__init__(env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop, ClusterMetadata, name=name)
        self.peerRpcIds = dict()

    def getConnectionToForward(self, metadata):
        raise NotImplementedError()

    def getMetadataToForward(self, metadata):
        return P2PMetadata(metadata.branch)
