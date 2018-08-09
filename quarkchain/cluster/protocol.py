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

    def get_connection_to_forward(self, metadata):
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

    def validate_connection(self, connection):
        ''' Subclass can override this to validate the connection '''
        return True

    def get_metadata_to_forward(self, metadata):
        return metadata

    def close_connection(self, conn):
        pass

    async def handle_metadata_and_raw_data(self, metadata, rawData):
        forwardConn = self.get_connection_to_forward(metadata)
        if forwardConn:
            check(self.validate_connection(forwardConn))
            return forwardConn.write_raw_data(self.get_metadata_to_forward(metadata), rawData)
        await super().handle_metadata_and_raw_data(metadata, rawData)


class ForwardingVirtualConnection():
    ''' A forwarding virtual connection only forward to to a virtual connection
    No need to inherit AbstractConnection since it does not maintain RPC state.
    '''
    def __init__(self, vConn):
        self.vConn = vConn

    def write_raw_data(self, metadata, rawData):
        self.vConn.readDeque.append((metadata, rawData))
        if not self.vConn.readEvent.is_set():
            self.vConn.readEvent.set()

    def close(self):
        # Write EOF
        self.write_raw_data(None, None)


class VirtualConnection(AbstractConnection):

    def __init__(self, proxyConn, opSerMap, opNonRpcMap, opRpcMap, loop=None, metadataClass=Metadata, name=None):
        super().__init__(opSerMap, opNonRpcMap, opRpcMap, loop, metadataClass, name=name)
        self.readDeque = deque()
        self.readEvent = asyncio.Event()
        self.proxyConn = proxyConn
        self.forwardConn = ForwardingVirtualConnection(self)

    async def read_metadata_and_raw_data(self):
        ''' Override AbstractConnection.read_metadata_and_raw_data()
        '''
        while len(self.readDeque) == 0:
            self.readEvent.clear()
            await self.readEvent.wait()

        metadata, rawDataWithoutSize = self.readDeque.popleft()
        return metadata, rawDataWithoutSize

    def write_raw_data(self, metadata, rawData):
        self.proxyConn.write_raw_data(
            self.get_metadata_to_write(metadata),
            rawData)

    def get_forwarding_connection(self):
        return self.forwardConn

    def get_metadata_to_write(self, metadata):
        ''' Metadata when a forwarding conn write back to the proxy connection
        '''
        raise NotImplementedError()


class NullConnection(AbstractConnection):

    def __init__(self):
        super().__init__(dict(), dict(), dict(), None, Metadata, name="NULL_CONNECTION")

    def write_raw_data(self, metadata, rawData):
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
    def get_byte_size():
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
    def get_byte_size():
        return 12


class P2PConnection(ProxyConnection):
    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop=None, name=None):
        super().__init__(env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop, P2PMetadata, name=name)

    def get_cluster_peer_id(self):
        ''' To be implemented by subclass '''
        raise NotImplementedError()

    def get_connection_to_forward(self, metadata):
        ''' To be implemented by subclass '''
        raise NotImplementedError()

    def get_metadata_to_forward(self, metadata):
        return ClusterMetadata(metadata.branch, self.get_cluster_peer_id())

    def validate_connection(self, connection):
        return isinstance(connection, ClusterConnection)


class ClusterConnection(ProxyConnection):
    def __init__(self, env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop=None, name=None):
        super().__init__(env, reader, writer, opSerMap, opNonRpcMap, opRpcMap, loop, ClusterMetadata, name=name)
        self.peerRpcIds = dict()

    def get_connection_to_forward(self, metadata):
        raise NotImplementedError()

    def get_metadata_to_forward(self, metadata):
        return P2PMetadata(metadata.branch)
