import asyncio
from collections import deque

from quarkchain.core import uint64, Branch
from quarkchain.protocol import Connection, AbstractConnection
from quarkchain.protocol import Metadata
from quarkchain.utils import check

ROOT_SHARD_ID = 0
ROOT_BRANCH = Branch(ROOT_SHARD_ID)


class ProxyConnection(Connection):
    def __init__(
        self,
        env,
        reader,
        writer,
        op_ser_map,
        op_non_rpc_map,
        op_rpc_map,
        loop=None,
        metadata_class=None,
        name=None,
    ):
        super().__init__(
            env,
            reader,
            writer,
            op_ser_map,
            op_non_rpc_map,
            op_rpc_map,
            loop=loop,
            metadata_class=metadata_class,
            name=name,
        )

    def get_connection_to_forward(self, metadata):
        """ Returns the Connection object to forward a request for metadata.
        Returns None if the request should not be forwarded for metadata.

        Subclass should override this if the branch is on another node and this node
        will forward the request without deserialize the request.

        metadata is a uint32 having shard size and shard id encoded as
            shard_size | shard_id
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
        """
        return None

    def validate_connection(self, connection):
        """ Subclass can override this to validate the connection """
        return True

    def get_metadata_to_forward(self, metadata):
        return metadata

    def close_connection(self, conn):
        pass

    async def handle_metadata_and_raw_data(self, metadata, raw_data):
        forward_conn = self.get_connection_to_forward(metadata)
        if forward_conn:
            check(self.validate_connection(forward_conn))
            return forward_conn.write_raw_data(
                self.get_metadata_to_forward(metadata), raw_data
            )
        await super().handle_metadata_and_raw_data(metadata, raw_data)


class ForwardingVirtualConnection:
    """ A forwarding virtual connection only forward writes to to a virtual connection
    No need to inherit AbstractConnection since it does not maintain RPC state.
    """

    def __init__(self, v_conn):
        self.v_conn = v_conn

    def write_raw_data(self, metadata, raw_data):
        self.v_conn.read_deque.append((metadata, raw_data))
        if not self.v_conn.read_event.is_set():
            self.v_conn.read_event.set()

    def close(self):
        # Write EOF
        self.write_raw_data(None, None)


class VirtualConnection(AbstractConnection):
    def __init__(
        self,
        proxy_conn,
        op_ser_map,
        op_non_rpc_map,
        op_rpc_map,
        loop=None,
        metadata_class=Metadata,
        name=None,
    ):
        super().__init__(
            op_ser_map, op_non_rpc_map, op_rpc_map, loop, metadata_class, name=name
        )
        self.read_deque = deque()
        self.read_event = asyncio.Event()
        self.proxy_conn = proxy_conn
        self.forward_conn = ForwardingVirtualConnection(self)

    async def read_metadata_and_raw_data(self):
        """ Override AbstractConnection.read_metadata_and_raw_data()
        """
        while len(self.read_deque) == 0:
            self.read_event.clear()
            await self.read_event.wait()

        metadata, raw_data_without_size = self.read_deque.popleft()
        return metadata, raw_data_without_size

    def write_raw_data(self, metadata, raw_data):
        self.proxy_conn.write_raw_data(self.get_metadata_to_write(metadata), raw_data)

    def get_forwarding_connection(self):
        """Returns a connection that forwards writes into this virtual connection"""
        return self.forward_conn

    def get_metadata_to_write(self, metadata):
        """ Metadata when a forwarding conn write back to the proxy connection
        """
        raise NotImplementedError()


class NullConnection(AbstractConnection):
    def __init__(self):
        super().__init__(dict(), dict(), dict(), None, Metadata, name="NULL_CONNECTION")

    def write_raw_data(self, metadata, raw_data):
        pass


NULL_CONNECTION = NullConnection()


class P2PMetadata(Metadata):
    """ Metadata for P2P (cluster to cluster) connections"""

    FIELDS = [("branch", Branch)]

    def __init__(self, branch=None):
        self.branch = branch if branch else Branch(ROOT_SHARD_ID)

    @staticmethod
    def get_byte_size():
        return 4


class ClusterMetadata(Metadata):
    """ Metadata for intra-cluster (master and slave) connections """

    FIELDS = [("branch", Branch), ("cluster_peer_id", uint64)]

    def __init__(self, branch=None, cluster_peer_id=0):
        self.branch = branch if branch else Branch(ROOT_SHARD_ID)
        self.cluster_peer_id = cluster_peer_id

    @staticmethod
    def get_byte_size():
        return 12


class P2PConnection(ProxyConnection):
    def __init__(
        self,
        env,
        reader,
        writer,
        op_ser_map,
        op_non_rpc_map,
        op_rpc_map,
        loop=None,
        metadata_class=None,
    ):
        super().__init__(
            env,
            reader,
            writer,
            op_ser_map,
            op_non_rpc_map,
            op_rpc_map,
            loop,
            P2PMetadata,
            name=metadata_class,
        )

    def get_cluster_peer_id(self):
        """ To be implemented by subclass """
        raise NotImplementedError()

    def get_connection_to_forward(self, metadata):
        """ To be implemented by subclass """
        raise NotImplementedError()

    def get_metadata_to_forward(self, metadata):
        return ClusterMetadata(metadata.branch, self.get_cluster_peer_id())

    def validate_connection(self, connection):
        return isinstance(connection, ClusterConnection)


class ClusterConnection(ProxyConnection):
    def __init__(
        self,
        env,
        reader,
        writer,
        op_ser_map,
        op_non_rpc_map,
        op_rpc_map,
        loop=None,
        name=None,
    ):
        super().__init__(
            env,
            reader,
            writer,
            op_ser_map,
            op_non_rpc_map,
            op_rpc_map,
            loop,
            ClusterMetadata,
            name=name,
        )
        self.peer_rpc_ids = dict()

    def get_connection_to_forward(self, metadata):
        raise NotImplementedError()

    def get_metadata_to_forward(self, metadata):
        return P2PMetadata(metadata.branch)
