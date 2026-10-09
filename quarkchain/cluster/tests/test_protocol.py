import asyncio
import unittest
from unittest.mock import call, MagicMock

from quarkchain.cluster.protocol import ClusterConnection, P2PConnection
from quarkchain.cluster.protocol import ClusterMetadata, P2PMetadata
from quarkchain.env import DEFAULT_ENV
from quarkchain.core import uint32, Branch, Serializable
from quarkchain.protocol import ConnectionState
from quarkchain.p2p.p2p_manager import SecurePeer

FORWARD_BRANCH = Branch(123)
EMPTY_BRANCH = Branch(456)
PEER_ID = b"\xab" * 32
CLUSTER_PEER_ID = 123
OP = 66
OP_RESPONSE = 67
RPC_ID = 123456


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


class DummyPackage(Serializable):
    FIELDS = [("value", uint32)]

    def __init__(self, value):
        self.value = value


async def handle_package(connection, package):
    return package


OP_SER_MAP = {OP: DummyPackage, OP_RESPONSE: DummyPackage}
OP_RPC_MAP = {OP: (OP, handle_package)}


class DummyP2PConnection(P2PConnection):
    def __init__(self, env, reader, writer):
        super().__init__(env, reader, writer, OP_SER_MAP, {}, OP_RPC_MAP)
        self.mockClusterConnection = MagicMock()
        self.mockClusterConnection.__class__ = ClusterConnection

    def get_cluster_peer_id(self):
        return CLUSTER_PEER_ID

    def get_connection_to_forward(self, metadata):
        if metadata.branch == FORWARD_BRANCH:
            return self.mockClusterConnection
        return None


class DummyClusterConnection(ClusterConnection):
    def __init__(self, env, reader, writer):
        super().__init__(env, reader, writer, OP_SER_MAP, {}, OP_RPC_MAP)
        self.mockP2PConnection = MagicMock()
        self.mockP2PConnection.__class__ = P2PConnection

    def get_connection_to_forward(self, metadata):
        if metadata.branch == FORWARD_BRANCH:
            return self.mockP2PConnection
        return None


class TestP2PConnection(unittest.TestCase):
    def test_cancelled_rpc_future_is_removed(self):
        reader = AsyncMock()
        writer = MagicMock()

        async def run():
            conn = DummyP2PConnection(DEFAULT_ENV, reader, writer)
            conn.state = ConnectionState.ACTIVE
            future = conn.write_rpc_request(OP, DummyPackage(999))
            self.assertEqual(list(conn.rpc_future_map), [1])

            future.cancel()
            await asyncio.sleep(0)
            self.assertNotIn(1, conn.rpc_future_map)
            self.assertIn(1, conn._cancelled_rpc_ids)

        asyncio.run(run())

    def test_late_response_for_cancelled_rpc_is_ignored(self):
        reader = AsyncMock()
        writer = MagicMock()

        async def run():
            conn = DummyP2PConnection(DEFAULT_ENV, reader, writer)
            conn.state = ConnectionState.ACTIVE
            future = conn.write_rpc_request(OP, DummyPackage(999))
            future.cancel()
            await asyncio.sleep(0)

            response = bytearray([OP_RESPONSE])
            response += (1).to_bytes(8, byteorder="big")
            response += DummyPackage(1000).serialize()
            await conn.handle_metadata_and_raw_data(P2PMetadata(EMPTY_BRANCH), response)

            self.assertFalse(conn.is_closed())
            self.assertNotIn(1, conn.rpc_future_map)
            self.assertNotIn(1, conn._cancelled_rpc_ids)

        asyncio.run(run())

    def test_abort_in_flight_rpcs_ignores_cancelled_future(self):
        reader = AsyncMock()
        writer = MagicMock()

        async def run():
            conn = DummyP2PConnection(DEFAULT_ENV, reader, writer)
            conn.state = ConnectionState.ACTIVE
            future = conn.write_rpc_request(OP, DummyPackage(999))
            future.cancel()

            # The cancellation callback runs on the next event-loop turn in
            # production, so the RPC can still be present during disconnect.
            SecurePeer.abort_in_flight_rpcs(conn)
            await asyncio.sleep(0)

            self.assertTrue(future.cancelled())
            self.assertEqual(conn.rpc_future_map, {})

        asyncio.run(run())

    def test_forward(self):
        meta = P2PMetadata(FORWARD_BRANCH)
        metaBytes = meta.serialize()
        request = DummyPackage(999)
        requestsBytes = request.serialize()
        requestSizeBytes = len(requestsBytes).to_bytes(4, byteorder="big")
        rawData = bytearray()
        rawData += bytes([OP])
        rawData += RPC_ID.to_bytes(8, byteorder="big")
        rawData += requestsBytes

        reader = AsyncMock()
        writer = MagicMock()
        reader.read.side_effect = [requestSizeBytes, metaBytes, rawData]

        async def run():
            conn = DummyP2PConnection(DEFAULT_ENV, reader, writer)
            await conn.loop_once()
            return conn
        conn = asyncio.run(run())

        conn.mockClusterConnection.write_raw_data.assert_called_once_with(
            ClusterMetadata(FORWARD_BRANCH, CLUSTER_PEER_ID), rawData
        )

    def test_no_forward(self):
        meta = P2PMetadata(EMPTY_BRANCH)
        metaBytes = meta.serialize()
        request = DummyPackage(999)
        requestsBytes = request.serialize()
        requestSizeBytes = len(requestsBytes).to_bytes(4, byteorder="big")
        rawData = bytearray()
        rawData += bytes([OP])
        rawData += RPC_ID.to_bytes(8, byteorder="big")
        rawData += requestsBytes

        reader = AsyncMock()
        writer = MagicMock()
        reader.read.side_effect = [requestSizeBytes, metaBytes, rawData]

        async def run():
            conn = DummyP2PConnection(DEFAULT_ENV, reader, writer)
            await conn.loop_once()
            return conn
        conn = asyncio.run(run())

        conn.mockClusterConnection.write_raw_data.assert_not_called()
        writer.write.assert_has_calls(
            [call(requestSizeBytes), call(metaBytes), call(rawData)]
        )


class TestClusterConnection(unittest.TestCase):
    def test_forward(self):
        meta = ClusterMetadata(FORWARD_BRANCH, CLUSTER_PEER_ID)
        metaBytes = meta.serialize()
        request = DummyPackage(999)
        requestsBytes = request.serialize()
        requestSizeBytes = len(requestsBytes).to_bytes(4, byteorder="big")
        rawData = bytearray()
        rawData += bytes([OP])
        rawData += RPC_ID.to_bytes(8, byteorder="big")
        rawData += requestsBytes

        reader = AsyncMock()
        writer = MagicMock()
        reader.read.side_effect = [requestSizeBytes, metaBytes, rawData]

        async def run():
            conn = DummyClusterConnection(DEFAULT_ENV, reader, writer)
            await conn.loop_once()
            return conn
        conn = asyncio.run(run())

        conn.mockP2PConnection.write_raw_data.assert_called_once_with(
            P2PMetadata(FORWARD_BRANCH), rawData
        )

    def test_no_forward(self):
        meta = ClusterMetadata(EMPTY_BRANCH)
        metaBytes = meta.serialize()
        request = DummyPackage(999)
        requestsBytes = request.serialize()
        requestSizeBytes = len(requestsBytes).to_bytes(4, byteorder="big")
        rawData = bytearray()
        rawData += bytes([OP])
        rawData += RPC_ID.to_bytes(8, byteorder="big")
        rawData += requestsBytes

        reader = AsyncMock()
        writer = MagicMock()
        reader.read.side_effect = [requestSizeBytes, metaBytes, rawData]

        async def run():
            conn = DummyClusterConnection(DEFAULT_ENV, reader, writer)
            await conn.loop_once()
            return conn
        conn = asyncio.run(run())

        conn.mockP2PConnection.write_raw_data.assert_not_called()
        writer.write.assert_has_calls(
            [call(requestSizeBytes), call(metaBytes), call(rawData)]
        )
