import grpc
import unittest

from concurrent import futures
from quarkchain.generated import grpc_pb2
from quarkchain.generated import grpc_pb2_grpc
from quarkchain.cluster.grpc_client import GrpcClient
from quarkchain.core import RootBlockHeader, RootBlock, MinorBlockHeader


class NormalServer(grpc_pb2_grpc.ClusterSlaveServicer):
    def AddRootBlock(self, request, context):
        return grpc_pb2.AddRootBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(
                code=0, message=str(request.minor_block_headers)
            )
        )

    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=0, message="Confirmed")
        )


class ErrorServer(grpc_pb2_grpc.ClusterSlaveServicer):
    def AddRootBlock(self, request, context):
        return grpc_pb2.AddRootBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=1, message=str(""))
        )

    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_pb2.AddRootBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=1, message="Confirmed")
        )


class TamperServer(grpc_pb2_grpc.ClusterSlaveServicer):
    """
    This server will succeed in connection but will not receive the same rootblock as sent from the client.
    """

    def AddRootBlock(self, request, context):
        return grpc_pb2.AddRootBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(
                code=0, message=str(request.minor_block_headers + 1)
            )
        )

    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_pb2.AddRootBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=0, message="Confirmed")
        )


class TestGrpcClient(unittest.TestCase):
    def build_test_server(self, test_server, port: int):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        grpc_pb2_grpc.add_ClusterSlaveServicer_to_server(test_server(), server)
        server.add_insecure_port("[::]:" + str(port))
        return server

    def test_exception_error(self):
        # This test is used to check connection exception, if there is no server or inconsistent ports, return False
        client_host = "localhost"
        client_port = 50011
        resp = GrpcClient(client_host, client_port).set_rootchain_confirmed_block()
        self.assertFalse(resp)

    def test_status_code_and_message(self):
        client_host = "localhost"
        server_port1 = 50041
        server_port2 = 50061
        server_port3 = 50071

        minor_header_list = [
            MinorBlockHeader(height=0, difficulty=5),
            MinorBlockHeader(height=1, difficulty=5),
        ]
        block = RootBlock(
            RootBlockHeader(create_time=42, difficulty=5),
            tracking_data="{}".encode("utf-8"),
            minor_block_header_list=minor_header_list,
        )

        server0 = self.build_test_server(NormalServer, server_port1)
        server0.start()

        client0 = GrpcClient(client_host, server_port1)
        resp0 = client0.set_rootchain_confirmed_block()
        self.assertTrue(resp0)
        resp1 = client0.add_root_block(root_block=block)
        self.assertTrue(resp1)
        server0.stop(None)

        server1 = self.build_test_server(ErrorServer, server_port2)
        server1.start()

        client1 = GrpcClient(client_host, server_port2)
        resp2 = client1.set_rootchain_confirmed_block()
        self.assertFalse(resp2)
        resp3 = client1.add_root_block(root_block=block)
        self.assertFalse(resp3)
        server1.stop(None)

        server2 = self.build_test_server(TamperServer, server_port3)
        server2.start()

        client2 = GrpcClient(client_host, server_port3)
        resp4 = client2.set_rootchain_confirmed_block()
        self.assertTrue(resp4)
        resp5 = client2.add_root_block(root_block=block)
        self.assertFalse(resp5)
        server2.stop(None)
