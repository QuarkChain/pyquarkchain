import grpc
import unittest

from concurrent import futures
from quarkchain.generated import grpc_pb2
from quarkchain.generated import grpc_pb2_grpc
from quarkchain.cluster.grpc_client import GrpcClient
from quarkchain.core import RootBlockHeader, RootBlock


class NormalServer(grpc_pb2_grpc.ClusterSlaveServicer):
    def AddRootBlock(self, request, context):
        return grpc_pb2.AddRootBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=0, message="Added")
        )

    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=0, message="Confirmed")
        )


class ErrorServer(grpc_pb2_grpc.ClusterSlaveServicer):
    def AddRootBlock(self, request, context):
        return grpc_pb2.AddRootBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=1, message="Added")
        )

    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_pb2.AddRootBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=1, message="Confirmed")
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

    def test_status_code(self):
        client_host = "localhost"
        server_port1 = 50041
        server_port2 = 50061
        server0 = self.build_test_server(NormalServer, server_port1)
        server0.start()

        block = RootBlock(
            RootBlockHeader(create_time=42, difficulty=5),
            tracking_data="{}".encode("utf-8"),
        )

        client1 = GrpcClient(client_host, server_port1)
        resp0 = client1.set_rootchain_confirmed_block()
        resp1 = client1.add_root_block(root_block=block)
        self.assertTrue(resp0)
        self.assertTrue(resp1)
        server0.stop(None)

        server1 = self.build_test_server(ErrorServer, server_port2)
        server1.start()

        client2 = GrpcClient(client_host, server_port2)
        resp2 = client2.set_rootchain_confirmed_block()
        resp3 = client2.add_root_block(root_block=block)
        self.assertFalse(resp2)
        self.assertFalse(resp3)
        server1.stop(None)
