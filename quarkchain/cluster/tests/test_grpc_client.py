import grpc
import unittest

from concurrent import futures
from quarkchain.generated import grpc_pb2
from quarkchain.generated import grpc_pb2_grpc
from quarkchain.cluster.grpc_client import GrpcClient


class NormalServer(grpc_pb2_grpc.ClusterSlaveServicer):
    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=0, message="Confirmed")
        )


class ErrorServer(grpc_pb2_grpc.ClusterSlaveServicer):
    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=1, message="Confirmed")
        )


class TestGrpcClient(unittest.TestCase):
    def build_test_server(self, test_server, port: int):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        grpc_pb2_grpc.add_ClusterSlaveServicer_to_server(test_server(), server)
        server.add_insecure_port("[::]:" + str(port))
        return server

    def test_exception_error(self):
        server_port = 50051
        server = self.build_test_server(NormalServer, server_port)
        server.start()

        client_host = "localhost"
        client_port = 50011
        client_future = GrpcClient(
            client_host, client_port
        ).set_rootchain_confirmed_block()
        self.assertFalse(client_future)

        server.stop(None)

    def test_status_code(self):
        client_host = "localhost"
        both_port1 = 50041
        both_port2 = 50061
        server0 = self.build_test_server(NormalServer, both_port1)
        server0.start()

        rsp0 = GrpcClient(client_host, both_port1).set_rootchain_confirmed_block()
        self.assertTrue(rsp0)

        server1 = self.build_test_server(ErrorServer, both_port2)
        server1.start()

        rsp1 = GrpcClient(client_host, both_port2).set_rootchain_confirmed_block()
        self.assertFalse(rsp1)

        server0.stop(None)
        server1.stop(None)


if __name__ == "__main__":
    unittest.main()
