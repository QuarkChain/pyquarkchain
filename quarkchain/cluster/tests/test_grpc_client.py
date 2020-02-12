import grpc
import grpc_testing
import unittest

from concurrent import futures
from quarkchain.generated import grpc_pb2
from quarkchain.generated import grpc_pb2_grpc
from quarkchain.cluster.grpc_client import GrpcClient
from grpc.framework.foundation import logging_pool


class StatusCode0(grpc_pb2_grpc.ClusterSlaveServicer):
    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=0, message="Confirmed")
        )


class StatusCode1(grpc_pb2_grpc.ClusterSlaveServicer):
    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=1, message="Confirmed")
        )


class TestGrpcClient(unittest.TestCase):
    def setUp(self):
        self.execution_thread = logging_pool.pool(1)
        self.real_time = grpc_testing.strict_real_time()
        self.real_time_test_channel = grpc_testing.channel(
            grpc_pb2.DESCRIPTOR.services_by_name.values(), self.real_time
        )

    def shutDown(self):
        self.execution_thread.shutdown(wait=True)

    def build_test_server(self, statuscode, port):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        grpc_pb2_grpc.add_ClusterSlaveServicer_to_server(statuscode(), server)
        server.add_insecure_port("[::]:" + port)
        return server

    def test_grpc_client(self):
        host = "localhost"
        port = "50051"
        client = GrpcClient(host, port)
        client.set_client(self.real_time_test_channel)
        client_future = self.execution_thread.submit(
            client.set_rootchain_confirmed_block
        )
        (
            invocation_metadata,
            request,
            rpc,
        ) = self.real_time_test_channel.take_unary_unary(
            method_descriptor=(
                grpc_pb2.DESCRIPTOR.services_by_name["ClusterSlave"].methods_by_name[
                    "SetRootChainConfirmedBlock"
                ]
            )
        )
        rpc.send_initial_metadata(())
        rpc.terminate(
            grpc_pb2.SetRootChainConfirmedBlockResponse(), (), grpc.StatusCode.OK, ""
        )

        client_future_value = client_future.result()
        self.assertEqual(grpc_pb2.SetRootChainConfirmedBlockRequest(), request)
        self.assertTrue(client_future_value)

    def test_exception_error(self):
        server_port = "50051"
        server = self.build_test_server(StatusCode0, server_port)
        server.start()

        client_host = "localhost"
        client_port = "50011"
        client_future = GrpcClient(
            client_host, client_port
        ).set_rootchain_confirmed_block()
        self.assertFalse(client_future)

        server.stop(None)

    def test_status_code(self):
        client_host = "localhost"
        both_port1 = "50041"
        both_port2 = "50061"
        server0 = self.build_test_server(StatusCode0, both_port1)
        server0.start()

        rsp0 = GrpcClient(client_host, both_port1).set_rootchain_confirmed_block()
        self.assertTrue(rsp0)

        server1 = self.build_test_server(StatusCode1, both_port2)
        server1.start()

        rsp1 = GrpcClient(client_host, both_port2).set_rootchain_confirmed_block()
        self.assertFalse(rsp1)

        server0.stop(None)
        server1.stop(None)


if __name__ == "__main__":
    unittest.main()
