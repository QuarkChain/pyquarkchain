import grpc
import grpc_testing
import unittest

from concurrent import futures
from quarkchain.cluster import gRPC_pb2
from quarkchain.cluster import gRPC_pb2_grpc
from quarkchain.cluster.gRPC_client_dev import GrpcClient
from grpc.framework.foundation import logging_pool


class StatusCode0(gRPC_pb2_grpc.ClusterSlaveServicer):
    def SetRootChainConfirmedBlock(self, request, context):
        return gRPC_pb2.SetRootChainConfirmedBlockResponse(
            status=gRPC_pb2.ClusterSlaveStatus(code=0, message="Confirmed")
        )


class StatusCode1(gRPC_pb2_grpc.ClusterSlaveServicer):
    def SetRootChainConfirmedBlock(self, request, context):
        return gRPC_pb2.SetRootChainConfirmedBlockResponse(
            status=gRPC_pb2.ClusterSlaveStatus(code=1, message="Confirmed")
        )


class TestGrpcClient(unittest.TestCase):
    def setUp(self):
        self.execution_thread = logging_pool.pool(1)
        self.real_time = grpc_testing.strict_real_time()
        self.real_time_test_channel = grpc_testing.channel(
            gRPC_pb2.DESCRIPTOR.services_by_name.values(), self.real_time
        )

    def shutDown(self):
        self.execution_thread.shutdown(wait=True)

    def test_grpc_client(self):
        client_future = self.execution_thread.submit(
            GrpcClient(self.real_time_test_channel).set_rootchain_confirmed_block
        )
        (
            invocation_metadata,
            request,
            rpc,
        ) = self.real_time_test_channel.take_unary_unary(
            method_descriptor=(
                gRPC_pb2.DESCRIPTOR.services_by_name["ClusterSlave"].methods_by_name[
                    "SetRootChainConfirmedBlock"
                ]
            )
        )
        rpc.send_initial_metadata(())
        rpc.terminate(
            gRPC_pb2.SetRootChainConfirmedBlockResponse(), (), grpc.StatusCode.OK, ""
        )

        client_future_value = client_future.result()
        self.assertEqual(gRPC_pb2.SetRootChainConfirmedBlockRequest(), request)
        self.assertIs(client_future_value, True)

    def test_exception_error(self):
        client_future = GrpcClient(
            grpc.insecure_channel("localhost:50011")
        ).set_rootchain_confirmed_block()
        self.assertIs(client_future, False)

    def test_status_code(self):
        server0 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        gRPC_pb2_grpc.add_ClusterSlaveServicer_to_server(StatusCode0(), server0)
        server0.add_insecure_port("[::]:50051")
        server0.start()

        client_future0 = GrpcClient(grpc.insecure_channel("localhost:50051"))
        response0 = client_future0.client.SetRootChainConfirmedBlock(
            gRPC_pb2.SetRootChainConfirmedBlockRequest()
        )
        self.assertIs(response0.status.code == 0, True)

        server1 = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        gRPC_pb2_grpc.add_ClusterSlaveServicer_to_server(StatusCode1(), server1)
        server1.add_insecure_port("[::]:50061")
        server1.start()

        client_future1 = GrpcClient(grpc.insecure_channel("localhost:50061"))
        response1 = client_future1.client.SetRootChainConfirmedBlock(
            gRPC_pb2.SetRootChainConfirmedBlockRequest()
        )

        self.assertIs(response1.status.code == 0, False)

        server0.stop(None)
        server1.stop(None)


if __name__ == "__main__":
    unittest.main()
