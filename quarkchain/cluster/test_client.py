import grpc
import grpc_testing
import unittest

from quarkchain.cluster import gRPC_pb2
from quarkchain.cluster.gRPC_client_dev import GrpcClient
from grpc.framework.foundation import logging_pool


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


if __name__ == "__main__":
    unittest.main()
