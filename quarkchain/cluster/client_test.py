import unittest
import time
import grpc
import grpc_testing
import client
import grpc_client_pb2
from grpc.framework.foundation import logging_pool


class TestClient(unittest.TestCase):
    def setUp(self):
        self.execution_thread = logging_pool.pool(1)

        self.test_channel = grpc_testing.channel(
            grpc_client_pb2.DESCRIPTOR.services_by_name.values(),
            grpc_testing.strict_real_time(),
        )

    def shutDown(self):
        self.execution_thread.shutdown(wait=True)

    def test_set_root_chain_confirmed_block(self):
        stub = client.GrpcClient(self.test_channel)
        self.execution_thread.submit(stub.set_root_chain_confirmed_block)

        if grpc_client_pb2.DESCRIPTOR.services_by_name.get("ClusterSlave") == None:
            service_descriptor = "Service is None!"
            methods_descriptor = "Method is None!"
        else:
            service_descriptor = grpc_client_pb2.DESCRIPTOR.services_by_name[
                "ClusterSlave"
            ]
            methods_descriptor = service_descriptor.methods_by_name[
                "SetRootChainConfirmedBlock"
            ]

        invocation_metadata, request, rpc = self.test_channel.take_unary_unary(
            methods_descriptor
        )
        rpc.send_initial_metadata(())
        rpc.terminate(
            grpc_client_pb2.SetRootChainConfirmedBlockResponse(),
            (),
            grpc.StatusCode.OK,
            "",
        )
        self.assertEqual(grpc_client_pb2.SetRootChainConfirmedBlockRequest(), request)


if __name__ == "__main__":
    unittest.main()
