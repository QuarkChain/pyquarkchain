import unittest
import grpc
import grpc_testing
from grpc.framework.foundation import logging_pool

from quarkchain.cluster.grpc_client import GrpcClient
from quarkchain.generated import grpc_client_pb2


class TestClient(unittest.TestCase):
    def setUp(self):
        self.execution_thread = logging_pool.pool(1)

        self.test_channel = grpc_testing.channel(
            grpc_client_pb2.DESCRIPTOR.services_by_name.values(),
            grpc_testing.strict_real_time(),
        )

    def tearDown(self):
        self.execution_thread.shutdown(wait=False)

    def test_set_root_chain_confirmed_block(self):
        stub = GrpcClient(self.test_channel)
        stub_future = self.execution_thread.submit(stub.set_root_chain_confirmed_block)

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
        self.assertIs(True, stub_future.result())

    def test_network_error(self):
        stub = GrpcClient(self.test_channel)
        stub_future = self.execution_thread.submit(stub.set_root_chain_confirmed_block)

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
            grpc.StatusCode.UNKNOWN,
            "",
        )
        self.assertEqual(grpc_client_pb2.SetRootChainConfirmedBlockRequest(), request)
        self.assertIs(False, stub_future.result())

    def test_result_error(self):  # case 2: server not response properly
        stub = GrpcClient(self.test_channel)
        stub_future = self.execution_thread.submit(stub.set_root_chain_confirmed_block)

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
        fake_response = grpc_client_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_client_pb2.ClusterSlaveStatus(code=1, message="not received")
        )
        rpc.terminate(
            fake_response, (), grpc.StatusCode.UNKNOWN, "",
        )
        self.assertEqual(
            grpc_client_pb2.SetRootChainConfirmedBlockRequest(),
            request,
            msg="request not equal",
        )
        self.assertIs(False, stub_future.result(), msg="result is not true")


if __name__ == "__main__":
    unittest.main()
