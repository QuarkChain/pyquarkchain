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

    def build_stub_and_rpc(self):
        host = None
        port = None
        # just to get a GrpcClient object
        grpc_object = GrpcClient(host, port)
        grpc_object.set_stub(self.test_channel)
        service_descriptor = grpc_client_pb2.DESCRIPTOR.services_by_name["ClusterSlave"]
        methods_descriptor = service_descriptor.methods_by_name[
            "SetRootChainConfirmedBlock"
        ]
        invocation_metadata, request, rpc = self.test_channel.take_unary_unary(
            methods_descriptor
        )
        return (
            self.execution_thread.submit(grpc_object.set_root_chain_confirmed_block),
            rpc,
        )

    def test_normal(self):
        stub_future, rpc = self.build_stub_and_rpc()
        rpc.send_initial_metadata(())
        # Corresponding to the condition of response stats code = 0 in grpc client. Success.
        rpc.terminate(
            grpc_client_pb2.SetRootChainConfirmedBlockResponse(),
            (),
            grpc.StatusCode.OK,
            "",
        )
        self.assertIs(True, stub_future.result())

    def test_network_error(self):
        stub_future, rpc = self.build_stub_and_rpc()
        rpc.send_initial_metadata(())

        # Corresponding to exception in grpc client.
        # Server side application throws an exception (or does something other than returning a Status code to terminate an RPC)
        rpc.terminate(
            grpc_client_pb2.SetRootChainConfirmedBlockResponse(),
            (),
            grpc.StatusCode.UNKNOWN,
            "",
        )
        self.assertIs(False, stub_future.result())

    def test_result_error(self):  # case 2: server not response properly
        stub_future, rpc = self.build_stub_and_rpc()
        service_descriptor = grpc_client_pb2.DESCRIPTOR.services_by_name["ClusterSlave"]
        methods_descriptor = service_descriptor.methods_by_name[
            "SetRootChainConfirmedBlock"
        ]
        invocation_metadata, request, rpc = self.test_channel.take_unary_unary(
            methods_descriptor
        )
        rpc.send_initial_metadata(())
        # Corresponding to the condition that the connection succeeds, but the returned result has an error.
        fake_response = grpc_client_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_client_pb2.ClusterSlaveStatus(code=1, message="not received")
        )
        rpc.terminate(
            fake_response, (), grpc.StatusCode.OK, "",
        )
        self.assertIs(False, stub_future.result())


if __name__ == "__main__":
    unittest.main()
