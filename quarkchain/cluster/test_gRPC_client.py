import grpc
import grpc_testing
import unittest

import gRPC_pb2
from gRPC_server import ClusterSlave


class TestGreeter(unittest.TestCase):
    def setUp(self):
        servicers = {
            gRPC_pb2.DESCRIPTOR.services_by_name["ClusterSlave"]: ClusterSlave()
        }

        self.test_server = grpc_testing.server_from_dictionary(
            servicers, grpc_testing.strict_real_time()
        )

    def test_gRPC_client(self):
        """ expect to get gRPC_server response """
        request = gRPC_pb2.SetRootChainConfirmedBlockRequest()

        SetRootChainConfirmedBlock_method = self.test_server.invoke_unary_unary(
            method_descriptor=(
                gRPC_pb2.DESCRIPTOR.services_by_name["ClusterSlave"].methods_by_name[
                    "SetRootChainConfirmedBlock"
                ]
            ),
            invocation_metadata={},
            request=request,
            timeout=1,
        )

        (
            response,
            metadata,
            code,
            details,
        ) = SetRootChainConfirmedBlock_method.termination()
        self.assertEqual(response.status.message, "Confirmed")
        self.assertEqual(code, grpc.StatusCode.OK)


if __name__ == "__main__":
    unittest.main()
