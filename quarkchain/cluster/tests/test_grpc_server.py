import grpc
import unittest
from concurrent import futures
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.tests.test_utils import get_test_env
from quarkchain.generated import grpc_pb2, grpc_pb2_grpc
from quarkchain.cluster.master import ClusterMaster


class TestGrpcServer(unittest.TestCase):
    def build_test_client(self, host: str, port: int):
        channel = grpc.insecure_channel("{}:{}".format(host, str(port)))
        client = grpc_pb2_grpc.ClusterMasterStub(channel)
        return client

    def test_grpc_response(self):
        env = get_test_env()
        root_state = RootState(env=env)
        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=None))
        servicer = ClusterMaster(root_state)
        grpc_pb2_grpc.add_ClusterMasterServicer_to_server(servicer, grpc_server)
        grpc_server.add_insecure_port(
            "{}:{}".format(
                env.cluster_config.GRPC_SERVER_HOST,
                str(env.cluster_config.GRPC_SERVER_PORT),
            )
        )
        grpc_server.start()

        cluster_env = env.cluster_config
        count = 0
        request = grpc_pb2.AddMinorBlockHeaderRequest()
        grpc_client = [
            self.build_test_client(
                cluster_env.GRPC_SERVER_HOST, cluster_env.GRPC_SERVER_PORT
            )
            for _ in range(2)
        ]
        for client in grpc_client:
            if client.AddMinorBlockHeader(request):
                count += 1

        self.assertEqual(count, 2)
        grpc_server.stop(None)
