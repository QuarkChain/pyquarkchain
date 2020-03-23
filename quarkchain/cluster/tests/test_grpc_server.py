import grpc
import unittest

from concurrent import futures
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.tests.test_utils import get_test_env
from quarkchain.generated import cluster_pb2, cluster_pb2_grpc
from quarkchain.cluster.master import ClusterMaster
from quarkchain.core import MinorBlockHeader


class TestGrpcServer(unittest.TestCase):
    def build_test_client(self, host: str, port: int):
        channel = grpc.insecure_channel("{}:{}".format(host, str(port)))
        client = cluster_pb2_grpc.ClusterMasterStub(channel)
        return client

    def test_store_minor_block(self):
        env = get_test_env()
        r_state = RootState(env=env)
        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=None))
        servicer = ClusterMaster(r_state)
        cluster_pb2_grpc.add_ClusterMasterServicer_to_server(servicer, grpc_server)
        grpc_server.add_insecure_port(
            "{}:{}".format(
                env.cluster_config.GRPC_SERVER_HOST,
                str(env.cluster_config.GRPC_SERVER_PORT),
            )
        )
        grpc_server.start()

        cluster_env = env.cluster_config
        grpc_client = self.build_test_client(
            cluster_env.GRPC_SERVER_HOST, cluster_env.GRPC_SERVER_PORT
        )

        mb = MinorBlockHeader()
        request = cluster_pb2.AddMinorBlockHeaderRequest(id=mb.get_hash())
        grpc_client.AddMinorBlockHeader(request)
        self.assertTrue(r_state.db.contain_minor_block_by_hash(mb.get_hash()))

        grpc_server.stop(None)
