import grpc
import unittest
import quarkchain.db

from concurrent import futures
from quarkchain.cluster.root_state import RootState
from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import get_test_env
from quarkchain.generated import grpc_pb2, grpc_pb2_grpc
from quarkchain.cluster.master import ClusterMaster


def create_default_state(env, diff_calc=None):
    r_state = RootState(env=env, diff_calc=diff_calc)
    s_state_list = dict()
    for full_shard_id in env.quark_chain_config.get_full_shard_ids():
        shard_state = ShardState(
            env=env, full_shard_id=full_shard_id, db=quarkchain.db.InMemoryDb()
        )
        mblock, coinbase_amount_map = shard_state.init_genesis_state(
            r_state.get_tip_block()
        )
        block_hash = mblock.header.get_hash()
        r_state.add_validated_minor_block_hash(
            block_hash, coinbase_amount_map.balance_map
        )
        s_state_list[full_shard_id] = shard_state

    # add a root block so that later minor blocks will be broadcasted to neighbor shards
    minor_header_list = []
    for state in s_state_list.values():
        minor_header_list.append(state.header_tip)

    root_block = r_state.create_block_to_mine(minor_header_list)
    assert r_state.add_block(root_block)
    for state in s_state_list.values():
        assert state.add_root_block(root_block)

    return r_state, s_state_list


class TestGrpcServer(unittest.TestCase):
    def build_test_client(self, host: str, port: int):
        channel = grpc.insecure_channel("{}:{}".format(host, str(port)))
        client = grpc_pb2_grpc.ClusterMasterStub(channel)
        return client

    def test_store_minor_block(self):
        env = get_test_env()
        r_state, s_states = create_default_state(env)
        grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=None))
        servicer = ClusterMaster(r_state)
        grpc_pb2_grpc.add_ClusterMasterServicer_to_server(servicer, grpc_server)
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
        s_state0 = s_states[2 | 0]
        b0 = s_state0.create_block_to_mine()
        request = grpc_pb2.AddMinorBlockHeaderRequest(id=b0.header.get_hash())
        grpc_client.AddMinorBlockHeader(request)
        self.assertTrue(r_state.db.contain_minor_block_by_hash(b0.header.get_hash()))

        grpc_server.stop(None)
