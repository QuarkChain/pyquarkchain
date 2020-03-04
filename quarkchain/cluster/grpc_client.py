import logging
import argparse
import grpc
from quarkchain.generated import grpc_pb2, grpc_pb2_grpc
from quarkchain.core import RootBlock, RootBlockHeader, MinorBlockHeader

HOST = "localhost"
PORT = 50051


class GrpcClient:
    def __init__(self, host: str = HOST, port: int = PORT):
        channel = grpc.insecure_channel("{}:{}".format(host, str(port)))
        self.client = grpc_pb2_grpc.ClusterSlaveStub(channel)

    def set_rootchain_confirmed_block(self) -> bool:
        request = grpc_pb2.SetRootChainConfirmedBlockRequest()
        try:
            response = self.client.SetRootChainConfirmedBlock(request)
        except Exception:
            return False

        if response.status.code == 0:
            return True
        else:
            return False

    def add_root_block(self, root_block: RootBlock) -> bool:
        id, prev_id, height = (
            root_block.header.get_hash(),
            root_block.header.hash_prev_block,
            root_block.header.height,
        )
        minor_block_header_list = [
            grpc_pb2.MinorBlockHeader(
                id=mh.get_hash(), full_shard_id=mh.branch.get_full_shard_id()
            )
            for mh in root_block.minor_block_header_list
        ]

        request = grpc_pb2.AddRootBlockRequest(
            id=id,
            prev_id=prev_id,
            height=height,
            minor_block_headers=minor_block_header_list,
        )
        try:
            response = self.client.AddRootBlock(request)
        except Exception:
            return False

        if response.status.code == 0:
            return True
        else:
            return False
