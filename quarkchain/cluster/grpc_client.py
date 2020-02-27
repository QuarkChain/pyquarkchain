import logging
import argparse
import grpc

from quarkchain.generated import grpc_pb2, grpc_pb2_grpc

HOST = "localhost"
PORT = 50051


class GrpcClient:
    def __init__(self, host: str = HOST, port: int = PORT):
        channel = grpc.insecure_channel("{}:{}".format(host, str(port)))
        self.client = grpc_pb2_grpc.ClusterSlaveStub(channel)

    def set_root_chain_confirmed_block(self) -> bool:
        request = grpc_pb2.SetRootChainConfirmedBlock()
        try:
            response = self.client.SetRootChainConfirmedBlock(request)
        except Exception:
            return False

        if response.status.code == 0:
            return True
        else:
            return False

    def add_root_block(self, root_block) -> bool:
        id, prev_id, height = (
            root_block.header.id,
            root_block.header.prev_id,
            root_block.header.height,
        )
        minor_block_header_list = []
        for m_header in root_block.minor_block_header_list:
            minor_block_header = grpc_pb2.MinorBlockHeader(
                id=m_header.id, full_shard_id=m_header.full_shard_id
            )
            minor_block_header_list.append(minor_block_header)

        request = grpc_pb2.AddRootBlock(
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

    def finalize_minor_block(self):
        request = grpc_pb2.FinalizeMinorBlock()
        try:
            response = self.client.AddRootBlock(request)
        except Exception:
            return False

        # more details of reponse will be added later.
        return True


if __name__ == "__main__":
    logging.basicConfig()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default=HOST, help="server host")
    parser.add_argument("--port", type=int, default=PORT, help="server port")

    args = parser.parse_args()
    HOST = args.host
    PORT = args.port

    client = GrpcClient(HOST, PORT)
    client.add_root_block()
