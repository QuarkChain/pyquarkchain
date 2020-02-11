import logging
import argparse
import grpc

from quarkchain.cluster import gRPC_pb2
from quarkchain.cluster import gRPC_pb2_grpc

HOST = "localhost"
PORT = "50051"


class GrpcClient:
    def __init__(self, channel):
        self.client = gRPC_pb2_grpc.ClusterSlaveStub(channel)

    def set_rootchain_confirmed_block(self) -> bool:
        request = gRPC_pb2.SetRootChainConfirmedBlockRequest()
        try:
            response = self.client.SetRootChainConfirmedBlock(request)
        except Exception as e:
            return False

        if response.status.code == 0:
            return True
        else:
            return False


if __name__ == "__main__":
    logging.basicConfig()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default=HOST, help="recipient to query")
    parser.add_argument("--port", type=str, default=PORT, help="recipient to query")

    args = parser.parse_args()
    HOST = args.host
    PORT = args.port

    client = GrpcClient(grpc.insecure_channel("{}:{}".format(HOST, PORT)))
    client.set_rootchain_confirmed_block()
