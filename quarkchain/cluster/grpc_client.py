import logging
import argparse
import grpc
from quarkchain.utils import Logger
from quarkchain.generated import grpc_pb2, grpc_pb2_grpc

HOST = "localhost"
PORT = 50051


class GrpcClient:
    def __init__(self, host: str = HOST, port: int = PORT):
        channel = grpc.insecure_channel("{}:{}".format(host, str(port)))
        self.client = grpc_pb2_grpc.ClusterSlaveStub(channel)

    def set_rootchain_confirmed_block(self, block) -> bool:
        request = grpc_pb2.SetRootChainConfirmedBlockRequest(message=block)
        try:
            response = self.client.SetRootChainConfirmedBlock(request)
        except Exception:
            return False

        if response.status.code == 0:
            return True
        else:
            return False


if __name__ == "__main__":
    logging.basicConfig()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default=HOST, help="server host")
    parser.add_argument("--port", type=int, default=PORT, help="server port")

    args = parser.parse_args()
    HOST = args.host
    PORT = args.port

    client = GrpcClient(HOST, PORT)
    client.set_rootchain_confirmed_block()
