import logging
import sys
import grpc

from quarkchain.generated import grpc_client_pb2_grpc
from quarkchain.generated import grpc_client_pb2


class GrpcClient(object):
    def __init__(self, host, port):
        self.channel = grpc.insecure_channel("{:}:{:}".format(host, port))
        self.stub = grpc_client_pb2_grpc.ClusterSlaveStub(channel=self.channel)

    def set_stub(self, channel):  # create stub according to desired channel.
        self.stub = grpc_client_pb2_grpc.ClusterSlaveStub(channel=channel)

    def set_root_chain_confirmed_block(self) -> bool:
        request = grpc_client_pb2.SetRootChainConfirmedBlockRequest()

        try:
            response = self.stub.SetRootChainConfirmedBlock(request)
        except Exception:
            return False

        if response.status.code == 0:
            return True
        else:
            return False


def main():
    logging.basicConfig()
    if len(sys.argv) < 3:
        print("Usage--python grpc_client.py host port")
    else:
        host = sys.argv[1]
        port = sys.argv[2]
    client = GrpcClient(host, port)
    client.set_root_chain_confirmed_block()


if __name__ == "__main__":
    main()
