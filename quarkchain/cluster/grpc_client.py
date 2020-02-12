import logging
import sys
import grpc

from quarkchain.generated import grpc_client_pb2_grpc
from quarkchain.generated import grpc_client_pb2


class GrpcClient(object):
    def __init__(self, channel):
        self.channel = channel
        self.stub = grpc_client_pb2_grpc.ClusterSlaveStub(channel=self.channel)

    def set_root_chain_confirmed_block(self) -> bool:
        if self.channel == None:
            self.channel = grpc.insecure_channel(
                "localhost:50051"
            )  # set default value for channel.

        request = (
            grpc_client_pb2.SetRootChainConfirmedBlockRequest()
        )  # more parameters to be added

        try:
            response = self.stub.SetRootChainConfirmedBlock(request)
        except Exception as e:
            # print(str(e))
            return False

        if response.status.code == 0:
            return True
        else:
            return False


def run(channel):
    client = GrpcClient(channel)
    client.set_root_chain_confirmed_block()


def main():
    logging.basicConfig()
    if len(sys.argv) < 3:
        print("Usage--python grpc_client.py host port")
    else:
        host = sys.argv[1]
        port = sys.argv[2]
    channel = grpc.insecure_channel("{:}:{:}".format(host, port))
    run(channel)


if __name__ == "__main__":
    main()
