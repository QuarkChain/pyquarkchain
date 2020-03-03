import logging
import argparse
import grpc
from quarkchain.generated import grpc_pb2, grpc_pb2_grpc
from concurrent import futures

HOST = "localhost"
PORT = 50051


class ClusterSlave(grpc_pb2_grpc.ClusterSlaveServicer):
    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=0, message="Grpc server")
        )


class GrpcServer:
    def __init__(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        grpc_pb2_grpc.add_ClusterSlaveServicer_to_server(ClusterSlave(), self.server)
        self.add_channel(HOST, PORT)

    def add_channel(self, host: str, port: int):
        channel = "{}:{}".format(host, str(port))
        self.server.add_insecure_port(channel)

    def start(self):
        self.server.start()
        self.server.wait_for_termination()

    def stop(self):
        self.server.stop(0)


if __name__ == "__main__":
    logging.basicConfig()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default=HOST, help="client host")
    parser.add_argument("--port", type=int, default=PORT, help="client port")

    args = parser.parse_args()
    HOST = args.host
    PORT = args.port

    server = GrpcServer()
    server.start()
