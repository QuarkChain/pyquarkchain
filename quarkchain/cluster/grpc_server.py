import logging
import argparse
import grpc
from quarkchain.generated import grpc_pb2, grpc_pb2_grpc
from quarkchain.utils import Logger
from concurrent import futures


class ClusterSlave(grpc_pb2_grpc.ClusterSlaveServicer):
    def SetRootChainConfirmedBlock(self, request, context):
        # TODO check request info to confirm blocks
        return grpc_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_pb2.ClusterSlaveStatus(code=0, message="Grpc server")
        )


class GrpcServer:
    def __init__(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.servicer = ClusterSlave()
        grpc_pb2_grpc.add_ClusterSlaveServicer_to_server(self.servicer, self.server)

    def add_channel(self, host: str, port: int):
        channel = "{}:{}".format(host, str(port))
        port = self.server.add_insecure_port(channel)

    def start(self):
        try:
            self.server.start()
        except Exception:
            return Logger.info("Server has been started!")

        self.server.wait_for_termination()

    def stop(self):
        self.server.stop(0)


if __name__ == "__main__":
    logging.basicConfig()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost", help="client host")
    parser.add_argument("--port", type=int, default=50051, help="client port")

    args = parser.parse_args()
    HOST = args.host
    PORT = args.port

    server = GrpcServer()
    server.add_channel(HOST, PORT)
    server.start()
