from concurrent import futures
import logging

import grpc

import grpc_client_pb2
import grpc_client_pb2_grpc


class ClusterSlave(grpc_client_pb2_grpc.ClusterSlaveServicer):
    def SetRootChainConfirmedBlock(self, request, context):
        return grpc_client_pb2.SetRootChainConfirmedBlockResponse(
            status=grpc_client_pb2.ClusterSlaveStatus(code=0, message="received")
        )


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    grpc_client_pb2_grpc.add_ClusterSlaveServicer_to_server(ClusterSlave(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
