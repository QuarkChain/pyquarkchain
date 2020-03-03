import logging
import argparse
import grpc
from quarkchain.utils import logger
from quarkchain.generated import grpc_pb2, grpc_pb2_grpc

HOST = "localhost"
PORT = 50051
