# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the GRPC helloworld.Greeter client."""

from __future__ import print_function
import logging
import argparse
import grpc

import gRPC_pb2
import gRPC_pb2_grpc

HOST = "localhost"
PORT = "50051"


class gRPC_Client:
    def __init__(self, config):
        channel = grpc.insecure_channel(config)
        self.client = gRPC_pb2_grpc.ClusterSlaveStub(channel)

    def set_rootchain_confirmed_block(self):
        request = gRPC_pb2.SetRootChainConfirmedBlockRequest()
        try:
            response = self.client.SetRootChainConfirmedBlock(request)
        except Exception as e:
            print("Error reading command:\n {}".format(e))
            return False

        # Set RootChain confirmed block response
        if response.status.code == 0:
            return True
        else:
            print(response.status.code, response.status.message)
            return False


if __name__ == "__main__":
    logging.basicConfig()
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default=HOST, help="recipient to query")
    parser.add_argument("--port", type=str, default=PORT, help="recipient to query")

    args = parser.parse_args()
    HOST = args.host
    PORT = args.port

    client = gRPC_Client("{}:{}".format(HOST, PORT))
    client.set_rootchain_confirmed_block()
