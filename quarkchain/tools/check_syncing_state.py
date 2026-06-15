#! /usr/bin/env pypy3

import argparse
import time
from datetime import datetime
from quarkchain.jsonrpc_client import JsonRpcClient

TIMEOUT=10

def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def checkHeight(private_client: JsonRpcClient, public_client: JsonRpcClient):
    result_private = private_client.call("getRootBlockByHeight")
    result_public = public_client.call("getRootBlockByHeight")
    return {
        "height": int(result_private["height"], 16),
        "currentHeight": int(result_public["height"], 16),
    }


def query_height(private_client: JsonRpcClient, public_client: JsonRpcClient, args):
    format = "{time:20} {syncing:>15}{height:>30}{currentHeight:>30}"
    print(
        format.format(
            time="Timestamp",
            syncing="Syncing",
            height="LocalRootHeight",
            currentHeight="CurrentRootHeight",
        )
    )
    while True:
        while True:
            try:
                data = checkHeight(private_client, public_client)
                break
            except Exception as e:
                print("Failed to get the current root height", e)
                time.sleep(2)

        syncing_state = (False if data["height"] >= data["currentHeight"] else True)
        
        print(format.format(time=now(), syncing=str(syncing_state), height=data["height"], currentHeight=data["currentHeight"]))

        if syncing_state is False:
            break
        time.sleep(args.interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default="localhost", type=str, help="Cluster IP")


    parser.add_argument("--bootstrapip", default="jrpc.testnet2.quarkchain.io", type=str, help="Bootstrap Cluster IP")

    parser.add_argument(
        "-i", "--interval", default=10, type=int, help="Query interval in second"
    )

    args = parser.parse_args()

    private_endpoint = "http://{}:38391".format(args.ip)
    private_client = JsonRpcClient(private_endpoint, TIMEOUT)

    public_endpoint = "http://{}:38391".format(args.bootstrapip)
    public_client = JsonRpcClient(public_endpoint, TIMEOUT)




    query_height(private_client, public_client, args)


if __name__ == "__main__":
    # query syncing state
    main()
