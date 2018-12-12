#! /usr/bin/env pypy3

import argparse
import logging
import time
from datetime import datetime
import jsonrpcclient
import psutil
import numpy
from decimal import Decimal

TIMEOUT=10

# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def checkHeight(private_client, public_client, timeout=TIMEOUT):
    result_private = private_client.send(
        jsonrpcclient.Request("getRootBlockByHeight"),
        timeout=timeout,)
    result_public = public_client.send(
        jsonrpcclient.Request("getRootBlockByHeight"),
        timeout=timeout,)
    return {
        "height": str(result_private["height"]),
        "currentHeight": str(result_public["height"]),
    }



def query_height(private_client, public_client, args):
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

        syncing_state = ("False" if data["height"] >= data["currentHeight"] else "True")
        
        print(format.format(time=now(), syncing=syncing_state, height=int(data["height"], 16), currentHeight=int(data["currentHeight"], 16)))

        if data["height"] >= data["currentHeight"]:
            break
        time.sleep(args.interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ip", default="localhost", type=str, help="Cluster IP")


    parser.add_argument("--bootstrapip", default="54.70.162.141", type=str, help="Bootstrap Cluster IP")

    parser.add_argument(
        "-i", "--interval", default=10, type=int, help="Query interval in second"
    )

    args = parser.parse_args()

    private_endpoint = "http://{}:38391".format(args.ip)
    private_client = jsonrpcclient.HTTPClient(private_endpoint)

    public_endpoint = "http://{}:38391".format(args.bootstrapip)
    public_client = jsonrpcclient.HTTPClient(public_endpoint)




    query_height(private_client, public_client, args)


if __name__ == "__main__":
    # query syncing state
    main()
