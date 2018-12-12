#! /usr/bin/env python3

import argparse
import logging
import time
from datetime import datetime
import jsonrpcclient
import psutil
import numpy
from decimal import Decimal


# disable jsonrpcclient verbose logging
logging.getLogger("jsonrpcclient.client.request").setLevel(logging.WARNING)
logging.getLogger("jsonrpcclient.client.response").setLevel(logging.WARNING)


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def checkHeight(private_client, public_client):
    s_p = private_client.send(jsonrpcclient.Request("getRootBlockByHeight"))
    s_b = public_client.send(jsonrpcclient.Request("getRootBlockByHeight"))
    return {
        "time": now(),
        "syncing": "True",
        "height": str(s_p["height"]),
        "currentHeight": str(s_b["height"]),
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
        data = checkHeight(private_client, public_client)
        if data["height"] >= data["currentHeight"]:
            break

        print(format.format(time=now(), syncing="True", height=int(data["height"], 16), currentHeight=int(data["currentHeight"], 16)))
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
