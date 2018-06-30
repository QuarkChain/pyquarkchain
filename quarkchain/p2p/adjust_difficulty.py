import monitoring

import argparse
import asyncio
import json
from datetime import datetime
from jsonrpc_async import Server

"""
this is a centralized place that sets mining difficulty
it crawls the network to gather node count and set mean block interval accordingly

we can always do more fancy improvements such as setting this based on past N block intervals
"""


async def async_adjust(idx, server, root, minor, mining):
    response = await server.setTargetBlockTime(root, minor)
    print("idx={};response={}".format(idx, response))
    await server.setMining(mining)


async def async_adjust_difficulty(args):
    ip_lookup = json.loads(args.ip_lookup)
    count = 0
    while True:
        try:
            clusters = monitoring.find_all_clusters(args.ip, args.p2p_port, args.jrpc_port, ip_lookup)
            num_nodes = len(clusters)
            if count == num_nodes:
                raise Exception("no change")
            servers = [(idx, Server("http://{}".format(cluster))) for idx, cluster in enumerate(clusters)]
            await asyncio.gather(
                *[async_adjust(idx,
                               server,
                               num_nodes * args.base_root,
                               num_nodes * args.base_minor,
                               not args.do_not_mine)
                  for (idx, server) in servers])
            print("Successfully set {} nodes to root={},minor={} @{}".format(
                num_nodes,
                num_nodes * args.base_root,
                num_nodes * args.base_minor,
                datetime.now()))
            count = num_nodes
        except Exception as ex:
            print("Got Exception: {}, continuing in {}".format(ex, args.interval))
            pass
        await asyncio.sleep(args.interval)


def main():
    parser = argparse.ArgumentParser()
    # do not use "localhost", use the private ip if you run this from EC2
    parser.add_argument(
        "--ip", default="54.186.3.84", type=str)
    parser.add_argument(
        "--p2p_port", default=38291, type=int)
    parser.add_argument(
        "--jrpc_port", default=38491, type=int)
    # eg: '{"172.31.15.196": "54.186.3.84"}'
    parser.add_argument(
        "--ip_lookup", default='{}', type=str)
    parser.add_argument(
        "--interval", default=5, type=int)
    parser.add_argument(
        "--base_root", default=60, type=int)
    parser.add_argument(
        "--base_minor", default=10, type=int)
    parser.add_argument(
        "--do_not_mine", default=False, type=bool)
    args = parser.parse_args()

    asyncio.get_event_loop().run_until_complete(async_adjust_difficulty(args))


if __name__ == "__main__":
    main()
