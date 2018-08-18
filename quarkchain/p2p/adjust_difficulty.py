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
    """
    loops forever
    """
    ip_lookup = json.loads(args.ip_lookup)
    count = 0
    while True:
        try:
            clusters = monitoring.find_all_clusters(
                args.ip, args.p2p_port, args.jrpc_port, ip_lookup
            )
            num_nodes = len(clusters)
            if count == num_nodes:
                raise Exception("no change")
            servers = [
                (idx, Server("http://{}".format(cluster)))
                for idx, cluster in enumerate(clusters)
            ]
            await asyncio.gather(
                *[
                    async_adjust(
                        idx,
                        server,
                        num_nodes * args.base_root,
                        num_nodes * args.base_minor,
                        not args.do_not_mine,
                    )
                    for (idx, server) in servers
                ]
            )
            print(
                "Successfully set {} nodes to root={},minor={} @{}".format(
                    num_nodes,
                    num_nodes * args.base_root,
                    num_nodes * args.base_minor,
                    datetime.now(),
                )
            )
            count = num_nodes
        except Exception as ex:
            print("Got Exception: {}, continuing in {}".format(ex, args.interval))
            pass
        await asyncio.sleep(args.interval)


async def adjust_imbalanced_hashpower(args):
    """
    does not loop, just try once

    set to 10% clusters has 90% hash power
    hash power ratio of individual cluster would be 81:1 (do the math yourself)
    """
    ip_lookup = json.loads(args.ip_lookup)
    try:
        clusters = monitoring.find_all_clusters(
            args.ip, args.p2p_port, args.jrpc_port, ip_lookup
        )
        clusters.sort()
        num_nodes = len(clusters)
        if num_nodes < 10:
            raise Exception("no point with < 10 clusters")

        num_rich = int(num_nodes / 10)
        print("***********************************************")
        print("num_nodes = ", num_nodes)
        print("num_rich = ", num_rich)
        clusters_rich = clusters[:num_rich]
        clusters_poor = clusters[num_rich:]
        servers_rich = [
            (idx, Server("http://{}".format(cluster)))
            for idx, cluster in enumerate(clusters_rich)
        ]
        servers_poor = [
            (idx, Server("http://{}".format(cluster)))
            for idx, cluster in enumerate(clusters_poor)
        ]
        rich_root = int(num_nodes * args.base_root / 9)
        rich_minor = int(num_nodes * args.base_minor / 9)
        poor_root = num_nodes * args.base_root * 9
        poor_minor = num_nodes * args.base_minor * 9

        await asyncio.gather(
            *[
                async_adjust(idx, server, rich_root, rich_minor, not args.do_not_mine)
                for (idx, server) in servers_rich
            ]
        )
        print(
            "Successfully set {} nodes to root={},minor={} @{}".format(
                len(servers_rich), rich_root, rich_minor, datetime.now()
            )
        )
        print("rich clusters: ", clusters_rich)

        await asyncio.gather(
            *[
                async_adjust(idx, server, poor_root, poor_minor, not args.do_not_mine)
                for (idx, server) in servers_poor
            ]
        )
        print(
            "Successfully set {} nodes to root={},minor={} @{}".format(
                len(servers_poor), poor_root, poor_minor, datetime.now()
            )
        )
        print("poor clusters: ", clusters_poor)
    except Exception as ex:
        print("Got Exception: {}".format(ex, args.interval))
        pass


def main():
    parser = argparse.ArgumentParser()
    # do not use "localhost", use the private ip if you run this from EC2
    parser.add_argument("--ip", default="54.213.57.24", type=str)
    parser.add_argument("--p2p_port", default=38291, type=int)
    parser.add_argument("--jrpc_port", default=38491, type=int)
    # eg: '{"172.31.15.196": "54.186.3.84"}'
    parser.add_argument("--ip_lookup", default="{}", type=str)
    parser.add_argument("--interval", default=5, type=int)
    parser.add_argument("--base_root", default=60, type=int)
    parser.add_argument("--base_minor", default=10, type=int)
    parser.add_argument("--do_not_mine", default=False, type=bool)
    parser.add_argument("--balanced", default=False, type=bool)
    args = parser.parse_args()

    if args.balanced:
        asyncio.get_event_loop().run_until_complete(async_adjust_difficulty(args))
    else:
        asyncio.get_event_loop().run_until_complete(adjust_imbalanced_hashpower(args))


if __name__ == "__main__":
    main()
