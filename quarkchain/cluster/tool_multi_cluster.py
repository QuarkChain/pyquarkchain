"""
tool_multi_cluster.py - starts multiple clusters on localhost, and have them inter-connect using either simple network or real p2p
Usage:
tool_multi_cluster.py accepts the same arguments as cluster.py
additional arguments:
--num_clusters
also note that p2p bootstrap key is fixed to a test value

Examples:
1. simple network, with one (random) cluster mining
python tool_multi_cluster.py --start_simulated_mining
2. p2p module, with one (random) cluster mining
python tool_multi_cluster.py --p2p --start_simulated_mining
"""


import argparse
import asyncio
import os
import random

from quarkchain.p2p.utils import colors, COLOR_END
from quarkchain.cluster import cluster as cl
from quarkchain.cluster.cluster_config import ClusterConfig


async def main():
    parser = argparse.ArgumentParser()
    ClusterConfig.attach_arguments(parser)
    parser.add_argument("--num_clusters", default=2, type=int)
    args = parser.parse_args()
    clusters = []
    mine_i = random.randint(0, args.num_clusters - 1)
    mine = args.start_simulated_mining
    if mine:
        print("cluster {} will be mining".format(mine_i))
    else:
        print("No one will be mining")

    db_path_root = args.db_path_root
    p2p_port = args.p2p_port
    for i in range(args.num_clusters):
        args.start_simulated_mining = mine and i == mine_i
        args.db_path_root = "{}_C{}".format(db_path_root, i)

        # set up p2p bootstrapping, with fixed bootstrap key for now
        if args.p2p:
            if i == 0:
                args.privkey = (
                    "31552f186bf90908ce386fb547dd0410bf443309125cc43fd0ffd642959bf6d9"
                )
            else:
                args.privkey = ""

            args.bootnodes = "enode://c571e0db93d17cc405cb57640826b70588a6a28785f38b21be471c609ca12fcb06cb306ac44872908f5bed99046031a5af82072d484e3ef9029560c1707193a0@127.0.0.1:{}".format(
                p2p_port
            )

        config = ClusterConfig.create_from_args(args)
        print("Cluster {} config file: {}".format(i, config.json_filepath))
        print(config.to_json())

        clusters.append(
            cl.Cluster(config, "{}C{}{}_".format(colors[i % len(colors)], i, COLOR_END))
        )

        args.p2p_port += 1
        args.port_start += 100
        args.json_rpc_port += 1
        args.json_rpc_private_port += 1

    tasks = list()
    tasks.append(asyncio.ensure_future(clusters[0].run()))
    await asyncio.sleep(3)
    for cluster in clusters[1:]:
        tasks.append(asyncio.ensure_future(cluster.run()))
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        try:
            for cluster in clusters:
                asyncio.get_event_loop().run_until_complete(cluster.shutdown())
        except Exception:
            pass


if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        try:
            cl.kill_child_processes(os.getpid())
        except Exception:
            pass
    finally:
        loop.close()
