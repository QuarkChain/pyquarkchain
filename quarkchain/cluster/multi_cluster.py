import argparse
import asyncio
import os
import random

from devp2p.utils import colors, COLOR_END
from quarkchain.cluster import cluster as cl
from quarkchain.cluster.cluster_config import ClusterConfig


async def main():
    parser = argparse.ArgumentParser()
    ClusterConfig.attach_arguments(parser)
    parser.add_argument("--num_clusters", default=2, type=int)
    args = parser.parse_args()
    clusters = []
    mine_i = random.randint(0, args.num_clusters - 1)
    if args.mine:
        print("cluster {} will be mining".format(mine_i))
    else:
        print("No one will be mining")
    mine = args.mine
    db_path_root = args.db_path_root
    for i in range(args.num_clusters):
        args.mine = mine and i == mine_i
        args.db_path_root = "{}_C{}".format(db_path_root, i)

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
        args.devp2p_port += 1

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
