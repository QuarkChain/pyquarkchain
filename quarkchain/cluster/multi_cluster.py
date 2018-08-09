import argparse
import asyncio
import os
import random
import socket

from quarkchain.cluster import cluster as cl

from devp2p.utils import colors, COLOR_END


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num_cluster", default=2, type=int)
    parser.add_argument(
        "--num_slaves", default=4, type=int)
    parser.add_argument(
        "--port_start", default=cl.PORT, type=int)
    parser.add_argument(
        "--db_path_root", default="./db", type=str)
    parser.add_argument(
        "--p2p_port", default=48291, type=int)
    parser.add_argument(
        "--json_rpc_port", default=48391, type=int)
    parser.add_argument(
        "--json_rpc_private_port", default=48491, type=int)
    parser.add_argument(
        "--seed_host", default=cl.DEFAULT_ENV.config.P2P_SEED_HOST)
    parser.add_argument(
        "--seed_port", default=cl.DEFAULT_ENV.config.P2P_SEED_PORT)
    parser.add_argument(
        "--clean", default=False)
    parser.add_argument(
        "--devp2p", default=True, type=bool)
    parser.add_argument(
        "--devp2p_ip", default='', type=str)
    parser.add_argument(
        "--devp2p_start_port", default=29000, type=int)
    parser.add_argument(
        "--devp2p_bootstrap_host", default=socket.gethostbyname(socket.gethostname()), type=str)
    parser.add_argument(
        "--devp2p_bootstrap_port", default=29000, type=int)
    parser.add_argument(
        "--devp2p_min_peers", default=2, type=int)
    parser.add_argument(
        "--devp2p_max_peers", default=5, type=int)
    parser.add_argument(
        "--mine", default=False, type=bool)

    args = parser.parse_args()
    clusters = []
    mine_i = random.randint(0, args.num_cluster - 1)
    if args.mine:
        print("cluster {} will be mining".format(mine_i))
    else:
        print("No one will be mining")
    for i in range(args.num_cluster):
        config = cl.create_cluster_config(
            slave_count=args.num_slaves,
            ip=cl.IP,
            p2p_port=args.p2p_port + i,
            cluster_port_start=args.port_start + i * 100,
            json_rpc_port=args.json_rpc_port + i,
            json_rpc_private_port=args.json_rpc_private_port + i,
            seed_host=args.seed_host,
            seed_port=args.seed_port,
            db_path_root="{}_C{}".format(args.db_path_root, i),
            devp2p=args.devp2p,
            devp2p_ip=args.devp2p_ip,
            devp2p_port=args.devp2p_start_port + i,
            devp2p_bootstrap_host=args.devp2p_bootstrap_host,
            devp2p_bootstrap_port=args.devp2p_bootstrap_port,
            devp2p_min_peers=args.devp2p_min_peers,
            devp2p_max_peers=args.devp2p_max_peers,
            devp2p_additional_bootstraps='',
        )
        mine = args.mine and i == mine_i
        filename = cl.dump_config_to_file(config)
        clusters.append(
            cl.Cluster(
                config, filename, mine, args.clean, False, "{}C{}{}_".format(colors[i % len(colors)], i, COLOR_END)
        ))

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


if __name__ == '__main__':
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
