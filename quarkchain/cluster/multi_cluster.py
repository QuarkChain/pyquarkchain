import argparse
import cluster as cl
import asyncio
import random

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--num_cluster", default=2, type=int)
    parser.add_argument(
        "--num_slaves", default=4, type=int)
    parser.add_argument(
        "--port_start", default=cl.PORT, type=int)
    parser.add_argument(
        "--db_prefix", default="./db_", type=str)
    parser.add_argument(
        "--p2p_port", default=48291, type=int)
    parser.add_argument(
        "--json_rpc_port", default=58291, type=int)
    parser.add_argument(
        "--seed_host", default=cl.DEFAULT_ENV.config.P2P_SEED_HOST)
    parser.add_argument(
        "--seed_port", default=cl.DEFAULT_ENV.config.P2P_SEED_PORT)
    parser.add_argument(
        "--clean", default=True)
    parser.add_argument(
        "--devp2p", default=True, type=bool)
    parser.add_argument(
        "--devp2p_port", default=29000, type=int)
    parser.add_argument(
        "--devp2p_bootstrap_host", default='0.0.0.0', type=str)
    parser.add_argument(
        "--devp2p_bootstrap_port", default=29000, type=int)
    parser.add_argument(
        "--devp2p_min_peers", default=2, type=int)
    parser.add_argument(
        "--devp2p_max_peers", default=5, type=int)

    args = parser.parse_args()
    clusters = []
    mine_i = random.randint(0, args.num_cluster - 1)
    print("cluster {} will be mining".format(mine_i))
    for i in range(args.num_cluster):
        config = cl.create_cluster_config(
            slaveCount=args.num_slaves,
            ip=cl.IP,
            p2pPort=args.p2p_port + i,
            clusterPortStart=args.port_start + i * 100,
            jsonRpcPort=args.json_rpc_port + i,
            seedHost=args.seed_host,
            seedPort=args.seed_port,
            dbPrefix="{}C{}_".format(args.db_prefix, i),
            devp2p=args.devp2p,
            devp2p_port=args.devp2p_port + i,
            devp2p_bootstrap_host=args.devp2p_bootstrap_host,
            devp2p_bootstrap_port=args.devp2p_bootstrap_port,
            devp2p_min_peers=args.devp2p_min_peers,
            devp2p_max_peers=args.devp2p_max_peers,
        )
        mine = False
        if i == mine_i:
            mine = True
        filename = cl.dump_config_to_file(config)
        clusters.append(cl.Cluster(config, filename, mine, args.clean, "C{}_".format(i)))

    tasks = []
    for cluster in clusters:
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
    finally:
        loop.close()
