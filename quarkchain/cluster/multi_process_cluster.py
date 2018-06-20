import argparse
import cluster as cl
import asyncio
import random
from devp2p.utils import colors, COLOR_END
from cluster import kill_child_processes, print_output
from asyncio import subprocess

async def run_cluster(**kwargs):
    cmd = "python cluster.py --cluster_config={} --num_slaves={} --port_start={} " \
          "--db_prefix={} --p2p_port={} --json_rpc_port={} --seed_host={} --seed_port={} " \
          "--devp2p_port={} --devp2p_bootstrap_host={} --devp2p_bootstrap_port={} " \
          "--devp2p_min_peers={} --devp2p_max_peers={}".format(
              kwargs['cluster_config'], kwargs['num_slaves'], kwargs['port_start'],
              kwargs['db_prefix'], kwargs['p2p_port'], kwargs['json_rpc_port'],
              kwargs['seed_host'], kwargs['seed_port'],
              kwargs['devp2p_port'], kwargs['devp2p_bootstrap_host'], kwargs['devp2p_bootstrap_port'],
              kwargs['devp2p_min_peers'], kwargs['devp2p_max_peers'])
    if kwargs['mine']:
        cmd += " --mine=True"
    if kwargs['devp2p']:
        cmd += " --devp2p=True"
    if kwargs['clean']:
        cmd += " --clean=True"
    return await asyncio.create_subprocess_exec(*cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


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
        "--devp2p_start_port", default=29000, type=int)
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
        config = dict(
            cluster_config="cluster_config.json",
            num_slaves=args.num_slaves,
            port_start=args.port_start + i * 100,
            db_prefix="{}C{}_".format(args.db_prefix, i),
            p2p_port=args.p2p_port + i,
            json_rpc_port=args.json_rpc_port + i,
            seed_host=args.seed_host,
            seed_port=args.seed_port,
            devp2p_port=args.devp2p_start_port + i,
            devp2p_bootstrap_host=args.devp2p_bootstrap_host,
            devp2p_bootstrap_port=args.devp2p_bootstrap_port,
            devp2p_min_peers=args.devp2p_min_peers,
            devp2p_max_peers=args.devp2p_max_peers,
            clean=args.clean,
            devp2p=args.devp2p,
            mine=i == mine_i,
            cluster_id="{}C{}{}_".format(colors[i % len(colors)] ,i, COLOR_END),
        )
        clusters.append(config)

    procs = []
    procs.append((clusters[0]['cluster_id'], await run_cluster(**clusters[0])))
    await asyncio.sleep(3)
    for cluster in clusters[1:]:
        procs.append((cluster['cluster_id'], await run_cluster(**cluster)))
    for prefix, proc in procs:
        asyncio.ensure_future(print_output(prefix, proc.stdout))
    await asyncio.gather(*[proc.wait() for prefix, proc in procs])


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        try:
            kill_child_processes(os.getpid())
        except Exception:
            pass
    finally:
        loop.close()
