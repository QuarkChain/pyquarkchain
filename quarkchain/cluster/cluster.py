import argparse
import asyncio
import json
import os
import signal
import socket
import tempfile
from asyncio import subprocess

import psutil

from quarkchain.cluster.utils import create_cluster_config
from quarkchain.config import DEFAULT_ENV

IP = "127.0.0.1"
PORT = 38000


def kill_child_processes(parent_pid, sig=signal.SIGTERM):
    """ Kill all the subprocesses recursively """
    try:
        parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
        return
    children = parent.children(recursive=True)
    print("================================ SHUTTING DOWN CLUSTER ================================")
    for process in children:
        try:
            print("SIGTERM >>> " + " ".join(process.cmdline()[1:]))
        except Exception:
            pass
        process.send_signal(sig)


def dump_config_to_file(config):
    fd, filename = tempfile.mkstemp()
    with os.fdopen(fd, 'w') as tmp:
        json.dump(config, tmp)
    return filename


async def run_master(config_file_path, db_path_root, server_port, json_rpc_port, json_rpc_private_port, seed_host,
                     seed_port, mine, clean, **kwargs):
    cmd = "pypy3 master.py --cluster_config={} --db_path_root={} " \
          "--server_port={} --local_port={} --json_rpc_private_port={} --seed_host={} --seed_port={} " \
          "--devp2p_ip={} --devp2p_port={} --devp2p_bootstrap_host={} " \
          "--devp2p_bootstrap_port={} --devp2p_min_peers={} --devp2p_max_peers={} " \
          "--devp2p_additional_bootstraps={}".format(
        config_file_path, db_path_root, server_port, json_rpc_port, json_rpc_private_port, seed_host, seed_port,
        kwargs['devp2p_ip'], kwargs['devp2p_port'], kwargs['devp2p_bootstrap_host'],
        kwargs['devp2p_bootstrap_port'], kwargs['devp2p_min_peers'], kwargs['devp2p_max_peers'],
        kwargs['devp2p_additional_bootstraps'])
    if mine:
        cmd += " --mine=true"
    if kwargs['devp2p']:
        cmd += " --devp2p=true"
    if clean:
        cmd += " --clean=true"
    return await asyncio.create_subprocess_exec(*cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


async def run_slave(port, id, shard_mask_list, db_path_root, clean, enable_transaction_history):
    cmd = "pypy3 slave.py --node_port={} --shard_mask={} --node_id={} --db_path_root={}".format(
        port, shard_mask_list[0], id, db_path_root)
    if clean:
        cmd += " --clean=true"
    if enable_transaction_history:
        cmd += " --enable_transaction_history=true"
    return await asyncio.create_subprocess_exec(*cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


async def print_output(prefix, stream):
    while True:
        line = await stream.readline()
        if not line:
            break
        print("{}: {}".format(prefix, line.decode("ascii").strip()))


class Cluster:

    def __init__(self, config, config_file_path, mine, clean, enable_transaction_history, cluster_id=''):
        self.config = config
        self.config_file_path = config_file_path
        self.procs = []
        self.shutdown_called = False
        self.mine = mine
        self.clean = clean
        self.enable_transaction_history = enable_transaction_history
        self.cluster_id = cluster_id

    async def wait_and_shutdown(self, prefix, proc):
        ''' If one process terminates shutdown the entire cluster '''
        await proc.wait()
        if self.shutdown_called:
            return

        print("{} is dead. Shutting down the cluster...".format(prefix))
        await self.shutdown()

    async def run_master(self):
        master = await run_master(config_file_path=self.config_file_path,
                                  db_path_root=self.config["master"]["db_path_root"],
                                  server_port=self.config["master"]["server_port"],
                                  json_rpc_port=self.config["master"]["json_rpc_port"],
                                  json_rpc_private_port=self.config["master"]["json_rpc_private_port"],
                                  seed_host=self.config["master"]["seed_host"],
                                  seed_port=self.config["master"]["seed_port"], mine=self.mine, clean=self.clean,
                                  devp2p=self.config["master"]["devp2p"], devp2p_ip=self.config["master"]["devp2p_ip"],
                                  devp2p_port=self.config["master"]["devp2p_port"],
                                  devp2p_bootstrap_host=self.config["master"]["devp2p_bootstrap_host"],
                                  devp2p_bootstrap_port=self.config["master"]["devp2p_bootstrap_port"],
                                  devp2p_min_peers=self.config["master"]["devp2p_min_peers"],
                                  devp2p_max_peers=self.config["master"]["devp2p_max_peers"],
                                  devp2p_additional_bootstraps=self.config["master"]["devp2p_additional_bootstraps"])
        prefix = "{}MASTER".format(self.cluster_id)
        asyncio.ensure_future(print_output(prefix, master.stdout))
        self.procs.append((prefix, master))

    async def run_slaves(self):
        for slave in self.config["slaves"]:
            s = await run_slave(port=slave["port"], id=slave["id"], shard_mask_list=slave["shard_masks"],
                                db_path_root=slave["db_path_root"], clean=self.clean,
                                enable_transaction_history=self.enable_transaction_history)
            prefix = "{}SLAVE_{}".format(self.cluster_id, slave["id"])
            asyncio.ensure_future(print_output(prefix, s.stdout))
            self.procs.append((prefix, s))

    async def run(self):
        await self.run_master()
        await self.run_slaves()

        await asyncio.gather(*[self.wait_and_shutdown(prefix, proc) for prefix, proc in self.procs])

    async def shutdown(self):
        self.shutdown_called = True
        kill_child_processes(os.getpid())

    def start_and_loop(self):
        try:
            asyncio.get_event_loop().run_until_complete(self.run())
        except KeyboardInterrupt:
            try:
                asyncio.get_event_loop().run_until_complete(self.shutdown())
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cluster_config", default="cluster_config.json", type=str)
    parser.add_argument(
        "--num_slaves", default=4, type=int)
    parser.add_argument(
        "--mine", default=False, type=bool)
    parser.add_argument(
        "--port_start", default=PORT, type=int)
    parser.add_argument(
        "--db_path_root", default="./db", type=str)
    parser.add_argument(
        "--p2p_port", default=DEFAULT_ENV.config.P2P_SERVER_PORT)
    parser.add_argument(
        "--json_rpc_port", default=38391, type=int)
    parser.add_argument(
        "--json_rpc_private_port", default=38491, type=int)
    parser.add_argument(
        "--enable_transaction_history",
        action="store_true",
        default=False,
        dest="enable_transaction_history",
    )

    parser.add_argument(
        "--seed_host", default=DEFAULT_ENV.config.P2P_SEED_HOST)
    parser.add_argument(
        "--seed_port", default=DEFAULT_ENV.config.P2P_SEED_PORT)
    parser.add_argument(
        "--clean", default=False)
    parser.add_argument(
        "--devp2p", default=False, type=bool)
    '''
    set devp2p_ip so that peers can connect to this cluster
    leave empty if you want to use `socket.gethostbyname()`, but it may cause this cluster to be unreachable by peers
    '''
    parser.add_argument(
        "--devp2p_ip", default='', type=str)
    parser.add_argument(
        "--devp2p_port", default=29000, type=int)
    parser.add_argument(
        "--devp2p_bootstrap_host", default=socket.gethostbyname(socket.gethostname()), type=str)
    parser.add_argument(
        "--devp2p_bootstrap_port", default=29000, type=int)
    parser.add_argument(
        "--devp2p_min_peers", default=2, type=int)
    parser.add_argument(
        "--devp2p_max_peers", default=10, type=int)
    parser.add_argument(
        "--devp2p_additional_bootstraps", default='', type=str)

    args = parser.parse_args()

    if args.num_slaves <= 0:
        config = json.load(open(args.cluster_config))
        filename = args.cluster_config
    else:
        config = create_cluster_config(
            slave_count=args.num_slaves,
            ip=IP,
            p2p_port=args.p2p_port,
            cluster_port_start=args.port_start,
            json_rpc_port=args.json_rpc_port,
            json_rpc_private_port=args.json_rpc_private_port,
            seed_host=args.seed_host,
            seed_port=args.seed_port,
            db_path_root = args.db_path_root,
            devp2p=args.devp2p,
            devp2p_ip=args.devp2p_ip,
            devp2p_port=args.devp2p_port,
            devp2p_bootstrap_host=args.devp2p_bootstrap_host,
            devp2p_bootstrap_port=args.devp2p_bootstrap_port,
            devp2p_min_peers=args.devp2p_min_peers,
            devp2p_max_peers=args.devp2p_max_peers,
            devp2p_additional_bootstraps=args.devp2p_additional_bootstraps,
        )
        if not config:
            return -1
        filename = dump_config_to_file(config)

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    cluster = Cluster(config, filename, args.mine, args.clean, args.enable_transaction_history)
    if args.enable_transaction_history:
        print("Starting cluster with transaction history enabled...")

    cluster.start_and_loop()


if __name__ == '__main__':
    main()
