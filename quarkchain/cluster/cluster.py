import argparse
import asyncio
import logging
import os
import platform
import signal
import sys
from asyncio import subprocess

import psutil

from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.env import DEFAULT_ENV


PYTHON = "pypy3" if platform.python_implementation() == "PyPy" else "python3"


def kill_child_processes(parent_pid):
    """ Kill all the subprocesses recursively """
    try:
        parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
        return
    children = parent.children(recursive=True)
    print(
        "================================ SHUTTING DOWN CLUSTER ================================"
    )
    for process in children:
        try:
            print("SIGTERM >>> " + " ".join(process.cmdline()[1:]))
        except Exception:
            pass
        process.send_signal(signal.SIGTERM)
        process.wait()


async def run_master(config_file, extra_cmd):
    cmd = "{} -u master.py --cluster_config={}".format(PYTHON, config_file)
    cmd += extra_cmd
    return await asyncio.create_subprocess_exec(
        *cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )


async def run_slave(config_file, id, profile):
    cmd = "{} -u slave.py --cluster_config={} --node_id={}".format(
        PYTHON, config_file, id
    )
    if profile:
        cmd += " --enable_profiler=true"
    return await asyncio.create_subprocess_exec(
        *cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )


async def run_prom(config, bal):
    balance = " --enable_count_balance" if bal else ""
    cmd = (
        f"{PYTHON} -u prom.py --interval {config.INTERVAL} --tokens {config.TOKENS} --port {config.PORT}"
        + balance
    )
    await asyncio.create_subprocess_exec(*cmd.split(" "))


async def print_output(prefix, stream):
    while True:
        try:
            line = await stream.readline()
            if not line:
                break
            print("{}: {}".format(prefix, line.decode("ascii").strip()))
        except Exception as e:
            print("{}: reading line exception {}".format(prefix, e))


class Cluster:
    def __init__(self, config: ClusterConfig, cluster_id="", args=None):
        self.config = config
        self.procs = []
        self.shutdown_called = False
        self.cluster_id = cluster_id
        self.check_db_only = False
        self.args = args

    async def wait_and_shutdown(self, prefix, proc):
        """ If one process terminates shutdown the entire cluster """
        status = await proc.wait()
        if self.shutdown_called:
            # ignore subsequent return code, only the first one matters
            return None

        print("{} is dead. Shutting down the cluster...".format(prefix))
        await self.shutdown()
        return status

    async def run_master(self):
        extra_cmd = ""
        if self.check_db_only:
            extra_cmd += " --check_db=true --check_db_rblock_from={0} --check_db_rblock_to={1} --check_db_rblock_batch={2}".format(
                self.args.check_db_rblock_from,
                self.args.check_db_rblock_to,
                self.args.check_db_rblock_batch,
            )
        if "MASTER" in self.args.profile.split(","):
            extra_cmd += " --enable_profiler=true"
        master = await run_master(self.config.json_filepath, extra_cmd)
        prefix = "{}MASTER".format(self.cluster_id)
        asyncio.ensure_future(print_output(prefix, master.stdout))
        self.procs.append((prefix, master))

    async def run_slaves(self):
        for slave in self.config.SLAVE_LIST:
            s = await run_slave(
                self.config.json_filepath,
                slave.ID,
                slave.ID in self.args.profile.split(","),
            )
            prefix = "{}SLAVE_{}".format(self.cluster_id, slave.ID)
            asyncio.ensure_future(print_output(prefix, s.stdout))
            self.procs.append((prefix, s))

    async def run_prom(self):
        await run_prom(self.config.PROMETHEUS, self.args.bal)

    async def run(self):
        await self.run_master()
        # p2p discovery / crawling mode will disable slaves
        if not (
            self.config.P2P.DISCOVERY_ONLY
            or self.config.P2P.CRAWLING_ROUTING_TABLE_FILE_PATH
        ):
            await self.run_slaves()
        if self.args.prom:
            await self.run_prom()
        status_list = await asyncio.gather(
            *[self.wait_and_shutdown(prefix, proc) for prefix, proc in self.procs]
        )
        ret_code = (
            1
            if any(status != 0 and status is not None for status in status_list)
            else 0
        )
        sys.exit(ret_code)

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

    def check_db(self):
        self.check_db_only = True
        self.start_and_loop()


def parse_args():
    parser = argparse.ArgumentParser()
    ClusterConfig.attach_arguments(parser)
    parser.add_argument("--profile", default="", type=str)
    parser.add_argument("--check_db_rblock_from", default=-1, type=int)
    parser.add_argument("--check_db_rblock_to", default=0, type=int)
    parser.add_argument("--check_db_rblock_batch", default=1, type=int)
    args = parser.parse_args()

    env = DEFAULT_ENV.copy()
    env.cluster_config = ClusterConfig.create_from_args(args)
    env.arguments = args

    return env


def main():
    logging.getLogger("asyncio").setLevel(logging.ERROR)
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    env = parse_args()
    config = env.cluster_config
    args = env.arguments

    print("Cluster config file: {}".format(config.json_filepath))
    print(config.to_json())

    cluster = Cluster(config, args=args)

    if args.check_db:
        cluster.check_db()
    else:
        cluster.start_and_loop()


if __name__ == "__main__":
    main()
