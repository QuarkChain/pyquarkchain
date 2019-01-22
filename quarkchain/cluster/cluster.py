import argparse
import asyncio
import os
import platform
import signal
from asyncio import subprocess

import psutil

from quarkchain.cluster.cluster_config import ClusterConfig


PYTHON = "pypy3" if platform.python_implementation() == "PyPy" else "python3"


def kill_child_processes(parent_pid, sig=signal.SIGTERM):
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
        process.send_signal(sig)


async def run_master(config_file):
    cmd = "{} -u master.py --cluster_config={}".format(PYTHON, config_file)
    return await asyncio.create_subprocess_exec(
        *cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )


async def run_slave(config_file, id):
    cmd = "{} -u slave.py --cluster_config={} --node_id={}".format(
        PYTHON, config_file, id
    )
    return await asyncio.create_subprocess_exec(
        *cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )


async def print_output(prefix, stream):
    while True:
        line = await stream.readline()
        if not line:
            break
        print("{}: {}".format(prefix, line.decode("ascii").strip()))


class Cluster:
    def __init__(self, config, cluster_id=""):
        self.config = config
        self.procs = []
        self.shutdown_called = False
        self.cluster_id = cluster_id

    async def wait_and_shutdown(self, prefix, proc):
        """ If one process terminates shutdown the entire cluster """
        await proc.wait()
        if self.shutdown_called:
            return

        print("{} is dead. Shutting down the cluster...".format(prefix))
        await self.shutdown()

    async def run_master(self):
        master = await run_master(self.config.json_filepath)
        prefix = "{}MASTER".format(self.cluster_id)
        asyncio.ensure_future(print_output(prefix, master.stdout))
        self.procs.append((prefix, master))

    async def run_slaves(self):
        for slave in self.config.SLAVE_LIST:
            s = await run_slave(self.config.json_filepath, slave.ID)
            prefix = "{}SLAVE_{}".format(self.cluster_id, slave.ID)
            asyncio.ensure_future(print_output(prefix, s.stdout))
            self.procs.append((prefix, s))

    async def run(self):
        await self.run_master()
        await self.run_slaves()

        await asyncio.gather(
            *[self.wait_and_shutdown(prefix, proc) for prefix, proc in self.procs]
        )

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
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    parser = argparse.ArgumentParser()
    ClusterConfig.attach_arguments(parser)
    args = parser.parse_args()

    config = ClusterConfig.create_from_args(args)
    print("Cluster config file: {}".format(config.json_filepath))
    print(config.to_json())

    cluster = Cluster(config)

    cluster.start_and_loop()


if __name__ == "__main__":
    main()
