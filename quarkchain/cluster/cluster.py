import argparse
import asyncio
import json
import os
import tempfile

from asyncio import subprocess

from quarkchain.utils import is_p2
from quarkchain.config import DEFAULT_ENV

IP = "127.0.0.1"
PORT = 38000


def create_temp_cluster_config(args):
    num_slaves = args.num_slaves
    if num_slaves <= 0 or not is_p2(num_slaves):
        print("Number of slaves must be power of 2")
        return None

    config = dict()
    config["master"] = {
        "ip": IP,
        "port": args.port_start,
        "db_path": args.db_prefix + "m",
        "server_port": args.p2p_port
    }
    config["slaves"] = []
    for i in range(num_slaves):
        mask = i | num_slaves
        config["slaves"].append({
            "id": "S{}".format(i),
            "ip": IP,
            "port": args.port_start + i + 1,
            "shard_masks": [mask],
            "db_path": args.db_prefix + str(i)
        })

    return config


def dump_config_to_file(config):
    fd, filename = tempfile.mkstemp()
    with os.fdopen(fd, 'w') as tmp:
        json.dump(config, tmp)
    return filename


async def run_master(port, configFilePath, dbPath, serverPort):
    cmd = "python3 master.py --node_port={} --cluster_config={} --db_path={} --server_port={}".format(
        port, configFilePath, dbPath, serverPort)
    return await asyncio.create_subprocess_exec(*cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


async def run_slave(port, id, shardMaskList, dbPath):
    cmd = "python3 slave.py --node_port={} --shard_mask={} --node_id={} --db_path={}".format(
        port, shardMaskList[0], id, dbPath)
    return await asyncio.create_subprocess_exec(*cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


async def print_output(prefix, stream):
    while True:
        line = await stream.readline()
        if not line:
            break
        print("{}: {}".format(prefix, line.decode("ascii").strip()))


class Cluster:

    def __init__(self, config, configFilePath):
        self.config = config
        self.configFilePath = configFilePath
        self.procs = []
        self.shutdownCalled = False

    async def waitAndShutdown(self, prefix, proc):
        ''' If one process terminates shutdown the entire cluster '''
        await proc.wait()
        if self.shutdownCalled:
            return

        print("{} is dead. Shutting down the cluster...".format(prefix))
        await self.shutdown()

    async def run(self):
        master = await run_master(
            port=self.config["master"]["port"],
            configFilePath=self.configFilePath,
            dbPath=self.config["master"]["db_path"],
            serverPort=self.config["master"]["server_port"])
        asyncio.ensure_future(print_output("MASTER", master.stdout))

        self.procs.append(("MASTER", master))
        for slave in self.config["slaves"]:
            s = await run_slave(
                port=slave["port"],
                id=slave["id"],
                shardMaskList=slave["shard_masks"],
                dbPath=slave["db_path"])
            prefix = "SLAVE_{}".format(slave["id"])
            asyncio.ensure_future(print_output(prefix, s.stdout))
            self.procs.append((prefix, s))

        await asyncio.gather(*[self.waitAndShutdown(prefix, proc) for prefix, proc in self.procs])

    async def shutdown(self):
        self.shutdownCalled = True
        for prefix, proc in self.procs:
            try:
                proc.terminate()
            except Exception:
                pass
        await asyncio.gather(*[proc.wait() for prefix, proc in self.procs])

    def startAndLoop(self):
        try:
            asyncio.get_event_loop().run_until_complete(self.run())
        except KeyboardInterrupt:
            asyncio.get_event_loop().run_until_complete(self.shutdown())


def main():
    global PORT
    env = DEFAULT_ENV.copy()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cluster_config", default="cluster_config.json", type=str)
    parser.add_argument(
        "--num_slaves", default=4, type=int)
    parser.add_argument(
        "--port_start", default=PORT, type=int)
    parser.add_argument(
        "--db_prefix", default="./db_", type=str)
    parser.add_argument(
        "--p2p_port", default=env.config.P2P_SERVER_PORT)
    args = parser.parse_args()

    if args.num_slaves <= 0:
        config = json.load(open(args.cluster_config))
        filename = args.cluster_config
    else:
        config = create_temp_cluster_config(args)
        if not config:
            return -1
        filename = dump_config_to_file(config)

    cluster = Cluster(config, filename)
    cluster.startAndLoop()


if __name__ == '__main__':
    main()
