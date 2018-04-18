import argparse
import asyncio
import json

from asyncio import subprocess

from quarkchain.utils import is_p2

TEMP_CLUSTER_CONFIG = "cluster_config_temp.json"
IP = "127.0.0.1"
PORT = 38000


def creatre_temp_cluster_config(num_slaves):
    if num_slaves <= 0 or not is_p2(num_slaves):
        print("Number of slaves must be power of 2")
        return None

    config = dict()
    config["master"] = {
        "ip": IP,
        "port": PORT,
    }
    config["slaves"] = []
    for i in range(num_slaves):
        mask = i | num_slaves
        config["slaves"].append({
            "id": "S{}".format(i),
            "ip": IP,
            "port": PORT + i + 1,
            "shard_masks": [mask]
        })

    return config


def dump_config_to_file(config):
    with open(TEMP_CLUSTER_CONFIG, "w") as outfile:
        json.dump(config, outfile)


async def run_master(port):
    cmd = "python master.py --node_port={} --cluster_config={}".format(port, TEMP_CLUSTER_CONFIG)
    return await asyncio.create_subprocess_exec(*cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


async def run_slave(port, id, shardMaskList):
    cmd = "python slave.py --node_port={} --shard_mask={} --node_id={} --in_memory_db=true".format(
        port, shardMaskList[0], id)
    return await asyncio.create_subprocess_exec(*cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


async def print_output(prefix, stream):
    while True:
        line = await stream.readline()
        if not line:
            break
        print("{}: {}".format(prefix, line.decode("ascii").strip()))


class Cluster:

    def __init__(self, config):
        self.config = config
        self.procs = []

    async def run(self):
        master = await run_master(self.config["master"]["port"])
        asyncio.ensure_future(print_output("MASTER", master.stdout))

        self.procs.append(master)
        for slave in self.config["slaves"]:
            s = await run_slave(slave["port"], slave["id"], slave["shard_masks"])
            asyncio.ensure_future(print_output("SLAVE_{}".format(slave["id"]), s.stdout))
            self.procs.append(s)

        await asyncio.gather(*[proc.wait() for proc in self.procs])

    async def shutdown(self):
        self.procs[0].terminate()
        await asyncio.gather(*[proc.wait() for proc in self.procs])

    def startAndLoop(self):
        try:
            asyncio.get_event_loop().run_until_complete(self.run())
        except KeyboardInterrupt:
            asyncio.get_event_loop().run_until_complete(self.shutdown())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cluster_config", default="cluster_config.json", type=str)
    parser.add_argument(
        "--num_slaves", default=4, type=int)
    args = parser.parse_args()

    if args.num_slaves <= 0:
        config = json.load(open(args.cluster_config))
    else:
        config = creatre_temp_cluster_config(args.num_slaves)
        if not config:
            return -1
    dump_config_to_file(config)

    cluster = Cluster(config)
    cluster.startAndLoop()


if __name__ == '__main__':
    main()
