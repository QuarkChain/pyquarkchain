import argparse
import asyncio
import json

from asyncio import subprocess


async def run_master(port):
    cmd = "python master.py --node_port={}".format(port)
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
    args = parser.parse_args()
    config = json.load(open(args.cluster_config))

    cluster = Cluster(config)
    cluster.startAndLoop()


if __name__ == '__main__':
    main()
