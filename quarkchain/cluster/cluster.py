import argparse
import asyncio
import json
import os
import tempfile

from asyncio import subprocess

from quarkchain.config import DEFAULT_ENV
from quarkchain.cluster.utils import create_cluster_config
from quarkchain.utils import is_p2

IP = "127.0.0.1"
PORT = 38000


def dump_config_to_file(config):
    fd, filename = tempfile.mkstemp()
    with os.fdopen(fd, 'w') as tmp:
        json.dump(config, tmp)
    return filename


async def run_master(port, configFilePath, dbPath, serverPort, jsonRpcPort, seedHost, seedPort, clean):
    cmd = "python3 master.py --node_port={} --cluster_config={} --db_path={} " \
          "--server_port={} --local_port={} --seed_host={} --seed_port={}".format(
              port, configFilePath, dbPath, serverPort, jsonRpcPort, seedHost, seedPort)
    if clean:
        cmd += " --clean=true"
    return await asyncio.create_subprocess_exec(*cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


async def run_slave(port, id, shardMaskList, dbPath, clean):
    cmd = "python3 slave.py --node_port={} --shard_mask={} --node_id={} --db_path={}".format(
        port, shardMaskList[0], id, dbPath)
    if clean:
        cmd += " --clean=true"
    return await asyncio.create_subprocess_exec(*cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


async def run_miner(shardMask, txCount, jrpcPort):
    cmd = "python3 miner.py --shard_mask={} --tx_count={} --jrpc_port={}".format(shardMask, txCount, jrpcPort)
    return await asyncio.create_subprocess_exec(*cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)


async def print_output(prefix, stream):
    while True:
        line = await stream.readline()
        if not line:
            break
        print("{}: {}".format(prefix, line.decode("ascii").strip()))


class Cluster:

    def __init__(self, config, configFilePath, num_miners, tx_count, mineRoot, clean):
        self.config = config
        self.configFilePath = configFilePath
        self.procs = []
        self.shutdownCalled = False
        self.num_miners = num_miners
        self.tx_count = tx_count
        self.mineRoot = mineRoot
        self.clean = clean

    async def waitAndShutdown(self, prefix, proc):
        ''' If one process terminates shutdown the entire cluster '''
        await proc.wait()
        if self.shutdownCalled:
            return

        print("{} is dead. Shutting down the cluster...".format(prefix))
        await self.shutdown()

    async def runMaster(self):
        master = await run_master(
            port=self.config["master"]["port"],
            configFilePath=self.configFilePath,
            dbPath=self.config["master"]["db_path"],
            serverPort=self.config["master"]["server_port"],
            jsonRpcPort=self.config["master"]["json_rpc_port"],
            seedHost=self.config["master"]["seed_host"],
            seedPort=self.config["master"]["seed_port"],
            clean=self.clean)
        asyncio.ensure_future(print_output("MASTER", master.stdout))
        self.procs.append(("MASTER", master))

    async def runSlaves(self):
        for slave in self.config["slaves"]:
            s = await run_slave(
                port=slave["port"],
                id=slave["id"],
                shardMaskList=slave["shard_masks"],
                dbPath=slave["db_path"],
                clean=self.clean)
            prefix = "SLAVE_{}".format(slave["id"])
            asyncio.ensure_future(print_output(prefix, s.stdout))
            self.procs.append((prefix, s))

    async def runMiners(self):
        jrpcPort = self.config["master"]["json_rpc_port"]
        # Create miners for shards
        for i in range(self.num_miners):
            miner = await run_miner(self.num_miners | i, self.tx_count, jrpcPort)
            prefix = "MINER_{}".format(i)
            asyncio.ensure_future(print_output(prefix, miner.stdout))
            self.procs.append((prefix, miner))

        if self.mineRoot:
            # Create a miner that covers root chain
            miner = await run_miner(0, self.tx_count, jrpcPort)
            prefix = "MINER_R"
            asyncio.ensure_future(print_output(prefix, miner.stdout))
            self.procs.append((prefix, miner))

    async def run(self):
        await self.runMaster()
        await self.runSlaves()

        # Give some time for the cluster to initiate
        await asyncio.sleep(3)

        await self.runMiners()

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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cluster_config", default="cluster_config.json", type=str)
    parser.add_argument(
        "--num_slaves", default=4, type=int)
    parser.add_argument(
        "--num_miners", default=4, type=int)
    parser.add_argument(
        "--not_mine_root", default=False, type=bool)
    parser.add_argument(
        "--num_tx_per_block", default=100, type=int)
    parser.add_argument(
        "--port_start", default=PORT, type=int)
    parser.add_argument(
        "--db_prefix", default="./db_", type=str)
    parser.add_argument(
        "--p2p_port", default=DEFAULT_ENV.config.P2P_SERVER_PORT)
    parser.add_argument(
        "--json_rpc_port", default=38391, type=int)
    parser.add_argument(
        "--seed_host", default=DEFAULT_ENV.config.P2P_SEED_HOST)
    parser.add_argument(
        "--seed_port", default=DEFAULT_ENV.config.P2P_SEED_PORT)
    parser.add_argument(
        "--clean", default=False)

    args = parser.parse_args()

    if args.num_slaves <= 0:
        config = json.load(open(args.cluster_config))
        filename = args.cluster_config
    else:
        config = create_cluster_config(
            slaveCount=args.num_slaves,
            ip=IP,
            p2pPort=args.p2p_port,
            clusterPortStart=args.port_start,
            jsonRpcPort=args.json_rpc_port,
            seedHost=args.seed_host,
            seedPort=args.seed_port,
            dbPrefix=args.db_prefix,
        )
        if not config:
            return -1
        filename = dump_config_to_file(config)

    if not is_p2(args.num_miners):
        print("--num_miners must be power of 2")
        return -1

    cluster = Cluster(config, filename, args.num_miners, args.num_tx_per_block, not args.not_mine_root, args.clean)
    cluster.startAndLoop()


if __name__ == '__main__':
    main()
