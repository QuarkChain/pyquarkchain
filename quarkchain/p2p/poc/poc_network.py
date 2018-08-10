import argparse
import asyncio
import json
import os
import tempfile

from asyncio import subprocess

PORT = 55000


async def run_app(bootstrap_port, node_port, node_num, min_peers, max_peers):
    cmd = (
        "python poc_app.py --bootstrap_port={} --node_port={} "
        "--node_num={} --min_peers={} --max_peers={}".format(
            bootstrap_port, node_port, node_num, min_peers, max_peers
        )
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


class Network:
    def __init__(self, config):
        self.config = config
        self.procs = []
        self.shutdown_called = False

    async def wait_and_shutdown(self, prefix, proc):
        await proc.wait()
        if self.shutdown_called:
            return

    async def run_apps(self):
        """
        run bootstrap node (first process) first, sleep for 3 seconds
        """
        app = self.config["apps"][0]
        s = await run_app(
            bootstrap_port=app["bootstrap_port"],
            node_port=app["node_port"],
            node_num=app["node_num"],
            min_peers=app["min_peers"],
            max_peers=app["max_peers"],
        )
        prefix = "APP_{}".format(app["id"])
        asyncio.ensure_future(print_output(prefix, s.stdout))
        self.procs.append((prefix, s))
        await asyncio.sleep(3)
        for app in self.config["apps"][1:]:
            s = await run_app(
                bootstrap_port=app["bootstrap_port"],
                node_port=app["node_port"],
                node_num=app["node_num"],
                min_peers=app["min_peers"],
                max_peers=app["max_peers"],
            )
            prefix = "APP_{}".format(app["id"])
            asyncio.ensure_future(print_output(prefix, s.stdout))
            self.procs.append((prefix, s))

    async def run(self):
        await self.run_apps()
        await asyncio.gather(
            *[self.wait_and_shutdown(prefix, proc) for prefix, proc in self.procs]
        )

    async def shutdown(self):
        self.shutdown_called = True
        for prefix, proc in self.procs:
            try:
                proc.terminate()
            except Exception:
                pass
        await asyncio.gather(*[proc.wait() for prefix, proc in self.procs])

    def start_and_loop(self):
        try:
            asyncio.get_event_loop().run_until_complete(self.run())
        except KeyboardInterrupt:
            print("got KeyboardInterrupt, shutdown everything")
            asyncio.get_event_loop().run_until_complete(self.shutdown())


def create_app_config(app_count, network_port_start, min_peers, max_peers):
    if app_count <= 0:
        print("App count must greater than 0")
        return None

    config = dict()
    config["apps"] = []
    for i in range(app_count):
        config["apps"].append(
            {
                "id": "{:03}".format(i),
                "bootstrap_port": network_port_start,  # use first host as bootstrap
                "node_port": network_port_start + i,
                "node_num": i,
                "min_peers": min_peers,
                "max_peers": max_peers,
            }
        )

    return config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_apps", default=10, type=int)
    parser.add_argument("--port_start", default=PORT, type=int)
    parser.add_argument("--min_peers", default=2, type=int)
    parser.add_argument("--max_peers", default=10, type=int)

    args = parser.parse_args()

    config = create_app_config(
        app_count=args.num_apps,
        network_port_start=args.port_start,
        min_peers=args.min_peers,
        max_peers=args.max_peers,
    )

    network = Network(config)
    network.start_and_loop()


if __name__ == "__main__":
    main()
