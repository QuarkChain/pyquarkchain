import argparse
import asyncio
import json
import os
import tempfile

from asyncio import subprocess

PORT = 29000


async def run_app(bootnode, listen_host, listen_port, max_peers, privkey):
    cmd = (
        "python trinity_app.py "
        "--bootnode={} "
        "--listen_host={} "
        "--listen_port={} "
        "--max_peers={} "
        "--privkey={}".format(bootnode, listen_host, listen_port, max_peers, privkey)
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
    def __init__(self, num_apps, port_start, max_peers):
        self.num_apps = num_apps
        self.port_start = port_start
        self.max_peers = max_peers
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
        bootnode = "enode://c571e0db93d17cc405cb57640826b70588a6a28785f38b21be471c609ca12fcb06cb306ac44872908f5bed99046031a5af82072d484e3ef9029560c1707193a0@127.0.0.1:{}".format(
            self.port_start
        )
        s = await run_app(
            bootnode=bootnode,
            listen_host="127.0.0.1",
            listen_port=self.port_start,
            max_peers=self.max_peers,
            privkey="31552f186bf90908ce386fb547dd0410bf443309125cc43fd0ffd642959bf6d9",
        )
        prefix = "APP_{}".format(0)
        asyncio.ensure_future(print_output(prefix, s.stdout))
        self.procs.append((prefix, s))
        await asyncio.sleep(3)
        for id in range(1, self.num_apps):
            s = await run_app(
                bootnode=bootnode,
                listen_host="127.0.0.1",
                listen_port=self.port_start + id,
                max_peers=self.max_peers,
                privkey="",
            )
            prefix = "APP_{}".format(id)
            asyncio.ensure_future(print_output(prefix, s.stdout))
            self.procs.append((prefix, s))
            await asyncio.sleep(.5)

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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_apps", default=10, type=int)
    parser.add_argument("--port_start", default=PORT, type=int)
    parser.add_argument("--max_peers", default=25, type=int)

    args = parser.parse_args()

    network = Network(
        num_apps=args.num_apps, port_start=args.port_start, max_peers=args.max_peers
    )
    network.start_and_loop()


if __name__ == "__main__":
    main()
