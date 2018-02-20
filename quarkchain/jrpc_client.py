
import asyncio
# from quarkchain.local import LocalClient
from quarkchain.protocol import Connection, ConnectionState
from quarkchain.local import OP_SER_MAP, LocalCommandOp, JsonRpcRequest
from quarkchain.config import DEFAULT_ENV
import argparse
import json


class LocalClient(Connection):

    def __init__(self, loop, env, reader, writer):
        super().__init__(env, reader, writer, OP_SER_MAP, dict(), dict())
        self.loop = loop
        self.miningBlock = None
        self.isMiningBlockRoot = None

    async def start(self):
        self.state = ConnectionState.ACTIVE
        asyncio.ensure_future(self.activeAndLoopForever())

    async def callJrpc(self, method, params=None):
        jrpcRequest = {
            "jsonrpc": "2.0",
            "method": method,
        }
        if params is not None:
            jrpcRequest["params"] = params

        op, resp, rpcId = await self.writeRpcRequest(
            LocalCommandOp.JSON_RPC_REQUEST,
            JsonRpcRequest(json.dumps(jrpcRequest).encode()))
        return json.loads(resp.jrpcResponse.decode())

    def closeWithError(self, error):
        print("Closing with error {}".format(error))
        return super().closeWithError(error)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local_port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    parser.add_argument(
        "--method", default=None, type=str)
    args = parser.parse_args()

    if args.method is None:
        raise RuntimeError("method must be specified")

    return args


def main():
    args = parse_args()
    loop = asyncio.get_event_loop()
    coro = asyncio.open_connection("127.0.0.1", args.local_port, loop=loop)
    reader, writer = loop.run_until_complete(coro)
    client = LocalClient(loop, DEFAULT_ENV, reader, writer)
    asyncio.ensure_future(client.start())
    jrpcResp = loop.run_until_complete(client.callJrpc(args.method))
    print(json.dumps(jrpcResp, sort_keys=True, indent=4))

    client.close()
    loop.run_until_complete(client.waitUntilClosed())
    loop.close()


if __name__ == '__main__':
    main()
