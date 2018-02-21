import asyncio
# from quarkchain.local import LocalClient
from quarkchain.protocol import Connection
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, RootBlock, MinorBlock
from quarkchain.local import OP_SER_MAP, LocalCommandOp
from quarkchain.local import GetBlockTemplateRequest
from quarkchain.local import SubmitNewBlockRequest
import argparse


class LocalClient(Connection):

    def __init__(self, loop, env, reader, writer):
        super().__init__(env, reader, writer, OP_SER_MAP, dict(), dict())
        self.loop = loop
        self.miningBlock = None
        self.isMiningBlockRoot = None

    def mineNext(self):
        asyncio.ensure_future(self.startMining())

    async def startMining(self):
        print("starting mining")
        try:
            req = GetBlockTemplateRequest(self.env.config.GENESIS_ACCOUNT)
            op, resp, rpcId = await self.writeRpcRequest(
                LocalCommandOp.GET_BLOCK_TEMPLATE_REQUEST,
                req)
        except Exception as e:
            print("Caught exception when calling GetBlockTemplateRequest {}".format(e))
            self.loop.call_later(1, self.mineNext)
            return

        if len(resp.blockData) == 0:
            print("No block is available to mine, check shard mask or mine root block flag")
            self.loop.call_later(1, self.mineNext)
            return

        if resp.isRootBlock:
            block = RootBlock.deserialize(resp.blockData)
            print("Starting mining on root block, height {} ...".format(
                block.header.height))
            # Determine whether continue to mine previous block
            if self.miningBlock is not None and self.isMiningBlockRoot and \
                    block.header.height == self.miningBlock.header.height:
                block = self.miningBlock
        else:
            block = MinorBlock.deserialize(resp.blockData)
            print("Starting mining on shard {}, height {} ...".format(
                block.header.branch.getShardId(), block.header.height))
            # Determine whether continue to mine previous block
            if self.miningBlock is not None and not self.isMiningBlockRoot and \
                    block.header.height == self.miningBlock.header.height and \
                    block.header.branch == self.miningBlock.header.branch and \
                    block.header.createTime == self.miningBlock.header.createTime:
                block = self.miningBlock

        self.miningBlock = block
        self.isMiningBlockRoot = resp.isRootBlock

        for i in range(100000):
            block.header.nonce += 1
            metric = int.from_bytes(block.header.getHash(), byteorder="big") * block.header.difficulty
            if metric < 2 ** 256:
                print("mined on nonce {} with Txs {}".format(i, 0 if self.isMiningBlockRoot else len(block.txList)))
                submitReq = SubmitNewBlockRequest(resp.isRootBlock, block.serialize())

                try:
                    op, submitResp, rpcId = await self.writeRpcRequest(
                        LocalCommandOp.SUBMIT_NEW_BLOCK_REQUEST,
                        submitReq)
                except Exception as e:
                    print("Caught exception when calling SubmitNewBlockRequest {}".format(e))
                    self.loop.call_later(1, self.mineNext)
                    return

                if submitResp.resultCode == 0:
                    print("Mined block")
                else:
                    print("Mined failed, code {}, message {}".format(submitResp.resultCode, submitResp.resultMessage))
                break
        asyncio.ensure_future(self.startMining())

    async def start(self):
        asyncio.ensure_future(self.activeAndLoopForever())
        asyncio.ensure_future(self.startMining())

    def closeWithError(self, error):
        print("Closing with error {}".format(error))
        return super().closeWithError(error)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local_port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    parser.add_argument(
        "--miner_address", default=DEFAULT_ENV.config.GENESIS_ACCOUNT.serialize().hex(), type=str)
    args = parser.parse_args()

    env = DEFAULT_ENV.copy()
    env.config.LOCAL_SERVER_PORT = args.local_port
    env.config.GENESIS_ACCOUNT = Address.createFrom(args.miner_address)
    return env


def main():
    env = parse_args()
    loop = asyncio.get_event_loop()
    coro = asyncio.open_connection(
        "127.0.0.1", env.config.LOCAL_SERVER_PORT, loop=loop)
    reader, writer = loop.run_until_complete(coro)
    client = LocalClient(loop, env, reader, writer)
    asyncio.ensure_future(client.start())

    try:
        loop.run_until_complete(client.waitUntilClosed())
    except KeyboardInterrupt:
        pass

    client.close()
    loop.close()


if __name__ == '__main__':
    main()
