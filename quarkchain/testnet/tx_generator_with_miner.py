import asyncio
from quarkchain.local import OP_SER_MAP, LocalCommandOp
from quarkchain.local import SubmitNewBlockRequest, GetBlockTemplateRequest
from quarkchain.protocol import Connection
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Transaction, TransactionInput, TransactionOutput, Code
from quarkchain.core import Identity, RootBlock, MinorBlock
import argparse
from quarkchain.genesis import create_genesis_blocks
from quarkchain.utils import Logger


class TxGeneratorClient(Connection):
    # Assume the network only contains genesis block

    def __init__(self, loop, env, reader, writer, genesisId):
        super().__init__(env, reader, writer, OP_SER_MAP, dict(), dict())
        self.loop = loop
        self.genesisId = genesisId
        self.utxoPool = [[] for i in range(self.env.config.SHARD_SIZE)]
        self.miningBlock = None
        self.isMiningBlockRoot = None
        self.utxoGenerateFuture = dict()

    async def start(self):
        asyncio.ensure_future(self.activeAndLoopForever())
        asyncio.ensure_future(self.generateGenesisTx())

    async def generateGenesisTx(self):
        INIT_TX_PER_SHARD = self.env.config.TRANSACTION_LIMIT_PER_BLOCK
        prevTxList = []
        rBlock, mBlockList = create_genesis_blocks(self.env)
        for shardId in range(self.env.config.SHARD_SIZE):
            prevTxList.append(mBlockList[shardId].txList[0])

        for txId in range(INIT_TX_PER_SHARD):
            for shardId in range(self.env.config.SHARD_SIZE):
                prevTx = prevTxList[shardId]
                prevOutIdx = 0 if txId == 0 else 1
                tx = Transaction(
                    inList=[TransactionInput(prevTx.getHash(), prevOutIdx)],
                    code=Code.getTransferCode(),
                    outList=[
                        TransactionOutput(
                            prevTx.outList[0].address,
                            10000),
                        TransactionOutput(
                            prevTx.outList[0].address,
                            prevTx.outList[prevOutIdx].quarkash - 10000)])

                tx.sign([self.genesisId.getKey()])
                prevTxList[shardId] = tx
                self.utxoPool[shardId].append(tx)

        asyncio.ensure_future(self.startMining())

    def mineNext(self):
        asyncio.ensure_future(self.startMining())

    async def generateTx(self, shardId, prevTxList):
        newTxList = []
        for oldTx in prevTxList[1:]:
            tx = Transaction(
                inList=[TransactionInput(oldTx.getHash(), 0)],
                code=Code.getTransferCode(),
                outList=[oldTx.outList[0]])
            tx.sign([self.genesisId.getKey()])
            newTxList.append(tx)
        self.utxoPool[shardId] = newTxList
        future = self.utxoGenerateFuture[shardId]
        del self.utxoGenerateFuture[shardId]
        future.set_result(newTxList)

    async def startMining(self):
        print("starting mining")
        while True:
            try:
                req = GetBlockTemplateRequest(
                    self.env.config.GENESIS_ACCOUNT, includeTx=False)
                op, resp, rpcId = await self.writeRpcRequest(
                    LocalCommandOp.GET_BLOCK_TEMPLATE_REQUEST,
                    req)
            except Exception as e:
                print("Caught exception when calling GetBlockTemplateRequest {}".format(e))
                Logger.logException()
                await asyncio.sleep(1)
                continue

            if len(resp.blockData) == 0:
                print(
                    "No block is available to mine, check shard mask or mine root block flag")
                await asyncio.sleep(1)
                continue

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
                else:
                    if self.utxoPool[block.header.branch.getShardId()] is None:
                        await self.utxoGenerateFuture[block.header.branch.getShardId()]
                    # Fill transactions
                    block.txList.extend(
                        self.utxoPool[block.header.branch.getShardId()])
                    block.finalizeMerkleRoot()

            self.miningBlock = block
            self.isMiningBlockRoot = resp.isRootBlock

            metric = 2 ** 256
            for i in range(100000):
                block.header.nonce += 1
                metric = int.from_bytes(
                    block.header.getHash(), byteorder="big") * block.header.difficulty
                if metric < 2 ** 256:
                    break

            if metric >= 2 ** 256:
                # Try next block
                continue

            print("mined on nonce {} with Txs {}".format(
                i, 0 if self.isMiningBlockRoot else len(block.txList)))
            submitReq = SubmitNewBlockRequest(
                resp.isRootBlock, block.serialize())

            # Generate new Txs asynchronously before submitting the block
            # Assume the following block submit will succeed
            if not self.isMiningBlockRoot:
                shardId = block.header.branch.getShardId()
                self.utxoPool[shardId] = None
                self.utxoGenerateFuture[shardId] = asyncio.Future()
                asyncio.ensure_future(self.generateTx(shardId, block.txList[1:]))

            try:
                op, submitResp, rpcId = await self.writeRpcRequest(
                    LocalCommandOp.SUBMIT_NEW_BLOCK_REQUEST,
                    submitReq)
            except Exception as e:
                print(
                    "Caught exception when calling SubmitNewBlockRequest {}".format(e))
                self.close()
                break

            if submitResp.resultCode == 0:
                print("Mined block")
            else:
                print("Mined failed, code {}, message {}".format(
                    submitResp.resultCode, submitResp.resultMessage))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local_port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    parser.add_argument(
        "--genesis_key", default=None, type=str)
    args = parser.parse_args()

    if args.genesis_key is None:
        raise RuntimeError("genesis key must be supplied")
    return args


def main():
    args = parse_args()
    genesisId = Identity.createFromKey(bytes.fromhex(args.genesis_key))
    loop = asyncio.get_event_loop()
    coro = asyncio.open_connection(
        "127.0.0.1", args.local_port, loop=loop)
    reader, writer = loop.run_until_complete(coro)
    client = TxGeneratorClient(loop, DEFAULT_ENV, reader, writer, genesisId)
    asyncio.ensure_future(client.start())

    try:
        loop.run_until_complete(client.waitUntilClosed())
    except KeyboardInterrupt:
        pass

    client.close()
    loop.close()


if __name__ == '__main__':
    main()
