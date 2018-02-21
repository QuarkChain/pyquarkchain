import asyncio
from quarkchain.local import OP_SER_MAP, AddNewTransactionListRequest, LocalCommandOp, NewTransaction
from quarkchain.protocol import Connection
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Transaction, TransactionInput, TransactionOutput, Code
from quarkchain.core import Identity
import argparse
from quarkchain.genesis import create_genesis_blocks


class TxGeneratorClient(Connection):
    # Assume the network only contains genesis block

    def __init__(self, loop, env, reader, writer, genesisId):
        super().__init__(env, reader, writer, OP_SER_MAP, dict(), dict())
        self.loop = loop
        self.genesisId = genesisId

    async def start(self):
        asyncio.ensure_future(self.activeAndLoopForever())
        asyncio.ensure_future(self.generateGenesisTx())

    async def generateGenesisTx(self):
        INIT_TX_PER_SHARD = 10000
        txList = []
        prevTxList = []
        rBlock, mBlockList = create_genesis_blocks(self.env)
        for shardId in range(self.env.config.SHARD_SIZE):
            prevTxList.append(mBlockList[shardId].txList[0])

        for txId in range(INIT_TX_PER_SHARD):
            for shardId in range(self.env.config.SHARD_SIZE):
                prevTx = prevTxList[shardId]
                tx = Transaction(
                    inList=[TransactionInput(prevTx.getHash(), 0)],
                    code=Code.getTransferCode(),
                    outList=[
                        TransactionOutput(prevTx.outList[0].address, prevTx.outList[
                                          0].quarkash - 10000),
                        TransactionOutput(prevTx.outList[0].address, 10000)])

                tx.sign([self.genesisId.getKey()])
                prevTxList[shardId] = tx
                txList.append(NewTransaction(shardId, tx))

        try:
            op, resp, rpcId = await self.writeRpcRequest(
                LocalCommandOp.ADD_NEW_TRANSACTION_LIST_REQUEST,
                AddNewTransactionListRequest(txList))
        except Exception as e:
            print("Failed to call AddNewTransactionListRequest {}".format(e))
            self.close()
            return

        print("Submitted {} genesis tx".format(resp.numTxAdded))


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

    loop.close()


if __name__ == '__main__':
    main()
