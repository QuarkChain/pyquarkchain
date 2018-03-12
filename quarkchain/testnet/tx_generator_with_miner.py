import asyncio
from quarkchain.local import OP_SER_MAP, LocalCommandOp
from quarkchain.local import SubmitNewBlockRequest, GetBlockTemplateRequest
from quarkchain.protocol import Connection
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Transaction, TransactionInput, TransactionOutput, Code
from quarkchain.core import Address, Identity, RootBlock, MinorBlock
import argparse
from quarkchain.genesis import create_genesis_blocks
from quarkchain.utils import Logger, set_logging_level


class AddressesToFund:
    ''' Maintains a dict that maps shardId to (address, value)
        where value is the amount to fund from the genesis account to the address
    '''

    def __init__(self, inputFile, shardSize):
        ''' The inputFile contains address and value in the following format
            address_in_hex,amount
        '''
        self.shardToAddresses = dict()
        try:
            with open(inputFile) as f:
                for line in f:
                    addressHex, valueStr = line.split(",")
                    address = Address.createFrom(addressHex)
                    shardId = address.getShardId(shardSize)
                    addressList = self.shardToAddresses.setdefault(shardId, [])
                    addressList.append((address, int(valueStr) * DEFAULT_ENV.config.QUARKSH_TO_JIAOZI))
        except Exception:
            # All or nothing - any error will lead to no funding for any address
            self.shardToAddresses = dict()
            Logger.logException()

    def getAddresses(self, shardId):
        return self.shardToAddresses.get(shardId, [])

    def getFundingRequired(self, shardId):
        addresses = self.getAddresses(shardId)
        return sum([address[1] for address in addresses])


class TxGeneratorClient(Connection):
    # Assume the network only contains genesis block

    def __init__(self, loop, env, reader, writer, genesisId, addressesToFund, num_tx_generated):
        super().__init__(env, reader, writer, OP_SER_MAP, dict(), dict())
        self.loop = loop
        self.genesisId = genesisId
        self.addressesToFund = addressesToFund
        self.num_tx_generated = num_tx_generated
        self.txPool = [[] for i in range(self.env.config.SHARD_SIZE)]
        self.miningBlock = None
        self.isMiningBlockRoot = None
        self.utxoGenerateFuture = dict()

    async def start(self):
        asyncio.ensure_future(self.activeAndLoopForever())
        asyncio.ensure_future(self.generateGenesisTx())

    async def generateGenesisTx(self):
        ''' The transactions generated consists of
            1. transactions to fund the given addresses
            2. artificial transactions (the total number of which is num_tx_generated)
        '''
        TX_VALUE = 1 * self.env.config.QUARKSH_TO_JIAOZI
        rBlock, mBlockList = create_genesis_blocks(self.env)

        for shardId in range(self.env.config.SHARD_SIZE):
            minorBlockCoinbaseTx = mBlockList[shardId].txList[0]
            remaining = minorBlockCoinbaseTx.outList[0].quarkash - self.addressesToFund.getFundingRequired(shardId)
            if remaining <= self.num_tx_generated * TX_VALUE:
                raise RuntimeError("Insufficient fund for addresses on shard {}".format(shardId))

            prevTx = minorBlockCoinbaseTx
            # Fund addresses
            outList = []
            for address in self.addressesToFund.getAddresses(shardId):
                outList.append(TransactionOutput(address[0], address[1]))
            outList.append(TransactionOutput(prevTx.outList[0].address, remaining))

            tx = Transaction(
                inList=[TransactionInput(prevTx.getHash(), 0)],
                code=Code.getTransferCode(),
                outList=outList,
            )
            tx.sign([self.genesisId.getKey()])
            prevTx = tx
            self.txPool[shardId].append(tx)

            # Generate artificial transactions
            for txId in range(self.num_tx_generated):
                prevLastOutputIndex = len(prevTx.outList) - 1
                prevLastOutput = prevTx.outList[-1]
                tx = Transaction(
                    inList=[TransactionInput(prevTx.getHash(), prevLastOutputIndex)],
                    code=Code.getTransferCode(),
                    outList=[
                        TransactionOutput(
                            prevLastOutput.address,
                            TX_VALUE),
                        TransactionOutput(
                            prevLastOutput.address,
                            prevLastOutput.quarkash - TX_VALUE),
                    ],
                )
                tx.sign([self.genesisId.getKey()])
                prevTx = tx
                self.txPool[shardId].append(tx)

        asyncio.ensure_future(self.startMining())

    def mineNext(self):
        asyncio.ensure_future(self.startMining())

    async def generateTx(self, shardId, prevTxList):
        Logger.info("Start creating transactions for shard {}".format(shardId))
        newTxList = []
        for oldTx in prevTxList:
            tx = Transaction(
                inList=[TransactionInput(oldTx.getHash(), 0)],
                code=Code.getTransferCode(),
                outList=[oldTx.outList[0]])
            tx.sign([self.genesisId.getKey()])
            newTxList.append(tx)
        self.txPool[shardId] = newTxList
        future = self.utxoGenerateFuture[shardId]
        del self.utxoGenerateFuture[shardId]
        Logger.info("Finished creating transactions for shard {}".format(shardId))
        future.set_result(newTxList)

    async def startMining(self):
        Logger.info("starting mining")
        while True:
            try:
                req = GetBlockTemplateRequest(
                    self.env.config.GENESIS_ACCOUNT, includeTx=True)
                op, resp, rpcId = await self.writeRpcRequest(
                    LocalCommandOp.GET_BLOCK_TEMPLATE_REQUEST,
                    req)
            except Exception as e:
                print("Caught exception when calling GetBlockTemplateRequest {}".format(e))
                Logger.logException()
                await asyncio.sleep(1)
                continue

            if len(resp.blockData) == 0:
                Logger.error("No block is available to mine, check shard mask or mine root block flag")
                await asyncio.sleep(1)
                continue

            if resp.isRootBlock:
                block = RootBlock.deserialize(resp.blockData)
                # Determine whether continue to mine previous block
                if self.miningBlock is not None and self.isMiningBlockRoot and \
                        block.header.height == self.miningBlock.header.height:
                    block = self.miningBlock
                Logger.info("Starting mining on root block, height {}, nonce {} ...".format(
                    block.header.height, block.header.nonce))

            else:
                block = MinorBlock.deserialize(resp.blockData)
                # Determine whether continue to mine previous block
                if self.miningBlock is not None and not self.isMiningBlockRoot and \
                        block.header.height == self.miningBlock.header.height and \
                        block.header.branch == self.miningBlock.header.branch and \
                        block.header.createTime == self.miningBlock.header.createTime:
                    block = self.miningBlock
                else:
                    if self.txPool[block.header.branch.getShardId()] is None:
                        await self.utxoGenerateFuture[block.header.branch.getShardId()]
                    # Fill transactions
                    block.txList.extend(
                        self.txPool[block.header.branch.getShardId()])
                    block.finalizeMerkleRoot()
                Logger.info("Starting mining on shard {}, height {}, nonce {} ...".format(
                    block.header.branch.getShardId(), block.header.height, block.header.nonce))

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

            Logger.info("Mined on nonce {} with Txs {}".format(
                i, 0 if self.isMiningBlockRoot else len(block.txList)))
            submitReq = SubmitNewBlockRequest(
                resp.isRootBlock, block.serialize())

            # Generate new Txs asynchronously before submitting the block
            # Assume the following block submit will succeed
            if not self.isMiningBlockRoot:
                shardId = block.header.branch.getShardId()
                self.txPool[shardId] = None
                self.utxoGenerateFuture[shardId] = asyncio.Future()
                # The last num_tx_generated transactions are always the artificial ones
                artificialTransactions = block.txList[-self.num_tx_generated:]
                asyncio.ensure_future(self.generateTx(shardId, artificialTransactions))

            try:
                op, submitResp, rpcId = await self.writeRpcRequest(
                    LocalCommandOp.SUBMIT_NEW_BLOCK_REQUEST,
                    submitReq)
            except Exception as e:
                Logger.error(
                    "Caught exception when calling SubmitNewBlockRequest {}".format(e))
                self.close()
                break

            if submitResp.resultCode == 0:
                Logger.info("Mined block appended")
            else:
                Logger.info("Mined failed, code {}, message {}".format(
                    submitResp.resultCode, submitResp.resultMessage))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local_port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    parser.add_argument(
        "--genesis_key", default=None, type=str)
    parser.add_argument("--log_level", default="info", type=str)
    parser.add_argument("--addresses_to_fund_file", default="addresses_to_fund.txt", type=str)
    parser.add_argument(
        "--num_tx_generated_per_block", default=DEFAULT_ENV.config.TRANSACTION_LIMIT_PER_BLOCK, type=int)
    args = parser.parse_args()

    set_logging_level(args.log_level)

    if args.genesis_key is None:
        raise RuntimeError("genesis key must be supplied")
    return args


def main():
    set_logging_level("INFO")
    args = parse_args()
    genesisId = Identity.createFromKey(bytes.fromhex(args.genesis_key))
    addressesToFund = AddressesToFund(args.addresses_to_fund_file, DEFAULT_ENV.config.SHARD_SIZE)
    loop = asyncio.get_event_loop()
    coro = asyncio.open_connection(
        "127.0.0.1", args.local_port, loop=loop)
    reader, writer = loop.run_until_complete(coro)
    client = TxGeneratorClient(
        loop, DEFAULT_ENV, reader, writer, genesisId, addressesToFund, args.num_tx_generated_per_block)
    asyncio.ensure_future(client.start())

    try:
        loop.run_until_complete(client.waitUntilClosed())
    except KeyboardInterrupt:
        pass

    client.close()
    loop.close()


if __name__ == '__main__':
    main()
