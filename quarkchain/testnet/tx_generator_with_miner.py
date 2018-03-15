import argparse
import asyncio
import json

from quarkchain.local import OP_SER_MAP, GetUtxoRequest, LocalCommandOp, UtxoItem
from quarkchain.local import SubmitNewBlockRequest, GetBlockTemplateRequest
from quarkchain.protocol import Connection
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Transaction, TransactionInput, TransactionOutput, Code
from quarkchain.core import Address, Branch, Identity, RootBlock, MinorBlock
from quarkchain.genesis import create_genesis_blocks
from quarkchain.testnet.bank_accounts import BANK_ACCOUNTS
from quarkchain.utils import Logger, set_logging_level


class AddressesToFund:
    ''' Maintains a dict that maps shardId to (address, value)
        where value is the amount to fund from the genesis account to the address
    '''

    def __init__(self, inputFile, shardSize):
        ''' The inputFile contains address and value in the following format
            address_in_hex,amount
        '''
        self.shardSize = shardSize
        self.shardToAddresses = dict()
        try:
            accountList = json.load(open(inputFile))
            for account in accountList:
                address = Address.createFrom(account["address"])
                shardId = address.getShardId(shardSize)
                addressList = self.shardToAddresses.setdefault(shardId, [])
                addressList.append((address, account["value"] * DEFAULT_ENV.config.QUARKSH_TO_JIAOZI))
        except Exception:
            # All or nothing - any error will lead to no funding for any address
            self.shardToAddresses = dict()
            Logger.logException()

    def addAddressToFund(self, address: Address, value):
        addressList = self.shardToAddresses.setdefault(address.getShardId(self.shardSize), [])
        addressList.append((address, value * DEFAULT_ENV.config.QUARKSH_TO_JIAOZI))

    def getAddresses(self, shardId):
        return self.shardToAddresses.get(shardId, [])

    def getFundingRequired(self, shardId):
        addresses = self.getAddresses(shardId)
        return sum([address[1] for address in addresses])


def add_bank_accounts_to_fund(addressesToFund: AddressesToFund, shardSize):
    for account in BANK_ACCOUNTS:
        address = Address.createFrom(account["address"])
        for shardId in range(shardSize):
            branch = Branch.create(shardSize, shardId)
            addressInShard = address.addressInBranch(branch)
            addressesToFund.addAddressToFund(addressInShard, 10000000)


class TxGeneratorClient(Connection):
    # Assume the network only contains genesis block
    TX_VALUE = 1 * DEFAULT_ENV.config.QUARKSH_TO_JIAOZI

    def __init__(self, loop, env, reader, writer, genesisId, addressesToFund, numTxGenerated):
        super().__init__(env, reader, writer, OP_SER_MAP, dict(), dict())
        self.loop = loop
        self.genesisId = genesisId
        self.addressesToFund = addressesToFund
        self.numTxGenerated = numTxGenerated
        self.txPool = [[] for i in range(self.env.config.SHARD_SIZE)]
        self.miningBlock = None
        self.isMiningBlockRoot = None
        self.utxoGenerateFuture = dict()

        _, mBlockList = create_genesis_blocks(self.env)
        self.minorBlockCoinbaseTxList = [
            mBlockList[shardId].txList[0] for shardId in range(self.env.config.SHARD_SIZE)]

    def start(self):
        asyncio.ensure_future(self.activeAndLoopForever())
        asyncio.ensure_future(self.generateInitialTxs())
        asyncio.ensure_future(self.startMining())

    def __createTxToFundAddresses(self, shardId, fundingTx):
        ''' Returns a TX with one output for each address to be funded and
            a last output to keep the remaining balance
        '''
        required = self.addressesToFund.getFundingRequired(shardId)
        remaining = fundingTx.outList[0].quarkash - required
        if remaining <= self.numTxGenerated * self.TX_VALUE:
            raise RuntimeError("Insufficient fund for addresses on shard {}, {} > {}".format(
                shardId, required / 10**18, fundingTx.outList[0].quarkash / 10**18))

        outList = []
        for address in self.addressesToFund.getAddresses(shardId):
            outList.append(TransactionOutput(address[0], address[1]))

        outList.append(TransactionOutput(fundingTx.outList[0].address, remaining))

        tx = Transaction(
            inList=[TransactionInput(fundingTx.getHash(), 0)],
            code=Code.getTransferCode(),
            outList=outList,
        )
        tx.sign([self.genesisId.getKey()])
        return tx

    def __createArtificialTxs(self, utxoItem, numTxToGenerate):
        '''Returns a list of artificial transactions by splitting the utxoItem
           into UTXO value of TX_VALUE
        '''
        txList = []
        while len(txList) < numTxToGenerate:
            if utxoItem.txOutput.quarkash <= self.TX_VALUE:
                break
            tx = Transaction(
                inList=[utxoItem.txInput],
                code=Code.getTransferCode(),
                outList=[
                    TransactionOutput(
                        utxoItem.txOutput.address,
                        self.TX_VALUE),
                    TransactionOutput(
                        utxoItem.txOutput.address,
                        utxoItem.txOutput.quarkash - self.TX_VALUE),
                ],
            )
            tx.sign([self.genesisId.getKey()])
            utxoItem = UtxoItem(
                TransactionInput(tx.getHash(), 1),
                TransactionOutput(tx.outList[1].address, tx.outList[1].quarkash),
            )
            txList.append(tx)
        return txList

    async def __getUtxo(self, shardId, address, limit):
        try:
            op, resp, rpcId = await self.writeRpcRequest(
                LocalCommandOp.GET_UTXO_REQUEST,
                GetUtxoRequest(shardId=shardId, address=address, limit=limit))
        except Exception as e:
            Logger.errorException()
            self.close()
        return resp.utxoItemList

    async def generateInitialTxsForShard(self, shardId, fundAddresses=False):
        utxoItemList = []
        self.txPool[shardId] = []

        if fundAddresses and self.addressesToFund.getAddresses(shardId):
            address = self.addressesToFund.getAddresses(shardId)[0][0]
            utxos = await self.__getUtxo(shardId, address, 1)
            # If the address to fund already has UTXO it means the address has been funded
            # This also means other addresses on the same shard are also funded because
            # they are all funded through the same transasction
            if not utxos:
                minorBlockCoinbaseTx = self.minorBlockCoinbaseTxList[shardId]
                tx = self.__createTxToFundAddresses(shardId, minorBlockCoinbaseTx)
                self.txPool[shardId].append(tx)
                Logger.info("[{}] Created transaction to fund addresses".format(shardId))

                utxoItemList.append(UtxoItem(
                    TransactionInput(tx.getHash(), len(tx.outList) - 1),
                    TransactionOutput(tx.outList[-1].address, tx.outList[-1].quarkash)))
                numTxToGenerate = self.numTxGenerated

        if not utxoItemList:
            branch = Branch.create(self.env.config.SHARD_SIZE, shardId)
            address = self.env.config.GENESIS_ACCOUNT.addressInBranch(branch)
            utxoItemList = await self.__getUtxo(shardId, address, self.numTxGenerated)
            Logger.info("[{}] Got {} UTXOs".format(shardId, len(utxoItemList)))
            numTxToGenerate = self.numTxGenerated - len(utxoItemList)

        for utxoItem in utxoItemList:
            if numTxToGenerate > 0 and utxoItem.txOutput.quarkash > self.TX_VALUE:
                txList = self.__createArtificialTxs(utxoItem, numTxToGenerate)
                self.txPool[shardId].extend(txList)
                numTxToGenerate -= len(txList)
            else:
                tx = Transaction(
                    inList=[utxoItem.txInput],
                    code=Code.getTransferCode(),
                    outList=[utxoItem.txOutput])
                tx.sign([self.genesisId.getKey()])
                self.txPool[shardId].append(tx)

    async def generateInitialTxs(self):
        ''' The transactions generated consists of
            1. transactions to fund the given addresses
            2. artificial transactions (the total number of which is numTxGenerated)
        '''
        await asyncio.gather(
            *[self.generateInitialTxsForShard(shardId, True) for shardId in range(self.env.config.SHARD_SIZE)])

    async def generateTxsForShard(self, shardId, prevTxList):
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
                Logger.info("[R] Starting mining height {}, nonce {} ...".format(
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
                Logger.info("[{}] Starting mining height {}, nonce {} ...".format(
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

            Logger.info("Mined on nonce {} with {} Txs".format(
                i, 0 if self.isMiningBlockRoot else len(block.txList)))
            submitReq = SubmitNewBlockRequest(
                resp.isRootBlock, block.serialize())

            # Generate new Txs asynchronously before submitting the block
            # Assume the following block submit will succeed
            if not self.isMiningBlockRoot:
                shardId = block.header.branch.getShardId()
                self.txPool[shardId] = None
                self.utxoGenerateFuture[shardId] = asyncio.Future()
                # The last numTxGenerated transactions are always the artificial ones
                artificialTransactions = block.txList[-self.numTxGenerated:]
                asyncio.ensure_future(self.generateTxsForShard(shardId, artificialTransactions))

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
                if not self.isMiningBlockRoot:
                    await self.generateInitialTxsForShard(shardId)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--local_port", default=DEFAULT_ENV.config.LOCAL_SERVER_PORT, type=int)
    parser.add_argument(
        "--genesis_key", default=None, type=str)
    parser.add_argument("--log_level", default="info", type=str)
    parser.add_argument("--addresses_to_fund_file", default="addresses_to_fund.json", type=str)
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
    add_bank_accounts_to_fund(addressesToFund, DEFAULT_ENV.config.SHARD_SIZE)

    loop = asyncio.get_event_loop()
    coro = asyncio.open_connection(
        "127.0.0.1", args.local_port, loop=loop)
    reader, writer = loop.run_until_complete(coro)
    client = TxGeneratorClient(
        loop, DEFAULT_ENV, reader, writer, genesisId, addressesToFund, args.num_tx_generated_per_block)
    client.start()

    try:
        loop.run_until_complete(client.waitUntilClosed())
    except KeyboardInterrupt:
        pass

    client.close()
    loop.close()


if __name__ == '__main__':
    main()
