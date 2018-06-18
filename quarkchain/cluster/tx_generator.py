import asyncio
import random
import time

from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Branch, Code, Transaction
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.loadtest.accounts import LOADTEST_ACCOUNTS
from quarkchain.utils import Logger


def random_full_shard_id(shardSize, shardId):
    fullShardId = random.randint(0, (2 ** 32) - 1)
    shardMask = shardSize - 1
    return fullShardId & (~shardMask) | shardId


class Account:
    def __init__(self, address, key):
        self.address = address
        self.key = key


class TransactionGenerator:

    def __init__(self, branch, slaveServer):
        self.branch = branch
        self.slaveServer = slaveServer
        self.running = False

        self.accounts = []
        for item in LOADTEST_ACCOUNTS:
            account = Account(
                Address(bytes.fromhex(item["address"])[:20], self.branch.getShardId()),
                bytes.fromhex(item["key"]))
            self.accounts.append(account)

    def generate(self, xShardPecent):
        """Generate a bunch of transactions in the network
        The total number of transactions generated each time
        """
        if self.running:
            return False
        shardState = self.slaveServer.shardStateMap[self.branch.value]
        if shardState.headerTip.height < len(LOADTEST_ACCOUNTS) / 500 + 2:
            return False

        self.running = True
        asyncio.ensure_future(self.__gen(xShardPecent))
        return True

    async def __gen(self, xShardPecent):
        Logger.info("[{}] start generating {} transactions".format(
            self.branch.getShardId(), len(self.accounts),
        ))
        startTime = time.time()
        txList = []
        for account in self.accounts:
            nonce = self.slaveServer.getTransactionCount(account.address)
            tx = self.createTransaction(account, nonce, xShardPecent)
            txList.append(tx)
            if len(txList) >= 100:
                self.slaveServer.addTxList(txList)
                txList = []
                await asyncio.sleep(0)  # yield CPU so that other stuff won't be held for too long

        endTime = time.time()
        Logger.info("[{}] generated {} transactions in {:.2f} seconds".format(
            self.branch.getShardId(), len(self.accounts), endTime - startTime
        ))
        self.running = False

    def createTransaction(self, account, nonce, xShardPecent):
        config = DEFAULT_ENV.config
        toAddress = random.choice(self.accounts).address
        fromShard = self.branch.getShardId()
        toShard = fromShard
        shardSize = self.branch.getShardSize()
        if random.randint(1, 100) <= xShardPecent:
            # x-shard tx
            toShard = random.randint(0, config.SHARD_SIZE - 1)
            if toShard == self.branch.getShardId():
                toShard = (toShard + 1) % config.SHARD_SIZE
        evmTx = EvmTransaction(
            nonce=nonce,
            gasprice=random.randint(1, 10) * (10 ** 8),
            startgas=30000,
            to=toAddress.recipient,
            value=random.randint(1, 100) * (10 ** 15),
            data=b'',
            fromFullShardId=random_full_shard_id(shardSize, fromShard),
            toFullShardId=random_full_shard_id(shardSize, toShard),
            networkId=config.NETWORK_ID)
        evmTx.sign(account.key)
        return Transaction(code=Code.createEvmCode(evmTx))
