import asyncio
import random
import time

from typing import Optional

from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Code, Transaction
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
                Address.createFrom(item["address"]),
                bytes.fromhex(item["key"]))
            self.accounts.append(account)

    def generate(self, numTx, xShardPercent, tx: Transaction):
        """Generate a bunch of transactions in the network
        The total number of transactions generated each time
        """
        if self.running:
            return False
        shardState = self.slaveServer.shardStateMap[self.branch.value]
        if shardState.headerTip.height < len(LOADTEST_ACCOUNTS) / 500 + 2:
            # to allow all the load test accounts to get funded
            Logger.warning("Cannot generate transactions since not all the accounts have been funded")
            return False

        self.running = True
        asyncio.ensure_future(self.__gen(numTx, xShardPercent, tx))
        return True

    async def __gen(self, numTx, xShardPercent, sampleTx: Transaction):
        Logger.info("[{}] start generating {} transactions with {}% cross-shard".format(
            self.branch.getShardId(), numTx, xShardPercent
        ))
        if numTx <= 0:
            return
        startTime = time.time()
        txList = []
        total = 0
        sampleEvmTx = sampleTx.code.getEvmTransaction()
        for account in self.accounts:
            inShardAddress = Address(account.address.recipient, self.branch.getShardId())
            nonce = self.slaveServer.getTransactionCount(inShardAddress)
            tx = self.createTransaction(account, nonce, xShardPercent, sampleEvmTx)
            if not tx:
                continue
            txList.append(tx)
            total += 1
            if len(txList) >= 600 or total >= numTx:
                self.slaveServer.addTxList(txList)
                txList = []
                await asyncio.sleep(random.uniform(8, 12))  # yield CPU so that other stuff won't be held for too long

            if total >= numTx:
                break

        endTime = time.time()
        Logger.info("[{}] generated {} transactions in {:.2f} seconds".format(
            self.branch.getShardId(), total, endTime - startTime
        ))
        self.running = False

    def createTransaction(self, account, nonce, xShardPercent, sampleEvmTx) -> Optional[Transaction]:
        config = DEFAULT_ENV.config
        shardSize = self.branch.getShardSize()
        shardMask = shardSize - 1
        fromShard = self.branch.getShardId()

        # skip if from shard is specified and not matching current branch
        # FIXME: it's possible that clients want to specify '0x0' as the full shard ID, however it will not be supported
        if sampleEvmTx.fromFullShardId and (sampleEvmTx.fromFullShardId & shardMask) != fromShard:
            return None

        if sampleEvmTx.fromFullShardId:
            fromFullShardId = sampleEvmTx.fromFullShardId
        else:
            fromFullShardId = account.address.fullShardId & (~shardMask) | fromShard

        if not sampleEvmTx.to:
            toAddress = random.choice(self.accounts).address
            recipient = toAddress.recipient
            toFullShardId = toAddress.fullShardId & (~shardMask) | fromShard
        else:
            recipient = sampleEvmTx.to
            toFullShardId = fromFullShardId

        if random.randint(1, 100) <= xShardPercent:
            # x-shard tx
            toShard = random.randint(0, config.SHARD_SIZE - 1)
            if toShard == self.branch.getShardId():
                toShard = (toShard + 1) % config.SHARD_SIZE
            toFullShardId = toFullShardId & (~shardMask) | toShard

        value = sampleEvmTx.value
        if not sampleEvmTx.data:
            value = random.randint(1, 100) * (10 ** 15)

        evmTx = EvmTransaction(
            nonce=nonce,
            gasprice=sampleEvmTx.gasprice,
            startgas=sampleEvmTx.startgas,
            to=recipient,
            value=value,
            data=sampleEvmTx.data,
            fromFullShardId=fromFullShardId,
            toFullShardId=toFullShardId,
            networkId=config.NETWORK_ID)
        evmTx.sign(account.key)
        return Transaction(code=Code.createEvmCode(evmTx))
