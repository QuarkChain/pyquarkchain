import asyncio
import random
import time
from typing import Optional

from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Code, Transaction
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.loadtest.accounts import LOADTEST_ACCOUNTS
from quarkchain.utils import Logger


def random_full_shard_id(shard_size, shard_id):
    full_shard_id = random.randint(0, (2 ** 32) - 1)
    shard_mask = shard_size - 1
    return full_shard_id & (~shard_mask) | shard_id


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
                Address.create_from(item["address"]),
                bytes.fromhex(item["key"]))
            self.accounts.append(account)

    def generate(self, numTx, x_shard_percent, tx: Transaction):
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
        asyncio.ensure_future(self.__gen(numTx, x_shard_percent, tx))
        return True

    async def __gen(self, numTx, x_shard_percent, sampleTx: Transaction):
        Logger.info("[{}] start generating {} transactions with {}% cross-shard".format(
            self.branch.get_shard_id(), numTx, x_shard_percent
        ))
        if numTx <= 0:
            return
        startTime = time.time()
        tx_list = []
        total = 0
        sampleEvmTx = sampleTx.code.get_evm_transaction()
        for account in self.accounts:
            inShardAddress = Address(account.address.recipient, self.branch.get_shard_id())
            nonce = self.slaveServer.get_transaction_count(inShardAddress)
            tx = self.create_transaction(account, nonce, x_shard_percent, sampleEvmTx)
            if not tx:
                continue
            tx_list.append(tx)
            total += 1
            if len(tx_list) >= 600 or total >= numTx:
                self.slaveServer.add_tx_list(tx_list)
                tx_list = []
                await asyncio.sleep(random.uniform(8, 12))  # yield CPU so that other stuff won't be held for too long

            if total >= numTx:
                break

        endTime = time.time()
        Logger.info("[{}] generated {} transactions in {:.2f} seconds".format(
            self.branch.get_shard_id(), total, endTime - startTime
        ))
        self.running = False

    def create_transaction(self, account, nonce, x_shard_percent, sampleEvmTx) -> Optional[Transaction]:
        config = DEFAULT_ENV.config
        shard_size = self.branch.get_shard_size()
        shard_mask = shard_size - 1
        fromShard = self.branch.get_shard_id()

        # skip if from shard is specified and not matching current branch
        # FIXME: it's possible that clients want to specify '0x0' as the full shard ID, however it will not be supported
        if sampleEvmTx.fromFullShardId and (sampleEvmTx.fromFullShardId & shard_mask) != fromShard:
            return None

        if sampleEvmTx.fromFullShardId:
            fromFullShardId = sampleEvmTx.fromFullShardId
        else:
            fromFullShardId = account.address.full_shard_id & (~shardMask) | fromShard

        if not sampleEvmTx.to:
            to_address = random.choice(self.accounts).address
            recipient = to_address.recipient
            toFullShardId = to_address.full_shard_id & (~shardMask) | fromShard
        else:
            recipient = sampleEvmTx.to
            toFullShardId = fromFullShardId

        if random.randint(1, 100) <= x_shard_percent:
            # x-shard tx
            toShard = random.randint(0, config.SHARD_SIZE - 1)
            if toShard == self.branch.get_shard_id():
                toShard = (toShard + 1) % config.SHARD_SIZE
            toFullShardId = toFullShardId & (~shardMask) | toShard

        value = sampleEvmTx.value
        if not sampleEvmTx.data:
            value = random.randint(1, 100) * (10 ** 15)

        evm_tx = EvmTransaction(
            nonce=nonce,
            gasprice=sampleEvmTx.gasprice,
            startgas=sampleEvmTx.startgas,
            to=recipient,
            value=value,
            data=sampleEvmTx.data,
            fromFullShardId=fromFullShardId,
            toFullShardId=toFullShardId,
            network_id=config.NETWORK_ID)
        evm_tx.sign(account.key)
        return Transaction(code=Code.create_evm_code(evm_tx))
