
from quarkchain.config import DEFAULT_ENV
from quarkchain.core import Address, Transaction, TransactionInput, TransactionOutput, Code, random_bytes
import random


def get_test_env(genesisAccount=Address.createEmptyAccount(), genesisQuarkash=10000, genesisMinorQuarkash=1000):
    env = DEFAULT_ENV
    env.config.setShardSize(2)
    env.config.SKIP_MINOR_DIFFICULTY_CHECK = True
    env.config.SKIP_ROOT_DIFFICULTY_CHECK = True
    env.config.SKIP_MINOR_COINBASE_CHECK = True
    env.config.GENESIS_ACCOUNT = genesisAccount
    env.config.GENESIS_COIN = genesisQuarkash
    env.config.GENESIS_MINOR_COIN = genesisMinorQuarkash
    env.config.TESTNET_MASTER_ACCOUNT = genesisAccount
    return env


def create_test_transaction(fromId, fromTxId, toAddress, amount=100, remaining=100, shardId=0, outputIndex=0):
    acc1 = Address.createFromIdentity(fromId, shardId)
    tx = Transaction(
        [TransactionInput(fromTxId, outputIndex)],
        Code(),
        [TransactionOutput(acc1, remaining), TransactionOutput(toAddress, amount)])
    tx.sign([fromId.getKey()])
    return tx


def create_random_test_transaction(fromId, toAddress, amount=100, remaining=100):
    return create_test_transaction(fromId, random_bytes(32), toAddress, random.randint(0, 100), random.randint(0, 100))
