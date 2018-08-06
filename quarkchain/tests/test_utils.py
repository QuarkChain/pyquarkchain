
from quarkchain.core import Address, Transaction, TransactionInput, TransactionOutput, Code, random_bytes
import random


def create_test_transaction(
        fromId,
        fromTxId,
        toAddress,
        amount=100,
        remaining=100,
        shardId=0,
        outputIndex=0,
        code=Code.getTransferCode()):
    acc1 = Address.createFromIdentity(fromId, shardId)
    tx = Transaction(
        inList=[TransactionInput(fromTxId, outputIndex)],
        code=code,
        outList=[TransactionOutput(acc1, remaining), TransactionOutput(toAddress, amount)])
    tx.sign([fromId.getKey()])
    return tx


def create_random_test_transaction(fromId, toAddress, amount=100, remaining=100):
    return create_test_transaction(fromId, random_bytes(32), toAddress, random.randint(0, 100), random.randint(0, 100))
