
import random

from quarkchain.core import Address, Transaction, TransactionInput, TransactionOutput, Code, random_bytes


def create_test_transaction(
        fromId,
        fromTxId,
        toAddress,
        amount=100,
        remaining=100,
        shardId=0,
        outputIndex=0,
        code=Code.get_transfer_code()):
    acc1 = Address.create_from_identity(fromId, shardId)
    tx = Transaction(
        inList=[TransactionInput(fromTxId, outputIndex)],
        code=code,
        outList=[TransactionOutput(acc1, remaining), TransactionOutput(toAddress, amount)])
    tx.sign([fromId.get_key()])
    return tx


def create_random_test_transaction(fromId, toAddress, amount=100, remaining=100):
    return create_test_transaction(fromId, random_bytes(32), toAddress, random.randint(0, 100), random.randint(0, 100))
