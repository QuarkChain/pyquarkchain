
import random

from quarkchain.core import Address, Transaction, TransactionInput, TransactionOutput, Code, random_bytes


def create_test_transaction(
        fromId,
        fromTxId,
        to_address,
        amount=100,
        remaining=100,
        shard_id=0,
        outputIndex=0,
        code=Code.get_transfer_code()):
    acc1 = Address.create_from_identity(fromId, shard_id)
    tx = Transaction(
        in_list=[TransactionInput(fromTxId, outputIndex)],
        code=code,
        out_list=[TransactionOutput(acc1, remaining), TransactionOutput(to_address, amount)])
    tx.sign([fromId.get_key()])
    return tx


def create_random_test_transaction(fromId, to_address, amount=100, remaining=100):
    return create_test_transaction(fromId, random_bytes(32), to_address, random.randint(0, 100), random.randint(0, 100))
