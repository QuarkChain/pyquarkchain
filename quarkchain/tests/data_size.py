import sys

from quarkchain.core import Code, Address, Identity
from quarkchain.core import RootBlockHeader, MinorBlockHeader
from quarkchain.core import Transaction, TransactionInput, TransactionOutput

size_list = [
    ("RootBlockHeader", RootBlockHeader()),
    ("MinorBlockHeader", MinorBlockHeader()),
]


def get_output():
    outputs = []
    fromId = Identity.createRandomIdentity()
    acc1 = Address.createFromIdentity(fromId, 0)
    tx = Transaction(
        [TransactionInput(bytes(32), 0)],
        Code(),
        [TransactionOutput(acc1, 0)])
    tx.sign([fromId.getKey()])

    for name, obj in size_list:
        outputs.append("{}: {}".format(name, len(obj.serialize())))
    outputs.append("Transaction: {}".format(len(tx.serialize())))
    return "\n".join(outputs)


def main():
    if len(sys.argv) <= 1:
        sys.exit(1)

    if sys.argv[1] == "print":
        sys.stdout.write(get_output())

    if sys.argv[1] == "check":
        read = sys.stdin.read()
        if get_output() != read:
            print("data size mismatched!")
            sys.exit(1)


if __name__ == '__main__':
    main()
