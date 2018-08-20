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
    from_id = Identity.create_random_identity()
    acc1 = Address.create_from_identity(from_id, 0)
    tx = Transaction(
        [TransactionInput(bytes(32), 0)], Code(), [TransactionOutput(acc1, 0)]
    )
    tx.sign([from_id.get_key()])

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
            print("data size mismatched! read\n%s\nreal\n%s" % (read, get_output()))
            sys.exit(1)


if __name__ == "__main__":
    main()
