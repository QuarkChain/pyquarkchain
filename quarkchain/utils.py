import argparse
from quarkchain.core import Identity, Address


def int_left_most_bit(v):
    """ Could be replaced by better raw implementation
    """
    b = 0
    while v != 0:
        v //= 2
        b += 1
    return b


def is_p2(v):
    return (v & (v - 1)) == 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cmd")
    args = parser.parse_args()

    if args.cmd == "create_account":
        iden = Identity.createRandomIdentity()
        addr = Address.createFromIdentity(iden)
        print("Key: %s" % iden.getKey().hex())
        print("Account: %s" % addr.serialize().hex())


if __name__ == '__main__':
    main()
