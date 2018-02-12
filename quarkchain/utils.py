from Crypto.Hash import keccak


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


def sha3_256(x):
    if isinstance(x, bytearray):
        x = bytes(x)
    if not isinstance(x, bytes):
        raise RuntimeError("sha3_256 only accepts bytes or bytearray")
    return keccak.new(digest_bits=256, data=x).digest()


def check(condition):
    """ Unlike assert, which can be optimized out,
    check will always check whether condition is satisfied or throw AssertionError if not
    """
    if not condition:
        raise AssertionError()
