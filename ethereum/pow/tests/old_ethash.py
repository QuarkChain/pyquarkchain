"""
Original hex-based ethash implementation, preserved as a reference baseline
for tests and benchmarks.
"""

import copy

from eth_utils import encode_hex, decode_hex
from Crypto.Hash import keccak

WORD_BYTES = 4
HASH_BYTES = 64
MIX_BYTES = 128
ACCESSES = 64
DATASET_PARENTS = 256
CACHE_ROUNDS = 3
FNV_PRIME = 0x01000193


def _sha3_256_raw(x): return keccak.new(digest_bits=256, data=x).digest()
def _sha3_512_raw(x): return keccak.new(digest_bits=512, data=x).digest()


def decode_int(s):
    return int(encode_hex(s[::-1]), 16) if s else 0


def encode_int(s):
    a = "%x" % s
    return b"" if s == 0 else decode_hex("0" * (len(a) % 2) + a)[::-1]


def serialize_hash(h):
    return b"".join([encode_int(x).ljust(4, b"\x00") for x in h])


def deserialize_hash(h):
    return [decode_int(h[i:i + WORD_BYTES]) for i in range(0, len(h), WORD_BYTES)]


def sha3_512(x):
    if isinstance(x, list):
        x = serialize_hash(x)
    return deserialize_hash(_sha3_512_raw(x))


def sha3_256(x):
    if isinstance(x, list):
        x = serialize_hash(x)
    return deserialize_hash(_sha3_256_raw(x))


def fnv(v1, v2):
    return (v1 * FNV_PRIME ^ v2) % 2 ** 32


def mkcache(cache_size, seed):
    n = cache_size // HASH_BYTES
    o = [sha3_512(seed)]
    for i in range(1, n):
        o.append(sha3_512(o[-1]))
    for _ in range(CACHE_ROUNDS):
        for i in range(n):
            v = o[i][0] % n
            o[i] = sha3_512([a ^ b for a, b in zip(o[(i - 1 + n) % n], o[v])])
    return o


def calc_dataset_item(cache, i):
    n = len(cache)
    r = HASH_BYTES // WORD_BYTES
    mix = copy.copy(cache[i % n])
    mix[0] ^= i
    mix = sha3_512(mix)
    for j in range(DATASET_PARENTS):
        cache_index = fnv(i ^ j, mix[j % r])
        mix = list(map(fnv, mix, cache[cache_index % n]))
    return sha3_512(mix)


def hashimoto_light(full_size, cache, header, nonce):
    n = full_size // HASH_BYTES
    w = MIX_BYTES // WORD_BYTES
    mixhashes = MIX_BYTES // HASH_BYTES
    s = sha3_512(header + nonce[::-1])
    mix = []
    for _ in range(mixhashes):
        mix.extend(s)
    for i in range(ACCESSES):
        p = fnv(i ^ s[0], mix[i % w]) % (n // mixhashes) * mixhashes
        newdata = []
        for j in range(mixhashes):
            newdata.extend(calc_dataset_item(cache, p + j))
        mix = list(map(fnv, mix, newdata))
    cmix = []
    for i in range(0, len(mix), 4):
        cmix.append(fnv(fnv(fnv(mix[i], mix[i+1]), mix[i+2]), mix[i+3]))
    return {
        b"mix digest": serialize_hash(cmix),
        b"result": serialize_hash(sha3_256(s + cmix)),
    }
