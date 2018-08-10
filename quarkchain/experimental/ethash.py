#!/usr/bin/python3

import copy
import sha3

# constants of ethash
WORD_BYTES = 4  # bytes in word
DATASET_BYTES_INIT = 2 ** 30  # bytes in dataset at genesis
DATASET_BYTES_GROWTH = 2 ** 23  # dataset growth per epoch
CACHE_BYTES_INIT = 2 ** 24  # bytes in cache at genesis
CACHE_BYTES_GROWTH = 2 ** 17  # cache growth per epoch
CACHE_MULTIPLIER = 1024  # Size of the DAG relative to the cache
EPOCH_LENGTH = 30000  # blocks per epoch
MIX_BYTES = 128  # width of mix
HASH_BYTES = 64  # hash length in bytes
DATASET_PARENTS = 256  # number of parents of each dataset element
CACHE_ROUNDS = 3  # number of rounds in cache production
ACCESSES = 64  # number of accesses in hashimoto loop

# Assumes little endian bit ordering (same as Intel architectures)


def serialize_hash(h):
    return b"".join([x.to_bytes(4, byteorder="little") for x in h])


def deserialize_hash(h):
    return [
        int.from_bytes(h[i : i + WORD_BYTES], byteorder="little")
        for i in range(0, len(h), WORD_BYTES)
    ]


def hash_words(h, sz, x):
    if isinstance(x, list):
        x = serialize_hash(x)
    y = h(x)
    return deserialize_hash(y)


def serialize_cache(ds):
    return "".join([serialize_hash(h) for h in ds])


serialize_dataset = serialize_cache

# sha3 hash function, outputs 64 bytes


def sha3_512(x):
    return hash_words(lambda v: sha3.sha3_512(v).digest(), 64, x)


def sha3_256(x):
    return hash_words(lambda v: sha3.sha3_256(v).digest(), 32, x)


def xor(a, b):
    return a ^ b


def isprime(x):
    for i in range(2, int(x ** 0.5) + 1):
        if x % i == 0:
            return False
    return True


def get_cache_size(block_number):
    sz = CACHE_BYTES_INIT + CACHE_BYTES_GROWTH * (block_number // EPOCH_LENGTH)
    sz -= HASH_BYTES
    while not isprime(sz / HASH_BYTES):
        sz -= 2 * HASH_BYTES
    return sz


def get_full_size(block_number):
    sz = DATASET_BYTES_INIT + DATASET_BYTES_GROWTH * (block_number // EPOCH_LENGTH)
    sz -= MIX_BYTES
    while not isprime(sz / MIX_BYTES):
        sz -= 2 * MIX_BYTES
    return sz


def mkcache(cache_size, seed):
    n = cache_size // HASH_BYTES

    # Sequentially produce the initial dataset
    o = [sha3_512(seed)]
    for i in range(1, n):
        o.append(sha3_512(o[-1]))

    # Use a low-round version of randmemohash
    for _ in range(CACHE_ROUNDS):
        for i in range(n):
            v = o[i][0] % n
            o[i] = sha3_512(list(map(xor, o[(i - 1 + n) % n], o[v])))

    return o


FNV_PRIME = 0x01000193


def fnv(v1, v2):
    return ((v1 * FNV_PRIME) ^ v2) % 2 ** 32


def calc_dataset_item(cache, i):
    n = len(cache)
    r = HASH_BYTES // WORD_BYTES
    # initialize the mix
    mix = copy.copy(cache[i % n])
    mix[0] ^= i
    mix = sha3_512(mix)
    # fnv it with a lot of random cache nodes based on i
    for j in range(DATASET_PARENTS):
        cache_index = fnv(i ^ j, mix[j % r])
        mix = list(map(fnv, mix, cache[cache_index % n]))
    return sha3_512(mix)


def calc_dataset(full_size, cache):
    return [calc_dataset_item(cache, i) for i in range(full_size // HASH_BYTES)]


def hashimoto(header, nonce, full_size, dataset_lookup):
    n = full_size / HASH_BYTES
    w = MIX_BYTES // WORD_BYTES
    mixhashes = MIX_BYTES / HASH_BYTES
    # combine header+nonce into a 64 byte seed
    s = sha3_512(header + nonce[::-1])
    # start the mix with replicated s
    mix = []
    for _ in range(MIX_BYTES / HASH_BYTES):
        mix.extend(s)
    # mix in random dataset nodes
    for i in range(ACCESSES):
        p = fnv(i ^ s[0], mix[i % w]) % (n // mixhashes) * mixhashes
        newdata = []
        for j in range(MIX_BYTES / HASH_BYTES):
            newdata.extend(dataset_lookup(p + j))
        mix = list(map(fnv, mix, newdata))
    # compress mix
    cmix = []
    for i in range(0, len(mix), 4):
        cmix.append(fnv(fnv(fnv(mix[i], mix[i + 1]), mix[i + 2]), mix[i + 3]))
    return {
        "mix digest": serialize_hash(cmix),
        "result": serialize_hash(sha3_256(s + cmix)),
    }


# light-way to hash the header.  Mainly used to verify a hash of data is
# correct when a new block is announced
def hashimoto_light(full_size, cache, header, nonce):
    return hashimoto(header, nonce, full_size, lambda x: calc_dataset_item(cache, x))


# heavy-way to hash the header.  Mainly used to mine the data.
def hashimoto_full(full_size, dataset, header, nonce):
    return hashimoto(header, nonce, full_size, lambda x: dataset[x])


def mine(full_size, dataset, header, difficulty):
    target = zpad(encode_int(2 ** 256 // difficulty), 64)[::-1]
    from random import randint

    nonce = randint(0, 2 ** 64)
    while hashimoto_full(full_size, dataset, header, nonce) > target:
        nonce = (nonce + 1) % 2 ** 64
    return nonce


def get_seedhash(block):
    s = "\x00" * 32
    for i in range(block.number // EPOCH_LENGTH):
        s = serialize_hash(sha3_256(s))
    return s
