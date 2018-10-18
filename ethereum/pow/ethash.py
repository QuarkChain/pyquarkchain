import copy
from functools import lru_cache
from typing import Callable, Dict, List

from ethereum.pow.ethash_utils import *

cache_seeds = [b"\x00" * 32]  # type: List[bytes]


def mkcache(cache_size: int, block_number) -> List[List[int]]:
    while len(cache_seeds) <= block_number // EPOCH_LENGTH:
        new_seed = serialize_hash(ethash_sha3_256(cache_seeds[-1]))
        cache_seeds.append(new_seed)

    seed = cache_seeds[block_number // EPOCH_LENGTH]
    return _get_cache(seed, cache_size // HASH_BYTES)


@lru_cache(10)
def _get_cache(seed, n) -> List[List[int]]:
    # Sequentially produce the initial dataset
    o = [ethash_sha3_512(seed)]
    for i in range(1, n):
        o.append(ethash_sha3_512(o[-1]))

    # Use a low-round version of randmemohash
    for _ in range(CACHE_ROUNDS):
        for i in range(n):
            v = o[i][0] % n
            o[i] = ethash_sha3_512(list(map(xor, o[(i - 1 + n) % n], o[v])))

    return o


def calc_dataset_item(cache: List[List[int]], i: int) -> List[int]:
    n = len(cache)
    r = HASH_BYTES // WORD_BYTES
    # initialize the mix
    mix = copy.copy(cache[i % n])  # type: List[int]
    mix[0] ^= i
    mix = ethash_sha3_512(mix)
    # fnv it with a lot of random cache nodes based on i
    for j in range(DATASET_PARENTS):
        cache_index = fnv(i ^ j, mix[j % r])
        mix = list(map(fnv, mix, cache[cache_index % n]))
    return ethash_sha3_512(mix)


def calc_dataset(full_size, cache) -> List[List[int]]:
    o = []
    for i in range(full_size // HASH_BYTES):
        o.append(calc_dataset_item(cache, i))
    return o


def hashimoto(
    header: bytes,
    nonce: bytes,
    full_size: int,
    dataset_lookup: Callable[[int], List[int]],
) -> Dict:
    n = full_size // HASH_BYTES
    w = MIX_BYTES // WORD_BYTES
    mixhashes = MIX_BYTES // HASH_BYTES
    # combine header+nonce into a 64 byte seed
    s = ethash_sha3_512(header + nonce[::-1])
    mix = []
    for _ in range(MIX_BYTES // HASH_BYTES):
        mix.extend(s)
    # mix in random dataset nodes
    for i in range(ACCESSES):
        p = fnv(i ^ s[0], mix[i % w]) % (n // mixhashes) * mixhashes
        newdata = []
        for j in range(mixhashes):
            newdata.extend(dataset_lookup(p + j))
        mix = list(map(fnv, mix, newdata))
    # compress mix
    cmix = []
    for i in range(0, len(mix), 4):
        cmix.append(fnv(fnv(fnv(mix[i], mix[i + 1]), mix[i + 2]), mix[i + 3]))
    return {
        b"mix digest": serialize_hash(cmix),
        b"result": serialize_hash(ethash_sha3_256(s + cmix)),
    }


def hashimoto_light(
    full_size: int, cache: List[List[int]], header: bytes, nonce: bytes
) -> Dict:
    return hashimoto(header, nonce, full_size, lambda x: calc_dataset_item(cache, x))


def hashimoto_full(dataset: List[List[int]], header: bytes, nonce: bytes) -> Dict:
    return hashimoto(header, nonce, len(dataset) * HASH_BYTES, lambda x: dataset[x])
