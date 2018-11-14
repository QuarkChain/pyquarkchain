import bisect
import ctypes
import sha3
import time
import unittest

FNV_PRIME_64 = 0x100000001b3
UINT64_MAX = 2 ** 64

CACHE_ENTRIES = 1024 * 64
ACCESS_ROUND = 128
WORD_BYTES = 8


def serialize_hash(h):
    return b"".join([x.to_bytes(WORD_BYTES, byteorder="little") for x in h])


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


def fnv64(v1, v2):
    return ((v1 * FNV_PRIME_64) ^ v2) % UINT64_MAX


def make_cache(entries, seed):
    o = sha3_512(seed)
    cache = []
    cache_set = set()

    for i in range(entries // len(o)):
        for e in o:
            if e not in cache_set:
                cache.append(e)
                cache_set.add(e)

        o = sha3_512(seed + i.to_bytes(4, byteorder="big"))

    cache.sort()
    return cache


def select(data, n):
    left = 0
    right = len(data)

    while True:
        pivot = data[left]
        i = left + 1
        e = right
        while i < e:
            if pivot < data[i]:
                e = e - 1
                tmp = data[e]
                data[e] = data[i]
                data[i] = tmp
            else:
                i = i + 1
        i = i - 1
        data[left] = data[i]
        data[i] = pivot

        if i == n:
            return pivot
        elif n < i:
            right = i
        else:
            left = i + 1


class TestSelect(unittest.TestCase):
    def test_simple(self):
        data = [1, 0, 2, 4, 3]
        self.assertEqual(select(data[:], 0), 0)
        self.assertEqual(select(data[:], 1), 1)
        self.assertEqual(select(data[:], 2), 2)
        self.assertEqual(select(data[:], 3), 3)
        self.assertEqual(select(data[:], 4), 4)

    def test_cache(self):
        data = make_cache(128, bytes())
        sdata = data[:]
        sdata.sort()
        for i in range(len(data)):
            self.assertEqual(select(data[:], i), sdata[i])


def list_to_uint64_array(l):
    array = (ctypes.c_uint64 * len(l))()
    for i in range(len(l)):
        array[i] = l[i]
    return array


class QkcHashCache:
    def __init__(self, native, ptr):
        self._ptr = ptr
        self._native = native

    def copy(self):
        return QkcHashCache(self._native, self._native._cache_copy(self._ptr))

    def __del__(self):
        self._native._cache_destroy(self._ptr)


class QkcHashNative:
    def __init__(self):
        self._lib = ctypes.CDLL("libqkchash.so")

        self._hash_func = self._lib.qkc_hash
        self._hash_func.restype = None
        self._hash_func.argtypes = (
            ctypes.c_void_p,  # cache pointer
            ctypes.POINTER(ctypes.c_uint64),  # input seed
            ctypes.POINTER(ctypes.c_uint64),
        )  # output result

        self._cache_create = self._lib.cache_create
        self._cache_create.restype = ctypes.c_void_p
        self._cache_create.argtypes = (
            ctypes.POINTER(ctypes.c_uint64),  # cache array pointer
            ctypes.c_uint32,
        )  # cache size

        self._cache_destroy = self._lib.cache_destroy
        self._cache_destroy.restype = None
        self._cache_destroy.argtypes = (ctypes.c_void_p,)

        self._cache_copy = self._lib.cache_copy
        self._cache_copy.restype = ctypes.c_void_p
        self._cache_copy.argtypes = (ctypes.c_void_p,)

    def make_cache(self, entries, seed):
        cache = list_to_uint64_array(make_cache(entries, seed))
        ptr = self._cache_create(cache, len(cache))
        return QkcHashCache(self, ptr)

    def dup_cache(self, cache):
        return cache.copy()

    def calculate_hash(self, header, nonce, cache):
        s = sha3_512(header + nonce[::-1])
        seed = list_to_uint64_array(s)
        result = (ctypes.c_uint64 * 4)()

        self._hash_func(cache._ptr, seed, result)

        return {
            "mix digest": serialize_hash(result),
            "result": serialize_hash(sha3_256(s + result[:])),
        }


def qkchash(header, nonce, cache):
    s = sha3_512(header + nonce[::-1])
    lcache = cache[:]
    lcache_set = set(cache)

    mix = []
    for i in range(2):
        mix.extend(s)

    for i in range(ACCESS_ROUND):
        new_data = []

        p = fnv64(i ^ s[0], mix[i % len(mix)])
        for j in range(len(mix)):
            # Find the pth element and remove it
            remove_idx = p % len(lcache)
            new_data.append(lcache[remove_idx])
            lcache_set.remove(lcache[remove_idx])
            del lcache[remove_idx]

            # Generate random data and insert it
            p = fnv64(p, new_data[j])
            if p not in lcache_set:
                bisect.insort(lcache, p)
                lcache_set.add(p)

            # Find the next element
            p = fnv64(p, new_data[j])

        for j in range(len(mix)):
            mix[j] = fnv64(mix[j], new_data[j])

    cmix = []
    for i in range(0, len(mix), 4):
        cmix.append(fnv64(fnv64(fnv64(mix[i], mix[i + 1]), mix[i + 2]), mix[i + 3]))
    return {
        "mix digest": serialize_hash(cmix),
        "result": serialize_hash(sha3_256(s + cmix)),
    }


def test_qkchash_perf():
    N = 10
    start_time = time.time()
    cache = make_cache(CACHE_ENTRIES, bytes())
    used_time = time.time() - start_time
    print("make_cache time: %.2f" % (used_time))

    start_time = time.time()
    h0 = []
    for nonce in range(N):
        h0.append(qkchash(bytes(4), nonce.to_bytes(4, byteorder="big"), cache))
    used_time = time.time() - start_time
    print(
        "Python version, time used: %.2f, hashes per sec: %.2f"
        % (used_time, N / used_time)
    )

    # Native version
    native = QkcHashNative()
    cache = native.make_cache(CACHE_ENTRIES, bytes())

    start_time = time.time()
    h1 = []
    N = 1000
    for nonce in range(N):
        dup_cache = native.dup_cache(cache)
        h1.append(
            native.calculate_hash(
                bytes(4), nonce.to_bytes(4, byteorder="big"), dup_cache
            )
        )
    used_time = time.time() - start_time
    print(
        "Native version, time used: %.2f, hashes per sec: %.2f"
        % (used_time, N / used_time)
    )

    print("Equal: ", h0 == h1[0 : len(h0)])


if __name__ == "__main__":
    test_qkchash_perf()
