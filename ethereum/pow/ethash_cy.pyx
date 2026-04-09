# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: initializedcheck=False
"""
Cython-accelerated Ethash routines.

R3 — ``mix_parents``: inner loop of calc_dataset_item (256-iter FNV mixing).
R4 — ``cy_calc_dataset_item``, ``cy_hashimoto_light``: full functions that
      call C keccak directly, eliminating all Python-layer overhead for hashing.
"""

import numpy as np
cimport numpy as cnp
cimport cython
from libc.stdint cimport uint8_t, uint32_t, uint64_t
from libc.string cimport memcpy, memset

cnp.import_array()

# ---------- C keccak (keccak_tiny.c, linked at build time) ----------
cdef extern from "keccak_tiny.h":
    int keccak_256(uint8_t* out, size_t outlen,
                   const uint8_t* inp, size_t inlen) nogil
    int keccak_512(uint8_t* out, size_t outlen,
                   const uint8_t* inp, size_t inlen) nogil

# ---------- Ethash constants ----------
cdef uint32_t FNV_PRIME    = 0x01000193u

cdef enum:
    _DATASET_PARENTS = 256
    _R            = 16    # HASH_BYTES / WORD_BYTES
    _HASH_BYTES   = 64
    _MIX_BYTES    = 128
    _ACCESSES     = 64
    _MIX_WORDS    = 32    # MIX_BYTES / WORD_BYTES
    _MIX_HASHES   = 2     # MIX_BYTES / HASH_BYTES
    _CACHE_ROUNDS = 3

# ---------- Inline C helpers ----------

cdef inline void _keccak_512_u32(uint32_t* out, const uint32_t* inp) noexcept nogil:
    """keccak-512: 64 bytes in → 16 uint32 out."""
    keccak_512(<uint8_t*>out, 64, <const uint8_t*>inp, 64)

cdef inline void _keccak_512_bytes(uint32_t* out,
                                   const uint8_t* inp,
                                   size_t inlen) noexcept nogil:
    """keccak-512: arbitrary bytes in → 16 uint32 out."""
    keccak_512(<uint8_t*>out, 64, inp, inlen)

cdef inline void _keccak_256_u32(uint32_t* out,
                                 const uint32_t* inp,
                                 size_t n_u32) noexcept nogil:
    """keccak-256: n_u32 uint32 words in → 8 uint32 out."""
    keccak_256(<uint8_t*>out, 32, <const uint8_t*>inp, n_u32 * 4)


# =====================================================================
# cy_mkcache — build ethash cache using C keccak
# =====================================================================

def cy_mkcache(const uint8_t[::1] seed, Py_ssize_t n):
    """Build ethash cache: n rows of 16 uint32, using C keccak-512.

    Parameters
    ----------
    seed : bytes (as uint8 array)
        32-byte seed for this epoch.
    n : int
        Number of cache rows (cache_size // HASH_BYTES).

    Returns
    -------
    numpy ndarray of shape (n, 16), dtype uint32.
    """
    result = np.empty((n, 16), dtype=np.uint32)
    cdef uint32_t[:, ::1] o = result
    cdef uint32_t* ptr = &o[0, 0]
    cdef Py_ssize_t i, rnd
    cdef uint32_t v
    cdef uint32_t xored[16]

    # o[0] = keccak_512(seed)
    keccak_512(<uint8_t*>ptr, 64, &seed[0], seed.shape[0])

    # o[i] = keccak_512(o[i-1])
    for i in range(1, n):
        _keccak_512_u32(&ptr[i * _R], &ptr[(i - 1) * _R])

    # CACHE_ROUNDS of RandMemoHash
    for rnd in range(_CACHE_ROUNDS):
        for i in range(n):
            v = ptr[i * _R] % <uint32_t>n
            # xored = o[(i-1+n) % n] ^ o[v]
            for k in range(_R):
                xored[k] = ptr[(((i - 1 + n) % n) * _R) + k] ^ ptr[(v * _R) + k]
            _keccak_512_u32(&ptr[i * _R], xored)

    return result


# =====================================================================
# R3 — mix_parents (kept for backward compatibility)
# =====================================================================

@cython.boundscheck(False)
@cython.wraparound(False)
def mix_parents(uint32_t[::1] mix,
                const uint32_t[:, ::1] cache,
                uint64_t i):
    """In-place parent mixing for one dataset item (R3 API)."""
    cdef Py_ssize_t n = cache.shape[0]
    cdef Py_ssize_t j, k
    cdef uint32_t cache_index, mix_word
    cdef uint32_t i32 = <uint32_t>i

    for j in range(_DATASET_PARENTS):
        mix_word = mix[j % _R]
        cache_index = ((i32 ^ <uint32_t>j) * FNV_PRIME) ^ mix_word
        cache_index = cache_index % <uint32_t>n
        for k in range(_R):
            mix[k] = (mix[k] * FNV_PRIME) ^ cache[cache_index, k]


# =====================================================================
# R4 — full calc_dataset_item + hashimoto_light in C/Cython
# =====================================================================

cdef inline void _calc_dataset_item(uint32_t* out,
                                    const uint32_t* cache,
                                    Py_ssize_t n,
                                    uint32_t idx) noexcept nogil:
    """Pure C calc_dataset_item.  Writes 16 uint32 to *out*."""
    cdef uint32_t mix[16]
    cdef Py_ssize_t j, k
    cdef uint32_t cache_index, mix_word

    # mix = cache[idx % n]; mix[0] ^= idx
    memcpy(mix, &cache[(idx % n) * _R], 64)
    mix[0] ^= idx
    # mix = keccak_512(mix)
    _keccak_512_u32(mix, mix)
    # parent mixing
    for j in range(_DATASET_PARENTS):
        mix_word = mix[j % _R]
        cache_index = ((idx ^ <uint32_t>j) * FNV_PRIME) ^ mix_word
        cache_index = cache_index % <uint32_t>n
        for k in range(_R):
            mix[k] = (mix[k] * FNV_PRIME) ^ cache[(cache_index * _R) + k]
    # mix = keccak_512(mix)
    _keccak_512_u32(out, mix)


def cy_calc_dataset_item(const uint32_t[:, ::1] cache, uint32_t i):
    """Python-callable calc_dataset_item (R4). Returns ndarray uint32[16]."""
    cdef Py_ssize_t n = cache.shape[0]
    result = np.empty(16, dtype=np.uint32)
    cdef uint32_t[::1] result_view = result
    _calc_dataset_item(&result_view[0], &cache[0, 0], n, i)
    return result


def cy_hashimoto_light(Py_ssize_t full_size,
                       const uint32_t[:, ::1] cache,
                       const uint8_t[::1] header,
                       const uint8_t[::1] nonce):
    """Full hashimoto_light in Cython+C (R4).

    Returns dict identical to the Python version:
        {b"mix digest": bytes(32), b"result": bytes(32)}
    """
    cdef Py_ssize_t n = full_size // _HASH_BYTES
    cdef Py_ssize_t i, j, k, p
    cdef uint32_t s0
    cdef uint32_t s[16]
    cdef uint32_t mix[_MIX_WORDS]     # 32 uint32
    cdef uint32_t newdata[_MIX_WORDS]
    cdef uint32_t cmix[8]
    cdef uint32_t s_cmix[24]         # s(16) + cmix(8)
    cdef uint32_t result_hash[8]
    cdef Py_ssize_t cache_n = cache.shape[0]
    cdef const uint32_t* cache_ptr = &cache[0, 0]

    # nonce_rev = nonce[::-1]
    cdef Py_ssize_t header_len = header.shape[0]
    cdef Py_ssize_t nonce_len = nonce.shape[0]
    cdef uint8_t seed_buf[128]  # header (up to ~80) + nonce (8)
    memcpy(seed_buf, &header[0], header_len)
    # reverse nonce
    for i in range(nonce_len):
        seed_buf[header_len + i] = nonce[nonce_len - 1 - i]

    # s = keccak_512(header + nonce[::-1])
    _keccak_512_bytes(s, seed_buf, header_len + nonce_len)

    # mix = tile(s, 2)
    memcpy(mix, s, 64)
    memcpy(&mix[16], s, 64)

    s0 = s[0]

    for i in range(_ACCESSES):
        p = <Py_ssize_t>(((<uint32_t>i ^ s0) * FNV_PRIME) ^ mix[i % _MIX_WORDS])
        p = (p % (n // _MIX_HASHES)) * _MIX_HASHES
        for j in range(_MIX_HASHES):
            _calc_dataset_item(&newdata[j * _R], cache_ptr, cache_n, <uint32_t>(p + j))
        for k in range(_MIX_WORDS):
            mix[k] = (mix[k] * FNV_PRIME) ^ newdata[k]

    # compress mix → cmix (8 uint32)
    for i in range(8):
        cmix[i] = mix[i * 4]
        cmix[i] = (cmix[i] * FNV_PRIME) ^ mix[i * 4 + 1]
        cmix[i] = (cmix[i] * FNV_PRIME) ^ mix[i * 4 + 2]
        cmix[i] = (cmix[i] * FNV_PRIME) ^ mix[i * 4 + 3]

    # result = keccak_256(s + cmix)
    memcpy(s_cmix, s, 64)
    memcpy(&s_cmix[16], cmix, 32)
    _keccak_256_u32(result_hash, s_cmix, 24)

    # Return as Python dict with bytes values
    return {
        b"mix digest": (<uint8_t*>cmix)[:32],
        b"result": (<uint8_t*>result_hash)[:32],
    }
