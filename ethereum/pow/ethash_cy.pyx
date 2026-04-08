# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
# cython: initializedcheck=False
"""
Cython rewrite of the inner loop of ``calc_dataset_item``.

The Python version spends most of its time in the ``DATASET_PARENTS`` (256)
iteration loop, which is pure 32-bit integer arithmetic and an indexed
row XOR into a 16-word mix. This module exposes ``mix_parents`` which takes
the already-hashed mix (uint32[16]) plus the cache (uint32[:, 16]) and
performs the full parent loop in native code, writing back into ``mix``
in place.

The caller (ethash.py) is still responsible for the two keccak-512 calls
that bracket the loop.
"""

import numpy as np
cimport numpy as cnp
cimport cython
from libc.stdint cimport uint32_t, uint64_t

cnp.import_array()

# Ethash constants, mirrored here so we don't touch Python state in the loop.
cdef uint32_t FNV_PRIME = 0x01000193u
cdef Py_ssize_t DATASET_PARENTS = 256
cdef Py_ssize_t R = 16  # HASH_BYTES // WORD_BYTES


@cython.boundscheck(False)
@cython.wraparound(False)
def mix_parents(uint32_t[::1] mix,
                const uint32_t[:, ::1] cache,
                uint64_t i):
    """In-place parent mixing for one dataset item.

    Parameters
    ----------
    mix : uint32[16] (C-contiguous)
        The post-sha3_512 seed mix; updated in place.
    cache : uint32[n, 16] (C-contiguous)
        The ethash cache.
    i : uint64
        Dataset item index.
    """
    cdef Py_ssize_t n = cache.shape[0]
    cdef Py_ssize_t j, k
    cdef uint32_t cache_index, mix_word
    cdef uint32_t i32 = <uint32_t>i

    for j in range(DATASET_PARENTS):
        mix_word = mix[j % R]
        # 32-bit wraparound is implicit in uint32_t arithmetic
        cache_index = ((i32 ^ <uint32_t>j) * FNV_PRIME) ^ mix_word
        cache_index = cache_index % <uint32_t>n
        for k in range(R):
            mix[k] = (mix[k] * FNV_PRIME) ^ cache[cache_index, k]
