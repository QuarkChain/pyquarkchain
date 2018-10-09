#ifndef __QKC_UTIL
#define __QKC_UTIL

const uint32_t FNV_PRIME_32 = 0x01000193;
const uint64_t FNV_PRIME_64 = 0x100000001b3ULL;
const uint64_t FNV_OFFSET_BASE_64 = 0xcbf29ce484222325ULL;

/*
 * 32-bit FNV function
 */
uint32_t fnv32(uint32_t v1, uint32_t v2) {
    return (v1 * FNV_PRIME_32) ^ v2;
}

/*
 * 64-bit FNV function
 */
uint64_t fnv64(uint64_t v1, uint64_t v2) {
    return (v1 * FNV_PRIME_64) ^ v2;
}

/*
 * 64-bit FNV-1a function
 */
uint64_t fnv64_1a(uint64_t v1, uint64_t v2) {
    return (v1 ^ v2) * FNV_PRIME_64;
}

/*
 * Abort the code if cond is false.
 * Unlike assert(), which can be disabled by NDEBUG macro,
 * check() will always abort the code if cond is false.
 */
#define CHECK(COND) ((COND) ? (void)0 : abort())

#endif