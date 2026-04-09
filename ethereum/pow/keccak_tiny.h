/** libkeccak-tiny
 *
 * Copied from https://github.com/coruus/keccak-tiny
 * See keccak_tiny.c for local modifications.
 */
#ifndef KECCAK_TINY_H
#define KECCAK_TINY_H

#include <stdint.h>
#include <stdlib.h>

/* Original Keccak (padding 0x01) — used by Ethash */
int keccak_256(uint8_t* out, size_t outlen,
               const uint8_t* in, size_t inlen);
int keccak_512(uint8_t* out, size_t outlen,
               const uint8_t* in, size_t inlen);

/* FIPS-202 SHA-3 (padding 0x06) */
int sha3_256(uint8_t* out, size_t outlen,
             const uint8_t* in, size_t inlen);
int sha3_512(uint8_t* out, size_t outlen,
             const uint8_t* in, size_t inlen);

#endif /* KECCAK_TINY_H */
