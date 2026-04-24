use ndarray::Array2;
use numpy::{
    IntoPyArray, PyArray1, PyArray2, PyArrayMethods, PyReadonlyArray1, PyReadonlyArray2,
    PyUntypedArrayMethods,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use tiny_keccak::{Hasher, Keccak};

// ---------- Ethash constants ----------

const FNV_PRIME: u32 = 0x01000193;
const DATASET_PARENTS: usize = 256;
const R: usize = 16; // HASH_BYTES / WORD_BYTES = 64 / 4
const MIX_WORDS: usize = 32; // MIX_BYTES / WORD_BYTES = 128 / 4
const MIX_HASHES: usize = 2; // MIX_BYTES / HASH_BYTES = 128 / 64
const ACCESSES: usize = 64;
const CACHE_ROUNDS: usize = 3;

// ---------- Keccak helpers ----------
//
// Ethash uses the *original* Keccak spec (padding 0x01), not FIPS-202 SHA-3.
// tiny-keccak's Keccak::v256() / Keccak::v512() correctly use 0x01 padding.

#[inline]
fn keccak512_bytes(input: &[u8]) -> [u8; 64] {
    let mut out = [0u8; 64];
    let mut k = Keccak::v512();
    k.update(input);
    k.finalize(&mut out);
    out
}

#[inline]
fn keccak256_bytes(input: &[u8]) -> [u8; 32] {
    let mut out = [0u8; 32];
    let mut k = Keccak::v256();
    k.update(input);
    k.finalize(&mut out);
    out
}

/// keccak-512: 16 u32 words (64 bytes, little-endian) → 16 u32 words
#[inline]
fn keccak512_u32(inp: &[u32; 16]) -> [u32; 16] {
    let mut bytes = [0u8; 64];
    for (i, &w) in inp.iter().enumerate() {
        bytes[i * 4..i * 4 + 4].copy_from_slice(&w.to_le_bytes());
    }
    let h = keccak512_bytes(&bytes);
    let mut out = [0u32; 16];
    for i in 0..16 {
        out[i] = u32::from_le_bytes(h[i * 4..i * 4 + 4].try_into().unwrap());
    }
    out
}

// ---------- Core algorithms ----------

/// 256-iteration FNV parent-mixing loop.
///
///   for j in 0..256:
///       cache_index = ((idx ^ j) * FNV_PRIME) ^ mix[j % 16]
///       for k in 0..16: mix[k] = mix[k] * FNV_PRIME ^ cache[cache_index, k]
fn mix_parents_inner(mix: &mut [u32; 16], cache: &[u32], n: usize, idx: u32) {
    for j in 0..DATASET_PARENTS {
        let mix_word = mix[j % R];
        let cache_index =
            ((idx ^ j as u32).wrapping_mul(FNV_PRIME) ^ mix_word) as usize % n;
        for k in 0..R {
            mix[k] = mix[k].wrapping_mul(FNV_PRIME) ^ cache[cache_index * R + k];
        }
    }
}

/// Compute one dataset item from the cache.
fn calc_dataset_item_inner(cache: &[u32], n: usize, idx: u32) -> [u32; 16] {
    let base = (idx as usize % n) * R;
    let mut mix: [u32; 16] = cache[base..base + R].try_into().unwrap();
    mix[0] ^= idx;
    mix = keccak512_u32(&mix);
    mix_parents_inner(&mut mix, cache, n, idx);
    keccak512_u32(&mix)
}

// ---------- Python-exported functions ----------

/// Build the ethash cache: n rows of 16 uint32, using Keccak-512.
///
/// Parameters
/// ----------
/// seed : numpy uint8 array
///     32-byte epoch seed.
/// n : int
///     Number of cache rows (cache_size // HASH_BYTES).
///
/// Returns
/// -------
/// numpy ndarray shape (n, 16), dtype uint32.
#[pyfunction]
fn rs_mkcache<'py>(
    py: Python<'py>,
    seed: PyReadonlyArray1<'py, u8>,
    n: usize,
) -> Bound<'py, PyArray2<u32>> {
    let seed_slice = seed.as_slice().expect("seed must be contiguous");

    let mut cache = vec![0u32; n * R];

    // cache[0] = keccak512(seed)
    let h0 = keccak512_bytes(seed_slice);
    for i in 0..R {
        cache[i] = u32::from_le_bytes(h0[i * 4..i * 4 + 4].try_into().unwrap());
    }

    // cache[i] = keccak512(cache[i-1])
    for i in 1..n {
        let prev: [u32; 16] = cache[(i - 1) * R..i * R].try_into().unwrap();
        let h = keccak512_u32(&prev);
        cache[i * R..i * R + R].copy_from_slice(&h);
    }

    // CACHE_ROUNDS rounds of RandMemoHash
    for _ in 0..CACHE_ROUNDS {
        for i in 0..n {
            let v = cache[i * R] as usize % n;
            let prev_idx = (i + n - 1) % n;
            let mut xored = [0u32; 16];
            for k in 0..R {
                xored[k] = cache[prev_idx * R + k] ^ cache[v * R + k];
            }
            let h = keccak512_u32(&xored);
            cache[i * R..i * R + R].copy_from_slice(&h);
        }
    }

    let arr = Array2::from_shape_vec((n, R), cache).unwrap();
    arr.into_pyarray_bound(py)
}

/// In-place FNV parent mixing for one dataset item.
///
/// Mutates *mix* (uint32[16]) in place.
#[pyfunction]
fn mix_parents(
    mix_arr: Bound<'_, PyArray1<u32>>,
    cache: PyReadonlyArray2<'_, u32>,
    i: u64,
) -> PyResult<()> {
    let n = cache.shape()[0];
    let cache_slice = cache
        .as_slice()
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    // SAFETY: we hold the GIL; no other thread can alias this array.
    let mix_slice = unsafe { mix_arr.as_slice_mut() }
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    if mix_slice.len() != R {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "mix must have exactly 16 uint32 elements",
        ));
    }

    let mut mix: [u32; 16] = mix_slice.try_into().unwrap();
    mix_parents_inner(&mut mix, cache_slice, n, i as u32);
    mix_slice.copy_from_slice(&mix);
    Ok(())
}

/// Compute one dataset item from the cache. Returns uint32[16] ndarray.
#[pyfunction]
fn rs_calc_dataset_item<'py>(
    py: Python<'py>,
    cache: PyReadonlyArray2<'py, u32>,
    i: u32,
) -> Bound<'py, PyArray1<u32>> {
    let n = cache.shape()[0];
    let cache_slice = cache.as_slice().expect("cache must be C-contiguous");
    let result = calc_dataset_item_inner(cache_slice, n, i);
    PyArray1::from_slice_bound(py, &result)
}

/// Full Hashimoto light-client proof-of-work verification.
///
/// Returns dict: {b"mix digest": bytes(32), b"result": bytes(32)}
#[pyfunction]
fn rs_hashimoto_light<'py>(
    py: Python<'py>,
    full_size: usize,
    cache: PyReadonlyArray2<'py, u32>,
    header: PyReadonlyArray1<'py, u8>,
    nonce: PyReadonlyArray1<'py, u8>,
) -> PyResult<Bound<'py, PyDict>> {
    let n = full_size / 64; // full_size / HASH_BYTES
    let cache_n = cache.shape()[0];
    let cache_slice = cache
        .as_slice()
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let header_slice = header
        .as_slice()
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
    let nonce_slice = nonce
        .as_slice()
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;

    // seed = header + nonce[::-1]
    let mut seed = Vec::with_capacity(header_slice.len() + nonce_slice.len());
    seed.extend_from_slice(header_slice);
    seed.extend(nonce_slice.iter().rev().copied());

    // s = keccak512(seed)  →  16 u32 words
    let s_bytes = keccak512_bytes(&seed);
    let mut s = [0u32; 16];
    for i in 0..16 {
        s[i] = u32::from_le_bytes(s_bytes[i * 4..i * 4 + 4].try_into().unwrap());
    }

    // mix = [s, s]  (32 u32)
    let mut mix = [0u32; MIX_WORDS];
    mix[..16].copy_from_slice(&s);
    mix[16..].copy_from_slice(&s);

    let s0 = s[0];

    // 64 Hashimoto accesses
    for i in 0..ACCESSES {
        let p = (i as u32 ^ s0).wrapping_mul(FNV_PRIME) ^ mix[i % MIX_WORDS];
        let p = (p as usize % (n / MIX_HASHES)) * MIX_HASHES;

        let mut newdata = [0u32; MIX_WORDS];
        for j in 0..MIX_HASHES {
            let item = calc_dataset_item_inner(cache_slice, cache_n, (p + j) as u32);
            newdata[j * R..j * R + R].copy_from_slice(&item);
        }
        for k in 0..MIX_WORDS {
            mix[k] = mix[k].wrapping_mul(FNV_PRIME) ^ newdata[k];
        }
    }

    // Compress mix (32 u32) → cmix (8 u32) via FNV folding
    let mut cmix = [0u32; 8];
    for i in 0..8 {
        cmix[i] = mix[i * 4].wrapping_mul(FNV_PRIME) ^ mix[i * 4 + 1];
        cmix[i] = cmix[i].wrapping_mul(FNV_PRIME) ^ mix[i * 4 + 2];
        cmix[i] = cmix[i].wrapping_mul(FNV_PRIME) ^ mix[i * 4 + 3];
    }

    // result = keccak256(s_bytes ++ cmix_bytes)
    let mut s_cmix = [0u8; 96]; // 64 + 32
    s_cmix[..64].copy_from_slice(&s_bytes);
    for i in 0..8 {
        s_cmix[64 + i * 4..64 + i * 4 + 4].copy_from_slice(&cmix[i].to_le_bytes());
    }
    let result_hash = keccak256_bytes(&s_cmix);

    // mix_digest = cmix as little-endian bytes
    let mut mix_digest = [0u8; 32];
    for i in 0..8 {
        mix_digest[i * 4..i * 4 + 4].copy_from_slice(&cmix[i].to_le_bytes());
    }

    let d = PyDict::new_bound(py);
    d.set_item(
        PyBytes::new_bound(py, b"mix digest"),
        PyBytes::new_bound(py, &mix_digest),
    )?;
    d.set_item(
        PyBytes::new_bound(py, b"result"),
        PyBytes::new_bound(py, &result_hash),
    )?;
    Ok(d)
}

#[pymodule]
fn ethash_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rs_mkcache, m)?)?;
    m.add_function(wrap_pyfunction!(mix_parents, m)?)?;
    m.add_function(wrap_pyfunction!(rs_calc_dataset_item, m)?)?;
    m.add_function(wrap_pyfunction!(rs_hashimoto_light, m)?)?;
    Ok(())
}
