# Python 3.13 Upgrade Consensus Review Guide

[Chinese version](github-review.md)

This guide covers the review of these two commits. The main goal is to check whether the upgrade changes QuarkChain consensus behavior:

- [`pyquarkchain@f635479d`](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3): Python 3.13, Ethash, asyncio, JSON-RPC, and UPnP updates
- [`pyquarkchain@ba790a9a`](https://github.com/QuarkChain/pyquarkchain/commit/ba790a9a23148b00b9c0f52b32c6dc19eefbe7cd): replace `python-rocksdb` with `rocksdict`
- [`ethash@907b7d8`](https://github.com/QuarkChain/ethash/commit/907b7d8064d3be09536e754bbf469b442f2e213d): the `pyethash` C extension pinned in `requirements.txt`

## Review Goals

The review should answer three questions:

1. Does the same block get the same PoW, difficulty, and seal check result before and after the upgrade?
2. Does the same transaction or block get the same validity and state result before and after the upgrade?
3. Can two systems use different code, packages, or byte order and get different results?

Each reviewer does not need to read all 97 changed files. Choose a scope from P1, P2, or P3. For a code issue, post the file, line, permalink, and problem in the GitHub Issue. After the review, use the `Review Result` template in the Issue. State what you reviewed, what tests you ran, whether you approve, and whether the issue blocks the release.

## Review Levels

- P1: Review each file. Check that consensus results do not change.
- P2: Review by directory. Check that the node can connect, sync, submit blocks, and free resources. A line-by-line consensus check is not needed.
- P3: Tests, tools, CI, and other support files. These can usually be skipped unless a P1 or P2 test or build has a problem.

# P1: May Affect Consensus

## Project: pyquarkchain

Commit: [`f635479d`](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3)

### Directory: `ethereum/pow/`

#### File: `ethash_utils.py`

[View diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-f73cc0d1e271cf5db57437cec1bdfc303c2d30bd28392e0c966ca1f729450ace)

Main changes:

- `get_cache_size()` and `get_full_size()` now take an epoch instead of a block number and use an LRU cache. They are used by the production `pyethash` path.
- SHA3 input and output changed from Python `list[int]` values to little-endian NumPy `uint32` arrays.
- `isprime()` now uses `math.isqrt()` and adds boundary checks.

Consensus review points:

- Size results must match standard Ethash and `pyethash`.
- The other changes must not change the pure Python/NumPy result. The `isprime()` fix changes the old pure-Python cache size at epoch 520 from `84,934,592` to the standard value `84,934,336`.

#### File: `ethash.py`

[View diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-b3319c80fd7deb6f2713c9e4352ef42dd88bfcf1bc540db92e59312640f8f75d)

`pyethash` path:

- Uses `pyethash` to build the cache and uses `pyethash.hashimoto_light()` for Hashimoto by default.
- `_get_pyethash_cache()`: builds the cache with `pyethash` and keeps both a NumPy view and raw bytes to reduce conversion and copying.
- `mkcache_pyethash()`: uses the `pyethash` cache by default and falls back to Python for non-standard test sizes.
- `hashimoto_light_pyethash()`: calls `pyethash.hashimoto_light()` for Hashimoto.

Consensus review points:

- Production must use the pinned `pyethash` version.
- The same header, nonce, and epoch must return the same mix digest and final result as the old code.
- Code selection and cache changes must not change the result.

Pure `Python` path:

- Cache, dataset items, and mix values changed from Python lists to NumPy `uint32` arrays.
- FNV, dataset item, and Hashimoto code was rewritten with NumPy.
- This path is used when `pyethash` is not installed. Tests with a non-standard DAG size also use it.
- `set_ethash_lib()` is only used by tests to switch between Python and `pyethash`; it is not part of the production path.

Consensus review points:

- NumPy must follow the same rules and return the same result as the old pure-Python code. Standard inputs must also match `pyethash`.
- Cache and LRU changes may change speed, but must not change cache data.

#### File: `ethpow.py`

[View diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-5d5d623a89f40a8d70b39732e4332622e5566ffbc05dab18d592c1253d896938)

Changes:

- `check_pow()` and the miner now find the epoch before getting cache and full sizes.
- `ethpow.py` uses `ethash.mkcache()` and `ethash.hashimoto_light()` to call the Python/NumPy code or the `pyethash` C extension.
- `check_pow()` now uses an LRU cache. The PoW target formula did not change.

Review points:

- `check_pow()` checks block PoW. The same header hash, nonce, mixhash, difficulty, and `is_test` value must give the same result before and after the upgrade.
- A correct seal must pass. A changed header hash, nonce, or mixhash must fail.

### Directory: `quarkchain/evm/`

#### File: `messages.py`

[View diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-86002b55c71ac3de5e9baa62f48530739d2b838046925070d95f41ebee8af35c)

Two tuple assertions that were always true are now real assertions:

```python
assert tx.network_id == chain_config.ETH_CHAIN_ID
assert tx.eth_chain_id - BASE_ETH_CHAIN_ID - 1 == tx.from_chain_id
```

Review points:

- This code is in the EIP-155 transaction check path, so it may change which transactions are accepted.
- The normal mempool and block paths already check the same `network_id`. A valid config also has `ETH_CHAIN_ID = BASE_ETH_CHAIN_ID + chain_id + 1`. Check that valid transactions still work and bad transactions are rejected.

### Directory: `qkchash/`

#### File: `qkchash_llrb.h`

[View diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-04d4bb42e56d1323d9fdd9fba618d7ddb4c2c9470568487c0ef09ee48cfe8345)

The C++ move constructor syntax was fixed for new compilers. It should not change the tree or qkchash result.

Review point: qkchash is part of consensus hashing. The same fixed inputs must give the same result before and after the change. The library must also build and load with the new tools.

### Directory: repository root

#### File: `requirements.txt`

[View diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-4d7c51b1efe9043e44439a949dfd92e5827321b34082903477fd04876edb7552)

Changes:

- Pins `pyethash` to `QuarkChain/ethash@907b7d8`.
- Updates NumPy, eth-hash, eth-keys, rlp, rocksdict, cryptography, and other main packages.
- Replaces JSON-RPC and UPnP packages and changes some version ranges.

Review points:

- A new install must use the pinned `pyethash` commit. It must not silently use another wheel, an old version, or the pure-Python code.
- Signature recovery, encoding, hashing, and state reads must stay the same after the package updates.

> Note: The `update-requirements` branch will pin package versions to the versions in the tested Docker image. Package version ranges are not part of this review.

## Project: ethash

Commit: [`907b7d8`](https://github.com/QuarkChain/ethash/commit/907b7d8064d3be09536e754bbf469b442f2e213d)

This is the `pyethash` code pinned by `requirements.txt`, so review it with pyquarkchain P1. For a pyquarkchain node, focus on the two files used by Python light verification. The Go wrapper, benchmark, and full DAG tools need less review.

### Directory: `src/python/`

#### File: `core.c`

[View diff](https://github.com/QuarkChain/ethash/commit/907b7d8064d3be09536e754bbf469b442f2e213d#diff-1f2129a846b8bc01e68526303d548e2d91618bb4aa65db29671b367cca3e0637)

Changes:

- The `y#` buffer length changed from `int` to `Py_ssize_t` for the Python C API with `PY_SSIZE_T_CLEAN`.
- `cache_size` is changed to the `uint64_t` type used by the lower-level code.
- Cache cleanup now uses `ethash_light_delete()`. Checks and cleanup were added for allocation errors and temporary values.

Review points:

- The order and meaning of `hashimoto_light(block_number, cache, header, nonce)` arguments must not change. Return bytes must not change.
- `cache_size` comes from a Python bytes length. It is between 0 and `PY_SSIZE_T_MAX`, so changing it to `uint64_t` is safe on supported systems.
- The bytes returned by `mkcache_bytes()` must have the size from `ethash_get_cachesize()`. The data must not be cut off, freed too early, or used after it is freed.

### Directory: `src/libethash/`

#### File: `internal.c`

[View diff](https://github.com/QuarkChain/ethash/commit/907b7d8064d3be09536e754bbf469b442f2e213d#diff-03dbe64794ce31fc2e691e40600d91e8e369108d7a0c7237f069dfedf1bef14e)

Changes:

- `ethash_light_new()` now checks allocation errors.
- Full DAG mmap base, length, `msync()`, and `munmap()` handling were fixed.
- Light Hashimoto still gets its `uint64_t full_size` from the block number table.

Review points:

- Light verification cache, DAG size lookup, and Hashimoto rules must not change.
- Full DAG mmap changes must not change the data address passed to hashing.

> Note: The current size table covers epochs 0 through 2047, before block 61,440,000. The table must be extended for epoch 2048 and later. This is not an integer overflow issue in this change.

# P2: Review Needed, No Expected Consensus Change

## Project: pyquarkchain

### `quarkchain/db.py` and persistent database tests

Commit: [`ba790a9a`](https://github.com/QuarkChain/pyquarkchain/commit/ba790a9a23148b00b9c0f52b32c6dc19eefbe7cd)

The database dependency changed from `python-rocksdb` to `rocksdict`, with new persistent database tests. Check that the node can open, read, write, and close the database, and that existing data can still be read. This change does not change block checks or state rules.

### Directory: `quarkchain/cluster/`

Files:

- `cluster.py`, `jsonrpc.py`, `jsonrpc_server.py`, `master.py`, `miner.py`
- `prom.py`, `protocol.py`, `root_state.py`, `shard.py`, `shard_state.py`
- `simple_network.py`, `slave.py`, `subscription.py`, `tx_generator.py`

Asyncio and JSON-RPC APIs were updated. Background tasks are kept alive. Connection close, miner restart, `add_block_futures`, and sync cancellation were fixed. Check that the node does not hang, miss or repeat a block submit, or stop syncing. Block state changes and fork choice should not change.

### Directory: `quarkchain/p2p/`

Files:

- `auth.py`, `discovery.py`, `ecies.py`, `exceptions.py`, `nat.py`
- `p2p_manager.py`, `p2p_server.py`, `peer.py`, `service.py`
- `cancel_token/token.py`

Old loop arguments were removed. Cancellation, connection tasks, crypto APIs, and NAT/UPnP code were updated. Check handshake, identity checks, connection close, and reconnect behavior. Block validity rules are not part of this directory.

### Directory: top-level `quarkchain/` modules

Files: `protocol.py`, `jsonrpc_client.py`, `utils.py`

Connection handlers, RPC future cleanup, the JSON-RPC client, and event loop helpers were updated. Check errors, request cancellation, and cleanup so sync and transaction submit keep working.

## Project: ethash

### Directory: `src/libethash/`

Files: `io_posix.c`, `CMakeLists.txt`

POSIX mmap/I/O and C library build support were fixed. This does not directly change light verification rules. Check that the extension builds and that cleanup still works for full DAG users.

### Directory: repository root and `cmake/`

Files: `CMakeLists.txt`, `Makefile`, `build.sh`, `setup.py`, `cmake/modules/FindCryptoPP.cmake`

Build, link, and package steps were updated for Python 3.13 and new tools. Check that an install uses the expected Python C extension and links to the `libethash` code from this change.

# P3: Tests, Tools, CI, and Non-Production Code

## Project: pyquarkchain

### Tests, tools, experiments, CI, and docs

Includes:

- `ethereum/pow/tests/`
- `quarkchain/cluster/tests/`, `quarkchain/p2p/tests/`
- `quarkchain/tools/`, `quarkchain/cluster/tool_multi_cluster*.py`
- `quarkchain/p2p/tools/`, `quarkchain/experimental/ethash.py`
- `.github/`, `.gitignore`, `README.md`, `mainnet/singularity/Dockerfile`, `setup.py`

These changes cover test updates, benchmarks, tools, external mining, CI images, Docker, and install docs. They normally do not check blocks. They can be skipped unless P1 or P2 tests are missing, deployments differ, or the external miner gets a wrong target.

## Project: ethash

### Go wrapper, benchmarks, and tests

Includes:

- `compat.go`, `ethash.go`, `ethashc.go`, `go.mod`, `go.sum`
- `src/benchmark/`
- `test/`, `ethash_test.go`

These files are not used by the pyquarkchain node's `pyethash` path. They can help with cross-checks and build tests, but they do not need a line-by-line review for this work.

# Full Test

Unless a review finds a special case, run these three checks:

1. Run all Python tests: `python -m pytest`.
2. Start a node. Check that it connects, syncs blocks, and keeps running.
3. Start mining. Check that it gets work, submits results, and produces blocks.

If these checks miss a problem found during review, add a focused test and list it under `Tests` in the Issue.

# Final Check

## P1 Pass Rules

Before release, confirm that:

- Every P1 file was reviewed and has a result.
- All Python tests pass.
- A node can connect and sync blocks.
- Mining can get work, submit results, and produce blocks.
- The published Docker image uses the expected Python and package versions and the pinned `pyethash` commit.
- All consensus issues are fixed.

## Result

Choose one result:

- PASS: No unexpected consensus change was found. The release can continue.
- PASS WITH CONDITIONS: The release can continue after the listed conditions are met.
- BLOCK: A change may affect consensus. Do not release until it is fixed.
