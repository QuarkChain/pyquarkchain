# Python 3.13 Upgrade Consensus Review Guide

[English version](github-review-en.md)

本文用于组织以下两个提交的协作 review，目标是用最少的重复工作确认升级是否会影响 QuarkChain 共识：

- [`pyquarkchain@f635479d`](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3)：Python 3.13、Ethash、asyncio、JSON-RPC 和 UPnP 升级
- [`ethash@907b7d8`](https://github.com/QuarkChain/ethash/commit/907b7d8064d3be09536e754bbf469b442f2e213d)：`requirements.txt` 固定使用的 `pyethash` C 扩展

## Review 目标

本次 review 只需要回答以下三个问题：

1. 相同区块在升级前后是否得到相同的 PoW、difficulty 和 seal 验证结果？
2. 相同交易和区块在升级前后是否得到相同的有效性判断和状态转换结果？
3. 不同部署环境是否可能选择不同实现、依赖或字节序，从而产生不同结果？

不用每位 reviewer 都看完 97 个文件。请按本文的 P1、P2、P3 分类选择 review 范围。发现具体代码问题时，就在 Issue 中根据格式写明文件、行号、permalink 和问题。Review 完成后，按 Issue 中的 `Review Result` 模板说明看了哪些内容、跑了哪些测试、是否 approve，以及问题是否会阻塞发布。本文只说明要 review 什么，不记录认领人和进度。

## Review 方法

- P1：逐个文件看，确认修改前后的共识结果一致，并跑对应测试。
- P2：按目录看，重点检查节点能否正常连接、同步、提交区块和释放资源。这部分不用逐行对比共识结果。
- P3：测试、工具和 CI 等辅助文件。一般不用看；如果 P1/P2 的测试或构建有问题，再检查这里。

# P1：可能影响共识，优先 review

## 项目：pyquarkchain

提交：[`f635479d`](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3)

### 目录：`ethereum/pow/`

#### 文件：`ethash_utils.py`

[查看文件 diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-f73cc0d1e271cf5db57437cec1bdfc303c2d30bd28392e0c966ca1f729450ace)

主要修改：

- SHA3 输入输出由 Python `list[int]` 改为显式 little-endian NumPy `uint32` 数组。
- `get_cache_size()` 和 `get_full_size()` 的参数从 block number 改为 epoch。
- `isprime()` 改用 `math.isqrt()`，并补齐平方根边界和小于 2 的判断。
- size 计算增加 LRU cache。

共识关注点：

- cache/full size 必须与标准 Ethash 及 `pyethash` 完全一致。
- 调用方必须全部传 epoch，不能仍传 block number。
- little-endian 序列化必须与旧实现和 C 实现一致。
- `isprime()` 修复会使旧纯 Python fallback 在 epoch 520 的 cache size 从 `84,934,592` 变为标准值 `84,934,336`；需要确认线上旧节点实际使用的实现。


#### 文件：`ethash.py`

[查看文件 diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-b3319c80fd7deb6f2713c9e4352ef42dd88bfcf1bc540db92e59312640f8f75d)

主要修改：

- cache、dataset item 和 mix 从 Python list 重写为 NumPy `uint32` 数组。
- 使用 NumPy 重写 FNV、dataset item 和 Hashimoto 的核心计算，计算规则应与原实现保持一致。
- 增加 Python/`pyethash` 后端调度和运行时切换。
- 默认使用 `pyethash.hashimoto_light()`，使用非标准 DAG size 的测试场景回退到 Python 实现。
- 调用 `pyethash` 时，将节点使用的 8 字节 nonce 转换为 C 扩展要求的整数格式，nonce 的值保持不变。

共识关注点：

- 对相同的 header、nonce 和 epoch，NumPy 实现、`pyethash` 和修改前的实现必须产生相同的 mix digest 和最终结果。
- 后端自动选择不能让不同机器计算出不同结果。
- cache 切换和 LRU 行为只能影响性能，不能改变 cache 内容。


#### 文件：`ethpow.py`

[查看文件 diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-5d5d623a89f40a8d70b39732e4332622e5566ffbc05dab18d592c1253d896938)

主要修改：

- `check_pow()` 和 miner 统一先计算 epoch，再取得 cache/full size。
- `ethpow.py` 统一通过 `ethash.mkcache()` 和 `ethash.hashimoto_light()` 调用纯 Python/NumPy 实现或 `pyethash` C 扩展。
- `check_pow()` 增加 LRU cache，PoW target 公式保持不变。

共识关注点：

- `check_pow()` 是区块 PoW 验证入口。相同的 header hash、nonce、mixhash、difficulty 和 `is_test`，升级前后必须得到相同的验证结果。
- 正确的 seal 必须通过；修改 header hash、nonce 或 mixhash 后必须验证失败。


### 目录：`quarkchain/evm/`

#### 文件：`messages.py`

[查看文件 diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-86002b55c71ac3de5e9baa62f48530739d2b838046925070d95f41ebee8af35c)

主要修改：把两个始终为真的 tuple assert 修复为真实断言：

```python
assert tx.network_id == chain_config.ETH_CHAIN_ID
assert tx.eth_chain_id - BASE_ETH_CHAIN_ID - 1 == tx.from_chain_id
```

共识关注点：

- 代码位于 EIP-155 交易有效性检查路径，理论上可能改变有效交易集合。
- 正常 mempool 和区块执行路径会先检查同一 `network_id`，有效配置也保证 `ETH_CHAIN_ID = BASE_ETH_CHAIN_ID + chain_id + 1`；需要通过交易入口和恶意区块入口验证等价性。

### 目录：`qkchash/`

#### 文件：`qkchash_llrb.h`

[查看文件 diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-04d4bb42e56d1323d9fdd9fba618d7ddb4c2c9470568487c0ef09ee48cfe8345)

主要修改：修正 C++ move constructor 声明语法，预期只影响新编译器兼容性，不改变树结构或 qkchash 输出。

共识关注点：qkchash 属于共识哈希实现。需要确认修改前后固定输入向量一致，并确认使用新工具链能够构建和加载动态库。


### 目录：仓库根目录

#### 文件：`requirements.txt`

[查看文件 diff](https://github.com/QuarkChain/pyquarkchain/commit/f635479d08238b35c67d4da9e1eadd132be7d4b3#diff-4d7c51b1efe9043e44439a949dfd92e5827321b34082903477fd04876edb7552)

主要修改：

- 将 `pyethash` 固定到 `QuarkChain/ethash@907b7d8`。
- 升级 NumPy、eth-hash、eth-keys、rlp、rocksdict、cryptography 等核心依赖。
- 替换 JSON-RPC 和 UPnP 相关依赖，并扩大部分版本范围。

共识关注点：

- 新环境必须确实安装固定的 `pyethash` commit，不能静默使用不同 wheel、旧版本或纯 Python fallback。
- 密码学、RLP、数据库和 NumPy 依赖升级后，签名恢复、序列化、哈希和状态读取结果必须保持一致。

> 注：依赖版本范围将在 `update-requirements` 分支中按已验证的 Docker 镜像版本精确固定，不属于本次 review 范围。


## 项目：ethash

提交：[`907b7d8`](https://github.com/QuarkChain/ethash/commit/907b7d8064d3be09536e754bbf469b442f2e213d)

该提交是 `requirements.txt` 固定的 `pyethash` 实现，因此必须与 pyquarkchain 的 P1 一起 review。针对 pyquarkchain 节点，只需优先检查 Python light verification 实际调用的两个文件；Go wrapper、benchmark 和完整 DAG 工具不需要按同等深度 review。

### 目录：`src/python/`

#### 文件：`core.c`

[查看文件 diff](https://github.com/QuarkChain/ethash/commit/907b7d8064d3be09536e754bbf469b442f2e213d#diff-1f2129a846b8bc01e68526303d548e2d91618bb4aa65db29671b367cca3e0637)

主要修改：

- `y#` buffer 长度参数从 `int` 改为 `Py_ssize_t`，匹配 `PY_SSIZE_T_CLEAN` 下的 Python C API。
- `cache_size` 显式转换为底层结构要求的 `uint64_t`。
- cache 释放改用 `ethash_light_delete()`，并补充 OOM、临时结构释放和返回值类型转换。

共识关注点：

- `hashimoto_light(block_number, cache, header, nonce)` 的参数顺序、长度、nonce 类型和返回 bytes 必须不变。
- `cache_size` 来自 Python bytes 长度，满足 `0 <= cache_size <= PY_SSIZE_T_MAX`；转换到 `uint64_t` 在支持范围内不会溢出。
- `mkcache_bytes()` 返回长度必须与 `ethash_get_cachesize()` 一致，不能发生截断、借用内存失效或释放后访问。


### 目录：`src/libethash/`

#### 文件：`internal.c`

[查看文件 diff](https://github.com/QuarkChain/ethash/commit/907b7d8064d3be09536e754bbf469b442f2e213d#diff-03dbe64794ce31fc2e691e40600d91e8e369108d7a0c7237f069dfedf1bef14e)

主要修改：

- `ethash_light_new()` 增加分配失败检查。
- 修正完整 DAG mmap 的基址、长度、`msync()` 和 `munmap()` 处理。
- light Hashimoto 仍通过 block number 查表取得 `uint64_t full_size`。

共识关注点：

- 确认 light verification 的 cache、DAG size 查表和 Hashimoto 计算没有算法变化。
- mmap 修改主要影响完整 DAG 生命周期，但仍需确认没有改变传给 hashing 的数据起始地址。

> 注：当前 size 表覆盖 epoch 0-2047，即区块高度 61,440,000 之前。epoch 2048 之后需要扩展 size 表，与本次修改的整数溢出无关。

# P2：需要 review，但预计不改变共识规则

## 项目：pyquarkchain

### 目录：`quarkchain/cluster/`

文件：

- `cluster.py`, `jsonrpc.py`, `jsonrpc_server.py`, `master.py`, `miner.py`
- `prom.py`, `protocol.py`, `root_state.py`, `shard.py`, `shard_state.py`
- `simple_network.py`, `slave.py`, `subscription.py`, `tx_generator.py`

目录说明：升级 asyncio 和 JSON-RPC API，保存后台 task 强引用，修复连接关闭、miner restart、`add_block_futures` 以及批量同步取消清理。重点确认节点不会卡死、遗漏区块提交、重复提交或停止同步；成功执行一个区块时的状态转换和 fork choice 规则没有修改。


### 目录：`quarkchain/p2p/`

文件：

- `auth.py`, `discovery.py`, `ecies.py`, `exceptions.py`, `nat.py`
- `p2p_manager.py`, `p2p_server.py`, `peer.py`, `service.py`
- `cancel_token/token.py`

目录说明：移除废弃的 loop 参数，升级取消机制、连接任务和密码学 API，并用 `async-upnp-client` 重写 NAT/UPnP。这里主要检查握手、身份验证、连接关闭和断线重连是否正常，不需要检查区块有效性规则。


### 目录：`quarkchain/` 根模块

文件：`protocol.py`, `jsonrpc_client.py`, `utils.py`

目录说明：升级通用连接 handler、RPC future 清理、JSON-RPC client 和事件循环辅助代码。重点检查异常传播、请求取消和资源释放，避免影响同步与交易提交可用性。


## 项目：ethash

### 目录：`src/libethash/`

文件：`io_posix.c`, `CMakeLists.txt`

目录说明：修正 POSIX mmap/I/O 和 C 库构建兼容性。对 pyquarkchain 的 light verification 不直接改变算法，但需要确认扩展可稳定构建，并且资源清理修改没有破坏完整 DAG 用户。


### 目录：仓库根目录与 `cmake/`

文件：`CMakeLists.txt`, `Makefile`, `build.sh`, `setup.py`, `cmake/modules/FindCryptoPP.cmake`

目录说明：适配 Python 3.13 和新工具链的构建、链接及打包流程。重点确认安装后使用的是预期的 Python C 扩展，并且链接的是本次修改对应的 `libethash` 代码。


# P3：测试、工具、CI 和非生产代码，可最后 review

## 项目：pyquarkchain

### 目录：测试、工具、实验、CI 和文档

包含：

- `ethereum/pow/tests/`
- `quarkchain/cluster/tests/`, `quarkchain/p2p/tests/`
- `quarkchain/tools/`, `quarkchain/cluster/tool_multi_cluster*.py`
- `quarkchain/p2p/tools/`, `quarkchain/experimental/ethash.py`
- `.github/`, `.gitignore`, `README.md`, `mainnet/singularity/Dockerfile`, `setup.py`

这些是测试迁移、benchmark、运维工具、external miner、CI 镜像、Docker 和安装文档的修改，通常不会参与节点的区块验证。一般可以跳过；如果 P1/P2 的测试覆盖不足、部署结果不一致，或 external miner 的目标值异常，再检查这些文件。

## 项目：ethash

### 目录：Go wrapper、benchmark 和测试

包含：

- `compat.go`, `ethash.go`, `ethashc.go`, `go.mod`, `go.sum`
- `src/benchmark/`
- `test/`, `ethash_test.go`

这些文件不在 pyquarkchain 节点调用 `pyethash` 的路径中。可以用它们做交叉验证或构建测试，但本次不要求逐行 review。

# 整体验证

没有特殊情况时，不需要按文件单独设计测试。完成下面三项即可：

1. 运行全部 Python 测试：`python -m pytest`。
2. 启动节点，确认可以正常连接、同步区块并持续运行。
3. 启动挖矿，确认可以正常获取任务、提交结果并产出区块。

如果 review 中发现以上测试没有覆盖到的问题，再补充针对性测试，并在 Issue 的 `Tests` 中说明。

# 最终验收

## P1 通过条件

发布前需要确认：

- 所有 P1 文件都有人 review 并给出结论。
- 全部 Python 测试通过。
- 节点可以正常连接并同步区块。
- 挖矿可以正常获取任务、提交结果并产出区块。
- 已确认发布的 Docker 镜像使用预期的 Python 和依赖版本，并安装了指定 commit 的 `pyethash`。
- 所有的共识问题都已经解决。

## 结论

最终结论从下面三项中选择：

- PASS：未发现非预期共识变化，可以发布。
- PASS WITH CONDITIONS：存在部署约束，满足列出的条件后可以发布。
- BLOCK：发现可能改变共识的行为，需要阻塞发布。
