import asyncio
import copy
import random
import time
import json
from abc import ABC, abstractmethod

import numpy
from aioprocessing import AioProcess, AioQueue
from queue import Queue

from typing import Callable, Union, Awaitable, Dict, Any, Optional, NamedTuple

from ethereum.pow.ethpow import EthashMiner, check_pow
from quarkchain.config import ConsensusType
from quarkchain.core import MinorBlock, RootBlock, RootBlockHeader, MinorBlockHeader
from quarkchain.utils import time_ms, Logger, sha3_256

Block = Union[MinorBlock, RootBlock]


def validate_seal(
    block_header: Union[RootBlockHeader, MinorBlockHeader],
    consensus_type: ConsensusType,
    adjusted_diff: int = None,  # for overriding
) -> None:
    diff = adjusted_diff if adjusted_diff is not None else block_header.difficulty
    if consensus_type == ConsensusType.POW_ETHASH:
        nonce_bytes = block_header.nonce.to_bytes(8, byteorder="big")
        if not check_pow(
            block_header.height,
            block_header.get_hash_for_mining(),
            block_header.mixhash,
            nonce_bytes,
            diff,
        ):
            raise ValueError("invalid pow proof")
    elif consensus_type == ConsensusType.POW_SHA3SHA3:
        nonce_bytes = block_header.nonce.to_bytes(8, byteorder="big")
        target = (2 ** 256 // (diff or 1) - 1).to_bytes(32, byteorder="big")
        h = sha3_256(sha3_256(block_header.get_hash_for_mining() + nonce_bytes))
        if not h < target:
            raise ValueError("invalid pow proof")


MiningWork = NamedTuple(
    "MiningWork", [("hash", bytes), ("height", int), ("difficulty", int)]
)

MiningResult = NamedTuple(
    "MiningResult", [("header_hash", bytes), ("nonce", int), ("mixhash", bytes)]
)


class MiningAlgorithm(ABC):
    @abstractmethod
    def mine(self, start_nonce: int, end_nonce: int) -> Optional[MiningResult]:
        pass


class Simulate(MiningAlgorithm):
    def __init__(self, work: MiningWork, **kwargs):
        self.target_time = kwargs["target_time"]
        self.work = work

    def mine(self, start_nonce: int, end_nonce: int) -> Optional[MiningResult]:
        time.sleep(0.1)
        if time.time() > self.target_time:
            return MiningResult(
                self.work.hash, random.randint(0, 2 ** 32 - 1), bytes(32)
            )
        return None


class Ethash(MiningAlgorithm):
    def __init__(self, work: MiningWork, **kwargs):
        is_test = kwargs.get("is_test", False)
        self.miner = EthashMiner(
            work.height, work.difficulty, work.hash, is_test=is_test
        )

    def mine(self, start_nonce: int, end_nonce: int) -> Optional[MiningResult]:
        nonce_found, mixhash = self.miner.mine(end_nonce - start_nonce, start_nonce)
        if not nonce_found:
            return None
        return MiningResult(
            self.miner.header_hash,
            int.from_bytes(nonce_found, byteorder="big"),
            mixhash,
        )


class DoubleSHA256(MiningAlgorithm):
    def __init__(self, work: MiningWork, **kwargs):
        self.target = (2 ** 256 // (work.difficulty or 1) - 1).to_bytes(
            32, byteorder="big"
        )
        self.header_hash = work.hash

    def mine(self, start_nonce: int, end_nonce: int) -> Optional[MiningResult]:
        for nonce in range(start_nonce, end_nonce):
            nonce_bytes = nonce.to_bytes(8, byteorder="big")
            h = sha3_256(sha3_256(self.header_hash + nonce_bytes))
            if h < self.target:
                return MiningResult(self.header_hash, nonce, bytes(32))
        return None


class Miner:
    def __init__(
        self,
        consensus_type: ConsensusType,
        create_block_async_func: Callable[[], Awaitable[Optional[Block]]],
        add_block_async_func: Callable[[Block], Awaitable[None]],
        get_mining_param_func: Callable[[], Dict[str, Any]],
        remote: bool = False,
    ):
        """Mining will happen on a subprocess managed by this class

        create_block_async_func: takes no argument, returns a block (either RootBlock or MinorBlock)
        add_block_async_func: takes a block, add it to chain
        get_mining_param_func: takes no argument, returns the mining-specific params
        """
        self.consensus_type = consensus_type

        self.create_block_async_func = create_block_async_func
        self.add_block_async_func = add_block_async_func
        self.get_mining_param_func = get_mining_param_func
        self.enabled = False
        self.process = None

        self.input_q = AioQueue()  # [(MiningWork, param dict)]
        self.output_q = AioQueue()  # [MiningResult]

        # header hash -> work
        self.work_map = {}  # type: Dict[bytes, Block]

        if not remote and consensus_type != ConsensusType.POW_SIMULATE:
            Logger.warning("Mining locally, could be slow and error-prone")
        # remote miner specific attributes
        self.remote = remote
        self.current_work = None  # type: Optional[Block]

    def start(self):
        self.enabled = True
        self._mine_new_block_async()

    def is_enabled(self):
        return self.enabled

    def disable(self):
        """Stop the mining process if there is one"""
        if self.enabled and self.process:
            # end the mining process
            self.input_q.put((None, {}))
        self.enabled = False

    def _mine_new_block_async(self):
        async def handle_mined_block():
            while True:
                res = await self.output_q.coro_get()  # type: MiningResult
                if not res:
                    return  # empty result means ending
                # start mining before processing and propagating mined block
                self._mine_new_block_async()
                block = self.work_map[res.header_hash]
                block.header.nonce = res.nonce
                block.header.mixhash = res.mixhash
                del self.work_map[res.header_hash]
                self._track(block)
                try:
                    # FIXME: Root block should include latest minor block headers while it's being mined
                    # This is a hack to get the latest minor block included since testnet does not check difficulty
                    if self.consensus_type == ConsensusType.POW_SIMULATE:
                        block = await self.create_block_async_func()
                        block.header.nonce = random.randint(0, 2 ** 32 - 1)
                        self._track(block)
                        self._log_status(block)
                    await self.add_block_async_func(block)
                except Exception as ex:
                    Logger.error(ex)

        async def mine_new_block():
            """Get a new block and start mining.
            If a mining process has already been started, update the process to mine the new block.
            """
            block = await self.create_block_async_func()
            if not block:
                self.input_q.put((None, {}))
                return
            mining_params = self.get_mining_param_func()
            mining_params["consensus_type"] = self.consensus_type
            # handle mining simulation's timing
            if "target_block_time" in mining_params:
                target_block_time = mining_params["target_block_time"]
                mining_params["target_time"] = (
                    block.header.create_time
                    + self._get_block_time(block, target_block_time)
                )
            work = MiningWork(
                block.header.get_hash_for_mining(),
                block.header.height,
                block.header.difficulty,
            )
            self.work_map[work.hash] = block
            if self.process:
                self.input_q.put((work, mining_params))
                return

            self.process = AioProcess(
                target=self.mine_loop,
                args=(work, mining_params, self.input_q, self.output_q),
            )
            self.process.start()
            await handle_mined_block()

        # no-op if enabled or mining remotely
        if not self.enabled or self.remote:
            return None
        return asyncio.ensure_future(mine_new_block())

    async def get_work(self, now=None) -> MiningWork:
        if not self.remote:
            raise ValueError("Should only be used for remote miner")

        if now is None:  # clock open for mock
            now = time.time()
        # 5 sec interval magic number
        if not self.current_work or now - self.current_work.header.create_time > 5:
            block = await self.create_block_async_func()
            if not block:
                raise RuntimeError("Failed to create block")
            self.current_work = block

        header = self.current_work.header
        header_hash = header.get_hash_for_mining()
        # store in memory for future retrieval during work submission
        self.work_map[header_hash] = self.current_work

        # clean up worker map
        # TODO: for now, same param as go-ethereum
        self.work_map = {
            h: b
            for h, b in self.work_map.items()
            if now - b.header.create_time < 7 * 12
        }

        return MiningWork(header_hash, header.height, header.difficulty)

    async def submit_work(self, header_hash: bytes, nonce: int, mixhash: bytes) -> bool:
        if not self.remote:
            raise ValueError("Should only be used for remote miner")

        if header_hash not in self.work_map:
            return False
        block = self.work_map[header_hash]
        header = copy.copy(block.header)
        header.nonce, header.mixhash = nonce, mixhash
        try:
            validate_seal(header, self.consensus_type)
        except ValueError:
            return False

        block.header = header  # actual update
        try:
            await self.add_block_async_func(block)
            del self.work_map[header_hash]
            self.current_work = None
            return True
        except Exception as ex:
            Logger.error(ex)
            return False

    @staticmethod
    def mine_loop(
        work: Optional[MiningWork], mining_params: Dict, input_q: Queue, output_q: Queue
    ):
        consensus_to_mining_algo = {
            ConsensusType.POW_SIMULATE: Simulate,
            ConsensusType.POW_ETHASH: Ethash,
            ConsensusType.POW_SHA3SHA3: DoubleSHA256,
        }
        # TODO: maybe add rounds to config json
        rounds = mining_params.get("rounds", 1000)
        progress = {}
        # outer loop for mining forever
        while True:
            # empty work means termination
            if not work:
                output_q.put(None)
                return

            consensus_type = mining_params["consensus_type"]
            mining_algo_gen = consensus_to_mining_algo[consensus_type]
            mining_algo = mining_algo_gen(work, **mining_params)
            # progress tracking if mining param contains shard info
            if "shard" in mining_params:
                shard = mining_params["shard"]
                # skip blocks with height lower or equal
                if shard in progress and progress[shard] >= work.height:
                    # get newer work and restart mining
                    work, mining_params = input_q.get(block=True)
                    continue
            # inner loop for iterating nonce
            start_nonce = 0
            while True:
                res = mining_algo.mine(start_nonce + 1, start_nonce + 1 + rounds)
                if res:
                    output_q.put(res)
                    if "shard" in mining_params:
                        progress[mining_params["shard"]] = work.height
                    work, mining_params = input_q.get(block=True)
                    break  # break inner loop to refresh mining params
                # no result for mining, check if new work arrives
                # if yes, discard current work and restart
                try:
                    work, mining_params = input_q.get_nowait()
                    break  # break inner loop to refresh mining params
                except Exception:  # queue empty
                    pass
                # update param and keep mining
                start_nonce += rounds

    @staticmethod
    def _track(block: Block):
        """Post-process block to track block propagation latency"""
        tracking_data = json.loads(block.tracking_data.decode("utf-8"))
        tracking_data["mined"] = time_ms()
        block.tracking_data = json.dumps(tracking_data).encode("utf-8")

    @staticmethod
    def _log_status(block: Block):
        is_root = isinstance(block, RootBlock)
        shard = "R" if is_root else block.header.branch.get_shard_id()
        count = len(block.minor_block_header_list) if is_root else len(block.tx_list)
        elapsed = time.time() - block.header.create_time
        Logger.info_every_sec(
            "[{}] {} [{}] ({:.2f}) {}".format(
                shard,
                block.header.height,
                count,
                elapsed,
                block.header.get_hash().hex(),
            ),
            60,
        )

    @staticmethod
    def _get_block_time(block: Block, target_block_time) -> float:
        if isinstance(block, MinorBlock):
            # Adjust the target block time to compensate computation time
            gas_used_ratio = block.meta.evm_gas_used / block.header.evm_gas_limit
            target_block_time = target_block_time * (1 - gas_used_ratio * 0.4)
            Logger.debug(
                "[{}] target block time {:.2f}".format(
                    block.header.branch.get_shard_id(), target_block_time
                )
            )
        return numpy.random.exponential(target_block_time)
