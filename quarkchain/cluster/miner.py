import asyncio
import random
import time
import json
from abc import ABC, abstractmethod

import numpy
from aioprocessing import AioProcess, AioQueue
from multiprocessing import Queue as MultiProcessingQueue

from typing import Callable, Union, Awaitable, Dict, Any

from ethereum.pow.ethpow import EthashMiner, check_pow
from quarkchain.config import ConsensusType
from quarkchain.core import MinorBlock, RootBlock, RootBlockHeader, MinorBlockHeader
from quarkchain.utils import time_ms, Logger, sha3_256


def validate_seal(
    block_header: Union[RootBlockHeader, MinorBlockHeader],
    consensus_type: ConsensusType,
) -> None:
    if consensus_type == ConsensusType.POW_ETHASH:
        nonce_bytes = block_header.nonce.to_bytes(8, byteorder="big")
        if not check_pow(
            block_header.height,
            block_header.get_hash_for_mining(),
            block_header.mixhash,
            nonce_bytes,
            block_header.difficulty,
        ):
            raise ValueError("invalid pow proof")
    elif consensus_type == ConsensusType.POW_SHA3SHA3:
        nonce_bytes = block_header.nonce.to_bytes(8, byteorder="big")
        target = (2 ** 256 // (block_header.difficulty or 1) - 1).to_bytes(
            32, byteorder="big"
        )
        h = sha3_256(sha3_256(block_header.get_hash_for_mining() + nonce_bytes))
        if not h < target:
            raise ValueError("invalid pow proof")


class MiningAlgorithm(ABC):
    @abstractmethod
    def mine(self, start_nonce: int, end_nonce: int) -> bool:
        pass

    def post_process_mined_block(self, block: Union[MinorBlock, RootBlock]):
        """Post-process block to track block propagation latency"""
        tracking_data = json.loads(block.tracking_data.decode("utf-8"))
        tracking_data["mined"] = time_ms()
        block.tracking_data = json.dumps(tracking_data).encode("utf-8")

    @staticmethod
    def _log_status(block: Union[RootBlock, MinorBlock]):
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


class Simulate(MiningAlgorithm):
    def __init__(self, block: Union[MinorBlock, RootBlock], **kwargs):
        target_block_time = kwargs["target_block_time"]
        self.target_time = block.header.create_time + self._get_block_time(
            block, target_block_time
        )

    @staticmethod
    def _get_block_time(block, target_block_time) -> float:
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

    def mine(self, start_nonce: int, end_nonce: int) -> bool:
        time.sleep(0.1)
        return time.time() > self.target_time

    def post_process_mined_block(self, block: Union[MinorBlock, RootBlock]):
        self._log_status(block)
        block.header.nonce = random.randint(0, 2 ** 32 - 1)
        super().post_process_mined_block(block)


class Ethash(MiningAlgorithm):
    def __init__(self, block: Union[MinorBlock, RootBlock], **kwargs):
        is_test = kwargs.get("is_test", False)
        self.miner = EthashMiner(
            block.header.height,
            block.header.difficulty,
            block.header.get_hash_for_mining(),
            is_test=is_test,
        )
        self.nonce_found, self.mixhash = None, None

    def mine(self, start_nonce: int, end_nonce: int) -> bool:
        nonce_found, mixhash = self.miner.mine(end_nonce - start_nonce, start_nonce)
        if not nonce_found:
            return False
        self.nonce_found = nonce_found
        self.mixhash = mixhash
        return True

    def post_process_mined_block(self, block: Union[MinorBlock, RootBlock]):
        if not self.nonce_found:
            raise RuntimeError("cannot post process since no nonce found")
        block.header.nonce = int.from_bytes(self.nonce_found, byteorder="big")
        block.header.mixhash = self.mixhash
        super().post_process_mined_block(block)


class DoubleSHA256(MiningAlgorithm):
    def __init__(self, block: Union[MinorBlock, RootBlock]):
        self.target = (2 ** 256 // (block.header.difficulty or 1) - 1).to_bytes(
            32, byteorder="big"
        )
        self.header_hash = block.header.get_hash_for_mining()
        self.nonce_found = None

    def mine(self, start_nonce: int, end_nonce: int) -> bool:
        for nonce in range(start_nonce, end_nonce):
            nonce_bytes = nonce.to_bytes(8, byteorder="big")
            h = sha3_256(sha3_256(self.header_hash + nonce_bytes))
            if h < self.target:
                self.nonce_found = nonce_bytes
                return True
        return False

    def post_process_mined_block(self, block: Union[MinorBlock, RootBlock]):
        block.header.nonce = int.from_bytes(self.nonce_found, byteorder="big")
        super().post_process_mined_block(block)


class Miner:
    def __init__(
        self,
        consensus_type: ConsensusType,
        create_block_async_func: Callable[[], Awaitable[Union[MinorBlock, RootBlock]]],
        add_block_async_func: Callable[[Union[MinorBlock, RootBlock]], Awaitable[None]],
        get_mining_param_func: Callable[[], Dict[str, Any]],
    ):
        """Mining will happen on a subprocess managed by this class

        create_block_async_func: takes no argument, returns a block (either RootBlock or MinorBlock)
        add_block_async_func: takes a block, add it to chain
        get_mining_param_func: takes no argument, returns the mining-specific params
        """
        if consensus_type == ConsensusType.POW_SIMULATE:
            self.mine_func = Miner.simulate_mine
        elif consensus_type == ConsensusType.POW_ETHASH:
            self.mine_func = Miner.mine_ethash
        elif consensus_type == ConsensusType.POW_SHA3SHA3:
            self.mine_func = Miner.mine_sha3sha3
        else:
            raise ValueError("Consensus? (╯°□°）╯︵ ┻━┻")

        self.create_block_async_func = create_block_async_func
        self.add_block_async_func = add_block_async_func
        self.get_mining_param_func = get_mining_param_func
        self.enabled = False
        self.process = None

        self.input_q = AioQueue()  # [(block, param dict)]
        self.output_q = AioQueue()  # [block]

    def start(self):
        self.enable()
        self._mine_new_block_async()

    def is_enabled(self):
        return self.enabled

    def enable(self):
        self.enabled = True

    def disable(self):
        """Stop the mining process if there is one"""
        if self.enabled and self.process:
            # end the mining process
            self.input_q.put((None, {}))
        self.enabled = False

    def _mine_new_block_async(self):
        async def handle_mined_block(instance: Miner):
            while True:
                block = await instance.output_q.coro_get()
                if not block:
                    return
                # start mining before processing and propagating mined block
                instance._mine_new_block_async()
                try:
                    await instance.add_block_async_func(block)
                except Exception as ex:
                    Logger.error(ex)

        async def mine_new_block(instance: Miner):
            """Get a new block and start mining.
            If a mining process has already been started, update the process to mine the new block.
            """
            block = await instance.create_block_async_func()
            mining_params = instance.get_mining_param_func()
            if instance.process:
                instance.input_q.put((block, mining_params))
                return

            instance.process = AioProcess(
                target=instance.mine_func,
                args=(block, instance.input_q, instance.output_q, mining_params),
            )
            instance.process.start()
            await handle_mined_block(instance)

        if not self.enabled:
            return None
        return asyncio.ensure_future(mine_new_block(self))

    @staticmethod
    def simulate_mine(
        block,
        input_q: MultiProcessingQueue,
        output_q: MultiProcessingQueue,
        mining_params: Dict,
    ):
        Miner._mine_loop(block, input_q, output_q, mining_params, Simulate)

    @staticmethod
    def mine_ethash(
        block: Union[MinorBlock, RootBlock],
        input_q: MultiProcessingQueue,
        output_q: MultiProcessingQueue,
        mining_params: Dict,
    ):
        Miner._mine_loop(block, input_q, output_q, mining_params, Ethash)

    @staticmethod
    def mine_sha3sha3(
        block: Union[MinorBlock, RootBlock],
        input_q: MultiProcessingQueue,
        output_q: MultiProcessingQueue,
        mining_params: Dict,
    ):
        Miner._mine_loop(block, input_q, output_q, mining_params, DoubleSHA256)

    @staticmethod
    def _mine_loop(
        block: Union[MinorBlock, RootBlock],
        input_q: MultiProcessingQueue,
        output_q: MultiProcessingQueue,
        mining_params: Dict,
        mining_algo_gen: Callable[..., MiningAlgorithm],
    ):
        # TODO: maybe add rounds to config json
        rounds = mining_params.get("rounds", 100)
        # outer loop for mining forever
        while True:
            # `None` block means termination
            if not block:
                output_q.put(None)
                return

            start_nonce = 0
            mining_algo = mining_algo_gen(block, **mining_params)
            # inner loop for iterating nonce
            while True:
                found = mining_algo.mine(start_nonce + 1, start_nonce + 1 + rounds)
                if found:
                    mining_algo.post_process_mined_block(block)
                    output_q.put(block)
                    block, _ = input_q.get(block=True)  # blocking
                    break  # break inner loop to refresh mining params
                # check if new block arrives. if yes, discard current progress and restart
                try:
                    block, _ = input_q.get_nowait()
                    break  # break inner loop to refresh mining params
                except Exception:  # queue empty
                    pass
                # update param and keep mining
                start_nonce += rounds
