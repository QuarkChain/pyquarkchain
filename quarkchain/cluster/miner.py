import asyncio
import random
import time
import json

import numpy
from aioprocessing import AioProcess, AioQueue
from multiprocessing import Queue as MultiProcessingQueue

from typing import Callable, Union, Awaitable, Dict, Any

from ethereum.pow.ethpow import EthashMiner, check_pow
from quarkchain.env import DEFAULT_ENV
from quarkchain.config import NetworkId, ConsensusType
from quarkchain.core import MinorBlock, RootBlock, RootBlockHeader, MinorBlockHeader
from quarkchain.utils import time_ms, Logger


def validate_seal(
    block_header: Union[RootBlockHeader, MinorBlockHeader],
    consensus_type: ConsensusType,
) -> None:
    if consensus_type == ConsensusType.POW_ETHASH:
        nonce_bytes = block_header.nonce.to_bytes(8, byteorder="big")
        if not check_pow(
            block_header.height,
            block_header.get_hash(),
            block_header.mixhash,
            nonce_bytes,
            block_header.difficulty,
        ):
            raise ValueError("invalid pow proof")

    return


class Miner:
    def __init__(
        self,
        consensus_type: ConsensusType,
        create_block_async_func: Callable[[], Awaitable[Union[MinorBlock, RootBlock]]],
        add_block_async_func: Callable[[Union[MinorBlock, RootBlock]], Awaitable[None]],
        get_mining_param_func: Callable[[], Dict[str, Any]],
        # TODO: clean this up if confirmed not used
        env,
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
        self.env = env

        self.input_q = AioQueue()  # [(block, target_time)]
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
            return
        return asyncio.ensure_future(mine_new_block(self))

    @staticmethod
    def __log_status(block):
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
    def __check_metric(metric):
        # Testnet does not check difficulty
        if DEFAULT_ENV.config.NETWORK_ID != NetworkId.MAINNET:
            return True
        return metric < 2 ** 256

    @staticmethod
    def __get_block_time(block, target_block_time) -> float:
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

    @staticmethod
    def simulate_mine(
        block,
        input_q: MultiProcessingQueue,
        output_q: MultiProcessingQueue,
        mining_params: Dict,
    ):
        """Sleep until the target time, or a new block is added to queue"""
        target_time = block.header.create_time + numpy.random.exponential(
            mining_params["target_block_time"]
        )
        while True:
            time.sleep(0.1)
            try:
                # raises if queue is empty
                block, mining_params = input_q.get_nowait()
                if not block:
                    output_q.put(None)
                    return
                target_time = block.header.create_time + Miner.__get_block_time(
                    block, mining_params["target_block_time"]
                )
            except Exception:
                # got nothing from queue
                pass
            if time.time() > target_time:
                Miner.__log_status(block)
                block.header.nonce = random.randint(0, 2 ** 32 - 1)
                Miner._post_process_mined_block(block)
                output_q.put(block)
                block, mining_params = input_q.get(block=True)  # blocking
                if not block:
                    output_q.put(None)
                    return
                target_time = block.header.create_time + Miner.__get_block_time(
                    block, mining_params["target_block_time"]
                )

    @staticmethod
    def mine_ethash(
        block: Union[MinorBlock, RootBlock],
        input_q: MultiProcessingQueue,
        output_q: MultiProcessingQueue,
        mining_params: Dict,
    ):
        # TODO: maybe add rounds to config json
        rounds = mining_params.get("rounds", 100)
        is_test = mining_params.get("is_test", False)
        # outer loop for mining forever
        while True:
            # `None` block means termination
            if not block:
                output_q.put(None)
                return

            header_hash = block.header.get_hash()
            block_number = block.header.height
            difficulty = block.header.difficulty
            miner = EthashMiner(block_number, difficulty, header_hash, is_test)
            start_nonce = 0
            # inner loop for iterating nonce
            while True:
                nonce_found, mixhash = miner.mine(rounds, start_nonce)
                # best case
                if nonce_found:
                    block.header.nonce = int.from_bytes(nonce_found, byteorder="big")
                    block.header.mixhash = mixhash
                    Miner._post_process_mined_block(block)
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

    @staticmethod
    def mine_sha3sha3(
        block,
        input_q: MultiProcessingQueue,
        output_q: MultiProcessingQueue,
        mining_params: Dict,
    ):
        pass

    @staticmethod
    def _post_process_mined_block(block: Union[MinorBlock, RootBlock]):
        if isinstance(block, RootBlock):
            extra_data = json.loads(block.header.extra_data.decode("utf-8"))
            extra_data["mined"] = time_ms()
            # NOTE this actually ruins POW mining; added for perf tracking
            block.header.extra_data = json.dumps(extra_data).encode("utf-8")
        else:
            extra_data = json.loads(block.meta.extra_data.decode("utf-8"))
            extra_data["mined"] = time_ms()
            # NOTE this actually ruins POW mining; added for perf tracking
            block.meta.extra_data = json.dumps(extra_data).encode("utf-8")
            block.header.hash_meta = block.meta.get_hash()
