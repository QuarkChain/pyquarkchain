import asyncio
import functools
import time
import unittest
from typing import Optional

from quarkchain.cluster.miner import Miner
from quarkchain.config import ConsensusType
from quarkchain.core import RootBlock, RootBlockHeader


class TestMiner(unittest.TestCase):
    def setUp(self):
        super().setUp()

        def miner_gen(consensus, create_func, add_func):
            m = Miner(consensus, create_func, add_func, self.get_mining_params, None)
            m.enable()
            return m

        self.miner_gen = miner_gen
        self.added_blocks = []

    @staticmethod
    def get_mining_params(rounds: Optional[int] = None):
        # guarantee target time is hit
        ret = {"target_block_time": 0.0, "is_test": True}
        if rounds is not None:
            ret["rounds"] = rounds
        return ret

    def test_mine_new_block_normal_case(self):
        async def create():
            if len(self.added_blocks) >= 5:
                return None  # stop the game
            return RootBlock(
                RootBlockHeader(
                    create_time=int(time.time()), extra_data="{}".encode("utf-8")
                )
            )

        async def add(block):
            nonlocal miner
            self.added_blocks.append(block)
            miner.mine_new_block_async()

        for consensus in (ConsensusType.POW_SIMULATE, ConsensusType.POW_ETHASH):
            miner = self.miner_gen(consensus, create, add)
            # should generate 5 blocks and then end
            loop = asyncio.get_event_loop()
            loop.run_until_complete(miner.mine_new_block_async())
            self.assertEqual(len(self.added_blocks), 5)

    def test_simulate_mine_handle_block_exception(self):
        i = 0

        async def create():
            nonlocal i
            if i >= 5:
                return None
            return RootBlock(
                RootBlockHeader(
                    create_time=int(time.time()), extra_data="{}".encode("utf-8")
                )
            )

        async def add(block):
            nonlocal i, miner
            try:
                if i % 2 == 0:
                    raise Exception("(╯°□°）╯︵ ┻━┻")
                else:
                    self.added_blocks.append(block)
                    miner.mine_new_block_async()
            finally:
                i += 1

        miner = self.miner_gen(ConsensusType.POW_SIMULATE, create, add)
        # only 2 blocks can be added
        loop = asyncio.get_event_loop()
        loop.run_until_complete(miner.mine_new_block_async())
        self.assertEqual(len(self.added_blocks), 2)

    def test_mine_ethash_new_block_overwrite(self):
        # set a super low `rounds`, and put blocks into input queue beforehand
        # which will make miner consistently drop current block and start mining new one
        block = RootBlock(
            RootBlockHeader(
                create_time=42,  # so we have deterministic hash
                extra_data="{}".encode("utf-8"),
                difficulty=5,  # low probability on successful mining at first try
            )
        )

        async def create():
            nonlocal block
            return block

        async def add(block_to_add):
            nonlocal miner
            self.added_blocks.append(block_to_add)
            miner.input_q.put((None, {}))

        miner = self.miner_gen(ConsensusType.POW_ETHASH, create, add)
        # only one round!
        miner.get_mining_param_func = functools.partial(
            self.get_mining_params, rounds=1
        )
        # insert 5 blocks beforehand
        for _ in range(5):
            miner.input_q.put((block, {}))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(miner.mine_new_block_async())
        # will only have 1 block mined
        self.assertEqual(len(self.added_blocks), 1)
