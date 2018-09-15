import asyncio
import time
import unittest
from typing import Optional

from quarkchain.cluster.miner import Miner
from quarkchain.config import ConsensusType
from quarkchain.core import RootBlock, RootBlockHeader


class TestMiner(unittest.TestCase):
    # used for stubbing `add_block_async_func`
    added_blocks = []
    miner = None  # type: Miner

    def setUp(self):
        super().setUp()
        TestMiner.added_blocks = []

        self.miner = Miner(
            ConsensusType.POW_SIMULATE,
            self.dummy_create_block_async,
            self.dummy_add_block_async,
            self.get_target_block_time,
            None,
        )
        self.miner.enable()
        TestMiner.miner = self.miner

    @staticmethod
    async def dummy_add_block_async(block) -> None:
        TestMiner.added_blocks.append(block)
        # keep calling mining
        TestMiner.miner.mine_new_block_async()

    @staticmethod
    async def dummy_create_block_async() -> Optional[RootBlock]:
        if len(TestMiner.added_blocks) >= 5:
            return None  # stop the game
        return RootBlock(RootBlockHeader(create_time=int(time.time()), extra_data="{}".encode("utf-8")))

    @staticmethod
    def get_target_block_time() -> float:
        # guarantee target time is hit
        return 0.0

    def test_simulate_mine(self):
        # should generate 5 blocks and then end
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.miner.mine_new_block_async())
        self.assertEqual(len(TestMiner.added_blocks), 5)

    def test_simulate_mine_handle_block_exception(self):
        i = 0

        async def add(block):
            nonlocal i
            try:
                if i % 2 == 0:
                    raise Exception("( う-´)づ︻╦̵̵̿╤──   \(˚☐˚”)/")
                else:
                    await TestMiner.dummy_add_block_async(block)
            finally:
                i += 1

        async def create():
            nonlocal i
            if i >= 5:
                return None
            return RootBlock(RootBlockHeader(create_time=int(time.time()), extra_data="{}".encode("utf-8")))

        self.miner.add_block_async_func = add
        self.miner.create_block_async_func = create
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.miner.mine_new_block_async())
        # only 2 blocks can be added
        self.assertEqual(len(TestMiner.added_blocks), 2)
