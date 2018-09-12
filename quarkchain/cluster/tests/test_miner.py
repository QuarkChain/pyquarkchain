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
        return RootBlock(RootBlockHeader(create_time=int(time.time())))

    @staticmethod
    def get_target_block_time() -> float:
        # guarantee target time is hit
        return 0.0

    def test_simulate_mine(self):
        # should generate 5 blocks and then end
        f = self.miner.mine_new_block_async()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(f)
        self.assertEqual(len(TestMiner.added_blocks), 5)
        # TODO: add more test cases
