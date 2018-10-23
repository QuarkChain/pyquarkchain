import asyncio
import time
import unittest
from typing import Optional

from quarkchain.cluster.miner import Miner, validate_seal, DoubleSHA256, MiningWork
from quarkchain.config import ConsensusType
from quarkchain.core import RootBlock, RootBlockHeader
from quarkchain.utils import sha3_256


class TestMiner(unittest.TestCase):
    def setUp(self):
        super().setUp()

        def miner_gen(consensus, create_func, add_func, **kwargs):
            m = Miner(
                consensus, create_func, add_func, self.get_mining_params, **kwargs
            )
            m.enabled = True
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
                RootBlockHeader(create_time=int(time.time())),
                tracking_data="{}".encode("utf-8"),
            )

        async def add(block):
            nonlocal miner
            self.added_blocks.append(block)

        for consensus in (
            ConsensusType.POW_SIMULATE,
            ConsensusType.POW_ETHASH,
            ConsensusType.POW_SHA3SHA3,
        ):
            miner = self.miner_gen(consensus, create, add)
            # should generate 5 blocks and then end
            loop = asyncio.get_event_loop()
            loop.run_until_complete(miner._mine_new_block_async())
            self.assertEqual(len(self.added_blocks), 5)

    def test_simulate_mine_handle_block_exception(self):
        i = 0

        async def create():
            nonlocal i
            if i >= 5:
                return None
            return RootBlock(
                RootBlockHeader(create_time=int(time.time())),
                tracking_data="{}".encode("utf-8"),
            )

        async def add(block):
            nonlocal i, miner
            try:
                if i % 2 == 0:
                    raise Exception("(╯°□°）╯︵ ┻━┻")
                else:
                    self.added_blocks.append(block)
            finally:
                i += 1

        miner = self.miner_gen(ConsensusType.POW_SIMULATE, create, add)
        # only 2 blocks can be added
        loop = asyncio.get_event_loop()
        loop.run_until_complete(miner._mine_new_block_async())
        self.assertEqual(len(self.added_blocks), 2)

    def test_sha3sha3(self):
        miner = self.miner_gen(ConsensusType.POW_SHA3SHA3, None, None)
        block = RootBlock(
            RootBlockHeader(create_time=42, difficulty=5),
            tracking_data="{}".encode("utf-8"),
        )
        work = MiningWork(block.header.get_hash_for_mining(), 42, 5)
        # only process one block, which is passed in. `None` means termination right after
        miner.input_q.put((None, {}))
        miner.mine_loop(
            work,
            {"consensus_type": ConsensusType.POW_SHA3SHA3},
            miner.input_q,
            miner.output_q,
        )
        mined_res = miner.output_q.get()
        self.assertEqual(mined_res.nonce, 8)
        block.header.nonce = mined_res.nonce
        validate_seal(block.header, ConsensusType.POW_SHA3SHA3)

    def test_only_remote(self):
        async def go():
            miner = self.miner_gen(ConsensusType.POW_SHA3SHA3, None, None)
            with self.assertRaises(ValueError):
                await miner.get_work()
            with self.assertRaises(ValueError):
                await miner.submit_work(b"", 42, b"")

        loop = asyncio.get_event_loop()
        loop.run_until_complete(go())

    def test_get_work(self):
        now = 42

        async def create():
            nonlocal now
            return RootBlock(RootBlockHeader(create_time=now, extra_data=b"{}"))

        miner = self.miner_gen(ConsensusType.POW_SHA3SHA3, create, None, remote=True)

        async def go():
            nonlocal now
            # no current work, will generate a new one
            work = await miner.get_work(now=now)
            self.assertEqual(len(work), 3)
            self.assertEqual(len(miner.work_map), 1)
            h = list(miner.work_map.keys())[0]
            self.assertEqual(work.hash, h)
            # cache hit
            now += 1
            work = await miner.get_work(now=now)
            self.assertEqual(work.hash, h)
            self.assertEqual(len(miner.work_map), 1)
            # new work if interval passed
            now += 10
            work = await miner.get_work(now=now)
            self.assertEqual(len(miner.work_map), 2)
            self.assertNotEqual(work.hash, h)
            # work map cleaned up if too much time passed
            now += 100
            await miner.get_work(now=now)
            self.assertEqual(len(miner.work_map), 1)  # only new work itself

        loop = asyncio.get_event_loop()
        loop.run_until_complete(go())

    def test_submit_work(self):
        now = 42
        block = RootBlock(
            RootBlockHeader(create_time=42, extra_data=b"{}", difficulty=5)
        )

        async def create():
            return block

        async def add(block_to_add):
            self.added_blocks.append(block_to_add)

        miner = self.miner_gen(ConsensusType.POW_SHA3SHA3, create, add, remote=True)

        async def go():
            work = await miner.get_work(now=now)
            self.assertEqual(work.height, 0)
            self.assertEqual(work.difficulty, 5)
            # submitted block doesn't exist
            res = await miner.submit_work(b"lolwut", 0, sha3_256(b""))
            self.assertFalse(res)

            solver = DoubleSHA256(work)
            sol = solver.mine(100, 200).nonce
            self.assertGreater(sol, 100)  # ensure non-solution is tried
            non_sol = sol - 1
            # invalid pow proof
            res = await miner.submit_work(work.hash, non_sol, sha3_256(b""))
            self.assertFalse(res)
            # valid submission, also check internal state afterwards
            res = await miner.submit_work(work.hash, sol, sha3_256(b""))
            self.assertTrue(res)
            self.assertEqual(miner.work_map, {})
            self.assertEqual(len(self.added_blocks), 1)
            self.assertIsNone(miner.current_work)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(go())

    def test_validate_seal_with_adjusted_diff(self):
        diff = 1000
        block = RootBlock(
            RootBlockHeader(create_time=42, difficulty=diff),
            tracking_data="{}".encode("utf-8"),
        )
        block.header.nonce = 0
        with self.assertRaises(ValueError):
            validate_seal(block.header, ConsensusType.POW_SHA3SHA3)

        # significantly lowering the diff should pass
        validate_seal(block.header, ConsensusType.POW_SHA3SHA3, adjusted_diff=1)
