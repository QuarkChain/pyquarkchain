import asyncio
import copy
import time
import unittest
from typing import Optional

from quarkchain.cluster.guardian import Guardian
from quarkchain.cluster.miner import DoubleSHA256, Miner, MiningWork, validate_seal
from quarkchain.config import ConsensusType
from quarkchain.core import RootBlock, RootBlockHeader, Address
from quarkchain.p2p import ecies
from quarkchain.utils import sha3_256

EMPTY_ADDR = Address.create_empty_account()


class TestMiner(unittest.TestCase):
    def setUp(self):
        super().setUp()

        dummy_tip = lambda: RootBlockHeader()

        def miner_gen(consensus, create_func, add_func, tip_func=dummy_tip, **kwargs):
            m = Miner(
                consensus,
                create_func,
                add_func,
                self.get_mining_params,
                tip_func,
                **kwargs
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
        async def create(coinbase_addr=None, retry=True):
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
            ConsensusType.POW_DOUBLESHA256,
        ):
            miner = self.miner_gen(consensus, create, add)
            # should generate 5 blocks and then end
            loop = asyncio.get_event_loop()
            loop.run_until_complete(miner._mine_new_block_async())
            self.assertEqual(len(self.added_blocks), 5)

    def test_simulate_mine_handle_block_exception(self):
        i = 0

        async def create(coibase_addr=None, retry=True):
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
        miner = self.miner_gen(ConsensusType.POW_DOUBLESHA256, None, None)
        block = RootBlock(
            RootBlockHeader(create_time=42, difficulty=5),
            tracking_data="{}".encode("utf-8"),
        )
        work = MiningWork(block.header.get_hash_for_mining(), 42, 5)
        # only process one block, which is passed in. `None` means termination right after
        miner.input_q.put((None, {}))
        miner.mine_loop(
            work,
            {"consensus_type": ConsensusType.POW_DOUBLESHA256},
            miner.input_q,
            miner.output_q,
        )
        mined_res = miner.output_q.get()
        block.header.nonce = mined_res.nonce
        validate_seal(block.header, ConsensusType.POW_DOUBLESHA256)

    def test_qkchash(self):
        miner = self.miner_gen(ConsensusType.POW_QKCHASH, None, None)
        block = RootBlock(
            RootBlockHeader(create_time=42, difficulty=5),
            tracking_data="{}".encode("utf-8"),
        )
        work = MiningWork(block.header.get_hash_for_mining(), 42, 5)
        # only process one block, which is passed in. `None` means termination right after
        miner.input_q.put((None, {}))
        miner.mine_loop(
            work,
            {"consensus_type": ConsensusType.POW_QKCHASH},
            miner.input_q,
            miner.output_q,
        )
        mined_res = miner.output_q.get()
        block.header.nonce = mined_res.nonce
        block.header.mixhash = mined_res.mixhash
        validate_seal(block.header, ConsensusType.POW_QKCHASH)

    def test_only_remote(self):
        async def go():
            miner = self.miner_gen(ConsensusType.POW_DOUBLESHA256, None, None)
            with self.assertRaises(ValueError):
                await miner.get_work(EMPTY_ADDR)
            with self.assertRaises(ValueError):
                await miner.submit_work(b"", 42, b"")

        loop = asyncio.get_event_loop()
        loop.run_until_complete(go())

    def test_get_work(self):
        now, height = 42, 42
        mock_tip = RootBlockHeader(height=height)

        async def create(coinbase_addr, **kwargs):
            nonlocal now, mock_tip
            return RootBlock(
                RootBlockHeader(
                    coinbase_address=coinbase_addr,
                    create_time=now,
                    extra_data=b"{}",
                    hash_prev_block=mock_tip.get_hash(),
                    height=mock_tip.height + 1,
                )
            )

        def tip_getter():
            nonlocal mock_tip
            return mock_tip

        miner = self.miner_gen(
            ConsensusType.POW_DOUBLESHA256, create, None, tip_getter, remote=True
        )

        async def go():
            nonlocal now, mock_tip
            # no current work, will generate a new one
            work, block = await miner.get_work(EMPTY_ADDR, now=now)
            self.assertEqual(len(work), 3)
            self.assertEqual(
                block.header.coinbase_address, Address.create_empty_account()
            )
            self.assertEqual(len(miner.work_map), 1)
            h = list(miner.work_map.keys())[0]
            self.assertEqual(work.hash, h)
            # cache hit and new block is linked to tip (by default)
            now += 1
            work, _ = await miner.get_work(EMPTY_ADDR, now=now)
            self.assertEqual(work.hash, h)
            self.assertEqual(work.height, 43)
            self.assertEqual(len(miner.work_map), 1)
            # cache hit, but current work is outdated because tip has updated
            mock_tip.height += 1
            work, _ = await miner.get_work(EMPTY_ADDR, now=now)
            h = work.hash
            self.assertEqual(len(miner.work_map), 2)
            self.assertEqual(work.height, 44)
            # new work if interval passed
            now += 11
            work, _ = await miner.get_work(EMPTY_ADDR, now=now)
            self.assertEqual(len(miner.work_map), 3)
            # height didn't change, but hash should
            self.assertNotEqual(work.hash, h)
            self.assertEqual(work.height, 44)
            # get work with specified coinbase address
            addr = Address.create_random_account(0)
            work, block = await miner.get_work(addr, now=now)
            self.assertEqual(block.header.coinbase_address, addr)
            self.assertEqual(len(miner.work_map), 4)
            self.assertEqual(len(miner.current_works), 2)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(go())

    def test_submit_work(self):
        now, height = 42, 42
        doublesha = ConsensusType.POW_DOUBLESHA256
        mock_tip = RootBlockHeader(height=height)
        block = RootBlock(
            RootBlockHeader(create_time=42, extra_data=b"{}", difficulty=5)
        )

        async def create(coinbase_addr=None, retry=True):
            nonlocal block, mock_tip
            ret = copy.deepcopy(block)
            ret.header.height = mock_tip.height + 1
            ret.header.hash_prev_block = mock_tip.get_hash()
            return ret

        async def add(block_to_add):
            validate_seal(block_to_add.header, doublesha)
            self.added_blocks.append(block_to_add)

        def tip_getter():
            nonlocal mock_tip
            return mock_tip

        miner = self.miner_gen(doublesha, create, add, tip_getter, remote=True)

        async def go():
            nonlocal mock_tip
            work, _ = await miner.get_work(EMPTY_ADDR, now=now)
            self.assertEqual(work.height, 43)
            self.assertEqual(work.difficulty, 5)
            # submitted block doesn't exist
            res = await miner.submit_work(b"lolwut", 0, sha3_256(b""))
            self.assertFalse(res)

            solver = DoubleSHA256(work)
            sol = solver.mine(200, 300).nonce
            self.assertGreater(sol, 200)  # ensure non-solution is tried
            non_sol = sol - 1
            # invalid pow proof
            res = await miner.submit_work(work.hash, non_sol, sha3_256(b""))
            self.assertFalse(res)
            # valid submission, but tip updated so should fail
            mock_tip.height += 1
            res = await miner.submit_work(work.hash, sol, sha3_256(b""))
            self.assertFalse(res)
            self.assertEqual(miner.work_map, {})
            # bring tip back and regenerate the work
            mock_tip.height -= 1
            work, _ = await miner.get_work(EMPTY_ADDR, now=now)
            # valid submission, also check internal state afterwards
            res = await miner.submit_work(work.hash, sol, sha3_256(b""))
            self.assertTrue(res)
            self.assertEqual(miner.work_map, {})
            self.assertEqual(len(self.added_blocks), 1)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(go())

    def test_submit_work_with_guardian(self):
        now = 42
        doublesha = ConsensusType.POW_DOUBLESHA256
        block = RootBlock(
            RootBlockHeader(
                create_time=42,
                extra_data=b"{}",
                difficulty=1000,
                hash_prev_block=RootBlockHeader().get_hash(),
            )
        )
        priv = ecies.generate_privkey()

        async def create(coinbase_addr=None, retry=True):
            return block

        async def add(block_to_add):
            h = block_to_add.header
            diff = h.difficulty
            if h.verify_signature(priv.public_key):
                diff = Guardian.adjust_difficulty(diff, h.height)
            validate_seal(block_to_add.header, doublesha, adjusted_diff=diff)

        miner = self.miner_gen(
            doublesha, create, add, remote=True, root_signer_private_key=priv
        )

        async def go():
            for i in range(42, 100):
                work, _ = await miner.get_work(EMPTY_ADDR, now=now)
                self.assertEqual(work.height, 0)

                # guardian: diff 1000 -> 1, any number should work
                res = await miner.submit_work(work.hash, i, sha3_256(b""))
                self.assertTrue(res)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(go())

    def test_submit_work_with_remote_guardian(self):
        now = 42
        doublesha = ConsensusType.POW_DOUBLESHA256
        block = RootBlock(
            RootBlockHeader(
                create_time=42,
                extra_data=b"{}",
                difficulty=1000,
                hash_prev_block=RootBlockHeader().get_hash(),
            )
        )
        priv = ecies.generate_privkey()

        async def create(coinbase_addr=None, retry=True):
            return block

        async def add(block_to_add):
            h = block_to_add.header
            diff = h.difficulty

            if h.verify_signature(priv.public_key):
                diff = Guardian.adjust_difficulty(diff, h.height)

            validate_seal(block_to_add.header, doublesha, adjusted_diff=diff)

        # just with the guardian public key
        miner = self.miner_gen(doublesha, create, add, remote=True)

        async def go():
            for i in range(42, 100):
                work, _ = await miner.get_work(EMPTY_ADDR, now=now)
                self.assertEqual(work.height, 0)

                # remote guardian: diff 1000 -> 1, any number should work
                # mimic the sign process of the remote guardian server
                block.header.nonce = i
                block.header.mixhash = sha3_256(b"")
                block.header.sign_with_private_key(priv)
                signature = block.header.signature

                # reset the signature to the default value
                block.header.signature = bytes(65)
                # submit the signature through the submit work
                res = await miner.submit_work(work.hash, i, sha3_256(b""), signature)
                self.assertTrue(res)
                res = await miner.submit_work(work.hash, i, sha3_256(b""), bytes(65))
                self.assertFalse(res)

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
            validate_seal(block.header, ConsensusType.POW_DOUBLESHA256)

        # significantly lowering the diff should pass
        validate_seal(block.header, ConsensusType.POW_DOUBLESHA256, adjusted_diff=1)
