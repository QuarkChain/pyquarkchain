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

            async def go():
                task = miner._mine_new_block_async()
                if task is not None:
                    await task

            asyncio.run(go())
            self.assertEqual(len(self.added_blocks), 5)

    def test_disable_with_subprocess_signals_via_queue(self):
        # When a subprocess is running, disable() must signal termination via
        # the input queue sentinel and must NOT cancel the handler task.
        # Cancelling would leave the subprocess's terminating None unconsumed in
        # output_q, so the next start() would read a stale None and exit.
        miner = self.miner_gen(ConsensusType.POW_DOUBLESHA256, None, None)

        async def go():
            async def handler():
                await asyncio.sleep(60)

            miner.enabled = True
            miner.process = object()  # pretend a subprocess is running
            miner._mining_task = asyncio.ensure_future(handler())
            task = miner._mining_task
            await asyncio.sleep(0)  # let the task start

            miner.disable()

            # sentinel queued for the subprocess to read and terminate on
            self.assertEqual(miner.input_q.get(timeout=5), (None, {}))
            # handler task must NOT be cancelled (it drains the output_q None)
            self.assertFalse(task.cancelled())
            self.assertFalse(task.done())
            self.assertFalse(miner.enabled)
            self.assertIsNone(miner._mining_task)

            task.cancel()  # cleanup
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(go())

    def test_disable_without_subprocess_cancels_task(self):
        # With no subprocess yet (e.g. disable() during block creation), there
        # is nothing to signal, so the handler task is cancelled directly.
        miner = self.miner_gen(ConsensusType.POW_DOUBLESHA256, None, None)

        async def go():
            async def handler():
                await asyncio.sleep(60)

            miner.enabled = True
            miner.process = None  # no subprocess spawned yet
            miner._mining_task = asyncio.ensure_future(handler())
            task = miner._mining_task
            await asyncio.sleep(0)

            miner.disable()

            self.assertFalse(miner.enabled)
            self.assertIsNone(miner._mining_task)
            with self.assertRaises(asyncio.CancelledError):
                await task
            self.assertTrue(task.cancelled())

        asyncio.run(go())

    def test_restart_after_termination_resumes_mining(self):
        # Regression: after a mining session ends (subprocess signaled to stop),
        # self.process must be reset so the next start() spawns a fresh
        # subprocess.  Without the reset, mine_new_block posts work to the dead
        # subprocess and mining silently never resumes.
        async def create(coinbase_addr=None, retry=True):
            # session 1 stops at 2 blocks, session 2 at 4
            if len(self.added_blocks) >= self.stop_at:
                return None
            return RootBlock(
                RootBlockHeader(create_time=int(time.time())),
                tracking_data="{}".encode("utf-8"),
            )

        async def add(block):
            self.added_blocks.append(block)

        miner = self.miner_gen(ConsensusType.POW_DOUBLESHA256, create, add)

        async def session():
            task = miner._mine_new_block_async()
            if task is not None:
                await task

        # session 1
        self.stop_at = 2
        asyncio.run(session())
        self.assertEqual(len(self.added_blocks), 2)
        # process handle cleared after the subprocess terminated
        self.assertIsNone(miner.process)

        # session 2 (restart): mining must actually resume and add more blocks
        self.stop_at = 4
        miner.enabled = True
        asyncio.run(session())
        self.assertEqual(len(self.added_blocks), 4)
        self.assertIsNone(miner.process)

    def test_disable_during_block_creation_prevents_rogue_subprocess(self):
        # Race: an in-flight mine_new_block task is suspended at
        # `await create_block_async_func`.  While suspended, disable() runs and
        # the old subprocess is reaped (self.process reset to None).  When the
        # task resumes it must re-check self.enabled and bail out; otherwise it
        # would spawn a fresh subprocess that disable() has already finished
        # tearing down and can no longer stop, leaving an unstoppable miner.
        miner = self.miner_gen(ConsensusType.POW_DOUBLESHA256, None, None)

        async def create(coinbase_addr=None, retry=True):
            # mimic disable() + subprocess reaping landing mid-await
            miner.process = object()  # a subprocess was running
            miner.disable()  # signals the subprocess, flips enabled to False
            miner.process = None  # handle_mined_block reaps the subprocess
            return RootBlock(
                RootBlockHeader(create_time=int(time.time())),
                tracking_data="{}".encode("utf-8"),
            )

        miner.create_block_async_func = create

        async def go():
            task = miner._mine_new_block_async()
            if task is not None:
                await task

        asyncio.run(go())
        # bailed out after the await: no rogue subprocess was spawned
        self.assertIsNone(miner.process)
        self.assertFalse(miner.enabled)
        self.assertEqual(len(self.added_blocks), 0)

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

        async def go():
            task = miner._mine_new_block_async()
            if task is not None:
                await task

        asyncio.run(go())
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

        asyncio.run(go())

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

        asyncio.run(go())

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

        asyncio.run(go())

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

        asyncio.run(go())

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

        asyncio.run(go())

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
