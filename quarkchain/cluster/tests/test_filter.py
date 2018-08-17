import unittest
from copy import copy

from quarkchain.cluster.filter import Filter
from quarkchain.cluster.tests.test_shard_state import create_default_shard_state
from quarkchain.cluster.tests.test_utils import (
    get_test_env,
    create_contract_creation_with_event_transaction,
    create_transfer_transaction,
)
from quarkchain.core import Identity, Address, Log
from quarkchain.evm.bloom import bits_in_number


class TestFilter(unittest.TestCase):
    def setUp(self):
        super().setUp()
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_id=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        tx = create_contract_creation_with_event_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_full_shard_id=acc1.full_shard_id,
        )
        self.assertTrue(state.add_tx(tx))
        b = state.create_block_to_mine(address=acc1, gas_limit=1000000)
        hit_block = b  # will be used later
        state.finalize_and_add_block(b)
        start_height = b.header.height
        # https://hastebin.com/debezaqocu.cs
        # 1 log with 2 topics - sha3(b'Hi(address)') and msg.sender
        log = Log.create_from_eth_log(state.evm_state.receipts[0].logs[0])

        # add other random blocks with normal tx
        for _ in range(10):
            tx = create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=Address.create_from_identity(
                    Identity.create_random_identity(), full_shard_id=0
                ),
                value=1,
                gas=40000,
            )
            self.assertTrue(state.add_tx(tx))
            b = state.create_block_to_mine(address=acc1, gas_limit=100000)
            state.finalize_and_add_block(b)
        self.assertEqual(b.header.height, start_height + 10)

        self.hit_block = hit_block
        self.log = log
        self.state = state
        self.start_height = start_height
        self.filer_gen_with_criteria = lambda criteria: Filter(
            state.db, [], criteria, start_height, start_height + 10
        )

    def test_bloom_bits_in_cstor(self):
        criteria = ["0x" + t.hex() for t in self.log.topics]
        f = self.filer_gen_with_criteria(criteria)
        # only use sha3(b'Hi(address)') to test bits
        expected_indexes = bits_in_number(f.bloom_bits[0][0])
        self.assertEqual([333, 522, 1419], expected_indexes)

    def test_get_block_candidates_hit(self):
        hit_criteria = [
            ["0x" + t.hex() for t in self.log.topics],  # exact match
            ["0x" + self.log.topics[0].hex(), None],  # one wild card
            [None, "0x" + self.log.topics[1].hex()],  # another wild card
            [None, ["0x" + self.log.topics[1].hex(), "0x1234"]],  # one item with OR
        ]
        for criteria in hit_criteria:
            f = self.filer_gen_with_criteria(criteria)
            blocks = f._get_block_candidates()
            self.assertEqual(len(blocks), 1)
            self.assertEqual(blocks[0].header.height, self.start_height)

    def test_get_block_candidates_miss(self):
        miss_criteria = [["0x" + self.log.topics[0].hex(), "0x1234"]]  # one miss match
        for criteria in miss_criteria:
            f = self.filer_gen_with_criteria(criteria)
            blocks = f._get_block_candidates()
            self.assertEqual(len(blocks), 0)

    def test_log_topics_match(self):
        criteria = ["0x" + t.hex() for t in self.log.topics]
        f = self.filer_gen_with_criteria(criteria)
        log = copy(self.log)
        log.topics = []
        # topic array length mismatch
        self.assertFalse(f._log_topics_match(log))
        # should match exactly
        log = copy(self.log)
        self.assertTrue(f._log_topics_match(log))
        # wild card match
        criteria[0] = None
        f = self.filer_gen_with_criteria(criteria)
        log = copy(self.log)
        self.assertTrue(f._log_topics_match(log))

    def test_get_logs(self):
        criteria = ["0x" + t.hex() for t in self.log.topics]
        f = self.filer_gen_with_criteria(criteria)
        logs = f._get_logs([self.hit_block])
        self.assertEqual([self.log], logs)
