import random
import unittest
from fractions import Fraction

from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import (
    get_test_env,
    create_transfer_transaction,
    create_contract_creation_transaction,
    contract_creation_tx,
)
from quarkchain.config import ConsensusType
from quarkchain.core import CrossShardTransactionDeposit, CrossShardTransactionList
from quarkchain.core import Identity, Address, TokenBalanceMap, MinorBlock
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.evm import opcodes
from quarkchain.evm.messages import apply_transaction
from quarkchain.evm.specials import SystemContract
from quarkchain.evm.state import State as EvmState
from quarkchain.genesis import GenesisManager
from quarkchain.utils import token_id_encode, sha3_256
from quarkchain.cluster.miner import validate_seal, QkchashMiner


def create_default_shard_state(
    env, shard_id=0, diff_calc=None, posw_override=False, no_coinbase=False
):
    genesis_manager = GenesisManager(env.quark_chain_config)
    shard_size = next(iter(env.quark_chain_config.shards.values())).SHARD_SIZE
    full_shard_id = shard_size | shard_id
    if posw_override:
        posw_config = env.quark_chain_config.shards[full_shard_id].POSW_CONFIG
        posw_config.ENABLED = True
        posw_config.WINDOW_SIZE = 3
    if no_coinbase:
        env.quark_chain_config.shards[full_shard_id].COINBASE_AMOUNT = 0
    shard_state = ShardState(env=env, full_shard_id=full_shard_id, diff_calc=diff_calc)
    shard_state.init_genesis_state(genesis_manager.create_root_block())
    return shard_state


class TestShardState(unittest.TestCase):
    def setUp(self):
        super().setUp()
        config = get_test_env().quark_chain_config
        self.root_coinbase = config.ROOT.COINBASE_AMOUNT
        self.shard_coinbase = next(iter(config.shards.values())).COINBASE_AMOUNT
        # to make test verification easier, assume following tax rate
        assert config.REWARD_TAX_RATE == 0.5
        self.tax_rate = config.reward_tax_rate  # type: Fraction
        self.genesis_token = config.genesis_token  # type: int
        self.genesis_token_str = config.GENESIS_TOKEN  # type: str

    def get_after_tax_reward(self, value: int) -> int:
        return value * self.tax_rate.numerator // self.tax_rate.denominator

    def test_shard_state_simple(self):
        env = get_test_env()
        state = create_default_shard_state(env)
        self.assertEqual(state.root_tip.height, 0)
        self.assertEqual(state.header_tip.height, 0)
        # make sure genesis minor block has the right coinbase after-tax
        self.assertEqual(
            state.header_tip.coinbase_amount_map.balance_map,
            {self.genesis_token: 2500000000000000000},
        )

    def test_init_genesis_state(self):
        env = get_test_env()
        state = create_default_shard_state(env)
        genesis_header = state.header_tip
        root_block = state.root_tip.create_block_to_append(nonce=1234)
        root_block.header.height = 0
        root_block.finalize()

        new_genesis_block, _ = state.init_genesis_state(root_block)
        self.assertNotEqual(
            new_genesis_block.header.get_hash(), genesis_header.get_hash()
        )
        # header tip is still the old genesis header
        self.assertEqual(state.header_tip, genesis_header)

        block = new_genesis_block.create_block_to_append()
        state.finalize_and_add_block(block)
        # extending new_genesis_block doesn't change header_tip due to root chain first consensus
        self.assertEqual(state.header_tip, genesis_header)
        self.assertEqual(genesis_header, state.db.get_minor_block_by_height(0).header)

        # extending the root block will change the header_tip
        root_block = root_block.create_block_to_append(nonce=1234).finalize()
        root_block.finalize()
        self.assertTrue(state.add_root_block(root_block))
        # ideally header_tip should be block.header but we don't track tips on fork chains for the moment
        # and thus it reverted all the way back to genesis
        self.assertEqual(state.header_tip, new_genesis_block.header)
        self.assertEqual(new_genesis_block, state.db.get_minor_block_by_height(0))

    def test_blocks_with_incorrect_version(self):
        env = get_test_env()
        state = create_default_shard_state(env=env)
        root_block = state.root_tip.create_block_to_append()
        root_block.header.version = 1
        with self.assertRaisesRegexp(ValueError, "incorrect root block version"):
            state.add_root_block(root_block.finalize())

        root_block.header.version = 0
        state.add_root_block(root_block.finalize())

        shard_block = state.create_block_to_mine()
        shard_block.header.version = 1
        with self.assertRaisesRegexp(ValueError, "incorrect minor block version"):
            state.finalize_and_add_block(shard_block)

        shard_block.header.version = 0
        state.finalize_and_add_block(shard_block)

    def test_gas_price(self):
        id_list = [Identity.create_random_identity() for _ in range(5)]
        acc_list = [Address.create_from_identity(i, full_shard_key=0) for i in id_list]
        env = get_test_env(
            genesis_account=acc_list[0],
            genesis_minor_quarkash=100000000,
            genesis_minor_token_balances={
                "QKC": 100000000,
                "QI": 100000000,
                "BTC": 100000000,
            },
        )

        qkc_token = token_id_encode("QKC")
        qi_token = token_id_encode("QI")
        btc_token = token_id_encode("BTC")

        qkc_prices = [42, 42, 100, 42, 41]
        qi_prices = [43, 101, 43, 41, 40]

        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        # 5 tx per block, make 5 blocks
        for nonce in range(5):  # block
            for acc_index in range(5):
                qkc_price, qi_price = (
                    (qkc_prices[nonce], qi_prices[nonce]) if acc_index == 0 else (0, 0)
                )
                state.add_tx(
                    create_transfer_transaction(
                        shard_state=state,
                        key=id_list[acc_index].get_key(),
                        from_address=acc_list[acc_index],
                        to_address=random.choice(acc_list),
                        value=0,
                        gas_price=qkc_price,
                        gas_token_id=qkc_token,
                        nonce=nonce * 2,
                    )
                )

                state.add_tx(
                    create_transfer_transaction(
                        shard_state=state,
                        key=id_list[acc_index].get_key(),
                        from_address=acc_list[acc_index],
                        to_address=random.choice(acc_list),
                        value=0,
                        gas_price=qi_price,
                        gas_token_id=qi_token,
                        nonce=nonce * 2 + 1,
                    )
                )

            b = state.create_block_to_mine(address=acc_list[1])
            state.finalize_and_add_block(b)

        # txs in block 3-5 are included
        # for testing purposes, update percentile to take max gas price
        state.gas_price_suggestion_oracle.percentile = 100
        gas_price = state.gas_price(token_id=qkc_token)
        self.assertEqual(gas_price, 100)

        # tx with token_id = QI and gas_price = 101 is included in block 2
        gas_price = state.gas_price(token_id=qi_token)
        self.assertEqual(gas_price, 43)

        # clear the cache, update percentile to take the second largest gas price
        state.gas_price_suggestion_oracle.cache.clear()
        state.gas_price_suggestion_oracle.percentile = 95
        gas_price = state.gas_price(token_id=qkc_token)
        self.assertEqual(gas_price, 42)
        gas_price = state.gas_price(token_id=qi_token)
        self.assertEqual(gas_price, 41)

        # allowed token id, but no tx with this token id in the latest blocks, set to default minimum gas price
        gas_price = state.gas_price(token_id=btc_token)
        self.assertEqual(gas_price, 0)

        # unrecognized token id
        gas_price = state.gas_price(token_id=1)
        self.assertIsNone(gas_price)

    def test_estimate_gas(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx_gen = lambda shard_key, data: create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2.address_in_shard(shard_key),
            value=12345,
            data=data,
        )
        tx = tx_gen(0, b"")
        estimate = state.estimate_gas(tx, acc1)
        self.assertEqual(estimate, 21000)
        tx = tx_gen(1, b"")
        estimate = state.estimate_gas(tx, acc1)
        self.assertEqual(estimate, 30000)
        tx = tx_gen(0, b"12123478123412348125936583475758")
        estimate = state.estimate_gas(tx, acc1)
        self.assertEqual(estimate, 23176)
        tx = tx_gen(1, b"12123478123412348125936583475758")
        estimate = state.estimate_gas(tx, acc1)
        self.assertEqual(estimate, 32176)

    def test_execute_tx(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
        )
        # Add this line to make sure `execute_tx` would reset `gas_used`
        state.evm_state.gas_used = state.evm_state.gas_limit
        res = state.execute_tx(tx, acc1)
        self.assertEqual(res, b"")

    def test_add_tx_incorrect_from_shard_id(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=1)
        acc2 = Address.create_random_account(full_shard_key=1)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        # state is shard 0 but tx from shard 1
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
        )
        self.assertFalse(state.add_tx(tx))
        self.assertIsNone(state.execute_tx(tx, acc1))

    def test_one_tx(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=50000,
        )
        state.evm_state.gas_used = state.evm_state.gas_limit
        self.assertTrue(state.add_tx(tx))

        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(block.tx_list[0], tx)
        self.assertEqual(block.header.create_time, 0)
        self.assertEqual(i, 0)

        # tx claims to use more gas than the limit and thus not included
        b1 = state.create_block_to_mine(address=acc3, gas_limit=49999)
        self.assertEqual(len(b1.tx_list), 0)

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            10000000 - opcodes.GTXCOST - 12345,
        )
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token), 12345
        )
        # shard miner only receives a percentage of reward because of REWARD_TAX_RATE
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST + self.shard_coinbase),
        )

        # Check receipts
        self.assertEqual(len(state.evm_state.receipts), 1)
        self.assertEqual(state.evm_state.receipts[0].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[0].gas_used, 21000)

        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        # Check receipts in storage
        resp = state.get_transaction_receipt(tx.get_hash())
        self.assertIsNotNone(resp)
        block, i, r = resp
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)
        self.assertEqual(r.success, b"\x01")
        self.assertEqual(r.gas_used, 21000)

        # Check Account has full_shard_key
        self.assertEqual(
            state.evm_state.get_full_shard_key(acc2.recipient), acc2.full_shard_key
        )

        tx_list, _ = state.db.get_transactions_by_address(acc1)
        self.assertEqual(tx_list[0].value, 12345)
        tx_list, _ = state.db.get_transactions_by_address(acc2)
        self.assertEqual(tx_list[0].value, 12345)

    def test_duplicated_tx(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
        )
        self.assertTrue(state.add_tx(tx))
        self.assertFalse(state.add_tx(tx))  # already in tx_queue

        self.assertEqual(len(state.tx_queue), 1)

        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(len(block.tx_list), 1)
        self.assertEqual(block.tx_list[0], tx)
        self.assertEqual(block.header.create_time, 0)
        self.assertEqual(i, 0)

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            10000000 - opcodes.GTXCOST - 12345,
        )
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token), 12345
        )
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST + self.shard_coinbase),
        )

        # Check receipts
        self.assertEqual(len(state.evm_state.receipts), 1)
        self.assertEqual(state.evm_state.receipts[0].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[0].gas_used, 21000)
        block, i = state.get_transaction_by_hash(tx.get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        # tx already confirmed
        self.assertTrue(state.db.contain_transaction_hash(tx.get_hash()))
        self.assertFalse(state.add_tx(tx))

    def test_add_invalid_tx_fail(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=999999999999999999999,  # insane
        )
        self.assertFalse(state.add_tx(tx))
        self.assertEqual(len(state.tx_queue), 0)

    def test_add_non_neighbor_tx_fail(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=3)  # not acc1's neighbor
        acc3 = Address.create_random_account(full_shard_key=8)  # acc1's neighbor

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=0,
            gas=1000000,
        )
        self.assertFalse(state.add_tx(tx))
        self.assertEqual(len(state.tx_queue), 0)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc3,
            value=0,
            gas=1000000,
        )
        self.assertTrue(state.add_tx(tx))
        self.assertEqual(len(state.tx_queue), 1)

    def test_exceeding_xshard_limit(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        # a huge number to make xshard tx limit become 0 so that no xshard tx can be
        # included in the block
        env.quark_chain_config.MAX_NEIGHBORS = 10 ** 18
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        # add a xshard tx with large startgas
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=state.get_xshard_gas_limit() + 1,
        )
        self.assertFalse(state.add_tx(tx))

        # xshard tx
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=50000,
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)

        # inshard tx
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc3,
            value=12345,
            gas=50000,
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)

    def test_two_tx_in_one_block(self):
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=2000000 + opcodes.GTXCOST
        )
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                value=1000000,
            )
        )

        b0 = state.create_block_to_mine(address=acc3)
        state.finalize_and_add_block(b0)
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token), 1000000
        )
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token), 1000000
        )
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST + self.shard_coinbase),
        )

        # Check Account has full_shard_key
        self.assertEqual(
            state.evm_state.get_full_shard_key(acc2.recipient), acc2.full_shard_key
        )

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=Address(
                    acc2.recipient, acc2.full_shard_key + 2
                ),  # set a different full shard id
                value=12345,
                gas=50000,
            )
        )
        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id2.get_key(),
                from_address=acc2,
                to_address=acc1,
                value=54321,
                gas=40000,
            )
        )
        # Inshard gas limit is 40000 - 20000
        b1 = state.create_block_to_mine(
            address=acc3, gas_limit=40000, xshard_gas_limit=20000
        )
        self.assertEqual(len(b1.tx_list), 0)
        b1 = state.create_block_to_mine(
            address=acc3, gas_limit=40000, xshard_gas_limit=0
        )
        self.assertEqual(len(b1.tx_list), 1)
        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 2)

        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            1000000 - opcodes.GTXCOST - 12345 + 54321,
        )
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token),
            1000000 - opcodes.GTXCOST + 12345 - 54321,
        )
        # 2 block rewards: 3 tx, 2 block rewards
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST * 3 + self.shard_coinbase * 2),
        )

        # Check receipts
        self.assertEqual(len(state.evm_state.receipts), 2)
        self.assertEqual(state.evm_state.receipts[0].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[0].gas_used, 21000)
        self.assertEqual(state.evm_state.receipts[1].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[1].gas_used, 42000)

        block, i = state.get_transaction_by_hash(b1.tx_list[0].get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 0)

        block, i = state.get_transaction_by_hash(b1.tx_list[1].get_hash())
        self.assertEqual(block, b1)
        self.assertEqual(i, 1)

        # Check acc2 full_shard_key doesn't change
        self.assertEqual(
            state.evm_state.get_full_shard_key(acc2.recipient), acc2.full_shard_key
        )

    def test_fork_does_not_confirm_tx(self):
        """Tx should only be confirmed and removed from tx queue by the best chain"""
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=2000000 + opcodes.GTXCOST
        )
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                value=1000000,
            )
        )

        b0 = state.create_block_to_mine(address=acc3)
        b1 = state.create_block_to_mine(address=acc3)
        b0.tx_list = []  # make b0 empty
        state.finalize_and_add_block(b0)

        # tx is added back to queue in the end of create_block_to_mine
        self.assertEqual(len(state.tx_queue), 1)

        self.assertEqual(len(b1.tx_list), 1)
        state.finalize_and_add_block(b1)
        # b1 is a fork and does not remove the tx from queue
        self.assertEqual(len(state.tx_queue), 1)

        b2 = state.create_block_to_mine(address=acc3)
        state.finalize_and_add_block(b2)
        self.assertEqual(len(state.tx_queue), 0)

    def test_revert_fork_put_tx_back_to_queue(self):
        """Tx in the reverted chain should be put back to the queue"""
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=2000000 + opcodes.GTXCOST
        )
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                value=1000000,
            )
        )

        b0 = state.create_block_to_mine(address=acc3)
        b1 = state.create_block_to_mine(address=acc3)
        state.finalize_and_add_block(b0)

        self.assertEqual(len(state.tx_queue), 0)

        b1.tx_list = []  # make b1 empty
        state.finalize_and_add_block(b1)
        self.assertEqual(len(state.tx_queue), 0)

        b2 = b1.create_block_to_append()
        state.finalize_and_add_block(b2)

        # now b1-b2 becomes the best chain and we expect b0 to be reverted and put the tx back to queue
        self.assertEqual(len(state.tx_queue), 1)

        b3 = b0.create_block_to_append()
        state.finalize_and_add_block(b3)
        self.assertEqual(len(state.tx_queue), 1)

        b4 = b3.create_block_to_append()
        state.finalize_and_add_block(b4)

        # b0-b3-b4 becomes the best chain
        self.assertEqual(len(state.tx_queue), 0)

    def test_stale_block_count(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.create_block_to_mine(address=acc3)
        b2 = state.create_block_to_mine(address=acc3)
        b2.header.create_time += 1

        state.finalize_and_add_block(b1)
        self.assertEqual(state.db.get_block_count_by_height(1), 1)

        state.finalize_and_add_block(b2)
        self.assertEqual(state.db.get_block_count_by_height(1), 2)

    def test_xshard_tx_sent(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state1 = create_default_shard_state(env=env1, shard_id=1)

        # Add a root block to update block gas limit so that xshard tx can be included
        root_block = (
            state.root_tip.create_block_to_append()
            .add_minor_block_header(state.header_tip)
            .add_minor_block_header(state1.header_tip)
            .finalize()
        )
        state.add_root_block(root_block)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
        )
        state.add_tx(tx)

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)

        self.assertEqual(state.evm_state.gas_used, 0)
        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(len(state.evm_state.xshard_list), 1)
        self.assertEqual(
            state.evm_state.xshard_list[0],
            CrossShardTransactionDeposit(
                tx_hash=tx.get_hash(),
                from_address=acc1,
                to_address=acc2,
                value=888888,
                gas_price=1,
                gas_token_id=self.genesis_token,
                transfer_token_id=self.genesis_token,
            ),
        )
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            10000000 - 888888 - (opcodes.GTXCOST + opcodes.GTXXSHARDCOST),
        )
        # Make sure the xshard gas is not used by local block
        self.assertEqual(state.evm_state.gas_used, opcodes.GTXCOST)
        # GTXXSHARDCOST is consumed by remote shard
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST + self.shard_coinbase),
        )

    def test_xshard_tx_sent_old(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env.quark_chain_config.ENABLE_EVM_TIMESTAMP = 2 ** 64
        state = create_default_shard_state(env=env, shard_id=0)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env1.quark_chain_config.ENABLE_EVM_TIMESTAMP = 2 ** 64
        state1 = create_default_shard_state(env=env1, shard_id=1)

        # Add a root block to update block gas limit so that xshard tx can be included
        root_block = (
            state.root_tip.create_block_to_append()
            .add_minor_block_header(state.header_tip)
            .add_minor_block_header(state1.header_tip)
            .finalize()
        )
        state.add_root_block(root_block)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
        )
        state.add_tx(tx)

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)

        self.assertEqual(state.evm_state.gas_used, 0)
        # Should succeed
        state.finalize_and_add_block(b1)
        self.assertEqual(len(state.evm_state.xshard_list), 1)
        self.assertEqual(
            state.evm_state.xshard_list[0],
            CrossShardTransactionDeposit(
                tx_hash=tx.get_hash(),
                from_address=acc1,
                to_address=acc2,
                value=888888,
                gas_price=1,
                gas_token_id=self.genesis_token,
                transfer_token_id=self.genesis_token,
            ),
        )
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            10000000 - 888888 - (opcodes.GTXCOST + opcodes.GTXXSHARDCOST),
        )
        # Make sure the xshard gas is not used by local block
        self.assertEqual(
            state.evm_state.gas_used, opcodes.GTXCOST + opcodes.GTXXSHARDCOST
        )
        # GTXXSHARDCOST is consumed by remote shard
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXCOST + self.shard_coinbase),
        )

    def test_xshard_tx_insufficient_gas(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        state.add_tx(
            create_transfer_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=acc2,
                value=888888,
                gas=opcodes.GTXCOST,
            )
        )

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 0)
        self.assertEqual(len(state.tx_queue), 0)

    def test_xshard_tx_received(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_random_account(full_shard_key=0)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env1 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=16)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .add_minor_block_header(state1.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)

        # Add one block in shard 0
        b0 = state0.create_block_to_mine()
        state0.finalize_and_add_block(b0)

        b1 = state1.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=888888,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    )
                ]
            ),
        )

        # Create a root block containing the block with the x-shard tx
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        state0.add_root_block(root_block)

        # Add b0 and make sure all x-shard tx's are added
        b2 = state0.create_block_to_mine(address=acc3)
        state0.finalize_and_add_block(b2)

        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 888888,
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXXSHARDCOST * 2 + self.shard_coinbase),
        )

        # X-shard gas used
        evm_state0 = state0.evm_state
        self.assertEqual(evm_state0.xshard_receive_gas_used, opcodes.GTXXSHARDCOST)

    def test_xshard_tx_received_ddos_fix(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_random_account(full_shard_key=0)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env1 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=16)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .add_minor_block_header(state1.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)

        # Add one block in shard 0
        b0 = state0.create_block_to_mine()
        state0.finalize_and_add_block(b0)

        b1 = state1.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=0,
        )
        b1.add_tx(tx)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=888888,
                        gas_price=0,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    )
                ]
            ),
        )

        # Create a root block containing the block with the x-shard tx
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        state0.add_root_block(root_block)

        # Add b0 and make sure all x-shard tx's are added
        b2 = state0.create_block_to_mine(address=acc3)
        state0.finalize_and_add_block(b2)

        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 888888,
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(self.shard_coinbase),
        )

        # X-shard gas used (to be fixed)
        evm_state0 = state0.evm_state
        self.assertEqual(evm_state0.xshard_receive_gas_used, 0)
        self.assertEqual(b2.meta.evm_gas_used, 0)
        self.assertEqual(b2.meta.evm_cross_shard_receive_gas_used, 0)

        # # Apply the fix
        b3 = MinorBlock.deserialize(b2.serialize())
        state0.env.quark_chain_config.XSHARD_GAS_DDOS_FIX_ROOT_HEIGHT = 0
        state0.finalize_and_add_block(b3)
        self.assertEqual(b3.meta.evm_gas_used, opcodes.GTXXSHARDCOST)
        self.assertEqual(
            b3.meta.evm_cross_shard_receive_gas_used, opcodes.GTXXSHARDCOST
        )

    def test_xshard_tx_received_exclude_non_neighbor(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=3)
        acc3 = Address.create_random_account(full_shard_key=0)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env1 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=3)

        b0 = state0.get_tip()

        b1 = state1.get_tip().create_block_to_append()
        tx = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx)

        # Create a root block containing the block with the x-shard tx
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        state0.add_root_block(root_block)

        b2 = state0.create_block_to_mine(address=acc3)
        state0.finalize_and_add_block(b2)

        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token), 10000000
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(self.shard_coinbase),
        )

        # No xshard tx is processed on the receiving side due to non-neighbor
        evm_state0 = state0.evm_state
        self.assertEqual(evm_state0.xshard_receive_gas_used, 0)

    def test_xshard_from_root_block(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        def _testcase_evm_not_enabled():
            env.quark_chain_config.ENABLE_EVM_TIMESTAMP = None
            return None, Address.create_random_account(0)

        def _testcase_evm_enabled():
            env.quark_chain_config.ENABLE_EVM_TIMESTAMP = 1
            return None, Address.create_random_account(0)

        def _testcase_evm_enabled_coinbase_is_code():
            env.quark_chain_config.ENABLE_EVM_TIMESTAMP = 1
            old_header_tip = state.header_tip
            # Let acc2 has some code
            tx = create_contract_creation_transaction(
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_full_shard_key=0,
            )
            state.add_tx(tx)
            b = state.create_block_to_mine()
            state.finalize_and_add_block(b)
            _, _, r = state.get_transaction_receipt(tx.get_hash())
            self.assertNotEqual(
                state.evm_state.get_code(r.contract_address.recipient), b""
            )
            return old_header_tip, r.contract_address

        for testcase_func in [
            _testcase_evm_not_enabled,
            _testcase_evm_enabled,
            _testcase_evm_enabled_coinbase_is_code,
        ]:
            missed_header, coinbase_addr = testcase_func()
            # Add a root block to update block gas limit so that xshard tx can be included
            root_block = state.root_tip.create_block_to_append()
            if missed_header:
                root_block.add_minor_block_header(missed_header)
            root_block.add_minor_block_header(state.header_tip)
            root_block.finalize(
                coinbase_tokens={env.quark_chain_config.genesis_token: 1000000},
                coinbase_address=coinbase_addr,
            )
            state.add_root_block(root_block)

            b0 = state.create_block_to_mine()
            state.finalize_and_add_block(b0)

            self.assertEqual(
                state.get_token_balance(coinbase_addr.recipient, self.genesis_token),
                1000000,
            )

    def test_xshard_for_two_root_blocks(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

        env0 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=1)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .add_minor_block_header(state1.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)

        # Add one block in shard 0
        b0 = state0.create_block_to_mine()
        state0.finalize_and_add_block(b0)

        b1 = state1.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
        )
        b1.add_tx(tx)

        # Add a x-shard tx from state1
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=888888,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    )
                ]
            ),
        )

        # Create a root block containing the block with the x-shard tx
        root_block0 = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        state0.add_root_block(root_block0)

        b2 = state0.get_tip().create_block_to_append()
        state0.finalize_and_add_block(b2)

        b3 = b1.create_block_to_append()
        b3.header.hash_prev_root_block = root_block.header.get_hash()

        # Add a x-shard tx from state1
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b3.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=bytes(32),
                        from_address=acc2,
                        to_address=acc1,
                        value=385723,
                        gas_price=3,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    )
                ]
            ),
        )

        root_block1 = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b2.header)
            .add_minor_block_header(b3.header)
            .finalize()
        )
        state0.add_root_block(root_block1)

        # Test x-shard gas limit when create_block_to_mine
        b6 = state0.create_block_to_mine(address=acc3, gas_limit=opcodes.GTXXSHARDCOST)
        self.assertEqual(b6.header.hash_prev_root_block, root_block1.header.get_hash())
        # There are two x-shard txs: one is root block coinbase with zero gas, and another is from shard 1
        b7 = state0.create_block_to_mine(
            address=acc3, gas_limit=2 * opcodes.GTXXSHARDCOST
        )
        self.assertEqual(b7.header.hash_prev_root_block, root_block1.header.get_hash())
        b8 = state0.create_block_to_mine(
            address=acc3, gas_limit=3 * opcodes.GTXXSHARDCOST
        )
        self.assertEqual(b8.header.hash_prev_root_block, root_block1.header.get_hash())

        # Add b0 and make sure all x-shard tx's are added
        b4 = state0.create_block_to_mine(address=acc3)
        self.assertEqual(b4.header.hash_prev_root_block, root_block1.header.get_hash())
        state0.finalize_and_add_block(b4)

        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 888888 + 385723,
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(
                opcodes.GTXXSHARDCOST * (2 + 3) + self.shard_coinbase
            ),
        )

        # Check gas used for receiving x-shard tx
        self.assertEqual(state0.evm_state.gas_used, 18000)
        self.assertEqual(state0.evm_state.xshard_receive_gas_used, 18000)

    def test_xshard_gas_limit(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_random_account(full_shard_key=0)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env1 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=16)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .add_minor_block_header(state1.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)

        # Add one block in shard 1 with 2 x-shard txs
        b1 = state1.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx0 = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx0)
        tx1 = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=111111,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx1)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx0.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=888888,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    ),
                    CrossShardTransactionDeposit(
                        tx_hash=tx1.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=111111,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    ),
                ]
            ),
        )

        # Create a root block containing the block with the x-shard tx
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b1.header)
            .finalize(
                coinbase_tokens={env0.quark_chain_config.genesis_token: 1000000},
                coinbase_address=acc1,
            )
        )
        state0.add_root_block(root_block)

        # Add b0 and make sure one x-shard tx's are added
        b2 = state0.create_block_to_mine(
            address=acc3, xshard_gas_limit=opcodes.GTXXSHARDCOST
        )
        state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXXSHARDCOST)

        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 888888,
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.get_after_tax_reward(opcodes.GTXXSHARDCOST * 2 + self.shard_coinbase),
        )

        # X-shard gas used
        evm_state0 = state0.evm_state
        self.assertEqual(evm_state0.xshard_receive_gas_used, opcodes.GTXXSHARDCOST)

        # Add b2 and make sure all x-shard tx's are added
        b2 = state0.create_block_to_mine(
            address=acc3, xshard_gas_limit=opcodes.GTXXSHARDCOST
        )
        state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXXSHARDCOST)
        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 888888 + 111111,
        )
        # X-shard gas used
        evm_state0 = state0.evm_state
        self.assertEqual(evm_state0.xshard_receive_gas_used, opcodes.GTXXSHARDCOST)

        # Add b3 and make sure no x-shard tx's are added
        b3 = state0.create_block_to_mine(
            address=acc3, xshard_gas_limit=opcodes.GTXXSHARDCOST
        )
        state0.finalize_and_add_block(b3, xshard_gas_limit=opcodes.GTXXSHARDCOST)
        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 888888 + 111111,
        )
        evm_state0 = state0.evm_state
        self.assertEqual(evm_state0.xshard_receive_gas_used, 0)

        b4 = state0.create_block_to_mine(
            address=acc3, xshard_gas_limit=opcodes.GTXXSHARDCOST
        )
        state0.finalize_and_add_block(b4, xshard_gas_limit=opcodes.GTXXSHARDCOST)
        self.assertNotEqual(
            b2.meta.xshard_tx_cursor_info, b3.meta.xshard_tx_cursor_info
        )
        self.assertEqual(b3.meta.xshard_tx_cursor_info, b4.meta.xshard_tx_cursor_info)
        evm_state0 = state0.evm_state
        self.assertEqual(evm_state0.xshard_receive_gas_used, 0)

        b5 = state0.create_block_to_mine(
            address=acc3,
            gas_limit=opcodes.GTXXSHARDCOST,
            xshard_gas_limit=2 * opcodes.GTXXSHARDCOST,
        )
        with self.assertRaisesRegexp(
            ValueError, "xshard_gas_limit \\d+ should not exceed total gas_limit"
        ):
            # xshard_gas_limit should be smaller than gas_limit
            state0.finalize_and_add_block(
                b5,
                gas_limit=opcodes.GTXXSHARDCOST,
                xshard_gas_limit=2 * opcodes.GTXXSHARDCOST,
            )

        b6 = state0.create_block_to_mine(
            address=acc3, xshard_gas_limit=opcodes.GTXXSHARDCOST
        )
        with self.assertRaisesRegexp(
            ValueError, "incorrect xshard gas limit, expected \\d+, actual \\d+"
        ):
            # xshard_gas_limit should be gas_limit // 2
            state0.finalize_and_add_block(b6)

    def test_xshard_gas_limit_from_multiple_shards(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_from_identity(id1, full_shard_key=8)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env1 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env2 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=16)
        state2 = create_default_shard_state(env=env1, shard_id=8)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .add_minor_block_header(state1.header_tip)
            .add_minor_block_header(state2.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)
        state2.add_root_block(root_block)

        # Add one block in shard 1 with 2 x-shard txs
        b1 = state1.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx0 = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx0)
        tx1 = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=111111,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b1.add_tx(tx1)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b1.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx0.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=888888,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    ),
                    CrossShardTransactionDeposit(
                        tx_hash=tx1.get_hash(),
                        from_address=acc2,
                        to_address=acc1,
                        value=111111,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    ),
                ]
            ),
        )

        # Add one block in shard 1 with 2 x-shard txs
        b2 = state2.get_tip().create_block_to_append()
        b2.header.hash_prev_root_block = root_block.header.get_hash()
        tx3 = create_transfer_transaction(
            shard_state=state1,
            key=id1.get_key(),
            from_address=acc2,
            to_address=acc1,
            value=12345,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
        )
        b2.add_tx(tx3)

        # Add a x-shard tx from remote peer
        state0.add_cross_shard_tx_list_by_minor_block_hash(
            h=b2.header.get_hash(),
            tx_list=CrossShardTransactionList(
                tx_list=[
                    CrossShardTransactionDeposit(
                        tx_hash=tx3.get_hash(),
                        from_address=acc3,
                        to_address=acc1,
                        value=12345,
                        gas_price=2,
                        gas_token_id=self.genesis_token,
                        transfer_token_id=self.genesis_token,
                    )
                ]
            ),
        )

        # Create a root block containing the block with the x-shard tx
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(b2.header)
            .add_minor_block_header(b1.header)
            .finalize(
                coinbase_tokens={env0.quark_chain_config.genesis_token: 1000000},
                coinbase_address=acc1,
            )
        )
        state0.add_root_block(root_block)

        # Add b0 and make sure one x-shard tx's are added
        b2 = state0.create_block_to_mine(xshard_gas_limit=opcodes.GTXXSHARDCOST)
        state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXXSHARDCOST)

        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 12345,
        )

        # X-shard gas used
        evm_state0 = state0.evm_state
        self.assertEqual(evm_state0.xshard_receive_gas_used, opcodes.GTXXSHARDCOST)

        # Add b2 and make sure all x-shard tx's are added
        b2 = state0.create_block_to_mine(xshard_gas_limit=opcodes.GTXXSHARDCOST)
        state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXXSHARDCOST)
        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 12345 + 888888,
        )

        # Add b3 and make sure no x-shard tx's are added
        b3 = state0.create_block_to_mine(xshard_gas_limit=opcodes.GTXXSHARDCOST)
        state0.finalize_and_add_block(b3, xshard_gas_limit=opcodes.GTXXSHARDCOST)
        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000 + 12345 + 888888 + 111111,
        )

    def test_xshard_rootblock_coinbase(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        env1 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=16)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .add_minor_block_header(state1.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)

        # Create a root block containing the block with the x-shard tx
        root_block = state0.root_tip.create_block_to_append().finalize(
            coinbase_tokens={env0.quark_chain_config.genesis_token: 1000000},
            coinbase_address=acc1,
        )
        state0.add_root_block(root_block)
        state1.add_root_block(root_block)

        # Add b0 and make sure one x-shard tx's are added
        b2 = state0.create_block_to_mine(xshard_gas_limit=opcodes.GTXXSHARDCOST)
        state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXXSHARDCOST)

        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state0.get_token_balance(acc1.recipient, self.genesis_token),
            10000000 + 1000000,
        )

        # Add b0 and make sure one x-shard tx's are added
        b3 = state1.create_block_to_mine(xshard_gas_limit=opcodes.GTXXSHARDCOST)
        state1.finalize_and_add_block(b3, xshard_gas_limit=opcodes.GTXXSHARDCOST)

        # Root block coinbase does not consume xshard gas
        self.assertEqual(
            state1.get_token_balance(acc1.recipient, self.genesis_token), 10000000
        )

    def test_xshard_smart_contract(self):
        pass

    def test_xshard_sender_gas_limit(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_random_account(full_shard_key=0)

        env0 = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=64
        )
        state0 = create_default_shard_state(env=env0, shard_id=0)

        # Add a root block to allow later minor blocks referencing this root block to
        # be broadcasted
        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(state0.header_tip)
            .finalize()
        )
        state0.add_root_block(root_block)

        b0 = state0.get_tip().create_block_to_append()
        b0.header.hash_prev_root_block = root_block.header.get_hash()
        tx0 = create_transfer_transaction(
            shard_state=state0,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=888888,
            gas=b0.meta.evm_xshard_gas_limit + 1,
            gas_price=1,
        )
        self.assertFalse(state0.add_tx(tx0))
        b0.add_tx(tx0)
        with self.assertRaisesRegexp(
            RuntimeError, "xshard evm tx exceeds xshard gas limit"
        ):
            state0.finalize_and_add_block(b0)

        b2 = state0.create_block_to_mine(
            xshard_gas_limit=opcodes.GTXCOST * 9, include_tx=False
        )
        b2.header.hash_prev_root_block = root_block.header.get_hash()
        tx2 = create_transfer_transaction(
            shard_state=state0,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=888888,
            gas=opcodes.GTXCOST * 10,
            gas_price=1,
        )
        self.assertFalse(state0.add_tx(tx2, xshard_gas_limit=opcodes.GTXCOST * 9))
        b2.add_tx(tx2)
        with self.assertRaisesRegexp(
            RuntimeError, "xshard evm tx exceeds xshard gas limit"
        ):
            state0.finalize_and_add_block(b2, xshard_gas_limit=opcodes.GTXCOST * 9)

        b1 = state0.get_tip().create_block_to_append()
        b1.header.hash_prev_root_block = root_block.header.get_hash()
        tx1 = create_transfer_transaction(
            shard_state=state0,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=888888,
            gas=b1.meta.evm_xshard_gas_limit,
            gas_price=1,
        )
        b1.add_tx(tx1)
        state0.finalize_and_add_block(b1)

    def test_fork_resolve(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        b0 = state.get_tip().create_block_to_append()
        b1 = state.get_tip().create_block_to_append()

        state.finalize_and_add_block(b0)
        self.assertEqual(state.header_tip, b0.header)

        # Fork happens, first come first serve
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b0.header)

        # Longer fork happens, override existing one
        b2 = b1.create_block_to_append()
        state.finalize_and_add_block(b2)
        self.assertEqual(state.header_tip, b2.header)

    def test_root_chain_first_consensus(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env0 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=1)

        genesis = state0.header_tip

        # Add one block and prepare a fork
        b0 = state0.get_tip().create_block_to_append(address=acc1)
        b2 = state0.get_tip().create_block_to_append(
            address=Address.create_empty_account()
        )

        state0.finalize_and_add_block(b0)
        state0.finalize_and_add_block(b2)

        b1 = state1.get_tip().create_block_to_append()
        evm_state = state1.run_block(b1)
        b1.finalize(
            evm_state=evm_state,
            coinbase_amount_map=TokenBalanceMap(evm_state.block_fee_tokens),
        )

        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(genesis)
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        state0.add_root_block(root_block)

        b00 = b0.create_block_to_append()
        state0.finalize_and_add_block(b00)
        self.assertEqual(state0.header_tip, b00.header)

        # Create another fork that is much longer (however not confirmed by root_block)
        b3 = b2.create_block_to_append()
        state0.finalize_and_add_block(b3)
        b4 = b3.create_block_to_append()
        state0.finalize_and_add_block(b4)
        self.assertGreater(b4.header.height, b00.header.height)
        self.assertEqual(state0.header_tip, b00.header)

    def test_shard_state_add_root_block(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env0 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state0 = create_default_shard_state(env=env0, shard_id=0)
        state1 = create_default_shard_state(env=env1, shard_id=1)

        genesis = state0.header_tip
        # Add one block and prepare a fork
        b0 = state0.get_tip().create_block_to_append(address=acc1)
        b2 = state0.get_tip().create_block_to_append(
            address=Address.create_empty_account()
        )

        state0.finalize_and_add_block(b0)
        state0.finalize_and_add_block(b2)

        b1 = state1.get_tip().create_block_to_append()
        evm_state = state1.run_block(b1)
        b1.finalize(
            evm_state=evm_state,
            coinbase_amount_map=TokenBalanceMap(evm_state.block_fee_tokens),
        )

        # Add one empty root block
        empty_root = state0.root_tip.create_block_to_append().finalize()
        state0.add_root_block(empty_root)

        root_block = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(genesis)
            .add_minor_block_header(b0.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )
        root_block1 = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(genesis)
            .add_minor_block_header(b2.header)
            .add_minor_block_header(b1.header)
            .finalize()
        )

        state0.add_root_block(root_block)

        b00 = b0.create_block_to_append()
        state0.finalize_and_add_block(b00)
        self.assertEqual(state0.header_tip, b00.header)

        # Create another fork that is much longer (however not confirmed by root_block)
        b3 = b2.create_block_to_append()
        state0.finalize_and_add_block(b3)
        b4 = b3.create_block_to_append()
        state0.finalize_and_add_block(b4)
        self.assertEqual(state0.header_tip, b00.header)
        self.assertEqual(state0.db.get_minor_block_by_height(2), b00)
        self.assertIsNone(state0.db.get_minor_block_by_height(3))

        b5 = b1.create_block_to_append()

        self.assertFalse(state0.add_root_block(root_block1))

        # Add one empty root block
        empty_root = root_block1.create_block_to_append().finalize()
        state0.add_root_block(empty_root)
        root_block2 = (
            empty_root.create_block_to_append()
            .add_minor_block_header(b3.header)
            .add_minor_block_header(b4.header)
            .add_minor_block_header(b5.header)
            .finalize()
        )

        self.assertTrue(state0.add_root_block(root_block2))
        self.assertEqual(state0.header_tip, b4.header)
        self.assertEqual(state0.meta_tip, b4.meta)
        self.assertEqual(state0.root_tip, root_block2.header)

        self.assertEqual(state0.db.get_minor_block_by_height(2), b3)
        self.assertEqual(state0.db.get_minor_block_by_height(3), b4)

    def test_shard_reorg_by_adding_root_block(self):
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)

        env0 = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state0 = create_default_shard_state(env=env0, shard_id=0)

        genesis = state0.header_tip
        # Add one block and include it in the root block
        b0 = state0.get_tip().create_block_to_append(address=acc1)
        b1 = state0.get_tip().create_block_to_append(address=acc2)

        root_block0 = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(genesis)
            .add_minor_block_header(b0.header)
            .finalize()
        )
        root_block1 = (
            state0.root_tip.create_block_to_append()
            .add_minor_block_header(genesis)
            .add_minor_block_header(b1.header)
            .finalize()
        )

        state0.finalize_and_add_block(b0)
        state0.add_root_block(root_block0)
        self.assertEqual(state0.header_tip, b0.header)

        state0.finalize_and_add_block(b1)
        self.assertEqual(state0.header_tip, b0.header)

        # Add another root block with higher TD
        root_block1.header.total_difficulty += root_block1.header.difficulty
        root_block1.header.difficulty *= 2

        self.assertTrue(state0.add_root_block(root_block1))
        self.assertEqual(state0.header_tip, b1.header)
        self.assertEqual(state0.meta_tip, b1.meta)
        self.assertEqual(state0.root_tip, root_block1.header)
        self.assertEqual(state0.evm_state.trie.root_hash, b1.meta.hash_evm_state_root)

    def test_shard_state_add_root_block_too_many_minor_blocks(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_quarkash=10000000, shard_size=1
        )
        state = create_default_shard_state(env=env, shard_id=0)

        max_mblock_in_rblock = state.shard_config.max_blocks_per_shard_in_one_root_block

        headers = [state.header_tip]
        for i in range(max_mblock_in_rblock):
            b = state.get_tip().create_block_to_append(address=acc1)
            state.finalize_and_add_block(b)
            headers.append(b.header)

        root_block = (
            state.root_tip.create_block_to_append()
            .extend_minor_block_header_list(headers)
            .finalize()
        )

        # Too many blocks
        with self.assertRaisesRegexp(
            ValueError, "too many minor blocks in the root block"
        ):
            state.add_root_block(root_block)

        self.assertEqual(
            state.get_unconfirmed_header_list(), headers[:max_mblock_in_rblock]
        )

        # 10 blocks is okay
        root_block.minor_block_header_list = headers[:max_mblock_in_rblock]
        root_block.finalize()
        state.add_root_block(root_block)

    def test_shard_state_fork_resolve_with_higher_root_chain(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        b0 = state.get_tip()  # genesis
        root_block = (
            state.root_tip.create_block_to_append()
            .add_minor_block_header(b0.header)
            .finalize()
        )

        self.assertEqual(state.header_tip, b0.header)
        self.assertTrue(state.add_root_block(root_block))

        b1 = state.get_tip().create_block_to_append()
        b2 = state.get_tip().create_block_to_append(nonce=1)
        b2.header.hash_prev_root_block = root_block.header.get_hash()
        b3 = state.get_tip().create_block_to_append(nonce=2)
        b3.header.hash_prev_root_block = root_block.header.get_hash()

        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)

        # Fork happens, although they have the same height, b2 survives since it confirms root block
        state.finalize_and_add_block(b2)
        self.assertEqual(state.header_tip, b2.header)

        # b3 confirms the same root block as b2, so it will not override b2
        state.finalize_and_add_block(b3)
        self.assertEqual(state.header_tip, b2.header)

    def test_shard_state_difficulty(self):
        env = get_test_env()
        for shard_config in env.quark_chain_config.shards.values():
            shard_config.GENESIS.DIFFICULTY = 10000

        env.quark_chain_config.SKIP_MINOR_DIFFICULTY_CHECK = False
        diff_calc = EthDifficultyCalculator(cutoff=9, diff_factor=2048, minimum_diff=1)
        env.quark_chain_config.NETWORK_ID = (
            1
        )  # other network ids will skip difficulty check
        state = create_default_shard_state(env=env, shard_id=0, diff_calc=diff_calc)

        # Check new difficulty
        b0 = state.create_block_to_mine(state.header_tip.create_time + 8)
        self.assertEqual(
            b0.header.difficulty,
            state.header_tip.difficulty // 2048 + state.header_tip.difficulty,
        )
        b0 = state.create_block_to_mine(state.header_tip.create_time + 9)
        self.assertEqual(b0.header.difficulty, state.header_tip.difficulty)
        b0 = state.create_block_to_mine(state.header_tip.create_time + 17)
        self.assertEqual(b0.header.difficulty, state.header_tip.difficulty)
        b0 = state.create_block_to_mine(state.header_tip.create_time + 24)
        self.assertEqual(
            b0.header.difficulty,
            state.header_tip.difficulty - state.header_tip.difficulty // 2048,
        )
        b0 = state.create_block_to_mine(state.header_tip.create_time + 35)
        self.assertEqual(
            b0.header.difficulty,
            state.header_tip.difficulty - state.header_tip.difficulty // 2048 * 2,
        )

    def test_shard_state_recovery_from_root_block(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        block_headers = [state.header_tip]
        block_meta = [state.meta_tip]
        for i in range(12):
            b = state.get_tip().create_block_to_append(address=acc1)
            state.finalize_and_add_block(b)
            block_headers.append(b.header)
            block_meta.append(b.meta)

        # add a fork
        b1 = state.db.get_minor_block_by_height(3)
        b1.header.create_time += 1
        state.finalize_and_add_block(b1)
        self.assertEqual(state.db.get_minor_block_by_hash(b1.header.get_hash()), b1)

        root_block = state.root_tip.create_block_to_append()
        root_block.minor_block_header_list = block_headers[:5]
        root_block.finalize()

        state.add_root_block(root_block)

        recovered_state = ShardState(env=env, full_shard_id=2 | 0)

        recovered_state.init_from_root_block(root_block)
        self.assertEqual(
            recovered_state.db.get_minor_block_by_hash(b1.header.get_hash()), b1
        )

        self.assertEqual(recovered_state.root_tip, root_block.header)
        self.assertEqual(recovered_state.header_tip, block_headers[4])
        self.assertEqual(recovered_state.confirmed_header_tip, block_headers[4])
        self.assertEqual(recovered_state.meta_tip, block_meta[4])
        self.assertEqual(
            recovered_state.evm_state.trie.root_hash, block_meta[4].hash_evm_state_root
        )

    def test_shard_state_recovery_from_genesis(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        block_headers = [state.header_tip]
        block_meta = [state.meta_tip]
        for i in range(12):
            b = state.get_tip().create_block_to_append(address=acc1)
            state.finalize_and_add_block(b)
            block_headers.append(b.header)
            block_meta.append(b.meta)

        # Add a few empty root blocks
        root_block = None
        for i in range(3):
            root_block = state.root_tip.create_block_to_append()
            root_block.finalize()
            state.add_root_block(root_block)

        recovered_state = ShardState(env=env, full_shard_id=2 | 0)

        # expect to recover from genesis
        recovered_state.init_from_root_block(root_block)

        genesis = state.db.get_minor_block_by_height(0)
        self.assertEqual(recovered_state.root_tip, root_block.header)
        self.assertEqual(recovered_state.header_tip, genesis.header)
        self.assertIsNone(recovered_state.confirmed_header_tip)
        self.assertEqual(recovered_state.meta_tip, genesis.meta)
        self.assertEqual(
            recovered_state.evm_state.trie.root_hash, genesis.meta.hash_evm_state_root
        )

    def test_add_block_receipt_root_not_match(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        b1 = state.create_block_to_mine(address=acc3)

        # Should succeed
        state.finalize_and_add_block(b1)
        evm_state = state.run_block(b1)
        b1.finalize(
            evm_state=evm_state, coinbase_amount_map=b1.header.coinbase_amount_map
        )
        b1.meta.hash_evm_receipt_root = bytes(32)

    def test_not_update_tip_on_root_fork(self):
        """ block's hash_prev_root_block must be on the same chain with root_tip to update tip.

                 +--+
              a. |r1|
                /+--+
               /   |
        +--+  /  +--+    +--+
        |r0|<----|m1|<---|m2| c.
        +--+  \  +--+    +--+
               \   |      |
                \+--+     |
              b. |r2|<----+
                 +--+

        Initial state: r0 <- m1
        Then adding r1, r2, m2 should not make m2 the tip because r1 is the root tip and r2 and r1
        are not on the same root chain.
        """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        # m1 is the genesis block
        m1 = state.db.get_minor_block_by_height(0)

        r1 = state.root_tip.create_block_to_append()
        r2 = state.root_tip.create_block_to_append()
        r1.minor_block_header_list.append(m1.header)
        r1.finalize()

        state.add_root_block(r1)

        r2.minor_block_header_list.append(m1.header)
        r2.header.create_time = r1.header.create_time + 1  # make r2, r1 different
        r2.finalize()
        self.assertNotEqual(r1.header.get_hash(), r2.header.get_hash())

        state.add_root_block(r2)

        self.assertEqual(state.root_tip, r1.header)

        m2 = m1.create_block_to_append(address=acc1)
        m2.header.hash_prev_root_block = r2.header.get_hash()

        state.finalize_and_add_block(m2)
        # m2 is added
        self.assertEqual(state.db.get_minor_block_by_hash(m2.header.get_hash()), m2)
        # but m1 should still be the tip
        self.assertEqual(state.header_tip, m1.header)

    def test_add_root_block_revert_header_tip(self):
        """ block's hash_prev_root_block must be on the same chain with root_tip to update tip.

                 +--+
                 |r1|<-------------+
                /+--+              |
               /   |               |
        +--+  /  +--+    +--+     +--+
        |r0|<----|m1|<---|m2| <---|m3|
        +--+  \  +--+    +--+     +--+
          |    \   |       \
          |     \+--+.     +--+
          |      |r2|<-----|r3| (r3 includes m2)
          |      +--+      +--+
          |
          |      +--+
          +-----+|r4| (r4 includes m1)
                 +--+

        Initial state: r0 <- m1 <- m2
        Adding r1, r2, m3 makes r1 the root_tip, m3 the header_tip
        Adding r3 should change the root_tip to r3, header_tip to m2
        Adding r4 (greater total diff) will reset root_tip to r4, header_tip to m2
        """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env, shard_id=0)

        # m1 is the genesis block
        m1 = state.db.get_minor_block_by_height(0)

        m2 = state.get_tip().create_block_to_append(address=acc1)
        state.finalize_and_add_block(m2)

        r0 = state.root_tip
        r1 = r0.create_block_to_append()
        r2 = r0.create_block_to_append()
        r1.minor_block_header_list.append(m1.header)
        r1.finalize()

        state.add_root_block(r1)

        r2.minor_block_header_list.append(m1.header)
        r2.header.create_time = r1.header.create_time + 1  # make r2, r1 different
        r2.finalize()
        self.assertNotEqual(r1.header.get_hash(), r2.header.get_hash())

        state.add_root_block(r2)

        self.assertEqual(state.root_tip, r1.header)

        m3 = state.create_block_to_mine(address=acc1)
        self.assertEqual(m3.header.hash_prev_root_block, r1.header.get_hash())
        state.finalize_and_add_block(m3)

        r3 = r2.create_block_to_append(address=acc1)
        r3.add_minor_block_header(m2.header)
        r3.finalize()
        state.add_root_block(r3)
        self.assertEqual(state.root_tip, r3.header)
        self.assertEqual(state.header_tip, m2.header)

        # greater total diff
        r4 = r0.create_block_to_append(difficulty=r3.header.total_difficulty * 2)
        r4.minor_block_header_list.append(m1.header)
        r4.finalize()
        state.add_root_block(r4)
        self.assertEqual(state.root_tip, r4.header)
        self.assertEqual(state.header_tip, m2.header)

    def test_posw_fetch_previous_coinbase_address(self):
        acc = Address.create_from_identity(
            Identity.create_random_identity(), full_shard_key=0
        )
        env = get_test_env(genesis_account=acc, genesis_minor_quarkash=0)
        state = create_default_shard_state(env=env, shard_id=0, posw_override=True)

        m = state.get_tip().create_block_to_append(address=acc)
        coinbase_blockcnt = state._get_posw_coinbase_blockcnt(
            m.header.hash_prev_minor_block
        )
        self.assertEqual(len(coinbase_blockcnt), 1)  # Genesis
        state.finalize_and_add_block(m)

        # Note PoSW window size is 3, configured in `create_default_shard_state`
        prev_addr = None
        for i in range(4):
            random_acc = Address.create_random_account(full_shard_key=0)
            m = state.get_tip().create_block_to_append(address=random_acc)
            coinbase_blockcnt = state._get_posw_coinbase_blockcnt(
                m.header.hash_prev_minor_block
            )
            self.assertEqual(len(coinbase_blockcnt), 2)
            # Count should all equal 1
            self.assertEqual(len(set(coinbase_blockcnt.values())), 1)
            self.assertEqual(list(coinbase_blockcnt.values())[0], 1)
            if prev_addr:  # Should always contain previous block's coinbase
                self.assertTrue(prev_addr in coinbase_blockcnt)
            state.finalize_and_add_block(m)
            prev_addr = random_acc.recipient

        # Cached should have certain items (>= 5)
        self.assertGreaterEqual(len(state.coinbase_addr_cache), 5)

    def test_posw_coinbase_send_under_limit(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        id2 = Identity.create_random_identity()
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=0)
        state = create_default_shard_state(env=env, shard_id=0, posw_override=True)
        state.shard_config.COINBASE_AMOUNT = 8
        state.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK = 2
        state.shard_config.POSW_CONFIG.WINDOW_SIZE = 4

        # Add a root block to have all the shards initialized, also include the genesis from
        # another shard to allow x-shard tx TO that shard
        root_block = state.root_tip.create_block_to_append()
        root_block.add_minor_block_header(
            create_default_shard_state(env=env, shard_id=1).header_tip
        )
        state.add_root_block(root_block.finalize())

        m = state.get_tip().create_block_to_append(address=acc1)
        state.finalize_and_add_block(m)
        self.assertEqual(len(state.evm_state.sender_disallow_map), 2)
        self.assertEqual(
            state.get_token_balance(acc1.recipient, self.genesis_token),
            state.shard_config.COINBASE_AMOUNT // 2,  # tax rate is 0.5
        )

        self.assertEqual(
            state.evm_state.sender_disallow_map, {bytes(20): 2, acc1.recipient: 2}
        )

        # Try to send money from that account
        tx0 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=Address.create_empty_account(0),
            value=1,
            gas=21000,
            gas_price=0,
        )
        res = state.execute_tx(tx0, acc1)
        self.assertIsNotNone(res, "tx should succeed")

        # Create a block including that tx, receipt should also report error
        self.assertTrue(state.add_tx(tx0))
        m = state.create_block_to_mine(address=acc2)
        state.finalize_and_add_block(m)
        self.assertEqual(
            state.get_token_balance(acc1.recipient, self.genesis_token),
            state.shard_config.COINBASE_AMOUNT // 2 - 1,  # tax rate is 0.5
        )
        self.assertEqual(
            state.evm_state.sender_disallow_map,
            {bytes(20): 2, acc1.recipient: 2, acc2.recipient: 2},
        )

        tx1 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=Address.create_empty_account(0),
            value=2,
            gas=21000,
            gas_price=0,
        )
        res = state.execute_tx(tx1)
        self.assertIsNone(res, "tx should fail")

        # Create a block including that tx, receipt should also report error
        self.assertTrue(state.add_tx(tx1))
        m = state.create_block_to_mine(address=acc2)
        state.finalize_and_add_block(m)
        self.assertEqual(
            state.get_token_balance(acc1.recipient, self.genesis_token),
            state.shard_config.COINBASE_AMOUNT // 2 - 1,  # tax rate is 0.5
        )
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token),
            state.shard_config.COINBASE_AMOUNT,  # tax rate is 0.5
        )
        self.assertEqual(
            state.evm_state.sender_disallow_map, {acc1.recipient: 2, acc2.recipient: 4}
        )

        tx2 = create_transfer_transaction(
            shard_state=state,
            key=id2.get_key(),
            from_address=acc2,
            to_address=Address.create_empty_account(0),
            value=5,
            gas=21000,
            gas_price=0,
        )
        res = state.execute_tx(tx2)
        self.assertIsNone(res, "tx should fail")

        tx3 = create_transfer_transaction(
            shard_state=state,
            key=id2.get_key(),
            from_address=acc2,
            to_address=Address.create_empty_account(0),
            value=4,
            gas=21000,
            gas_price=0,
        )
        res = state.execute_tx(tx3, acc2)
        self.assertIsNotNone(res, "tx should succeed")

    def test_posw_coinbase_send_equal_locked(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=0)
        state = create_default_shard_state(env=env, shard_id=0, posw_override=True)
        state.shard_config.COINBASE_AMOUNT = 10
        state.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK = 2
        state.shard_config.POSW_CONFIG.WINDOW_SIZE = 4

        # Add a root block to have all the shards initialized, also include the genesis from
        # another shard to allow x-shard tx TO that shard
        root_block = state.root_tip.create_block_to_append()
        root_block.add_minor_block_header(
            create_default_shard_state(env=env, shard_id=1).header_tip
        )
        state.add_root_block(root_block.finalize())

        m = state.create_block_to_mine(address=acc1)
        state.finalize_and_add_block(m)

        self.assertEqual(len(state.evm_state.sender_disallow_map), 2)
        self.assertEqual(
            state.get_token_balance(acc1.recipient, self.genesis_token),
            state.shard_config.COINBASE_AMOUNT // 2,  # tax rate is 0.5
        )

        self.assertEqual(
            state.evm_state.sender_disallow_map, {bytes(20): 2, acc1.recipient: 2}
        )

        # Try to send money from that account, the expected locked tokens are 4
        tx0 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=Address.create_empty_account(0),
            value=1,
            gas=21000,
            gas_price=0,
        )
        state.tx_queue.add_transaction(tx0)

        m = state.create_block_to_mine(address=acc1)
        state.finalize_and_add_block(m)

        r = state.get_transaction_receipt(tx0.get_hash())
        self.assertEqual(r[2].success, b"\x01")  # Success

        self.assertEqual(
            state.get_token_balance(acc1.recipient, self.genesis_token),
            state.shard_config.COINBASE_AMOUNT - 1,
        )

    def test_posw_coinbase_send_above_locked(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1 << 16)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=1000000)
        state = create_default_shard_state(env=env, shard_id=0, posw_override=True)
        state.shard_config.COINBASE_AMOUNT = 10
        state.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK = 500000
        state.shard_config.POSW_CONFIG.WINDOW_SIZE = 4

        # Add a root block to have all the shards initialized, also include the genesis from
        # another shard to allow x-shard tx TO that shard
        root_block = state.root_tip.create_block_to_append()
        root_block.add_minor_block_header(
            create_default_shard_state(env=env, shard_id=1).header_tip
        )
        state.add_root_block(root_block.finalize())

        m = state.create_block_to_mine(address=acc1)
        state.finalize_and_add_block(m)

        self.assertEqual(len(state.evm_state.sender_disallow_map), 2)
        self.assertEqual(
            state.get_token_balance(acc1.recipient, self.genesis_token),
            1000000 + state.shard_config.COINBASE_AMOUNT // 2,  # tax rate is 0.5
        )

        self.assertEqual(
            state.evm_state.sender_disallow_map,
            {bytes(20): 500000, acc1.recipient: 500000},
        )

        # Try to send money from that account, the expected locked tokens are 2 * 500000
        tx0 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=Address.create_empty_account(0),
            value=100,
            gas=21000,
            gas_price=0,
        )
        self.assertTrue(state.add_tx(tx0))
        tx1 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=2,
            gas=30000,
            gas_price=1,
            nonce=tx0.tx.to_evm_tx().nonce + 1,
        )
        self.assertTrue(state.add_tx(tx1))

        m = state.create_block_to_mine(address=acc1)
        self.assertEqual(len(m.tx_list), 2)
        state.finalize_and_add_block(m)

        r0 = state.get_transaction_receipt(tx0.get_hash())
        self.assertEqual(r0[2].success, b"")  # Failure
        r1 = state.get_transaction_receipt(tx1.get_hash())
        self.assertEqual(r1[2].success, b"")  # Failure

        self.assertEqual(
            state.get_token_balance(acc1.recipient, self.genesis_token),
            1000000
            + state.shard_config.COINBASE_AMOUNT
            - 30000 // 2,  # tax rate is 0.5
        )

    def test_posw_validate_minor_block_seal(self):
        acc = Address(b"\x01" * 20, full_shard_key=0)
        env = get_test_env(genesis_account=acc, genesis_minor_quarkash=256)
        state = create_default_shard_state(env=env, shard_id=0, posw_override=True)
        # Force PoSW
        state.shard_config.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
        state.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK = 1
        state.shard_config.POSW_CONFIG.WINDOW_SIZE = 256
        state.shard_config.POSW_CONFIG.DIFF_DIVIDER = 1000

        self.assertEqual(
            state.get_token_balance(acc.recipient, self.genesis_token), 256
        )
        genesis = Address(bytes(20), 0)
        self.assertEqual(
            state.get_token_balance(genesis.recipient, self.genesis_token), 0
        )

        # Genesis already has 1 block but zero stake, so no change to block diff
        m = state.get_tip().create_block_to_append(address=genesis, difficulty=1000)
        with self.assertRaises(ValueError):
            state.finalize_and_add_block(m)

        # Total stake * block PoSW is 256, so acc should pass the check no matter
        # how many blocks he mined before
        for i in range(4):
            for nonce in range(4):  # Try different nonce
                m = state.get_tip().create_block_to_append(
                    address=acc, difficulty=1000, nonce=nonce
                )
                state.validate_minor_block_seal(m)
            state.finalize_and_add_block(m)
            b1, extra1 = state.get_minor_block_by_hash(m.header.get_hash(), True)
            b2, extra2 = state.get_minor_block_by_height(m.header.height, True)
            self.assertTrue(m.header == b1.header == b2.header)
            self.assertDictEqual(extra1, extra2)
            self.assertEqual(extra1["effective_difficulty"], 1000 / 1000)
            self.assertEqual(extra1["posw_mineable_blocks"], 256)
            self.assertEqual(extra1["posw_mined_blocks"], i + 1)

    def test_posw_window_edge_cases(self):
        acc = Address(b"\x01" * 20, full_shard_key=0)
        env = get_test_env(genesis_account=acc, genesis_minor_quarkash=500)
        state = create_default_shard_state(
            env=env, shard_id=0, posw_override=True, no_coinbase=True
        )
        # Force PoSW
        state.shard_config.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
        state.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK = 500
        state.shard_config.POSW_CONFIG.WINDOW_SIZE = 2
        state.shard_config.POSW_CONFIG.DIFF_DIVIDER = 1000

        # Use 0 to denote blocks mined by others, 1 for blocks mined by acc,
        # stake * state per block = 1 for acc, 0 <- [curr], so current block
        # should enjoy the diff adjustment
        m = state.get_tip().create_block_to_append(address=acc, difficulty=1000)
        state.finalize_and_add_block(m)

        # Make sure stakes didn't change
        self.assertEqual(
            state.get_token_balance(acc.recipient, self.genesis_token), 500
        )
        # 0 <- 1 <- [curr], the window already has one block with PoSW benefit,
        # mining new blocks should fail
        m = state.get_tip().create_block_to_append(address=acc, difficulty=1000)
        with self.assertRaises(ValueError):
            state.finalize_and_add_block(m)

    def test_incorrect_coinbase_amount(self):
        env = get_test_env()
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        b = state.create_block_to_mine()
        evm_state = state.run_block(b)
        b.finalize(
            evm_state=evm_state,
            coinbase_amount_map=state.get_coinbase_amount_map(b.header.height),
        )
        state.add_block(b)

        b = state.create_block_to_mine()
        wrong_coinbase = state.get_coinbase_amount_map(b.header.height)
        wrong_coinbase.add({self.genesis_token: +1})
        b.finalize(evm_state=evm_state, coinbase_amount_map=wrong_coinbase)
        with self.assertRaises(ValueError):
            state.add_block(b)

    def test_shard_coinbase_decay(self):
        env = get_test_env()
        state = create_default_shard_state(env=env)
        coinbase = state.get_coinbase_amount_map(state.shard_config.EPOCH_INTERVAL)
        self.assertEqual(
            coinbase.balance_map,
            {
                env.quark_chain_config.genesis_token: state.shard_config.COINBASE_AMOUNT
                * env.quark_chain_config.BLOCK_REWARD_DECAY_FACTOR
                * env.quark_chain_config.REWARD_TAX_RATE
            },
        )
        coinbase = state.get_coinbase_amount_map(state.shard_config.EPOCH_INTERVAL + 1)
        self.assertEqual(
            coinbase.balance_map,
            {
                env.quark_chain_config.genesis_token: state.shard_config.COINBASE_AMOUNT
                * env.quark_chain_config.BLOCK_REWARD_DECAY_FACTOR
                * env.quark_chain_config.REWARD_TAX_RATE
            },
        )
        coinbase = state.get_coinbase_amount_map(state.shard_config.EPOCH_INTERVAL * 2)
        self.assertEqual(
            coinbase.balance_map,
            {
                env.quark_chain_config.genesis_token: state.shard_config.COINBASE_AMOUNT
                * env.quark_chain_config.BLOCK_REWARD_DECAY_FACTOR ** 2
                * env.quark_chain_config.REWARD_TAX_RATE
            },
        )

    def test_enable_tx_timestamp(self):
        # whitelist acc1, make tx to acc2
        # but do not whitelist acc2 and tx fails
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        id2 = Identity.create_random_identity()
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=5000000,
            gas=50000,
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine()
        self.assertEqual(len(b1.tx_list), 1)

        env.quark_chain_config.ENABLE_TX_TIMESTAMP = b1.header.create_time + 100
        env.quark_chain_config.TX_WHITELIST_SENDERS = [acc1.recipient.hex()]
        b2 = state.create_block_to_mine()
        self.assertEqual(len(b2.tx_list), 1)
        state.finalize_and_add_block(b2)

        tx2 = create_transfer_transaction(
            shard_state=state,
            key=id2.get_key(),
            from_address=acc2,
            to_address=acc3,
            value=12345,
            gas=50000,
        )
        env.quark_chain_config.ENABLE_TX_TIMESTAMP = None
        self.assertTrue(state.add_tx(tx2))
        b3 = state.create_block_to_mine()
        self.assertEqual(len(b3.tx_list), 1)
        env.quark_chain_config.ENABLE_TX_TIMESTAMP = b1.header.create_time + 100
        b4 = state.create_block_to_mine()
        self.assertEqual(len(b4.tx_list), 0)

        with self.assertRaisesRegexp(
            RuntimeError, "unwhitelisted senders not allowed before tx is enabled"
        ):
            state.finalize_and_add_block(b3)

    def test_enable_evm_timestamp_with_contract_create(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx = create_contract_creation_transaction(
            shard_state=state, key=id1.get_key(), from_address=acc1, to_full_shard_key=0
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine()
        self.assertEqual(len(b1.tx_list), 1)

        env.quark_chain_config.ENABLE_EVM_TIMESTAMP = b1.header.create_time + 100
        b2 = state.create_block_to_mine()
        self.assertEqual(len(b2.tx_list), 0)

        with self.assertRaisesRegexp(
            RuntimeError, "smart contract tx is not allowed before evm is enabled"
        ):
            state.finalize_and_add_block(b1)

    def test_enable_evm_timestamp_with_contract_call(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=50000,
            data=b"1234",
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine()
        self.assertEqual(len(b1.tx_list), 1)

        env.quark_chain_config.ENABLE_EVM_TIMESTAMP = b1.header.create_time + 100
        b2 = state.create_block_to_mine()
        self.assertEqual(len(b2.tx_list), 0)

        with self.assertRaisesRegexp(
            RuntimeError, "smart contract tx is not allowed before evm is enabled"
        ):
            state.finalize_and_add_block(b1)

    def test_qkchashx_qkchash_with_rotation_stats(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        state.shard_config.CONSENSUS_TYPE = ConsensusType.POW_QKCHASH
        # set the initial enabled Qkchashx block height to one
        state.env.quark_chain_config.ENABLE_QKCHASHX_HEIGHT = 1

        # generate and mine a minor block
        def _testcase_generate_and_mine_minor_block(qkchash_with_rotation_stats):
            block = state.get_tip().create_block_to_append(address=acc1, difficulty=5)
            evm_state = state.run_block(block)
            coinbase_amount_map = state.get_coinbase_amount_map(block.header.height)
            coinbase_amount_map.add(evm_state.block_fee_tokens)
            block.finalize(evm_state=evm_state, coinbase_amount_map=coinbase_amount_map)

            # mine the block using QkchashMiner
            miner = QkchashMiner(
                1,
                5,
                block.header.get_hash_for_mining(),
                qkchash_with_rotation_stats=qkchash_with_rotation_stats,
            )
            nonce_found, mixhash = miner.mine(rounds=100)
            block.header.nonce = int.from_bytes(nonce_found, byteorder="big")
            block.header.mixhash = mixhash
            return block

        b1 = _testcase_generate_and_mine_minor_block(True)
        # validate the minor block and make sure it works for qkchashX using the new flag
        validate_seal(
            b1.header, ConsensusType.POW_QKCHASH, qkchash_with_rotation_stats=True
        )
        with self.assertRaises(ValueError):
            validate_seal(
                b1.header, ConsensusType.POW_QKCHASH, qkchash_with_rotation_stats=False
            )
        state.finalize_and_add_block(b1)

        # change the enabled Qkchashx block height and make sure it works for original qkchash
        state.env.quark_chain_config.ENABLE_QKCHASHX_HEIGHT = 100

        b2 = _testcase_generate_and_mine_minor_block(False)
        validate_seal(
            b2.header, ConsensusType.POW_QKCHASH, qkchash_with_rotation_stats=False
        )
        with self.assertRaises(ValueError):
            validate_seal(
                b2.header, ConsensusType.POW_QKCHASH, qkchash_with_rotation_stats=True
            )
        state.finalize_and_add_block(b2)

    def test_failed_transaction_gas(self):
        """in-shard revert contract transaction validating the failed transaction gas used
        """
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={self.genesis_token_str: 200 * 10 ** 18},
        )
        state = create_default_shard_state(env=env)
        # Create failed contract with revert operation
        contract_creation_with_revert_bytecode = (
            "6080604052348015600f57600080fd5b50600080fdfe"
        )
        """
        pragma solidity ^0.5.1;
        contract RevertContract {
            constructor() public {
                revert();
            }
        }
        """
        # This transaction cost is calculated by remix, which is different than the opcodes.GTXCOST due to revert.
        FAILED_TRANSACTION_COST = 54416
        tx = contract_creation_tx(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_full_shard_key=acc1.full_shard_key,
            bytecode=contract_creation_with_revert_bytecode,
            gas_token_id=self.genesis_token,
            transfer_token_id=self.genesis_token,
        )
        # Should succeed
        self.assertTrue(state.add_tx(tx))
        b1 = state.create_block_to_mine(address=acc2)
        self.assertEqual(len(b1.tx_list), 1)

        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)

        # Check receipts and make sure the transaction is failed
        self.assertEqual(len(state.evm_state.receipts), 1)
        self.assertEqual(state.evm_state.receipts[0].state_root, b"")
        self.assertEqual(state.evm_state.receipts[0].gas_used, FAILED_TRANSACTION_COST)

        # Make sure the FAILED_TRANSACTION_COST is consumed by the sender
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            200 * 10 ** 18 - FAILED_TRANSACTION_COST,
        )
        # Make sure the accurate gas fee is obtained by the miner
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token),
            self.get_after_tax_reward(FAILED_TRANSACTION_COST + self.shard_coinbase),
        )
        self.assertEqual(
            b1.header.coinbase_amount_map.balance_map,
            {
                env.quark_chain_config.genesis_token: self.get_after_tax_reward(
                    FAILED_TRANSACTION_COST + self.shard_coinbase
                )
            },
        )

    def test_skip_under_priced_tx_to_block(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)

        # Price threshold for packing into blocks is 10
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        env.quark_chain_config.MIN_MINING_GAS_PRICE = 10

        state = create_default_shard_state(env=env)

        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)

        # Under-priced
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=50000,
            data=b"1234",
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine()
        self.assertEqual(len(b1.tx_list), 0)
        self.assertEqual(len(state.tx_queue), 0)

        # Qualified
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=50000,
            data=b"1234",
            gas_price=11,
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine()
        self.assertEqual(len(b1.tx_list), 1)
        self.assertEqual(len(state.tx_queue), 1)

    def test_get_root_chain_stakes(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        evm_state = state.evm_state  # type: EvmState

        # contract not deployed yet
        stakes, signer = state.get_root_chain_stakes(
            acc1.recipient, state.header_tip.get_hash(), mock_evm_state=evm_state
        )
        self.assertEqual(stakes, 0)
        self.assertEqual(signer, bytes(20))

        contract_runtime_bytecode = bytes.fromhex(
            "60806040526004361061007b5760003560e01c8063853828b61161004e578063853828b6146101b5578063a69df4b5146101ca578063f83d08ba146101df578063fd8c4646146101e75761007b565b806316934fc4146100d85780632e1a7d4d1461013c578063485d3834146101685780636c19e7831461018f575b336000908152602081905260409020805460ff16156100cb5760405162461bcd60e51b815260040180806020018281038252602681526020018061062e6026913960400191505060405180910390fd5b6100d5813461023b565b50005b3480156100e457600080fd5b5061010b600480360360208110156100fb57600080fd5b50356001600160a01b031661029b565b6040805194151585526020850193909352838301919091526001600160a01b03166060830152519081900360800190f35b34801561014857600080fd5b506101666004803603602081101561015f57600080fd5b50356102cf565b005b34801561017457600080fd5b5061017d61034a565b60408051918252519081900360200190f35b610166600480360360208110156101a557600080fd5b50356001600160a01b0316610351565b3480156101c157600080fd5b506101666103c8565b3480156101d657600080fd5b50610166610436565b6101666104f7565b3480156101f357600080fd5b5061021a6004803603602081101561020a57600080fd5b50356001600160a01b0316610558565b604080519283526001600160a01b0390911660208301528051918290030190f35b8015610297576002820154808201908111610291576040805162461bcd60e51b81526020600482015260116024820152706164646974696f6e206f766572666c6f7760781b604482015290519081900360640190fd5b60028301555b5050565b600060208190529081526040902080546001820154600283015460039093015460ff9092169290916001600160a01b031684565b336000908152602081905260409020805460ff1680156102f3575080600101544210155b6102fc57600080fd5b806002015482111561030d57600080fd5b6002810180548390039055604051339083156108fc029084906000818181858888f19350505050158015610345573d6000803e3d6000fd5b505050565b6203f48081565b336000908152602081905260409020805460ff16156103a15760405162461bcd60e51b81526004018080602001828103825260268152602001806106546026913960400191505060405180910390fd5b6003810180546001600160a01b0319166001600160a01b038416179055610297813461023b565b6103d06105fa565b5033600090815260208181526040918290208251608081018452815460ff16151581526001820154928101929092526002810154928201839052600301546001600160a01b031660608201529061042657600080fd5b61043381604001516102cf565b50565b336000908152602081905260409020805460ff16156104865760405162461bcd60e51b815260040180806020018281038252602b8152602001806106a1602b913960400191505060405180910390fd5b60008160020154116104df576040805162461bcd60e51b815260206004820152601b60248201527f73686f756c642068617665206578697374696e67207374616b65730000000000604482015290519081900360640190fd5b805460ff191660019081178255426203f48001910155565b336000908152602081905260409020805460ff166105465760405162461bcd60e51b815260040180806020018281038252602781526020018061067a6027913960400191505060405180910390fd5b805460ff19168155610433813461023b565b6000806105636105fa565b506001600160a01b03808416600090815260208181526040918290208251608081018452815460ff161580158252600183015493820193909352600282015493810193909352600301549092166060820152906105c75750600091508190506105f5565b60608101516000906001600160a01b03166105e35750836105ea565b5060608101515b604090910151925090505b915091565b6040518060800160405280600015158152602001600081526020016000815260200160006001600160a01b03168152509056fe73686f756c64206f6e6c7920616464207374616b657320696e206c6f636b656420737461746573686f756c64206f6e6c7920736574207369676e657220696e206c6f636b656420737461746573686f756c64206e6f74206c6f636b20616c72656164792d6c6f636b6564206163636f756e747373686f756c64206e6f7420756e6c6f636b20616c72656164792d756e6c6f636b6564206163636f756e7473a265627a7a72315820f2c044ad50ee08e7e49c575b49e8de27cac8322afdb97780b779aa1af44e40d364736f6c634300050b0032"
        )
        env.quark_chain_config.ROOT_CHAIN_POSW_CONTRACT_BYTECODE_HASH = sha3_256(
            contract_runtime_bytecode
        ).hex()
        contract_addr = SystemContract.ROOT_CHAIN_POSW.addr()
        evm_state.set_code(contract_addr, contract_runtime_bytecode)
        evm_state.commit()

        # contract deployed, but no stakes. signer defaults to the recipient
        stakes, signer = state.get_root_chain_stakes(
            acc1.recipient, state.header_tip.get_hash(), mock_evm_state=evm_state
        )
        self.assertEqual(stakes, 0)
        self.assertEqual(signer, acc1.recipient)

        def tx_gen(nonce, value, data: str):
            ret = create_transfer_transaction(
                nonce=nonce,
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=Address(contract_addr, 0),
                value=value,
                gas=100000,
                gas_price=0,
                data=bytes.fromhex(data),
            ).tx.to_evm_tx()
            ret.set_quark_chain_config(env.quark_chain_config)
            return ret

        add_stake_tx = lambda n, v: tx_gen(n, v, "")
        set_signer_tx = lambda n, v, a: tx_gen(
            n, v, "6c19e783000000000000000000000000" + a.recipient.hex()
        )
        withdraw_tx = lambda n: tx_gen(n, 0, "853828b6")
        unlock_tx = lambda n: tx_gen(n, 0, "a69df4b5")
        lock_tx = lambda n, v: tx_gen(n, v, "f83d08ba")

        # add stakes and set signer
        tx0 = add_stake_tx(0, 1234)
        success, _ = apply_transaction(evm_state, tx0, bytes(32))
        self.assertTrue(success)
        random_signer = Address.create_random_account()
        tx1 = set_signer_tx(1, 4321, random_signer)
        success, _ = apply_transaction(evm_state, tx1, bytes(32))
        self.assertTrue(success)

        evm_state.commit()
        stakes, signer = state.get_root_chain_stakes(
            acc1.recipient, state.header_tip.get_hash(), mock_evm_state=evm_state
        )
        self.assertEqual(stakes, 1234 + 4321)
        self.assertEqual(signer, random_signer.recipient)

        # can't withdraw during locking
        tx2 = withdraw_tx(2)
        success, _ = apply_transaction(evm_state, tx2, bytes(32))
        self.assertFalse(success)

        # unlock should succeed
        tx3 = unlock_tx(3)
        success, _ = apply_transaction(evm_state, tx3, bytes(32))
        self.assertTrue(success)
        # but still can't withdraw
        tx4 = withdraw_tx(4)
        success, _ = apply_transaction(evm_state, tx4, bytes(32))
        self.assertFalse(success)
        # and can't add stakes or set signer either
        tx5 = add_stake_tx(5, 100)
        success, _ = apply_transaction(evm_state, tx5, bytes(32))
        self.assertFalse(success)
        tx6 = set_signer_tx(6, 0, acc1)
        success, _ = apply_transaction(evm_state, tx6, bytes(32))
        self.assertFalse(success)

        # now stakes should be 0 when unlocked
        evm_state.commit()
        stakes, signer = state.get_root_chain_stakes(
            acc1.recipient, state.header_tip.get_hash(), mock_evm_state=evm_state
        )
        self.assertEqual(stakes, 0)
        self.assertEqual(signer, bytes(20))

        # 4 days passed, should be able to withdraw
        evm_state.timestamp += 3600 * 24 * 4
        balance_before = evm_state.get_balance(acc1.recipient)
        tx7 = withdraw_tx(7)
        success, _ = apply_transaction(evm_state, tx7, bytes(32))
        self.assertTrue(success)
        balance_after = evm_state.get_balance(acc1.recipient)
        self.assertEqual(balance_before + 5555, balance_after)

        # lock again
        tx8 = lock_tx(8, 42)
        success, _ = apply_transaction(evm_state, tx8, bytes(32))
        self.assertTrue(success)

        # should be able to get stakes
        evm_state.commit()
        stakes, signer = state.get_root_chain_stakes(
            acc1.recipient, state.header_tip.get_hash(), mock_evm_state=evm_state
        )
        self.assertEqual(stakes, 42)
        self.assertEqual(signer, random_signer.recipient)

    def test_remove_tx_from_queue_with_higher_nonce(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)

        tx1 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=11,
            nonce=1,
        )
        self.assertTrue(state.add_tx(tx1))

        tx2 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=22,
            nonce=1,
        )
        self.assertTrue(state.add_tx(tx2))

        tx3 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=33,
            nonce=0,
        )
        self.assertTrue(state.add_tx(tx3))

        self.assertEqual(len(state.tx_queue), 3)

        b0 = state.get_tip().create_block_to_append()
        b0.add_tx(tx3)
        b0.add_tx(tx1)

        self.assertEqual(len(b0.tx_list), 2)
        self.assertEqual(len(state.tx_queue), 3)

        state.finalize_and_add_block(b0)
        self.assertEqual(len(state.tx_queue), 0)

    def test_pay_as_gas_utility(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        evm_state = state.evm_state

        # contract not deployed yet
        refund_percentage, gas_price = evm_state.pay_as_gas(123, 1, 1, evm_state)
        self.assertEqual(refund_percentage, 0)
        self.assertEqual(gas_price, 0)

        contract_runtime_bytecode = bytes.fromhex(
            "6080604052600436106100f35760003560e01c80639ed2c7ef1161008a578063ce9e8c4711610059578063ce9e8c4714610299578063ceca0318146102c8578063dc68f0a0146102db578063f9c94eb7146102f0576100f3565b80639ed2c7ef14610217578063bb92885414610237578063beb92f5514610257578063bffa8bd014610277576100f3565b80636d27af8c116100c65780636d27af8c1461019e578063735e0e19146101c05780637e081270146101d35780639447e58c146101e8576100f3565b806313dee215146100f857806321a2b36e1461012e57806356e4b68b1461014e5780635ae8f7f114610170575b600080fd5b34801561010457600080fd5b50610118610113366004610d6f565b610310565b6040516101259190611471565b60405180910390f35b34801561013a57600080fd5b50610118610149366004610d6f565b61032d565b34801561015a57600080fd5b5061016361034a565b604051610125919061130d565b34801561017c57600080fd5b5061019061018b366004610dd9565b610359565b6040516101259291906114a7565b3480156101aa57600080fd5b506101be6101b9366004610e26565b61052d565b005b6101be6101ce366004610dd9565b6105f4565b3480156101df57600080fd5b50610163610817565b3480156101f457600080fd5b50610208610203366004610d51565b610826565b6040516101259392919061131b565b34801561022357600080fd5b506101be610232366004610d51565b610886565b34801561024357600080fd5b506101be610252366004610da9565b61095d565b34801561026357600080fd5b506101be610272366004610d33565b6109b7565b34801561028357600080fd5b5061028c610a03565b6040516101259190611463565b3480156102a557600080fd5b506102b96102b4366004610da9565b610a12565b6040516101259392919061147f565b6101be6102d6366004610d51565b610b1c565b3480156102e757600080fd5b5061028c610bcb565b3480156102fc57600080fd5b506101be61030b366004610d51565b610be1565b600460209081526000928352604080842090915290825290205481565b600560209081526000928352604080842090915290825290205481565b6001546001600160a01b031681565b6000805481906001600160a01b0316331461038f5760405162461bcd60e51b8152600401610386906113a3565b60405180910390fd5b600080600061039e8887610a12565b91945090925090506001600160801b0387811690871681029080840290849082816103c557fe5b04146103e35760405162461bcd60e51b815260040161038690611383565b6001600160801b03808b1660009081526004602090815260408083206001600160a01b038816845290915290205460035490911611156104355760405162461bcd60e51b8152600401610386906113d3565b6001600160801b038a1660009081526004602090815260408083206001600160a01b03871684529091529020548111156104815760405162461bcd60e51b8152600401610386906113f3565b6001600160801b038a1660009081526005602090815260408083206001600160a01b0387168452909152902054828101908110156104d15760405162461bcd60e51b815260040161038690611343565b6001600160801b038b1660008181526004602090815260408083206001600160a01b0390981680845297825280832080549690960390955591815260058252838120958152949052922091909155509092509050935093915050565b6001600160801b0382166000908152600260205260409020546001600160a01b0316331461056d5760405162461bcd60e51b815260040161038690611423565b8067ffffffffffffffff16600a11158015610593575060648167ffffffffffffffff1611155b6105af5760405162461bcd60e51b8152600401610386906113e3565b6001600160801b039091166000908152600260205260409020805467ffffffffffffffff909216600160a01b0267ffffffffffffffff60a01b19909216919091179055565b816001600160801b031660001061061d5760405162461bcd60e51b815260040161038690611413565b806001600160801b03166000106106465760405162461bcd60e51b815260040161038690611413565b6003546001600160801b039081168183160261520884029091161061067d5760405162461bcd60e51b815260040161038690611403565b610685610cb3565b6001600160801b03838116825282811660208084019190915260035486831660009081526004835260408082203383529093529190912054600160801b9091049091163490910110156106ea5760405162461bcd60e51b8152600401610386906113d3565b6001600160801b0380851660009081526002602090815260408083206004835281842060035482546001600160a01b03168652938190529190932054929390929116118061075757506001820154610757906001600160801b0380821691600160801b9004168787610c53565b6107735760405162461bcd60e51b815260040161038690611363565b3360009081526020829052604090205434908101908110156107a75760405162461bcd60e51b815260040161038690611353565b33600081815260209390935260409092205581546001830180546001600160801b0319166001600160801b03978816178716600160801b9690971695909502959095179093556001600160a01b031990931690911767ffffffffffffffff60a01b1916601960a11b179091555050565b6000546001600160a01b031681565b600260209081526000918252604091829020805483518085019094526001909101546001600160801b038082168552600160801b90910416918301919091526001600160a01b03811691600160a01b90910467ffffffffffffffff169083565b6001600160801b0381166000908152600260205260409020546001600160a01b03163314156108c75760405162461bcd60e51b8152600401610386906113c3565b6001600160801b0381166000908152600460209081526040808320338452909152902054806109085760405162461bcd60e51b815260040161038690611443565b6001600160801b038216600090815260046020908152604080832033808552925280832083905551909183156108fc02918491818181858888f19350505050158015610958573d6000803e3d6000fd5b505050565b6001546001600160a01b031633146109875760405162461bcd60e51b815260040161038690611453565b600380546001600160801b03928316600160801b029383166001600160801b031990911617909116919091179055565b6001546001600160a01b031633146109e15760405162461bcd60e51b815260040161038690611373565b600080546001600160a01b0319166001600160a01b0392909216919091179055565b6003546001600160801b031681565b6000806000610a1f610cca565b506001600160801b03858116600090815260026020908152604091829020825160608101845281546001600160a01b038082168352600160a01b90910467ffffffffffffffff1682850152845180860186526001909301548087168452600160801b900490951692820192909252918101919091528051909116610ab55760405162461bcd60e51b815260040161038690611393565b610abd610cb3565b506040810151805160208201516001600160801b039182168289160291168181610ae357fe5b04905060008111610b065760405162461bcd60e51b815260040161038690611443565b6020830151925192989297509550909350505050565b6001600160801b0381166000908152600460209081526040808320338452909152902054610b5c5760405162461bcd60e51b815260040161038690611433565b6001600160801b03811660009081526004602090815260408083203384529091529020543490810190811015610ba45760405162461bcd60e51b8152600401610386906113b3565b6001600160801b039091166000908152600460209081526040808320338452909152902055565b600354600160801b90046001600160801b031681565b6001600160801b038116600090815260056020908152604080832033845290915290205480610c225760405162461bcd60e51b815260040161038690611443565b6001600160801b03821660008181526005602090815260408083203380855292528220919091556109589183610c76565b6001600160801b038281168482160285821691831691909102105b949350505050565b6000610c80610cee565b84815260208082018590526040820184905282606083600064514b430002600019f1610cab57600080fd5b509392505050565b604080518082019091526000808252602082015290565b6040805160608101825260008082526020820152908101610ce9610cb3565b905290565b60405180606001604052806003906020820280388339509192915050565b8035610d1781611505565b92915050565b8035610d178161151c565b8035610d1781611525565b600060208284031215610d4557600080fd5b6000610c6e8484610d0c565b600060208284031215610d6357600080fd5b6000610c6e8484610d1d565b60008060408385031215610d8257600080fd5b6000610d8e8585610d1d565b9250506020610d9f85828601610d0c565b9150509250929050565b60008060408385031215610dbc57600080fd5b6000610dc88585610d1d565b9250506020610d9f85828601610d1d565b600080600060608486031215610dee57600080fd5b6000610dfa8686610d1d565b9350506020610e0b86828701610d1d565b9250506040610e1c86828701610d1d565b9150509250925092565b60008060408385031215610e3957600080fd5b6000610e458585610d1d565b9250506020610d9f85828601610d28565b610e5f816114d2565b82525050565b6000610e726018836114c9565b7f41766f6964206164646974696f6e206f766572666c6f772e0000000000000000815260200192915050565b6000610eab6012836114c9565b7120b23234ba34b7b71037bb32b9333637bb9760711b815260200192915050565b6000610ed96023836114c9565b7f496e76616c6964206e65772065786368616e676520726174652070726f706f7381526230b61760e91b602082015260400192915050565b6000610f1e601f836114c9565b7f4f6e6c792073757065727669736f722063616e207365742063616c6c65722e00815260200192915050565b6000610f576017836114c9565b7f41766f69642075696e74323536206f766572666c6f772e000000000000000000815260200192915050565b6000610f90600e836114c9565b6d24b73b30b634b2103a37b5b2b71760911b815260200192915050565b6000610fba6025836114c9565b7f4f6e6c792063616c6c65722063616e20696e766f6b6520746869732066756e638152643a34b7b71760d91b602082015260400192915050565b60006110016016836114c9565b7573686f756c6420626520612076616c6964207465726d60501b815260200192915050565b60006110336023836114c9565b7f4e6f7420616c6c6f77656420666f72206e617469766520746f6b656e2061646d81526234b71760e91b602082015260400192915050565b60006110786030836114c9565b7f53686f756c642068617665207265736572766520616d6f756e7420677265617481526f32b9103a3430b71036b4b734b6bab69760811b602082015260400192915050565b60006110ca601d836114c9565b7f53686f756c64206265206265747765656e203020616e6420313030252e000000815260200192915050565b60006111036023836114c9565b7f53686f756c64206861766520656e6f75676820726573657276657320746f207081526230bc9760e91b602082015260400192915050565b60006111486037836114c9565b7f52657175697265732065786368616e67652072617465202a203231303030203c81527f206d696e476173526573657276654d61696e7461696e2e000000000000000000602082015260400192915050565b60006111a76019836114c9565b7f56616c75652073686f756c64206265206e6f6e2d7a65726f2e00000000000000815260200192915050565b60006111e0601f836114c9565b7f4f6e6c792061646d696e2063616e2073657420726566756e6420726174652e00815260200192915050565b60006112196019836114c9565b7f73686f756c6420626520616e2065786974656420746f6b656e00000000000000815260200192915050565b6000611252601b836114c9565b7f53686f756c642068617665206e6f6e2d7a65726f2076616c75652e0000000000815260200192915050565b600061128b6028836114c9565b7f4f6e6c792073757065727669736f722063616e20736574206d696e20676173208152673932b9b2b93b329760c11b602082015260400192915050565b805160408301906112d984826112f2565b5060208201516112ec60208501826112f2565b50505050565b610e5f816114dd565b610e5f816114f5565b610e5f816114f8565b60208101610d178284610e56565b608081016113298286610e56565b6113366020830185611304565b610c6e60408301846112c8565b60208082528101610d1781610e65565b60208082528101610d1781610e9e565b60208082528101610d1781610ecc565b60208082528101610d1781610f11565b60208082528101610d1781610f4a565b60208082528101610d1781610f83565b60208082528101610d1781610fad565b60208082528101610d1781610ff4565b60208082528101610d1781611026565b60208082528101610d178161106b565b60208082528101610d17816110bd565b60208082528101610d17816110f6565b60208082528101610d178161113b565b60208082528101610d178161119a565b60208082528101610d17816111d3565b60208082528101610d178161120c565b60208082528101610d1781611245565b60208082528101610d178161127e565b60208101610d1782846112f2565b60208101610d1782846112fb565b6060810161148d8286611304565b61149a6020830185610e56565b610c6e60408301846112fb565b604081016114b58285611304565b6114c260208301846112fb565b9392505050565b90815260200190565b6000610d17826114e9565b6001600160801b031690565b6001600160a01b031690565b90565b67ffffffffffffffff1690565b61150e816114d2565b811461151957600080fd5b50565b61150e816114dd565b61150e816114f856fea365627a7a72315820c3e8c4e73e91f6ba6206616ddc36efdfb2b4a0760436ea08b93b77fcfc5264146c6578706572696d656e74616cf564736f6c634300050b0040"
        )
        env.quark_chain_config.MULTI_NATIVE_TOKEN_GAS_UTILITY_CONTRACT_BYTECODE = sha3_256(
            contract_runtime_bytecode
        ).hex()
        contract_addr = SystemContract.MULTI_NATIVE_TOKEN_GAS_UTILITY.addr()
        evm_state.set_code(contract_addr, contract_runtime_bytecode)
        evm_state.set_storage_data(contract_addr, 0, contract_addr)
        evm_state.set_storage_data(contract_addr, 1, acc1.recipient)
        evm_state.commit()

        def tx_gen(nonce, value, data: str):
            ret = create_transfer_transaction(
                nonce=nonce,
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=Address(contract_addr, 0),
                value=value,
                gas=1000000,
                gas_price=0,
                data=bytes.fromhex(data),
            ).tx.to_evm_tx()
            ret.set_quark_chain_config(env.quark_chain_config)
            return ret

        # set minGasReserveMaintain and minGasReserveInit to 1
        set_MinGasReserve = lambda n, v: tx_gen(
            n,
            v,
            "bb92885400000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000001",
        )
        # propose a new exchange rate for token id 123 with ratio 1 / 30000
        propose_new_exchange_rate = lambda n, v: tx_gen(
            n,
            v,
            "735e0e19000000000000000000000000000000000000000000000000000000000000007b00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000007530",
        )
        # set the refund rate to 60
        set_refund_rate = lambda n, v: tx_gen(
            n,
            v,
            "6d27af8c000000000000000000000000000000000000000000000000000000000000007b000000000000000000000000000000000000000000000000000000000000003c",
        )
        # check the balance of the gas reserve
        gas_reserve_balance = lambda n, v, a: tx_gen(
            n,
            v,
            "13dee215000000000000000000000000000000000000000000000000000000000000007b000000000000000000000000"
            + a.recipient.hex(),
        )
        # check the balance of native token
        native_token_balance = lambda n, v, a: tx_gen(
            n,
            v,
            "21a2b36e000000000000000000000000000000000000000000000000000000000000007b000000000000000000000000"
            + a.recipient.hex(),
        )

        # set minGasReserveMaintain and minGasReserveInit
        tx0 = set_MinGasReserve(0, 0)
        success, _ = apply_transaction(evm_state, tx0, bytes(32))
        self.assertTrue(success)
        # propose a new exchange rate
        tx1 = propose_new_exchange_rate(1, 10000)
        success, _ = apply_transaction(evm_state, tx1, bytes(32))
        self.assertTrue(success)
        # set the refund rate
        tx2 = set_refund_rate(2, 0)
        success, _ = apply_transaction(evm_state, tx2, bytes(32))
        # should be able to use the gas utility
        evm_state.commit()

        # get the gas utility information by calling the get_gas_utility_info function
        refund_percentage, gas_price = evm_state.get_gas_utility_info(
            123, 30000, evm_state
        )
        self.assertEqual(refund_percentage, 60)
        self.assertEqual(gas_price, 1)
        # exchange the Qkc with the native token
        refund_percentage, gas_price = evm_state.pay_as_gas(123, 1, 30000, evm_state)
        self.assertEqual(refund_percentage, 60)
        self.assertEqual(gas_price, 1)
        # check the balance of the gas reserve. amount of native token (30000) * exchang rate (1 / 30000) = 1 Qkc
        tx3 = gas_reserve_balance(3, 0, acc1)
        success, output = apply_transaction(evm_state, tx3, bytes(32))
        self.assertTrue(success)
        self.assertEqual(int.from_bytes(output, byteorder="big"), 9999)
        # check the balance of native token.
        tx4 = native_token_balance(4, 0, acc1)
        success, output = apply_transaction(evm_state, tx4, bytes(32))
        self.assertTrue(success)
        self.assertEqual(int.from_bytes(output, byteorder="big"), 30000)
