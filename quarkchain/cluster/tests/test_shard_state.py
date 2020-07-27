import random
import math
import time
import unittest
from fractions import Fraction
from os import urandom
from typing import Optional

from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import (
    get_test_env,
    create_transfer_transaction,
    create_contract_creation_transaction,
    contract_creation_tx,
    mock_pay_native_token_as_gas,
)
from quarkchain.config import ConsensusType
from quarkchain.constants import (
    GENERAL_NATIVE_TOKEN_CONTRACT_BYTECODE,
    ROOT_CHAIN_POSW_CONTRACT_BYTECODE,
    NON_RESERVED_NATIVE_TOKEN_CONTRACT_BYTECODE,
)
from quarkchain.core import CrossShardTransactionDeposit, CrossShardTransactionList
from quarkchain.core import Identity, Address, TokenBalanceMap, MinorBlock
from quarkchain.diff import EthDifficultyCalculator
from quarkchain.evm import opcodes
from quarkchain.evm.exceptions import InvalidNativeToken
from quarkchain.evm.messages import (
    apply_transaction,
    get_gas_utility_info,
    pay_native_token_as_gas,
    validate_transaction,
    convert_to_default_chain_token_gasprice,
)
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

    def test_get_total_balance(self):
        acc_size = 60
        id_list = [Identity.create_random_identity() for _ in range(acc_size)]
        acc_list = [Address.create_from_identity(i, full_shard_key=0) for i in id_list]
        batch_size = [1, 2, 3, 4, 6, 66]
        env = get_test_env(
            genesis_account=acc_list[0], genesis_minor_quarkash=100000000
        )

        qkc_token = token_id_encode("QKC")
        state = create_default_shard_state(env=env)
        # Add a root block to have all the shards initialized
        root_block = state.root_tip.create_block_to_append().finalize()
        state.add_root_block(root_block)
        for nonce, acc in enumerate(acc_list[1:]):
            tx = create_transfer_transaction(
                shard_state=state,
                key=id_list[0].get_key(),
                from_address=acc_list[0],
                to_address=acc,
                value=100,
                transfer_token_id=qkc_token,
                gas_price=0,
                nonce=nonce,
            )
            self.assertTrue(state.add_tx(tx), "the %d tx fails to be added" % nonce)

        b1 = state.create_block_to_mine(address=acc_list[0])
        state.finalize_and_add_block(b1)

        self.assertEqual(
            state.get_token_balance(acc_list[0].recipient, self.genesis_token),
            100000000
            - 100 * (acc_size - 1)
            + self.get_after_tax_reward(self.shard_coinbase),
        )
        self.assertEqual(
            state.get_token_balance(acc_list[1].recipient, self.genesis_token), 100
        )

        exp_balance = 100000000 + self.get_after_tax_reward(self.shard_coinbase)
        for batch in batch_size:
            num_of_calls = math.ceil(float(acc_size + 1) / batch)
            total = 0
            next_start = None
            for _ in range(num_of_calls):
                balance, next_start = state.get_total_balance(
                    qkc_token, state.header_tip.get_hash(), batch, next_start
                )
                total += balance
            self.assertEqual(
                exp_balance,
                total,
                "testcase with batch size %d return balance failed" % batch,
            )
            self.assertEqual(
                bytes(32),
                next_start,
                "testcase with batch size %d return start failed" % batch,
            )

        # Random start should also succeed
        state.get_total_balance(
            qkc_token, state.header_tip.get_hash(), 1, start=urandom(32)
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

    @mock_pay_native_token_as_gas()
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
            charge_gas_reserve=True,
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

        # unrecognized token id should return 0
        gas_price = state.gas_price(token_id=1)
        self.assertEqual(gas_price, 0)

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

    def test_xshard_root_block_coinbase(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)

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
            1  # other network ids will skip difficulty check
        )
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
        contract_creation_with_revert_bytecode = "6080604052348015600f57600080fd5b506040517f08c379a00000000000000000000000000000000000000000000000000000000081526004018080602001828103825260028152602001807f686900000000000000000000000000000000000000000000000000000000000081525060200191505060405180910390fdfe"
        """
        pragma solidity ^0.5.1;
        contract RevertContract {
            constructor() public {
                revert("hi");
            }
        }
        """
        # This transaction cost is calculated by remix, which is different than the opcodes.GTXCOST due to revert.
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

        failed_gas_cost = 58025
        self.assertEqual(state.evm_state.receipts[0].gas_used, failed_gas_cost)

        # Make sure the FAILED_TRANSACTION_COST is consumed by the sender
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            200 * 10 ** 18 - failed_gas_cost,
        )
        # Make sure the accurate gas fee is obtained by the miner
        self.assertEqual(
            state.get_token_balance(acc2.recipient, self.genesis_token),
            self.get_after_tax_reward(failed_gas_cost + self.shard_coinbase),
        )
        self.assertEqual(
            b1.header.coinbase_amount_map.balance_map,
            {
                env.quark_chain_config.genesis_token: self.get_after_tax_reward(
                    failed_gas_cost + self.shard_coinbase
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

        runtime_bytecode = ROOT_CHAIN_POSW_CONTRACT_BYTECODE
        runtime_start = runtime_bytecode.find(bytes.fromhex("608060405260"), 1)
        runtime_bytecode = runtime_bytecode[runtime_start:]
        env.quark_chain_config.ROOT_CHAIN_POSW_CONTRACT_BYTECODE_HASH = sha3_256(
            runtime_bytecode
        ).hex()
        contract_addr = SystemContract.ROOT_CHAIN_POSW.addr()
        evm_state.set_code(contract_addr, runtime_bytecode)
        evm_state.commit()

        # contract deployed, but no stakes. signer defaults to the recipient
        stakes, signer = state.get_root_chain_stakes(
            acc1.recipient, state.header_tip.get_hash(), mock_evm_state=evm_state
        )
        self.assertEqual(stakes, 0)
        self.assertEqual(signer, acc1.recipient)

        nonce = 0

        def tx_gen(value, data: str):
            nonlocal nonce
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
            nonce += 1
            ret.set_quark_chain_config(env.quark_chain_config)
            return ret

        add_stake_tx = lambda v: tx_gen(v, "")
        set_signer_tx = lambda v, a: tx_gen(
            v, "6c19e783000000000000000000000000" + a.recipient.hex()
        )
        withdraw_tx = lambda: tx_gen(0, "853828b6")
        unlock_tx = lambda: tx_gen(0, "a69df4b5")
        lock_tx = lambda v: tx_gen(v, "f83d08ba")

        # add stakes and set signer
        tx0 = add_stake_tx(1234)
        success, _ = apply_transaction(evm_state, tx0, bytes(32))
        self.assertTrue(success)
        random_signer = Address.create_random_account()
        tx1 = set_signer_tx(4321, random_signer)
        success, _ = apply_transaction(evm_state, tx1, bytes(32))
        self.assertTrue(success)

        evm_state.commit()
        stakes, signer = state.get_root_chain_stakes(
            acc1.recipient, state.header_tip.get_hash(), mock_evm_state=evm_state
        )
        self.assertEqual(stakes, 1234 + 4321)
        self.assertEqual(signer, random_signer.recipient)

        # can't withdraw during locking
        tx2 = withdraw_tx()
        success, _ = apply_transaction(evm_state, tx2, bytes(32))
        self.assertFalse(success)

        # unlock should succeed
        tx3 = unlock_tx()
        success, _ = apply_transaction(evm_state, tx3, bytes(32))
        self.assertTrue(success)
        # but still can't withdraw
        tx4 = withdraw_tx()
        success, _ = apply_transaction(evm_state, tx4, bytes(32))
        self.assertFalse(success)
        # and can't add stakes or set signer either
        tx5 = add_stake_tx(100)
        success, _ = apply_transaction(evm_state, tx5, bytes(32))
        self.assertFalse(success)
        tx6 = set_signer_tx(0, acc1)
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
        tx7 = withdraw_tx()
        success, _ = apply_transaction(evm_state, tx7, bytes(32))
        self.assertTrue(success)
        balance_after = evm_state.get_balance(acc1.recipient)
        self.assertEqual(balance_before + 5555, balance_after)

        # lock again
        tx8 = lock_tx(42)
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

    @staticmethod
    def __prepare_gas_reserve_contract(evm_state, supervisor) -> bytes:
        runtime_bytecode = GENERAL_NATIVE_TOKEN_CONTRACT_BYTECODE
        runtime_start = runtime_bytecode.find(bytes.fromhex("608060405260"), 1)
        # get rid of the constructor argument
        runtime_bytecode = runtime_bytecode[runtime_start:-32]

        contract_addr = SystemContract.GENERAL_NATIVE_TOKEN.addr()
        evm_state.set_code(contract_addr, runtime_bytecode)
        # Set caller
        evm_state.set_storage_data(contract_addr, 0, contract_addr)
        # Set supervisor
        evm_state.set_storage_data(contract_addr, 1, supervisor)
        # Set min gas reserve for maintenance
        evm_state.set_storage_data(contract_addr, 3, 30000)
        # Set min starting gas for use as gas
        evm_state.set_storage_data(contract_addr, 4, 1)
        evm_state.commit()
        return contract_addr

    def test_pay_native_token_as_gas_contract_api(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        state = create_default_shard_state(env=env)
        evm_state = state.evm_state
        evm_state.timestamp = int(time.time())

        # contract not deployed yet
        refund_percentage, gas_price = get_gas_utility_info(evm_state, 123, 1)
        self.assertEqual((refund_percentage, gas_price), (0, 0))

        contract_addr = self.__prepare_gas_reserve_contract(evm_state, acc1.recipient)

        nonce = 0

        def tx_gen(data: str, value=None, transfer_token_id=None):
            nonlocal nonce
            ret = create_transfer_transaction(
                nonce=nonce,
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=Address(contract_addr, 0),
                value=value or 0,
                gas=1000000,
                gas_price=0,
                data=bytes.fromhex(data),
                transfer_token_id=transfer_token_id,
            ).tx.to_evm_tx()
            nonce += 1
            ret.set_quark_chain_config(env.quark_chain_config)
            return ret

        # propose a new exchange rate for token id 123 with ratio 1 / 30000
        token_id = 123
        parsed_hex = lambda i: i.to_bytes(32, byteorder="big").hex()
        register = lambda: tx_gen("bf03314a", value=1, transfer_token_id=token_id)
        propose_new_exchange_rate = lambda v: tx_gen(
            "735e0e19" + parsed_hex(token_id) + parsed_hex(1) + parsed_hex(30000), v
        )
        # set the refund rate to 60
        set_refund_rate = lambda: tx_gen(
            "6d27af8c" + parsed_hex(token_id) + parsed_hex(60)
        )
        query_gas_reserve_balance = lambda a: tx_gen(
            "13dee215" + parsed_hex(token_id) + "0" * 24 + a.recipient.hex()
        )
        # check the balance of native token
        query_native_token_balance = lambda a: tx_gen(
            "21a2b36e" + parsed_hex(token_id) + "0" * 24 + a.recipient.hex()
        )
        # withdraw native tokens
        withdraw_native_token = lambda: tx_gen("f9c94eb7" + parsed_hex(token_id))

        # propose a new exchange rate, which will fail because no registration
        tx1 = propose_new_exchange_rate(100000)
        success, _ = apply_transaction(evm_state, tx1, bytes(32))
        self.assertFalse(success)
        # register and re-propose, should succeed
        evm_state.delta_token_balance(acc1.recipient, token_id, 1)
        register_tx = register()
        success, _ = apply_transaction(evm_state, register_tx, bytes(32))
        self.assertTrue(success)
        tx1_redo = propose_new_exchange_rate(100000)
        success, _ = apply_transaction(evm_state, tx1_redo, bytes(32))
        self.assertTrue(success)
        # set the refund rate
        tx2 = set_refund_rate()
        success, _ = apply_transaction(evm_state, tx2, bytes(32))
        self.assertTrue(success)

        # get the gas utility information by calling the get_gas_utility_info function
        refund_percentage, gas_price = get_gas_utility_info(evm_state, token_id, 60000)
        self.assertEqual((refund_percentage, gas_price), (60, 2))
        self.assertEqual(
            convert_to_default_chain_token_gasprice(evm_state, token_id, 60000), 2
        )
        # exchange the Qkc with the native token
        refund_percentage, gas_price = pay_native_token_as_gas(
            evm_state, token_id, 1, 60000
        )
        self.assertEqual((refund_percentage, gas_price), (60, 2))
        # check the balance of the gas reserve. amount of native token (60000) * exchange rate (1 / 30000) = 2 QKC
        tx3 = query_gas_reserve_balance(acc1)
        success, output = apply_transaction(evm_state, tx3, bytes(32))
        self.assertTrue(success)
        self.assertEqual(int.from_bytes(output, byteorder="big"), 99998)
        # check the balance of native token.
        tx4 = query_native_token_balance(acc1)
        success, output = apply_transaction(evm_state, tx4, bytes(32))
        self.assertTrue(success)
        # 1 token from registration
        self.assertEqual(int.from_bytes(output, byteorder="big"), 60000 + 1)
        # give the contract real native token and withdrawing should work
        evm_state.delta_token_balance(contract_addr, token_id, 60000)
        tx5 = withdraw_native_token()
        success, _ = apply_transaction(evm_state, tx5, bytes(32))
        self.assertTrue(success)
        self.assertEqual(evm_state.get_balance(acc1.recipient, token_id), 60000 + 1)
        self.assertEqual(evm_state.get_balance(contract_addr, token_id), 0)
        # check again the balance of native token.
        tx6 = query_native_token_balance(acc1)
        success, output = apply_transaction(evm_state, tx6, bytes(32))
        self.assertTrue(success)
        self.assertEqual(int.from_bytes(output, byteorder="big"), 0)

    def test_pay_native_token_as_gas_end_to_end(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        # genesis balance: 100 ether for both QKC and QI
        env = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={"QKC": int(1e20), "QI": int(1e20)},
        )
        state = create_default_shard_state(env=env)
        evm_state = state.evm_state
        evm_state.block_coinbase = Address.create_random_account(0).recipient

        contract_addr = self.__prepare_gas_reserve_contract(evm_state, acc1.recipient)

        nonce = 0
        token_id = token_id_encode("QI")
        gaslimit = 1000000

        def tx_gen(
            data: str,
            value: Optional[int] = None,
            addr: bytes = contract_addr,
            use_native_token: bool = False,
            gas_price: int = 0,
            increment_nonce=True,
        ):
            nonlocal nonce
            ret = create_transfer_transaction(
                nonce=nonce,
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=Address(addr, 0),
                value=value or 0,
                gas=gaslimit,
                gas_price=gas_price,
                data=bytes.fromhex(data),
                gas_token_id=token_id if use_native_token else None,
            ).tx.to_evm_tx()
            if increment_nonce:
                nonce += 1
            ret.set_quark_chain_config(env.quark_chain_config)
            return ret

        # propose a new exchange rate for native token with ratio 1 / 2
        parsed_hex = lambda i: i.to_bytes(32, byteorder="big").hex()
        propose_new_exchange_rate = lambda v: tx_gen(
            "735e0e19" + parsed_hex(token_id) + parsed_hex(1) + parsed_hex(2), v
        )
        unrequire_registered_token = lambda: tx_gen("764a27ef" + parsed_hex(0))
        # set the refund rate to 80
        set_refund_rate = lambda: tx_gen(
            "6d27af8c" + parsed_hex(token_id) + parsed_hex(80)
        )
        query_gas_reserve_balance = lambda a: tx_gen(
            "13dee215" + parsed_hex(token_id) + "0" * 24 + a.recipient.hex()
        )
        query_native_token_balance = lambda a: tx_gen(
            "21a2b36e" + parsed_hex(token_id) + "0" * 24 + a.recipient.hex()
        )

        tx = unrequire_registered_token()
        success, _ = apply_transaction(evm_state, tx, bytes(32))
        self.assertTrue(success)
        # propose a new exchange rate with 1 ether of QKC as reserve
        tx = propose_new_exchange_rate(int(1e18))
        success, _ = apply_transaction(evm_state, tx, bytes(32))
        self.assertTrue(success)
        # set the refund rate
        tx = set_refund_rate()
        success, _ = apply_transaction(evm_state, tx, bytes(32))
        self.assertTrue(success)
        evm_state.commit()

        # 1) craft a tx using native token for gas, with gas price as 10
        tx_w_native_token = tx_gen(
            "", 0, acc1.recipient, use_native_token=True, gas_price=10
        )
        success, _ = apply_transaction(evm_state, tx_w_native_token, bytes(32))
        self.assertTrue(success)

        # native token balance should update accordingly
        self.assertEqual(
            evm_state.get_balance(acc1.recipient, token_id), int(1e20) - gaslimit * 10
        )
        self.assertEqual(evm_state.get_balance(contract_addr, token_id), gaslimit * 10)
        query_tx = query_native_token_balance(acc1)
        success, output = apply_transaction(evm_state, query_tx, bytes(32))
        self.assertTrue(success)
        self.assertEqual(int.from_bytes(output, byteorder="big"), gaslimit * 10)
        # qkc balance should update accordingly:
        # should have 100 ether - 1 ether + refund
        sender_balance = (
            int(1e20) - int(1e18) + (gaslimit - 21000) * (10 // 2) * 8 // 10
        )
        self.assertEqual(evm_state.get_balance(acc1.recipient), sender_balance)
        contract_remaining_qkc = int(1e18) - gaslimit * 10 // 2
        self.assertEqual(evm_state.get_balance(contract_addr), contract_remaining_qkc)
        query_tx = query_gas_reserve_balance(acc1)
        success, output = apply_transaction(evm_state, query_tx, bytes(32))
        self.assertTrue(success)
        self.assertEqual(
            int.from_bytes(output, byteorder="big"), contract_remaining_qkc
        )
        # burned QKC for gas conversion
        self.assertEqual(
            evm_state.get_balance(bytes(20)), (gaslimit - 21000) * (10 // 2) * 2 // 10
        )
        # miner fee with 50% tax
        self.assertEqual(
            evm_state.get_balance(evm_state.block_coinbase), 21000 * (10 // 2) // 2
        )

        # 2) craft a tx that will use up gas reserve, should fail validation
        tx_use_up_reserve = tx_gen(
            "",
            0,
            acc1.recipient,
            use_native_token=True,
            gas_price=int(1e12) * 2,
            increment_nonce=False,
        )
        with self.assertRaises(InvalidNativeToken):
            apply_transaction(evm_state, tx_use_up_reserve, bytes(32))

    def test_mint_new_native_token(self):
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10 ** 20)
        state = create_default_shard_state(env=env)
        evm_state = state.evm_state  # type: EvmState

        runtime_bytecode = NON_RESERVED_NATIVE_TOKEN_CONTRACT_BYTECODE
        runtime_start = runtime_bytecode.find(bytes.fromhex("608060405260"), 1)
        # get rid of constructor arguments
        runtime_bytecode = runtime_bytecode[runtime_start:-64]
        contract_addr = SystemContract.NON_RESERVED_NATIVE_TOKEN.addr()
        evm_state.set_code(contract_addr, runtime_bytecode)
        evm_state.set_storage_data(contract_addr, 0, acc1.recipient)
        evm_state.timestamp = int(time.time())  # to make sure start_time not 0
        evm_state.commit()

        nonce = 0

        def tx_gen(data: str, value: Optional[int] = 0):
            nonlocal nonce
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
            nonce += 1
            ret.set_quark_chain_config(env.quark_chain_config)
            return ret

        token_id = 9999999  # token id to bid and win
        amount = 1000
        parsed_hex = lambda i: i.to_bytes(32, byteorder="big").hex()
        # set auction parameters: minimum bid price: 20 QKC, minimum increment: 5%, duration: one week
        set_auction_params = lambda: tx_gen(
            "3c69e3d2" + parsed_hex(20) + parsed_hex(5) + parsed_hex(3600 * 24 * 7)
        )
        resume_auction = lambda: tx_gen("32353fbd")
        bid_new_token = lambda v: tx_gen(
            "6aecd9d7"
            + parsed_hex(token_id)
            + parsed_hex(25 * 10 ** 18)
            + parsed_hex(0),
            v,
        )
        end_auction = lambda: tx_gen("fe67a54b")
        mint_new_token = lambda: tx_gen(
            "0f2dc31a" + parsed_hex(token_id) + parsed_hex(amount)
        )
        get_native_token_info = lambda: tx_gen("9ea41be7" + parsed_hex(token_id))

        tx0 = set_auction_params()
        success, _ = apply_transaction(evm_state, tx0, bytes(32))
        self.assertTrue(success)

        tx1 = resume_auction()
        success, _ = apply_transaction(evm_state, tx1, bytes(32))
        self.assertTrue(success)

        tx2 = bid_new_token(26 * 10 ** 18)
        success, _ = apply_transaction(evm_state, tx2, bytes(32))
        self.assertTrue(success)

        # End before ending time, should fail
        tx3 = end_auction()
        success, _ = apply_transaction(evm_state, tx3, bytes(32))
        self.assertFalse(success)

        # 7 days passed, this round of auction ends
        evm_state.timestamp += 3600 * 24 * 7
        tx4 = end_auction()
        success, _ = apply_transaction(evm_state, tx4, bytes(32))
        self.assertTrue(success)

        tx5 = get_native_token_info()
        success, output = apply_transaction(evm_state, tx5, bytes(32))
        self.assertTrue(success)
        self.assertNotEqual(int.from_bytes(output[:32], byteorder="big"), 0)
        self.assertEqual(output[44:64], acc1.recipient)
        self.assertEqual(int.from_bytes(output[64:96], byteorder="big"), 0)

        tx6 = mint_new_token()
        success, _ = apply_transaction(evm_state, tx6, bytes(32))
        self.assertTrue(success)

        tx7 = get_native_token_info()
        success, output = apply_transaction(evm_state, tx7, bytes(32))
        self.assertTrue(success)
        self.assertEqual(int.from_bytes(output[64:96], byteorder="big"), amount)

    @mock_pay_native_token_as_gas(lambda *x: (50, x[-1] * 2))
    def test_native_token_as_gas_in_shard(self):
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id2, full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={"QKC": 100000000, "QI": 100000000},
        )
        state = create_default_shard_state(env=env)
        evm_state = state.evm_state

        qkc_token = token_id_encode("QKC")
        qi_token = token_id_encode("QI")

        nonce = 0

        def tx_gen(value, token_id, to, increment_nonce=True):
            nonlocal nonce
            ret = create_transfer_transaction(
                nonce=nonce,
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=to,
                value=value,
                gas=1000000,
                gas_price=10,
                data=b"",
                gas_token_id=token_id,
            ).tx.to_evm_tx()
            if increment_nonce:
                nonce += 1
            ret.set_quark_chain_config(env.quark_chain_config)
            return ret

        self.assertEqual(
            evm_state.get_balance(acc1.recipient, token_id=qi_token), 100000000
        )

        # fail because gas reserve doesn't have QKC
        failed_tx = tx_gen(1000, qi_token, acc2, increment_nonce=False)
        with self.assertRaises(InvalidNativeToken):
            validate_transaction(evm_state, failed_tx)

        # need to give gas reserve enough QKC to pay for gas conversion
        evm_state.delta_token_balance(
            SystemContract.GENERAL_NATIVE_TOKEN.addr(), qkc_token, int(1e18)
        )

        tx0 = tx_gen(1000, qi_token, acc2)
        success, _ = apply_transaction(evm_state, tx0, bytes(32))
        self.assertTrue(success)

        self.assertEqual(
            evm_state.get_balance(acc1.recipient, token_id=qi_token),
            100000000 - 1000000 * 10,
        )
        self.assertEqual(
            evm_state.get_balance(acc1.recipient, token_id=qkc_token),
            100000000 - 1000 + 979000 * 10,
        )
        self.assertEqual(
            evm_state.get_balance(bytes(20), token_id=qkc_token),
            979000 * 10 + 21000 * 10,
        )

    # 10% refund rate, triple the gas price
    @mock_pay_native_token_as_gas(lambda *x: (10, x[-1] * 3))
    def test_native_token_as_gas_cross_shard(self):
        id1 = Identity.create_random_identity()
        id2 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        # cross-shard
        acc2 = Address.create_from_identity(id2, full_shard_key=1)
        miner = Address.create_random_account(full_shard_key=0)

        envs = [
            get_test_env(
                genesis_account=acc1,
                genesis_minor_token_balances={"QI": 100000000},
                charge_gas_reserve=True,
            )
            for _ in range(2)
        ]
        state = create_default_shard_state(env=envs[0])
        state_to = create_default_shard_state(env=envs[1], shard_id=1)

        qi_token = token_id_encode("QI")
        gas_price, gas_limit = 10, 1000000
        nonce = 0

        # add a root block to allow xshard tx
        rb = (
            state.root_tip.create_block_to_append()
            .add_minor_block_header(state.header_tip)
            .add_minor_block_header(state_to.header_tip)
            .finalize()
        )
        state.add_root_block(rb)
        state_to.add_root_block(rb)

        def tx_gen(to):
            nonlocal nonce
            ret = create_transfer_transaction(
                nonce=nonce,
                shard_state=state,
                key=id1.get_key(),
                from_address=acc1,
                to_address=to,
                value=0,
                gas=gas_limit,
                gas_price=gas_price,
                data=b"",
                gas_token_id=qi_token,
            )
            nonce += 1
            return ret

        tx = tx_gen(acc2)
        self.assertTrue(state.add_tx(tx))
        b = state.create_block_to_mine(address=miner)
        self.assertEqual(len(b.tx_list), 1)
        self.assertEqual(state.evm_state.gas_used, 0)

        state.finalize_and_add_block(b)
        self.assertEqual(len(state.evm_state.xshard_list), 1)
        self.assertEqual(
            state.evm_state.xshard_list[0],
            CrossShardTransactionDeposit(
                tx_hash=tx.get_hash(),
                from_address=acc1,
                to_address=acc2,
                value=0,
                gas_remained=gas_limit - opcodes.GTXXSHARDCOST - opcodes.GTXCOST,
                # gas token should be converted to QKC
                gas_token_id=self.genesis_token,
                transfer_token_id=self.genesis_token,
                # those two fields should reflect the mock
                gas_price=gas_price * 3,
                refund_rate=10,
            ),
        )

        # local shard state check
        self.assertEqual(
            state.get_token_balance(acc1.recipient, token_id=qi_token),
            100000000 - gas_price * gas_limit,
        )
        self.assertEqual(
            state.get_token_balance(miner.recipient, token_id=self.genesis_token),
            self.get_after_tax_reward(self.shard_coinbase + 21000 * gas_price * 3),
        )
        # native token as gas sent to system contract
        sys_addr = SystemContract.GENERAL_NATIVE_TOKEN.addr()
        self.assertEqual(
            state.get_token_balance(sys_addr, token_id=qi_token), gas_price * gas_limit
        )
        # while its QKC reserve has been deducted
        self.assertEqual(
            state.get_token_balance(sys_addr, token_id=self.genesis_token),
            int(1e18) - gas_limit * gas_price * 3,
        )

        # let the target get the xshard tx
        state_to.add_cross_shard_tx_list_by_minor_block_hash(
            h=b.header.get_hash(),
            tx_list=CrossShardTransactionList(tx_list=state.evm_state.xshard_list),
        )
        rb = (
            state_to.root_tip.create_block_to_append()
            .add_minor_block_header(b.header)
            .finalize()
        )
        state_to.add_root_block(rb)
        # process a shard block to catch up xshard deposits
        b_to = state_to.create_block_to_mine(address=miner.address_in_shard(1))
        state_to.finalize_and_add_block(b_to)
        # no change to native token
        self.assertEqual(
            state_to.get_token_balance(acc1.recipient, token_id=qi_token), 100000000
        )
        # QKC should have been partially refunded
        self.assertEqual(
            state_to.get_token_balance(acc1.recipient, token_id=self.genesis_token),
            (3 * gas_price) * (gas_limit - 30000) // 10,
        )
        # another part of QKC is burnt
        self.assertEqual(
            state_to.get_token_balance(bytes(20), token_id=self.genesis_token),
            (3 * gas_price) * (gas_limit - 30000) // 10 * 9,
        )
        # and miners
        self.assertEqual(
            state_to.get_token_balance(miner.recipient, token_id=self.genesis_token),
            self.get_after_tax_reward(self.shard_coinbase + (3 * gas_price) * 9000),
        )

    def test_posw_stake_by_block_decay_by_epoch(self):
        acc = Address(b"\x01" * 20, full_shard_key=0)
        env = get_test_env(genesis_account=acc, genesis_minor_quarkash=200)
        env.quark_chain_config.ENABLE_POSW_STAKING_DECAY_TIMESTAMP = 100
        state = create_default_shard_state(env=env, shard_id=0, posw_override=True)

        state.shard_config.CONSENSUS_TYPE = ConsensusType.POW_DOUBLESHA256
        state.shard_config.POSW_CONFIG.TOTAL_STAKE_PER_BLOCK = 100
        state.shard_config.POSW_CONFIG.WINDOW_SIZE = 256

        # created time is greater than threshold
        b1 = state.get_tip().create_block_to_append(create_time=101, address=acc)
        posw_info = state._posw_info(b1)
        # 200 qkc with 100 required per block, should equal 2 mineable blocks
        self.assertEqual(posw_info.posw_mineable_blocks, 200 / 100)

        # decay (factor = 0.5) should kick in and double mineable blocks
        b1.header.height = state.shard_config.EPOCH_INTERVAL
        posw_info = state._posw_info(b1)
        self.assertEqual(posw_info.posw_mineable_blocks, 200 / (100 / 2))

        # no effect before the enable timestamp
        b1.header.create_time = 99
        posw_info = state._posw_info(b1)
        self.assertEqual(posw_info.posw_mineable_blocks, 200 / 100)
