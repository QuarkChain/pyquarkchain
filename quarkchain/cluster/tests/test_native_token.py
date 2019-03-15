import unittest

from quarkchain.cluster.shard_state import ShardState
from quarkchain.cluster.tests.test_utils import (
    get_test_env,
    create_transfer_transaction,
    contract_creation_tx,
)
from quarkchain.core import Identity, Address
from quarkchain.core import CrossShardTransactionDeposit, CrossShardTransactionList
from quarkchain.evm import opcodes
from quarkchain.evm.messages import mk_contract_address
from quarkchain.utils import token_id_encode
from quarkchain.genesis import GenesisManager


def create_default_shard_state(env, shard_id=0, diff_calc=None):
    genesis_manager = GenesisManager(env.quark_chain_config)
    shard_size = next(iter(env.quark_chain_config.shards.values())).SHARD_SIZE
    full_shard_id = shard_size | shard_id
    shard_state = ShardState(env=env, full_shard_id=full_shard_id, diff_calc=diff_calc)
    shard_state.init_genesis_state(genesis_manager.create_root_block())
    return shard_state


class TestNativeTokenShardState(unittest.TestCase):
    def setUp(self):
        super().setUp()
        config = get_test_env().quark_chain_config
        self.root_coinbase = config.ROOT.COINBASE_AMOUNT
        self.shard_coinbase = next(iter(config.shards.values())).COINBASE_AMOUNT
        # to make test verification easier, assume following tax rate
        assert config.REWARD_TAX_RATE == 0.5
        self.tax_rate = config.reward_tax_rate  # type: Fraction
        self.GENESIS_TOKEN = config.GENESIS_TOKEN  # type: str
        self.genesis_token = config.genesis_token  # type: int

    def getAfterTaxReward(self, value: int) -> int:
        return value * self.tax_rate.numerator // self.tax_rate.denominator

    def test_native_token_transfer(self):
        """in-shard transfer QETH using genesis_token as gas
        """
        QETH = token_id_encode("QETH")
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={self.GENESIS_TOKEN: 10000000, "QETH": 99999},
        )
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=21000,
            gas_token_id=self.genesis_token,
            transfer_token_id=QETH,
        )
        self.assertTrue(state.add_tx(tx))
        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            10000000 - opcodes.GTXCOST,
        )
        self.assertEqual(state.get_token_balance(acc1.recipient, QETH), 99999 - 12345)
        self.assertEqual(state.get_token_balance(acc2.recipient, QETH), 12345)
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.getAfterTaxReward(opcodes.GTXCOST + self.shard_coinbase),
        )
        tx_list, _ = state.db.get_transactions_by_address(acc1)
        self.assertEqual(tx_list[0].value, 12345)
        self.assertEqual(tx_list[0].gas_token_id, self.genesis_token)
        self.assertEqual(tx_list[0].transfer_token_id, QETH)
        tx_list, _ = state.db.get_transactions_by_address(acc2)
        self.assertEqual(tx_list[0].value, 12345)
        self.assertEqual(tx_list[0].gas_token_id, self.genesis_token)
        self.assertEqual(tx_list[0].transfer_token_id, QETH)

    def test_native_token_transfer_0_value_success(self):
        """to prevent storage spamming, do not delta_token_balance does not take action if value is 0
        """
        MALICIOUS0 = token_id_encode("MALICIOUS0")
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={
                self.GENESIS_TOKEN: 10000000,
                "MALICIOUS0": 0,
            },
        )
        state = create_default_shard_state(env=env)
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc1,
            value=0,
            gas=opcodes.GTXCOST,
            gas_token_id=self.genesis_token,
            transfer_token_id=MALICIOUS0,
        )
        self.assertTrue(state.add_tx(tx))

        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            10000000 - opcodes.GTXCOST,
        )
        self.assertEqual(state.get_token_balance(acc1.recipient, MALICIOUS0), 0)
        # MALICIOUS0 shall not be in the dict
        self.assertNotEqual(
            state.get_balances(acc1.recipient),
            {self.genesis_token: 10000000 - opcodes.GTXCOST, MALICIOUS0: 0},
        )
        self.assertEqual(
            state.get_balances(acc1.recipient),
            {self.genesis_token: 10000000 - opcodes.GTXCOST},
        )

    def test_disallowed_unknown_token(self):
        """do not allow tx with unknown token id
        """
        MALICIOUS0 = token_id_encode("MALICIOUS0")
        MALICIOUS1 = token_id_encode("MALICIOUS1")
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={self.GENESIS_TOKEN: 10000000},
        )
        state = create_default_shard_state(env=env)
        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc1,
            value=0,
            gas=opcodes.GTXCOST,
            gas_token_id=self.genesis_token,
            transfer_token_id=MALICIOUS0,
        )
        self.assertFalse(state.add_tx(tx))

        tx1 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc1,
            value=0,
            gas=opcodes.GTXCOST,
            gas_token_id=MALICIOUS1,
            transfer_token_id=self.genesis_token,
        )
        self.assertFalse(state.add_tx(tx1))

    def test_native_token_gas(self):
        """in-shard transfer QETH using native token as gas
        """
        QETH = token_id_encode("QETH")
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_random_account(full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_token_balances={"QETH": 10000000}
        )
        state = create_default_shard_state(env=env)

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=acc2,
            value=12345,
            gas=21000,
            gas_token_id=QETH,
            transfer_token_id=QETH,
        )

        self.assertTrue(state.add_tx(tx))
        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(
            state.get_token_balance(acc1.recipient, QETH),
            10000000 - opcodes.GTXCOST - 12345,
        )
        self.assertEqual(state.get_token_balance(acc2.recipient, QETH), 12345)
        # tx fee
        self.assertEqual(
            state.get_token_balance(acc3.recipient, QETH),
            self.getAfterTaxReward(opcodes.GTXCOST),
        )
        # miner coinbase
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.getAfterTaxReward(self.shard_coinbase),
        )
        tx_list, _ = state.db.get_transactions_by_address(acc1)
        self.assertEqual(tx_list[0].value, 12345)
        self.assertEqual(tx_list[0].gas_token_id, QETH)
        self.assertEqual(tx_list[0].transfer_token_id, QETH)
        tx_list, _ = state.db.get_transactions_by_address(acc2)
        self.assertEqual(tx_list[0].value, 12345)
        self.assertEqual(tx_list[0].gas_token_id, QETH)
        self.assertEqual(tx_list[0].transfer_token_id, QETH)

    def test_xshard_native_token_sent(self):
        """x-shard transfer QETH using genesis_token as gas
        """
        QETH = token_id_encode("QETHXX")
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={
                self.GENESIS_TOKEN: 10000000,
                "QETHXX": 999999,
            },
        )
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
            gas_token_id=self.genesis_token,
            transfer_token_id=QETH,
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
                transfer_token_id=QETH,
            ),
        )
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            10000000 - (opcodes.GTXCOST + opcodes.GTXXSHARDCOST),
        )
        self.assertEqual(state.get_token_balance(id1.recipient, QETH), 999999 - 888888)

        # Make sure the xshard gas is not used by local block
        self.assertEqual(
            state.evm_state.gas_used, opcodes.GTXCOST + opcodes.GTXXSHARDCOST
        )
        # GTXXSHARDCOST is consumed by remote shard
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.getAfterTaxReward(opcodes.GTXCOST + self.shard_coinbase),
        )

    def test_xshard_native_token_received(self):
        QETH = token_id_encode("QETHXX")
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_random_account(full_shard_key=0)

        env0 = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={
                self.GENESIS_TOKEN: 10000000,
                "QETHXX": 999999,
            },
            shard_size=64,
        )
        env1 = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={
                self.GENESIS_TOKEN: 10000000,
                "QETHXX": 999999,
            },
            shard_size=64,
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
            gas_token_id=self.genesis_token,
            transfer_token_id=QETH,
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
                        transfer_token_id=QETH,
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
            state0.get_token_balance(acc1.recipient, QETH), 999999 + 888888
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.getAfterTaxReward(opcodes.GTXXSHARDCOST * 2 + self.shard_coinbase),
        )

        # X-shard gas used
        self.assertEqual(
            state0.evm_state.xshard_receive_gas_used, opcodes.GTXXSHARDCOST
        )

    def test_xshard_native_token_gas_sent(self):
        """x-shard transfer QETH using QETH as gas
        """
        QETH = token_id_encode("QETHXX")
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=1)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1, genesis_minor_token_balances={"QETHXX": 9999999}
        )
        state = create_default_shard_state(env=env, shard_id=0)
        env1 = get_test_env(genesis_account=acc1, genesis_minor_token_balances={})
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
            value=8888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_token_id=QETH,
            transfer_token_id=QETH,
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
                value=8888888,
                gas_price=1,
                gas_token_id=QETH,
                transfer_token_id=QETH,
            ),
        )
        self.assertEqual(
            state.get_token_balance(id1.recipient, QETH),
            9999999 - 8888888 - (opcodes.GTXCOST + opcodes.GTXXSHARDCOST),
        )

        # Make sure the xshard gas is not used by local block
        self.assertEqual(
            state.evm_state.gas_used, opcodes.GTXCOST + opcodes.GTXXSHARDCOST
        )
        # block coinbase for mining is still in genesis_token
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.getAfterTaxReward(self.shard_coinbase),
        )
        # GTXXSHARDCOST is consumed by remote shard
        self.assertEqual(
            state.get_token_balance(acc3.recipient, QETH),
            self.getAfterTaxReward(opcodes.GTXCOST),
        )

    def test_xshard_native_token_gas_received(self):
        QETH = token_id_encode("QETHXX")
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        acc2 = Address.create_from_identity(id1, full_shard_key=16)
        acc3 = Address.create_random_account(full_shard_key=0)

        env0 = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={"QETHXX": 9999999},
            shard_size=64,
        )
        env1 = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={"QETHXX": 9999999},
            shard_size=64,
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
            value=8888888,
            gas=opcodes.GTXXSHARDCOST + opcodes.GTXCOST,
            gas_price=2,
            gas_token_id=QETH,
            transfer_token_id=QETH,
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
                        value=8888888,
                        gas_price=2,
                        gas_token_id=QETH,
                        transfer_token_id=QETH,
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
            state0.get_token_balance(acc1.recipient, QETH), 9999999 + 8888888
        )
        # Half collected by root
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, self.genesis_token),
            self.getAfterTaxReward(self.shard_coinbase),
        )
        self.assertEqual(
            state0.get_token_balance(acc3.recipient, QETH),
            self.getAfterTaxReward(opcodes.GTXXSHARDCOST * 2),
        )

        # X-shard gas used
        self.assertEqual(
            state0.evm_state.xshard_receive_gas_used, opcodes.GTXXSHARDCOST
        )

    def test_contract_suicide(self):
        """
        Kill Call Data: 0x41c0e1b5
        """
        QETH = token_id_encode("QETH")
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        id2 = Identity.create_random_identity()
        acc2 = Address.create_from_identity(id2, full_shard_key=0)
        acc3 = Address.create_random_account(full_shard_key=0)

        env = get_test_env(
            genesis_account=acc1,
            genesis_minor_token_balances={
                self.GENESIS_TOKEN: 200 * 10 ** 18,
                "QETH": 99999,
            },
        )
        state = create_default_shard_state(env=env)

        # 1. create contract
        BYTECODE = "6080604052348015600f57600080fd5b5060948061001e6000396000f3fe6080604052600436106039576000357c01000000000000000000000000000000000000000000000000000000009004806341c0e1b514603b575b005b348015604657600080fd5b50604d604f565b005b3373ffffffffffffffffffffffffffffffffffffffff16fffea165627a7a7230582034cc4e996685dcadcc12db798751d2913034a3e963356819f2293c3baea4a18c0029"
        """
        pragma solidity ^0.5.1;
        contract Sample {
          function () payable external{}
          function kill() external {selfdestruct(msg.sender);}
        }
        """
        CREATION_GAS = 92417
        tx = contract_creation_tx(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_full_shard_key=acc1.full_shard_key,
            bytecode=BYTECODE,
            gas_token_id=self.genesis_token,
            transfer_token_id=self.genesis_token,
        )
        self.assertTrue(state.add_tx(tx))
        b1 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b1.tx_list), 1)
        state.finalize_and_add_block(b1)
        self.assertEqual(state.header_tip, b1.header)
        self.assertEqual(len(state.evm_state.receipts), 1)
        self.assertEqual(state.evm_state.receipts[0].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[0].gas_used, CREATION_GAS)
        contract_address = mk_contract_address(acc1.recipient, acc1.full_shard_key, 0)
        self.assertEqual(contract_address, state.evm_state.receipts[0].contract_address)
        self.assertEqual(
            acc1.full_shard_key, state.evm_state.receipts[0].contract_full_shard_key
        )
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            200 * 10 ** 18 - CREATION_GAS,
        )
        self.assertEqual(
            state.get_token_balance(acc3.recipient, self.genesis_token),
            self.getAfterTaxReward(CREATION_GAS + self.shard_coinbase),
        )
        tx_list, _ = state.db.get_transactions_by_address(acc1)
        self.assertEqual(tx_list[0].value, 0)
        self.assertEqual(tx_list[0].gas_token_id, self.genesis_token)
        self.assertEqual(tx_list[0].transfer_token_id, self.genesis_token)

        # 2. send some default token
        tx_send = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=Address(contract_address, acc1.full_shard_key),
            value=10 * 10 ** 18,
            gas=opcodes.GTXCOST + 40,
            gas_price=1,
            nonce=None,
            data=b"",
            gas_token_id=self.genesis_token,
            transfer_token_id=self.genesis_token,
        )
        self.assertTrue(state.add_tx(tx_send))
        b2 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b2.tx_list), 1)
        state.finalize_and_add_block(b2)
        self.assertEqual(state.header_tip, b2.header)
        self.assertEqual(len(state.evm_state.receipts), 1)
        self.assertEqual(state.evm_state.receipts[0].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[0].gas_used, opcodes.GTXCOST + 40)
        self.assertEqual(
            state.get_token_balance(id1.recipient, self.genesis_token),
            200 * 10 ** 18 - CREATION_GAS - (opcodes.GTXCOST + 40) - 10 * 10 ** 18,
        )
        self.assertEqual(
            state.get_token_balance(contract_address, self.genesis_token), 10 * 10 ** 18
        )

        # 3. suicide
        SUICIDE_GAS = 13199
        tx_kill = create_transfer_transaction(
            shard_state=state,
            key=id2.get_key(),
            from_address=acc2,
            to_address=Address(contract_address, acc1.full_shard_key),
            value=0,
            gas=1000000,
            gas_price=0,  # !!! acc2 has no token yet...
            nonce=None,
            data=bytes.fromhex("41c0e1b5"),
            gas_token_id=self.genesis_token,
            transfer_token_id=self.genesis_token,
        )
        self.assertTrue(state.add_tx(tx_kill))
        b3 = state.create_block_to_mine(address=acc3)
        self.assertEqual(len(b3.tx_list), 1)
        state.finalize_and_add_block(b3)
        self.assertEqual(state.header_tip, b3.header)
        self.assertEqual(len(state.evm_state.receipts), 1)
        self.assertEqual(state.evm_state.receipts[0].state_root, b"\x01")
        self.assertEqual(state.evm_state.receipts[0].gas_used, SUICIDE_GAS)
        self.assertEqual(
            state.get_token_balance(id2.recipient, self.genesis_token), 10 * 10 ** 18
        )
        self.assertEqual(
            state.get_token_balance(contract_address, self.genesis_token), 0
        )
