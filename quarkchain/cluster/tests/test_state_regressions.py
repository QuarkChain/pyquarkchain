"""Reproductions for two known defects in quarkchain/evm/state.py.

The failing expectations are marked ``xfail(strict=True)`` so the suite stays
green today; once the underlying code is fixed the marker starts failing as
XPASS and should be removed, turning each case into a regression test.
"""
import unittest

import pytest

from quarkchain.cluster.tests.test_shard_state import create_default_shard_state
from quarkchain.cluster.tests.test_utils import (
    create_transfer_transaction,
    get_test_env,
)
from quarkchain.core import Address, Identity
from quarkchain.db import InMemoryDb
from quarkchain.evm import vm
from quarkchain.evm.messages import VMExt, create_contract, mk_contract_address
from quarkchain.evm.state import State, TokenBalances
from quarkchain.evm import utils
from quarkchain.utils import token_id_encode

QKC = token_id_encode("QKC")


# ---------------------------------------------------------------------------
# 1. TokenBalances.reset() journals its undo onto a misspelled attribute
#    (`_balance` instead of `_balances`), so reverting a del_account does not
#    give the balances back.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason="state.py: reset() undo writes to _balance")
def test_token_balances_reset_undo_restores_dict_balances():
    b = TokenBalances(b"", InMemoryDb())
    b._balances = {QKC: 100}

    journal = []
    b.reset(journal)
    assert b.balance(QKC) == 0

    while journal:  # LIFO, same order as State.revert
        journal.pop()()

    # today the undo lands on a brand new `_balance` attribute instead
    assert not hasattr(b, "_balance")
    assert b.balance(QKC) == 100


@pytest.mark.xfail(strict=True, reason="state.py: reset() undo writes to _balance")
def test_del_account_revert_restores_balance():
    state = State()
    addr = b"\x01" * 20
    state.set_token_balance(addr, QKC, 100)
    state.commit()

    snapshot = state.snapshot()
    state.del_account(addr)
    assert state.get_balance(addr, QKC) == 0

    state.revert(snapshot)
    assert state.get_balance(addr, QKC) == 100


def test_del_account_revert_is_masked_when_balances_live_in_trie():
    """With >16 tokens the balances sit in the trie, whose undo is spelled
    correctly, so the bug is invisible -- which is why the existing test
    ``test_reset_balance_in_trie_and_revert`` passes."""
    state = State()
    addr = b"\x03" * 20
    tokens = [token_id_encode("Q" + chr(65 + i)) for i in range(17)]
    for i, t in enumerate(tokens):
        state.set_token_balance(addr, t, 100 + i)
    state.commit()

    snapshot = state.snapshot()
    state.del_account(addr)
    state.revert(snapshot)

    assert state.get_balance(addr, tokens[0]) == 100


# ---------------------------------------------------------------------------
# 2. An account's full_shard_key is frozen by the first *read* of the address,
#    because get_and_cache_account() caches a blank account stamped with the
#    then-current state.full_shard_key, and the cache lives for a whole block.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True, reason="state.py: blank account cached on read freezes full_shard_key"
)
def test_full_shard_key_frozen_by_a_pure_read():
    state = State()
    addr = b"\x02" * 20

    state.full_shard_key = 0xAAAA
    assert state.get_balance(addr, QKC) == 0  # read only, account does not exist

    state.full_shard_key = 0xBBBB
    state.delta_token_balance(addr, QKC, 100)  # first write
    state.commit()

    assert state.get_full_shard_key(addr) == 0xBBBB


def test_full_shard_key_without_the_preceding_read():
    state = State()
    addr = b"\x02" * 20

    state.full_shard_key = 0xAAAA
    state.full_shard_key = 0xBBBB
    state.delta_token_balance(addr, QKC, 100)
    state.commit()

    assert state.get_full_shard_key(addr) == 0xBBBB


# ---------------------------------------------------------------------------
# 3. Contract creation snapshots too late to restore a funded target's
#    storage when init code fails.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason="create_contract snapshots after clearing the existing target",
)
def test_failed_contract_creation_restores_existing_target_storage():
    state = State()
    sender = b"\x10" * 20
    full_shard_key = 1
    target = mk_contract_address(sender, utils.encode_int(0), full_shard_key)

    # This target passes the collision check: nonce == 0 and code is empty.
    # It is nevertheless an existing account because it has a balance.
    state.set_balance(target, 100)
    state.set_storage_data(target, 7, 42)
    state.set_nonce(sender, 1)  # create_contract derives address nonce 0
    state.full_shard_key = full_shard_key
    state.commit()

    ext = VMExt(state, sender, gas_price=0)
    msg = vm.Message(
        sender=sender,
        to=b"",
        value=0,
        gas=100000,
        data=b"\xfe",  # INVALID: init code fails before returning runtime code
        to_full_shard_key=full_shard_key,
        transfer_token_id=state.shard_config.default_chain_token,
    )

    result, _, _ = create_contract(ext, msg)

    assert result == 0
    assert state.get_balance(target) == 100
    assert state.get_storage_data(target, 7) == 42


def test_full_shard_key_unaffected_by_read_with_should_cache_false():
    state = State()
    addr = b"\x02" * 20

    state.full_shard_key = 0xAAAA
    assert state.get_balance(addr, QKC, should_cache=False) == 0

    state.full_shard_key = 0xBBBB
    state.delta_token_balance(addr, QKC, 100)
    state.commit()

    assert state.get_full_shard_key(addr) == 0xBBBB


class TestFullShardKeyInBlock(unittest.IsolatedAsyncioTestCase):
    """Block-level version. ShardState.add_tx needs a running event loop."""

    @staticmethod
    def _setup():
        id1 = Identity.create_random_identity()
        acc1 = Address.create_from_identity(id1, full_shard_key=0)
        env = get_test_env(genesis_account=acc1, genesis_minor_quarkash=10000000)
        return id1, acc1, env, create_default_shard_state(env=env)

    @pytest.mark.xfail(
        strict=True,
        reason="state.py: blank account cached on read freezes full_shard_key",
    )
    async def test_frozen_by_earlier_tx_in_same_block(self):
        """A zero-value transfer only touches the recipient, but it decides the
        full_shard_key that a later tx in the same block persists for it."""
        id1, acc1, env, state = self._setup()
        recipient = Identity.create_random_identity().recipient

        # with shard_size == 2, keys 0 and 2 both map to full_shard_id 2
        # (chain 0, shard 0), so both transfers stay in-shard
        tx1 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=Address(recipient, 0),
            value=0,
            nonce=0,
        )
        self.assertTrue(state.add_tx(tx1))
        tx2 = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=Address(recipient, 2),
            value=1000,
            nonce=1,
        )
        self.assertTrue(state.add_tx(tx2))

        block = state.create_block_to_mine(address=acc1)
        self.assertEqual(len(block.tx_list), 2)
        state.finalize_and_add_block(block)
        self.assertEqual(
            state.get_token_balance(recipient, env.quark_chain_config.genesis_token),
            1000,
        )

        # comes from tx1 (first touch), not tx2 (the tx that created the account)
        self.assertEqual(state.evm_state.get_full_shard_key(recipient), 2)

    async def test_when_only_the_creating_tx_runs(self):
        """Control: without the zero-value tx the key is the expected one."""
        id1, acc1, env, state = self._setup()
        recipient = Identity.create_random_identity().recipient

        tx = create_transfer_transaction(
            shard_state=state,
            key=id1.get_key(),
            from_address=acc1,
            to_address=Address(recipient, 2),
            value=1000,
            nonce=0,
        )
        self.assertTrue(state.add_tx(tx))
        block = state.create_block_to_mine(address=acc1)
        state.finalize_and_add_block(block)

        self.assertEqual(state.evm_state.get_full_shard_key(recipient), 2)
