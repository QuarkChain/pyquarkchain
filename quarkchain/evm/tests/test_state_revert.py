import pytest

from quarkchain.db import InMemoryDb
from quarkchain.evm.state import State
from quarkchain.utils import token_id_encode


@pytest.mark.parametrize("commit_after_revert", [False, True])
def test_del_account_revert_restores_balances(commit_after_revert, capsys):
    state = State(db=InMemoryDb())
    address = b"\x42" * 20
    balances = {token_id_encode("QKC"): 5, token_id_encode("QETH"): 7}
    code = b"\x60\x00\x60\x00\xf3"
    for token_id, balance in balances.items():
        state.set_token_balance(address, token_id, balance)
    state.set_nonce(address, 1)
    state.set_code(address, code)
    state.set_storage_data(address, 1, 42)
    state.commit()
    original_root = state.trie.root_hash

    snapshot = state.snapshot()
    state.del_account(address)
    assert state.get_balances(address) == {}
    assert state.get_nonce(address) == 0
    assert state.get_code(address) == b""
    assert state.get_storage_data(address, 1) == 0
    state.revert(snapshot)

    if commit_after_revert:
        # Touch the restored account so commit writes it back to the trie.
        state.set_nonce(address, 1)
        state.commit()
        assert state.cache == {}

    # Revert catches and prints journal exceptions; it must also be silent.
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert state.account_exists(address)
    assert state.get_nonce(address) == 1
    assert state.get_code(address) == code
    assert state.get_storage_data(address, 1) == 42
    for token_id, balance in balances.items():
        assert state.get_balance(address, token_id) == balance
    assert state.get_balances(address) == balances
    assert state.trie.root_hash == original_root
