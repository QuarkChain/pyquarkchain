import pytest

from quarkchain.db import InMemoryDb
from quarkchain.evm.state import TokenBalances
from quarkchain.utils import token_id_encode


def test_blank_account():
    b = TokenBalances(b"", InMemoryDb())
    assert b.balances == {}
    assert b.serialize() == b"\x00\xc0"


@pytest.mark.parametrize(
    "encoding, mapping",
    (
        (b"\x00\xc7\xc6\x80\x84\x05\xf5\xe1\x00", {0: 100000000}),
        (
            b"\x00\xdb\xcc\x80\x8a\x15-\x04\x9e\r<eL>\x00\xcd\x83\x13\x88\xf9\x88\x1b\xc1mgN\xc8\x00\x00",
            {0: 100000132341555000000000, token_id_encode("QETH"): int(2e18)},
        ),
        (
            b"\x00\xcf\xc2\x80\x80\xc5\x83\x13\x88\xf4\x80\xc5\x83\x13\x88\xf9\x80",
            {0: 0, token_id_encode("QETH"): 0, token_id_encode("QETC"): 0},
        ),
    ),
)
def test_encode_bytes(encoding, mapping):
    # starting from blank account
    b0 = TokenBalances(b"", InMemoryDb())
    for k, v in mapping.items():
        b0.delta(k, v)
    assert b0.balances == mapping
    assert b0.serialize() == encoding

    # starting from RLP encoding
    b1 = TokenBalances(encoding, InMemoryDb())
    assert b1.balances == mapping
    assert b1.serialize() == encoding
