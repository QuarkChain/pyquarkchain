import pytest

from quarkchain.db import InMemoryDb
from quarkchain.evm.state import TokenBalances
from quarkchain.utils import token_id_encode


def test_blank_account():
    b = TokenBalances(b"", InMemoryDb())
    assert b.balances == {}
    assert b.serialize() == b""


@pytest.mark.parametrize(
    "encoding, mapping",
    (
        (b"\x00\xc7\xc6\x80\x84\x05\xf5\xe1\x00", {0: 100000000}),
        (
            b"\x00\xdb\xcc\x80\x8a\x15-\x04\x9e\r<eL>\x00\xcd\x83\x13\x88\xf9\x88\x1b\xc1mgN\xc8\x00\x00",
            {0: 100000132341555000000000, token_id_encode("QETH"): int(2e18)},
        ),
        (
            b"\x00\xd1\xc2\x80\n\xc7\x83\x13\x88\xf4\x82\x03\xe8\xc5\x83\x13\x88\xf9d",
            {0: 10, token_id_encode("QETH"): 100, token_id_encode("QETC"): 1000},
        ),
        (
            b"\x00\xf8\x9f\xc6\x80\x84\x05\xf5\xe1\x00\xc8\x82\x03\xd6\x84w5\x94\x00\xc8\x82\x03\xd7\x84\x05\xf5\xe1\x00\xcc\x82\x03\xd8\x88\r\xe0\xb6\xb3\xa7d\x00\x00\xc8\x82\x03\xd9\x84\x05\xf5\xe1\x00\xc7\x82\x03\xda\x83\x0fB@\xc8\x82\x03\xdb\x84\x1d\xcde\x00\xc8\x82\x03\xdc\x84#\xc3F\x00\xc8\x82\x03\xdd\x84)\xb9'\x00\xc8\x82\x03\xde\x84/\xaf\x08\x00\xc8\x82\x03\xdf\x845\xa4\xe9\x00\xc8\x82\x03\xe0\x84;\x9a\xca\x00\xcc\x82\x03\xe1\x88\x01cEx]\x8a\x00\x00\xcc\x82\x03\xe2\x88\x02\x14\xe84\x8cO\x00\x00\xc9\x83\x13\x88\xf4\x84\x05\xf5\xe1\x00\xcd\x83\x13\x88\xf9\x88)\xa2$\x1a\xf6,\x00\x00",
            {
                0: int(1e8),
                token_id_encode("QETH"): int(3e18),
                token_id_encode("QETC"): int(1e8),
                token_id_encode("QA"): int(2e9),
                token_id_encode("QB"): int(1e8),
                token_id_encode("QC"): int(1e18),
                token_id_encode("QD"): int(1e8),
                token_id_encode("QE"): int(1e6),
                token_id_encode("QF"): int(5e8),
                token_id_encode("QG"): int(6e8),
                token_id_encode("QH"): int(7e8),
                token_id_encode("QI"): int(8e8),
                token_id_encode("QJ"): int(9e8),
                token_id_encode("QK"): int(10e8),
                token_id_encode("QL"): int(1e17),
                token_id_encode("QM"): int(15e16),
            },
        ),
    ),
)
def test_encode_bytes(encoding, mapping):
    # starting from blank account
    b0 = TokenBalances(b"", InMemoryDb())
    for k, v in mapping.items():
        b0.balances[k] = v
    assert b0.balances == mapping
    assert b0.serialize() == encoding

    # starting from RLP encoding
    b1 = TokenBalances(encoding, InMemoryDb())
    assert b1.balances == mapping
    assert b1.serialize() == encoding


@pytest.mark.parametrize(
    "encoding, mapping",
    (
        (b"\x00\xc0", {0: 0}),
        (
            b"\x00\xce\xcd\x83\x13\x88\xf9\x88\x1b\xc1mgN\xc8\x00\x00",
            {0: 0, token_id_encode("QETH"): int(2e18)},
        ),
        (
            b"\x00\xcf\xce\x83\x13\x88\xf9\x89\x1e?\xce;\x96\xdb\xf8\x00\x00",
            {0: 0, token_id_encode("QETH"): int(558e18), token_id_encode("QETC"): 0},
        ),
        (
            b"\x00\xce\xcd\x83\x13\x88\xf9\x88)\xa2$\x1a\xf6,\x00\x00",
            {
                0: 0,
                token_id_encode("QETH"): int(3e18),
                token_id_encode("QETC"): 0,
                token_id_encode("QA"): 0,
                token_id_encode("QB"): 0,
                token_id_encode("QC"): 0,
                token_id_encode("QD"): 0,
                token_id_encode("QE"): 0,
                token_id_encode("QF"): 0,
                token_id_encode("QG"): 0,
                token_id_encode("QH"): 0,
                token_id_encode("QI"): 0,
                token_id_encode("QJ"): 0,
                token_id_encode("QK"): 0,
                token_id_encode("QL"): 0,
                token_id_encode("QM"): 0,
            },
        ),
    ),
)
def test_encode_zero_balance(encoding, mapping):
    # starting from blank account
    b0 = TokenBalances(b"", InMemoryDb())
    for k, v in mapping.items():
        b0.balances[k] = v
    assert b0.balances == mapping
    assert b0.serialize() == encoding

    # starting from RLP encoding
    b1 = TokenBalances(encoding, InMemoryDb())
    assert b1.balances == {k: v for k, v in mapping.items() if v != 0}
    if b1.balances:
        assert b1.serialize() == encoding
    else:
        assert b1.serialize() == b""


def test_encoding_singularity():
    b0 = TokenBalances(b"", InMemoryDb())
    b0.balances[0] = 100
    b0.balances[1] = 100
    b0.balances[2] = 100
    b0.balances[3] = 100

    b1 = TokenBalances(b"", InMemoryDb())
    b1.balances[3] = 100
    b1.balances[2] = 100
    b1.balances[1] = 100
    b1.balances[0] = 100

    assert b0.serialize() == b1.serialize()
