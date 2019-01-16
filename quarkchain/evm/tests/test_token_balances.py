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
            b"\x00\xcf\xc2\x80\x80\xc5\x83\x13\x88\xf4\x80\xc5\x83\x13\x88\xf9\x80",
            {0: 0, token_id_encode("QETH"): 0, token_id_encode("QETC"): 0},
        ),
        (
            b"\x00\xf8X\xc2\x80\x80\xc4\x82\x03\xd6\x80\xc4\x82\x03\xd7\x80\xc4\x82\x03\xd8\x80\xc4\x82\x03\xd9\x80\xc4\x82\x03\xda\x80\xc4\x82\x03\xdb\x80\xc4\x82\x03\xdc\x80\xc4\x82\x03\xdd\x80\xc4\x82\x03\xde\x80\xc4\x82\x03\xdf\x80\xc4\x82\x03\xe0\x80\xc4\x82\x03\xe1\x80\xc4\x82\x03\xe2\x80\xc5\x83\x13\x88\xf4\x80\xcd\x83\x13\x88\xf9\x88)\xa2$\x1a\xf6,\x00\x00",
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
