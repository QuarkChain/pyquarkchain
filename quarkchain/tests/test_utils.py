import pytest
import random

from quarkchain.core import Address, random_bytes

from quarkchain.utils import token_id_encode, token_id_decode, ZZZZZZZZZZZZ


ENCODED_VALUES = [
    ("0", 0),
    ("Z", 35),
    ("00", 36),
    ("0Z", 71),
    ("1Z", 107),
    ("20", 108),
    ("ZZ", 1331),
    ("QKC", 35760),
    ("ZZZZZZZZZZZZ", ZZZZZZZZZZZZ),
    # ("2V4D00153RFRF", 2**64-1),
]


@pytest.mark.parametrize("name, id", ENCODED_VALUES)
def test_token_id_encode(name, id):
    assert token_id_encode(name) == id
    assert name == token_id_decode(id)


def test_random_token():
    COUNT = 100000
    random.seed(2)
    for i in range(COUNT):
        id = random.randint(0, ZZZZZZZZZZZZ)
        assert id == token_id_encode(token_id_decode(id))


def test_token_id_exceptions():
    with pytest.raises(AssertionError):
        token_id_encode("qkc")
    with pytest.raises(AssertionError):
        token_id_encode(" ")
    with pytest.raises(AssertionError):
        token_id_encode("btc")
    with pytest.raises(AssertionError):
        token_id_encode("ZZZZZZZZZZZZZ")
    with pytest.raises(AssertionError):
        token_id_decode(2 ** 64 - 1)
    with pytest.raises(AssertionError):
        token_id_decode(-1)
    with pytest.raises(AssertionError):
        token_id_decode(ZZZZZZZZZZZZ + 1)
