import pytest

from quarkchain.p2p.p2p_manager import encode_bytes


@pytest.mark.parametrize(
    "data, output",
    (
        (
            b"hello",
            (
                b"\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                b"hello",
            ),
        ),
        (
            b"hello world",
            (
                b"\x00\x00\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                b"hello world",
            ),
        ),
        (
            b"It was the best of times, it was the worst of times, it was the age of wisdom",
            (
                b"\x00\x00\x00M\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                b"It was the best of times, it was the worst of times, it was the age of wisdom",
            ),
        ),
    ),
)
def test_encode_bytes(data, output):
    assert encode_bytes(data) == output
