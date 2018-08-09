import rlp
from quarkchain.utils import sha3_256


class FakeHeader():
    """ A Fake Minor Block Header
    TODO: Move non-root-chain
    """

    def __init__(self, hash=b'\x00' * 32, number=0, timestamp=0, difficulty=1,
                 gas_limit=3141592, gas_used=0, uncles_hash=sha3_256(rlp.encode([]))):
        self.hash = hash
        self.number = number
        self.timestamp = timestamp
        self.difficulty = difficulty
        self.gas_limit = gas_limit
        self.gas_used = gas_used
        self.uncles_hash = uncles_hash

    def get_hash(self):
        return self.hash
