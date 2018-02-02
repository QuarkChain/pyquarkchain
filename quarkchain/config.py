
import quarkchain.db
from quarkchain.core import Address
from quarkchain.utils import is_p2, int_left_most_bit


class DefaultConfig:

    def __init__(self):
        self.SHARD_SIZE = 4
        self.SHARD_SIZE_BITS = 2
        self.GENESIS_ACCOUNT = Address.createFrom(
            '2bd6cc571427aa46a5e413ccbab5b9a759d08fb5142cbcb8')
        self.GENESIS_COIN = 10 ** 28
        self.GENESIS_DIFFICULTY = 100
        self.GENESIS_MINOR_DIFFICULTY = 25

    def setShardSize(self, shardSize):
        assert(is_p2(shardSize))
        self.SHARD_SIZE = shardSize
        self.SHARD_SIZE_BITS = int_left_most_bit(shardSize) - 1


class Env:

    def __init__(self, db=None, config=None):
        self.db = db or quarkchain.db.InMemoryDb()
        self.config = config or DefaultConfig()


DEFAULT_ENV = Env()
