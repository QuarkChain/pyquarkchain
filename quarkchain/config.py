
import quarkchain.db
from quarkchain.core import Address


class DefaultConfig:

    def __init__(self):
        self.SHARD_SIZE = 4
        self.SHARD_SIZE_BITS = 2
        self.GENESIS_ACCOUNT = Address.createFrom(
            '2bd6cc571427aa46a5e413ccbab5b9a759d08fb5142cbcb8')
        self.GENESIS_COIN = 10 ** 28
        self.GENESIS_DIFFICULTY = 100


class Env:

    def __init__(self, db=None, config=None):
        self.db = db or quarkchain.db.InMemoryDb()
        self.config = config or DefaultConfig()


DEFAULT_ENV = Env()
