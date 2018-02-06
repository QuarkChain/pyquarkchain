
import copy
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
        self.GENESIS_MINOR_COIN = 0
        self.GENESIS_DIFFICULTY = 100
        self.GENESIS_MINOR_DIFFICULTY = 25
        # 2018/2/2 5 am 7 min 38 sec
        self.GENESIS_CREATE_TIME = 1517547849
        self.PROOF_OF_PROGRESS_BLOCKS = 1
        self.SKIP_ROOT_DIFFICULTY_CHECK = False
        self.SKIP_MINOR_DIFFICULTY_CHECK = False

    def setShardSize(self, shardSize):
        assert(is_p2(shardSize))
        self.SHARD_SIZE = shardSize
        self.SHARD_SIZE_BITS = int_left_most_bit(shardSize) - 1

    def copy(self):
        return copy.copy(self)


class Env:

    def __init__(self, db=None, config=None):
        self.db = db or quarkchain.db.InMemoryDb()
        self.config = config or DefaultConfig()

    def copy(self):
        return copy.copy(self)


DEFAULT_ENV = Env()
