from quarkchain.rlp.utils import decode_hex

from quarkchain.evm import utils
from quarkchain.db import InMemoryDb, Db


default_config = dict(
    # Genesis block difficulty
    GENESIS_DIFFICULTY=131072,
    # Genesis block gas limit
    GENESIS_GAS_LIMIT=3141592,
    # Genesis block prevhash, coinbase, nonce
    GENESIS_PREVHASH=b"\x00" * 32,
    GENESIS_COINBASE=b"\x00" * 20,
    GENESIS_NONCE=utils.zpad(utils.encode_int(42), 8),
    GENESIS_MIXHASH=b"\x00" * 32,
    GENESIS_TIMESTAMP=0,
    GENESIS_EXTRA_DATA=b"",
    GENESIS_INITIAL_ALLOC={},
    # Minimum gas limit
    MIN_GAS_LIMIT=5000,
    MAX_GAS_LIMIT=2 ** 63 - 1,
    # Gas limit adjustment algo:
    # block.gas_limit=block.parent.gas_limit * 1023/1024 +
    #                   (block.gas_used * 6 / 5) / 1024
    GASLIMIT_EMA_FACTOR=1024,
    GASLIMIT_ADJMAX_FACTOR=1024,
    BLOCK_GAS_LIMIT=4712388,
    BLKLIM_FACTOR_NOM=3,
    BLKLIM_FACTOR_DEN=2,
    # Network ID
    NETWORK_ID=1,
    # Block reward
    BLOCK_REWARD=5000 * utils.denoms.finney,
    NEPHEW_REWARD=5000 * utils.denoms.finney // 32,  # BLOCK_REWARD / 32
    # In Byzantium
    BYZANTIUM_BLOCK_REWARD=3000 * utils.denoms.finney,
    BYZANTIUM_NEPHEW_REWARD=3000 * utils.denoms.finney // 32,  # BLOCK_REWARD / 32
    # GHOST constants
    UNCLE_DEPTH_PENALTY_FACTOR=8,
    MAX_UNCLE_DEPTH=6,  # max (block.number - uncle.number)
    MAX_UNCLES=2,
    # Difficulty adjustment constants
    DIFF_ADJUSTMENT_CUTOFF=13,
    BLOCK_DIFF_FACTOR=2048,
    MIN_DIFF=131072,
    # PoW info
    POW_EPOCH_LENGTH=30000,
    # Maximum extra data length
    MAX_EXTRADATA_LENGTH=32,
    # Exponential difficulty timebomb period
    EXPDIFF_PERIOD=100000,
    EXPDIFF_FREE_PERIODS=2,
    # Blank account initial nonce
    ACCOUNT_INITIAL_NONCE=0,
    PREV_HEADER_DEPTH=256,
    # Custom specials
    CUSTOM_SPECIALS={},
)
assert default_config["NEPHEW_REWARD"] == default_config["BLOCK_REWARD"] // 32


class Env(object):
    def __init__(self, db=None, config=None, global_config=None):
        self.db = InMemoryDb() if db is None else db
        assert isinstance(self.db, Db)
        self.config = config or dict(default_config)
        self.global_config = global_config or dict()
