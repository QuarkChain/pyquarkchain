import copy
import functools
import json
from enum import Enum
from fractions import Fraction
from typing import List, Optional

from eth_keys import KeyAPI

import quarkchain.db
import quarkchain.evm.config
from quarkchain.core import Address
from quarkchain.utils import check, is_p2, token_id_encode

# Decimal level
QUARKSH_TO_JIAOZI = 10 ** 18


class NetworkId:
    MAINNET = 1
    # TESTNET_FORD = 2
    TESTNET_PORSCHE = 3


def _is_config_field(s: str):
    return s.isupper() and not s.startswith("_")


class BaseConfig:
    def to_dict(self):
        ret = dict()
        for k, v in self.__class__.__dict__.items():
            if _is_config_field(k):
                ret[k] = getattr(self, k) if k in self.__dict__ else v
        return ret

    @classmethod
    def from_dict(cls, d):
        config = cls()
        for k, v in d.items():
            setattr(config, k, v)
        return config

    def to_json(self):
        return json.dumps(self.to_dict(), indent=4)

    @classmethod
    def from_json(cls, j):
        return cls.from_dict(json.loads(j))

    def __eq__(self, other):
        d1 = dict()
        d2 = dict()
        for k, v in self.__class__.__dict__.items():
            if _is_config_field(k):
                d1[k] = getattr(self, k) if k in self.__dict__ else v
        for k, v in other.__class__.__dict__.items():
            if _is_config_field(k):
                d2[k] = getattr(other, k) if k in other.__dict__ else v
        return d1 == d2


class RootGenesis(BaseConfig):
    VERSION = 0
    HEIGHT = 0
    HASH_PREV_BLOCK = bytes(32).hex()
    HASH_MERKLE_ROOT = bytes(32).hex()
    # 2018/2/2 5 am 7 min 38 sec
    TIMESTAMP = 1519147489
    DIFFICULTY = 1000000
    NONCE = 0


class ShardGenesis(BaseConfig):
    ROOT_HEIGHT = 0  # hash_prev_root_block should be the root block of this height
    VERSION = 0
    HEIGHT = 0
    HASH_PREV_MINOR_BLOCK = bytes(32).hex()
    HASH_MERKLE_ROOT = bytes(32).hex()
    EXTRA_DATA = b"It was the best of times, it was the worst of times, ... - Charles Dickens".hex()
    TIMESTAMP = RootGenesis.TIMESTAMP
    DIFFICULTY = 10000
    GAS_LIMIT = 30000 * 400  # 400 xshard tx
    NONCE = 0
    ALLOC = None  # dict() hex address -> qkc amount

    def __init__(self):
        self.ALLOC = dict()

    def to_dict(self):
        ret = super().to_dict()
        return ret


class ConsensusType(Enum):
    NONE = 0  # no shard
    POW_ETHASH = 1
    POW_DOUBLESHA256 = 2
    POW_SIMULATE = 3
    POW_QKCHASH = 4

    @classmethod
    def pow_types(cls):
        return [cls.POW_ETHASH, cls.POW_DOUBLESHA256, cls.POW_SIMULATE, cls.POW_QKCHASH]


class POWConfig(BaseConfig):
    TARGET_BLOCK_TIME = 10
    REMOTE_MINE = False


class POSWConfig(BaseConfig):
    ENABLED = False
    DIFF_DIVIDER = 20  # Something similar to Beta
    WINDOW_SIZE = 256  # For estimating effective hash power
    # TODO: needs better tuning / estimating
    # = total stakes / alpha
    TOTAL_STAKE_PER_BLOCK = (10 ** 9) * QUARKSH_TO_JIAOZI


class ChainConfig(BaseConfig):
    CHAIN_ID = 0
    SHARD_SIZE = 2
    DEFAULT_CHAIN_TOKEN = "TQKC"

    CONSENSUS_TYPE = ConsensusType.NONE
    # Only set when CONSENSUS_TYPE is not NONE
    CONSENSUS_CONFIG = None  # type: POWConfig
    GENESIS = None  # type: ShardGenesis

    COINBASE_ADDRESS = bytes(24).hex()
    COINBASE_AMOUNT = 5 * QUARKSH_TO_JIAOZI
    EPOCH_INTERVAL = 210000 * 60  # same as Bitcoin but considering 10s per block

    DIFFICULTY_ADJUSTMENT_CUTOFF_TIME = 7
    DIFFICULTY_ADJUSTMENT_FACTOR = 512

    EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK = 3

    POSW_CONFIG = None  # type: POSWConfig

    def __init__(self):
        self.GENESIS = ShardGenesis()
        self.POSW_CONFIG = POSWConfig()
        self._default_chain_token = None

    def to_dict(self):
        ret = super().to_dict()
        ret["CONSENSUS_TYPE"] = self.CONSENSUS_TYPE.name
        if self.CONSENSUS_TYPE == ConsensusType.NONE:
            del ret["CONSENSUS_CONFIG"]
            del ret["GENESIS"]
        else:
            ret["CONSENSUS_CONFIG"] = self.CONSENSUS_CONFIG.to_dict()
            if self.GENESIS:
                ret["GENESIS"] = self.GENESIS.to_dict()
        ret["POSW_CONFIG"] = self.POSW_CONFIG.to_dict()
        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.CONSENSUS_TYPE = ConsensusType[config.CONSENSUS_TYPE]
        if config.CONSENSUS_TYPE in ConsensusType.pow_types():
            config.CONSENSUS_CONFIG = POWConfig.from_dict(config.CONSENSUS_CONFIG)
            if config.GENESIS:
                config.GENESIS = ShardGenesis.from_dict(config.GENESIS)
        config.POSW_CONFIG = POSWConfig.from_dict(config.POSW_CONFIG)
        return config

    @property
    def default_chain_token(self):
        if self._default_chain_token is None:
            self._default_chain_token = token_id_encode(self.DEFAULT_CHAIN_TOKEN)
        return self._default_chain_token


class ShardConfig(ChainConfig):
    SHARD_ID = 0

    def __init__(self, chain_config: ChainConfig):
        for k, v in chain_config.__dict__.items():
            if _is_config_field(k):
                setattr(self, k, copy.deepcopy(v))

        self._root_config = None
        self._default_chain_token = None

    def get_full_shard_id(self) -> int:
        return (self.CHAIN_ID << 16) | self.SHARD_SIZE | self.SHARD_ID

    @property
    def root_config(self):
        return self._root_config

    @root_config.setter
    def root_config(self, value):
        self._root_config = value

    @property
    def max_blocks_per_shard_in_one_root_block(self) -> int:
        # TODO: need to add a constant to counter the block time variance
        return (
            int(
                self.root_config.CONSENSUS_CONFIG.TARGET_BLOCK_TIME
                / self.CONSENSUS_CONFIG.TARGET_BLOCK_TIME
            )
            + self.EXTRA_SHARD_BLOCKS_IN_ROOT_BLOCK
        )

    @property
    def max_stale_minor_block_height_diff(self):
        return int(
            self.root_config.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF
            * self.root_config.CONSENSUS_CONFIG.TARGET_BLOCK_TIME
            / self.CONSENSUS_CONFIG.TARGET_BLOCK_TIME
        )

    @property
    def max_minor_blocks_in_memory(self):
        return self.max_stale_minor_block_height_diff * 2


class RootConfig(BaseConfig):
    # To ignore super old blocks from peers
    # This means the network will fork permanently after a long partition
    MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF = 60

    CONSENSUS_TYPE = ConsensusType.NONE  # type: ConsensusType
    # Only set when CONSENSUS_TYPE is not NONE
    CONSENSUS_CONFIG = None  # type: POWConfig
    GENESIS = None  # type: RootGenesis

    COINBASE_ADDRESS = bytes(24).hex()
    COINBASE_AMOUNT = 120 * QUARKSH_TO_JIAOZI
    EPOCH_INTERVAL = 210000 * 10  # same as Bitcoin but considering 60s per block

    DIFFICULTY_ADJUSTMENT_CUTOFF_TIME = 40
    DIFFICULTY_ADJUSTMENT_FACTOR = 1024

    def __init__(self):
        self.GENESIS = RootGenesis()

    @property
    def max_root_blocks_in_memory(self):
        return self.MAX_STALE_ROOT_BLOCK_HEIGHT_DIFF * 2

    def to_dict(self):
        ret = super().to_dict()
        ret["CONSENSUS_TYPE"] = self.CONSENSUS_TYPE.name
        if self.CONSENSUS_TYPE == ConsensusType.NONE:
            del ret["CONSENSUS_CONFIG"]
            del ret["GENESIS"]
        else:
            ret["CONSENSUS_CONFIG"] = self.CONSENSUS_CONFIG.to_dict()
            ret["GENESIS"] = self.GENESIS.to_dict()
        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.CONSENSUS_TYPE = ConsensusType[config.CONSENSUS_TYPE]
        if config.CONSENSUS_TYPE in ConsensusType.pow_types():
            config.CONSENSUS_CONFIG = POWConfig.from_dict(config.CONSENSUS_CONFIG)
            config.GENESIS = RootGenesis.from_dict(config.GENESIS)
        return config


class QuarkChainConfig(BaseConfig):
    CHAIN_SIZE = 3

    MAX_NEIGHBORS = 32

    NETWORK_ID = NetworkId.TESTNET_PORSCHE
    TRANSACTION_QUEUE_SIZE_LIMIT_PER_SHARD = 10000
    BLOCK_EXTRA_DATA_SIZE_LIMIT = 1024

    GUARDIAN_PUBLIC_KEY = "ab856abd0983a82972021e454fcf66ed5940ed595b0898bcd75cbe2d0a51a00f5358b566df22395a2a8bf6c022c1d51a2c3defe654e91a8d244947783029694d"
    # at the early state, guardian privkey only specified in nodes certified by QuarkChain team
    GUARDIAN_PRIVATE_KEY = None

    # P2P
    P2P_PROTOCOL_VERSION = 0
    P2P_COMMAND_SIZE_LIMIT = (2 ** 32) - 1  # unlimited right now

    # Testing related
    SKIP_ROOT_DIFFICULTY_CHECK = False
    SKIP_ROOT_COINBASE_CHECK = False
    SKIP_MINOR_DIFFICULTY_CHECK = False

    GENESIS_TOKEN = "TQKC"

    ROOT = None  # type: RootConfig
    CHAINS = None
    # full_shard_id -> ShardConfig
    SHARDS = None  # type: Dict[int, ShardConfig]

    # On mining rewards
    REWARD_TAX_RATE = 0.5  # percentage of rewards should go to root block mining

    BLOCK_REWARD_DECAY_FACTOR = 0.5

    def __init__(self):
        self.loadtest_accounts = (
            []
        )  # for TransactionGenerator. initialized in cluster_config.py

        self.ROOT = RootConfig()
        self.ROOT.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
        self.ROOT.CONSENSUS_CONFIG = POWConfig()
        self.ROOT.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = 10

        self.CHAINS = []  # type: List[ChainConfig]
        self.shards = dict()  # type: Dict[int, ShardConfig]
        for chain_id in range(self.CHAIN_SIZE):
            chain_config = ChainConfig()
            chain_config.CHAIN_ID = chain_id
            chain_config.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
            chain_config.CONSENSUS_CONFIG = POWConfig()
            chain_config.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = 3
            self.CHAINS.append(chain_config)
            for shard_id in range(ChainConfig.SHARD_SIZE):
                s = ShardConfig(chain_config)
                s.root_config = self.ROOT
                s.SHARD_ID = shard_id
                self.shards[s.get_full_shard_id()] = s

        self._cached_guardian_private_key = None

        self.init_and_validate()

        self._genesis_token = None

    def init_and_validate(self):
        self._chain_id_to_shard_size = dict()
        chain_id_to_shard_ids = dict()
        for full_shard_id, shard_config in self.shards.items():
            chain_id = shard_config.CHAIN_ID
            shard_size = shard_config.SHARD_SIZE
            shard_id = shard_config.SHARD_ID
            check(full_shard_id == (chain_id << 16 | shard_size | shard_id))
            check(is_p2(shard_size))
            if chain_id in self._chain_id_to_shard_size:
                check(shard_size == self._chain_id_to_shard_size[chain_id])
            else:
                self._chain_id_to_shard_size[chain_id] = shard_size
            chain_id_to_shard_ids.setdefault(chain_id, set()).add(shard_id)

        # check the number of ShardConfigs matches SHARD_SIZE for each chain
        # and the SHARD_ID starts from 0 to (SHARD_SIZE - 1)
        for chain_id, shard_ids in chain_id_to_shard_ids.items():
            shard_size = self.get_shard_size_by_chain_id(chain_id)
            check(shard_ids == set(range(shard_size)))

        # check the chain id starts from 0 to (CHAIN_SIZE - 1)
        check(set(chain_id_to_shard_ids.keys()) == set(range(self.CHAIN_SIZE)))

    @property
    def block_reward_decay_factor(self) -> Fraction:
        ret = Fraction(self.BLOCK_REWARD_DECAY_FACTOR).limit_denominator()
        # a simple heuristic to make sure it's at least a percent number
        assert ret.denominator <= 100
        return ret

    @property
    def reward_tax_rate(self) -> Fraction:
        ret = Fraction(self.REWARD_TAX_RATE).limit_denominator()
        # a simple heuristic to make sure it's at least a percent number
        assert ret.denominator <= 100
        return ret

    @property
    def gas_limit(self):
        return ShardGenesis.GAS_LIMIT

    def get_full_shard_id_by_full_shard_key(self, full_shard_key: int) -> int:
        chain_id = full_shard_key >> 16
        shard_size = self.get_shard_size_by_chain_id(chain_id)
        shard_id = full_shard_key & (shard_size - 1)
        return chain_id << 16 | shard_size | shard_id

    def get_shard_size_by_chain_id(self, chain_id: int) -> int:
        return self._chain_id_to_shard_size[chain_id]

    def get_genesis_root_height(self, full_shard_id: int) -> int:
        """ Return the root block height at which the shard shall be created"""
        return self.shards[full_shard_id].GENESIS.ROOT_HEIGHT

    def get_full_shard_ids(self) -> List[int]:
        """ Return a list of full_shard_ids found in the config"""
        return list(self.shards.keys())

    def get_initialized_full_shard_ids_before_root_height(
        self, root_height: int
    ) -> List[int]:
        """ Return a list of ids of the shards that have been initialized before a certain root height"""
        ids = []
        for full_shard_id, config in self.shards.items():
            if config.GENESIS.ROOT_HEIGHT < root_height:
                ids.append(full_shard_id)
        return ids

    @property
    def guardian_public_key(self) -> KeyAPI.PublicKey:
        # noinspection PyCallByClass
        return KeyAPI.PublicKey(
            public_key_bytes=bytes.fromhex(self.GUARDIAN_PUBLIC_KEY)
        )

    @property
    def guardian_private_key(self) -> Optional[KeyAPI.PrivateKey]:
        if self._cached_guardian_private_key:
            return self._cached_guardian_private_key
        # cache miss
        ret = None
        if self.GUARDIAN_PRIVATE_KEY:
            # make sure private key and public key match
            # noinspection PyCallByClass
            privkey = KeyAPI.PrivateKey(
                private_key_bytes=bytes.fromhex(self.GUARDIAN_PRIVATE_KEY)
            )
            assert privkey.public_key == self.guardian_public_key
            ret = privkey
        self._cached_guardian_private_key = ret
        return ret

    def update(
        self,
        chain_size,
        shard_size_per_chain,
        root_block_time,
        minor_block_time,
        default_token,
    ):
        self.CHAIN_SIZE = chain_size

        self.ROOT = RootConfig()
        self.ROOT.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
        self.ROOT.CONSENSUS_CONFIG = POWConfig()
        self.ROOT.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = root_block_time
        self.GENESIS_TOKEN = default_token

        self.CHAINS = []
        self.shards = dict()
        for chain_id in range(chain_size):
            chain_config = ChainConfig()
            chain_config.CHAIN_ID = chain_id
            chain_config.SHARD_SIZE = shard_size_per_chain
            chain_config.CONSENSUS_TYPE = ConsensusType.POW_SIMULATE
            chain_config.CONSENSUS_CONFIG = POWConfig()
            chain_config.CONSENSUS_CONFIG.TARGET_BLOCK_TIME = minor_block_time
            chain_config.DEFAULT_CHAIN_TOKEN = default_token
            self.CHAINS.append(chain_config)
            for shard_id in range(shard_size_per_chain):
                shard_config = ShardConfig(chain_config)
                shard_config.root_config = self.ROOT
                shard_config.SHARD_ID = shard_id
                shard_config.COINBASE_ADDRESS = (
                    Address.create_from(shard_config.COINBASE_ADDRESS)
                    .address_in_shard(shard_config.get_full_shard_id())
                    .serialize()
                    .hex()
                )
                self.shards[shard_config.get_full_shard_id()] = shard_config
        self.init_and_validate()

    def to_dict(self):
        ret = super().to_dict()
        ret["ROOT"] = self.ROOT.to_dict()
        ret["CHAINS"] = [s.to_dict() for s in self.CHAINS]
        return ret

    @classmethod
    def from_dict(cls, d):
        config = super().from_dict(d)
        config.ROOT = RootConfig.from_dict(config.ROOT)
        chains = []
        shards = dict()
        for s in config.CHAINS:
            chain_config = ChainConfig.from_dict(s)
            chains.append(chain_config)
            for shard_id in range(chain_config.SHARD_SIZE):
                shard_config = ShardConfig(chain_config)
                shard_config.root_config = config.ROOT
                shard_config.SHARD_ID = shard_id
                shard_config.COINBASE_ADDRESS = (
                    Address.create_from(shard_config.COINBASE_ADDRESS)
                    .address_in_shard(shard_config.get_full_shard_id())
                    .serialize()
                    .hex()
                )
                # filter alloc addresses based on shard id
                alloc = dict()
                for address_hex, balances in shard_config.GENESIS.ALLOC.items():
                    address = Address.create_from(bytes.fromhex(address_hex))
                    address_shard_id = address.full_shard_key & (
                        chain_config.SHARD_SIZE - 1
                    )
                    if shard_id == address_shard_id:
                        alloc[address_hex] = balances
                shard_config.GENESIS.ALLOC = alloc
                shards[shard_config.get_full_shard_id()] = shard_config
        config.CHAINS = chains
        config.shards = shards
        config.init_and_validate()
        return config

    @property
    def genesis_token(self):
        if self._genesis_token is None:
            self._genesis_token = token_id_encode(self.GENESIS_TOKEN)
        return self._genesis_token


def get_default_evm_config():
    return dict(quarkchain.evm.config.config_metropolis)
