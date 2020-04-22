from quarkchain.config import get_default_evm_config
from quarkchain.constants import (
    PRECOMPILED_CONTRACTS_AFTER_EVM_ENABLED,
    PRECOMPILED_CONTRACT_MINT_MNT,
    PRECOMPILED_CONTRACT_QUERY_MNT_BALANCE,
    MAINNET_ENABLE_NON_RESERVED_NATIVE_TOKEN_CONTRACT_TIMESTAMP,
    MAINNET_ENABLE_GENERAL_NATIVE_TOKEN_CONTRACT_TIMESTAMP,
    MAINNET_ENABLE_POSW_STAKING_DECAY_TIMESTAMP,
)
from quarkchain.evm.config import Env as EvmEnv
from quarkchain.db import InMemoryDb
from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.evm.specials import (
    specials,
    _system_contracts,
    SystemContract,
    configure_special_contract_ts,
    configure_system_contract_ts,
)


class Env:
    def __init__(self, db=None, evm_config=None):
        self.db = db or InMemoryDb()
        self.__cluster_config = ClusterConfig()

        self.evm_config = evm_config or get_default_evm_config()
        self.evm_config["NETWORK_ID"] = self.quark_chain_config.NETWORK_ID
        self.evm_env = EvmEnv(db=self.db, config=self.evm_config)

    def set_network_id(self, network_id):
        self.quark_chain_config.NETWORK_ID = network_id
        self.evm_config["NETWORK_ID"] = network_id

    @property
    def quark_chain_config(self):
        return self.__cluster_config.QUARKCHAIN

    @property
    def cluster_config(self):
        return self.__cluster_config

    @cluster_config.setter
    def cluster_config(self, c):
        self.__cluster_config = c
        # Configure precompiled contracts according to hard fork config
        if c.QUARKCHAIN.ENABLE_EVM_TIMESTAMP is not None:
            for addr in PRECOMPILED_CONTRACTS_AFTER_EVM_ENABLED:
                configure_special_contract_ts(
                    specials, addr, c.QUARKCHAIN.ENABLE_EVM_TIMESTAMP
                )

        # Apply mainnet default ts
        if (
            c.QUARKCHAIN.ENABLE_NON_RESERVED_NATIVE_TOKEN_TIMESTAMP is None
            and c.QUARKCHAIN.NETWORK_ID == 1
        ):
            c.QUARKCHAIN.ENABLE_NON_RESERVED_NATIVE_TOKEN_TIMESTAMP = (
                MAINNET_ENABLE_NON_RESERVED_NATIVE_TOKEN_CONTRACT_TIMESTAMP
            )
        if c.QUARKCHAIN.ENABLE_NON_RESERVED_NATIVE_TOKEN_TIMESTAMP is not None:
            configure_system_contract_ts(
                _system_contracts,
                SystemContract.NON_RESERVED_NATIVE_TOKEN,
                c.QUARKCHAIN.ENABLE_NON_RESERVED_NATIVE_TOKEN_TIMESTAMP,
            )
            for precompiled in (
                PRECOMPILED_CONTRACT_MINT_MNT,
                PRECOMPILED_CONTRACT_QUERY_MNT_BALANCE,
            ):
                configure_special_contract_ts(
                    specials,
                    precompiled,
                    c.QUARKCHAIN.ENABLE_NON_RESERVED_NATIVE_TOKEN_TIMESTAMP,
                )

        if (
            c.QUARKCHAIN.ENABLE_GENERAL_NATIVE_TOKEN_TIMESTAMP is None
            and c.QUARKCHAIN.NETWORK_ID == 1
        ):
            c.QUARKCHAIN.ENABLE_GENERAL_NATIVE_TOKEN_TIMESTAMP = (
                MAINNET_ENABLE_GENERAL_NATIVE_TOKEN_CONTRACT_TIMESTAMP
            )
        if c.QUARKCHAIN.ENABLE_GENERAL_NATIVE_TOKEN_TIMESTAMP is not None:
            configure_system_contract_ts(
                _system_contracts,
                SystemContract.GENERAL_NATIVE_TOKEN,
                c.QUARKCHAIN.ENABLE_GENERAL_NATIVE_TOKEN_TIMESTAMP,
            )
        if (
            c.QUARKCHAIN.ENABLE_POSW_STAKING_DECAY_TIMESTAMP is None
            and c.QUARKCHAIN.NETWORK_ID == 1
        ):
            c.QUARKCHAIN.ENABLE_POSW_STAKING_DECAY_TIMESTAMP = (
                MAINNET_ENABLE_POSW_STAKING_DECAY_TIMESTAMP
            )

    def copy(self):
        ret = Env(self.db, dict(self.evm_config))
        ret.cluster_config = self.__cluster_config
        return ret


DEFAULT_ENV = Env()
