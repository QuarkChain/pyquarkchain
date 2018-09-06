import copy
from quarkchain.config import get_default_evm_config
from quarkchain.evm.config import Env as EvmEnv
from quarkchain.db import InMemoryDb
from quarkchain.cluster.cluster_config import ClusterConfig


class Env:
    def __init__(self, db=None, evm_config=None, cluster_config=None):
        self.db = db or InMemoryDb()
        self.cluster_config = cluster_config if cluster_config else ClusterConfig()

        self.evm_config = evm_config or get_default_evm_config()
        self.evm_config["NETWORK_ID"] = self.quark_chain_config.NETWORK_ID
        self.evm_env = EvmEnv(db=self.db, config=self.evm_config)

    def set_network_id(self, network_id):
        self.quark_chain_config.NETWORK_ID = network_id
        self.evm_config["NETWORK_ID"] = network_id

    @property
    def quark_chain_config(self):
        return self.cluster_config.QUARKCHAIN

    def copy(self):
        return Env(
            self.db,
            dict(self.evm_config),
            copy.copy(self.cluster_config) if self.cluster_config else None,
        )


DEFAULT_ENV = Env()