import asyncio
import random
import time
from typing import Optional

from quarkchain.config import QuarkChainConfig
from quarkchain.core import Address, Code, Transaction
from quarkchain.evm.transactions import Transaction as EvmTransaction
from quarkchain.utils import Logger


class Account:
    def __init__(self, address: Address, key: bytes):
        self.address = address
        self.key = key


class TransactionGenerator:
    def __init__(self, qkc_config: QuarkChainConfig, shard):
        self.qkc_config = qkc_config
        self.full_shard_id = shard.full_shard_id
        self.shard = shard
        self.running = False

        self.accounts = []
        for item in qkc_config.loadtest_accounts:
            account = Account(
                Address.create_from(item["address"]), bytes.fromhex(item["key"])
            )
            self.accounts.append(account)

    def generate(self, num_tx, x_shard_percent, tx: Transaction):
        """Generate a bunch of transactions in the network """
        if self.running or not self.shard.state.initialized:
            return False

        self.running = True
        asyncio.ensure_future(self.__gen(num_tx, x_shard_percent, tx))
        return True

    async def __gen(self, num_tx, x_shard_percent, sample_tx: Transaction):
        Logger.info(
            "[{}] start generating {} transactions with {}% cross-shard".format(
                self.full_shard_id, num_tx, x_shard_percent
            )
        )
        try:
            if num_tx <= 0:
                return
            start_time = time.time()
            tx_list = []
            total = 0
            sample_evm_tx = sample_tx.code.get_evm_transaction()
            for account in self.accounts:
                nonce = self.shard.state.get_transaction_count(
                    account.address.recipient
                )
                tx = self.create_transaction(
                    account, nonce, x_shard_percent, sample_evm_tx
                )
                if not tx:
                    continue
                tx_list.append(tx)
                total += 1
                if len(tx_list) >= 600 or total >= num_tx:
                    self.shard.add_tx_list(tx_list)
                    tx_list = []
                    await asyncio.sleep(
                        random.uniform(8, 12)
                    )  # yield CPU so that other stuff won't be held for too long

                if total >= num_tx:
                    break

            end_time = time.time()
        except Exception as e:
            Logger.error(e)
        Logger.info(
            "[{}] generated {} transactions in {:.2f} seconds".format(
                self.full_shard_id, total, end_time - start_time
            )
        )
        self.running = False

    def create_transaction(
        self, account, nonce, x_shard_percent, sample_evm_tx
    ) -> Optional[Transaction]:
        # skip if from shard is specified and not matching current branch
        # FIXME: it's possible that clients want to specify '0x0' as the full shard ID, however it will not be supported
        if (
            sample_evm_tx.from_full_shard_key
            and self.qkc_config.get_full_shard_id_by_full_shard_key(
                sample_evm_tx.from_full_shard_key
            )
            != self.full_shard_id
        ):
            return None

        if sample_evm_tx.from_full_shard_key:
            from_full_shard_key = sample_evm_tx.from_full_shard_key
        else:
            from_full_shard_key = self.full_shard_id

        if not sample_evm_tx.to:
            to_address = random.choice(self.accounts).address
            recipient = to_address.recipient
            to_full_shard_key = self.full_shard_id
        else:
            recipient = sample_evm_tx.to
            to_full_shard_key = from_full_shard_key

        if random.randint(1, 100) <= x_shard_percent:
            # x-shard tx
            to_full_shard_id = random.choice(
                self.qkc_config.get_full_shard_ids() - [self.full_shard_id]
            )
            to_full_shard_key = to_full_shard_id

        value = sample_evm_tx.value
        if not sample_evm_tx.data:
            value = random.randint(1, 100) * (10 ** 15)

        evm_tx = EvmTransaction(
            nonce=nonce,
            gasprice=sample_evm_tx.gasprice,
            startgas=sample_evm_tx.startgas,
            to=recipient,
            value=value,
            data=sample_evm_tx.data,
            gas_token_id=sample_evm_tx.gas_token_id,
            transfer_token_id=sample_evm_tx.transfer_token_id,
            from_full_shard_key=from_full_shard_key,
            to_full_shard_key=to_full_shard_key,
            network_id=self.qkc_config.NETWORK_ID,
        )
        evm_tx.sign(account.key)
        return Transaction(code=Code.create_evm_code(evm_tx))
