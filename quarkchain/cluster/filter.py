from typing import List, Optional, Union, Callable

from quarkchain.cluster.shard_db_operator import ShardDbOperator
from quarkchain.core import Address, Transaction, MinorBlock, Branch
from quarkchain.evm.bloom import bloom
from quarkchain.evm.messages import apply_transaction, Receipt, Log
from quarkchain.evm.state import State as EvmState
from quarkchain.utils import Logger


class Filter:
    """
    Filter class for logs, blocks, pending tx, etc.
    TODO: For now only supports filtering logs.
    """

    def __init__(
        self,
        db: ShardDbOperator,
        branch: Branch,
        state_gen_func: Callable[[MinorBlock], EvmState],
        addresses: List[Address],
        topics: List[Optional[Union[str, List[str]]]],
        start_block: int,
        end_block: int,
        block_hash: Optional[str] = None,
    ):
        self.db = db
        self.branch = branch
        self.state_gen_func = state_gen_func
        # if `addresses` present, should be in the same shard
        self.recipients = [addr.recipient for addr in addresses]
        self.topics = topics
        self.start_block = start_block
        self.end_block = end_block
        self.block_hash = block_hash  # TODO: not supported yet
        # construct bloom bits:
        # innermost: an integer with 3 bits set
        # outer: a list of those integers are connected by OR operator
        # outermost: a list of those lists are connected by AND operator
        self.bloom_bits = []  # type: List[List[int]]
        for r in self.recipients:
            b = bloom(r)
            self.bloom_bits.append([b])
        for tp in topics:
            if not tp:
                # regard as wildcard
                continue
            if isinstance(tp, list):  # list of str "0x..."
                bloom_list = []
                for sub_tp in tp:
                    bs = bytes.fromhex(sub_tp[2:])
                    bloom_list.append(bloom(bs))
                self.bloom_bits.append(bloom_list)
            else:  # str
                self.bloom_bits.append([bloom(bytes.fromhex(tp[2:]))])

    def _get_block_candidates(self) -> List[MinorBlock]:
        """Use given criteria to generate potential blocks matching the bloom."""
        ret = []
        for i in range(self.start_block, self.end_block + 1):
            block = self.db.get_minor_block_by_height(i)
            if not block:
                Logger.error(
                    "No block found for height {} at shard {}".format(
                        i, self.db.branch.get_shard_id()
                    )
                )
                continue
            should_skip_block = False
            # same byte order as in bloom.py
            header_bloom = block.header.bloom
            for bit_list in self.bloom_bits:
                if not any((header_bloom & i) == i for i in bit_list):
                    should_skip_block = True
                    break

            if not should_skip_block:
                ret.append(block)

        return ret

    def _get_logs(self, blocks: List[MinorBlock]) -> List[Log]:
        """Given potential blocks, re-run tx to find exact matches."""
        ret = []
        for block in blocks:
            tx_list = block.tx_list or []  # type: List[Transaction]
            # use evm state in the previous block
            evm_state = self.state_gen_func(
                self.db.get_minor_block_by_height(block.header.height - 1)
            )
            # execute all tx, then check all the generated logs
            for tx in tx_list:
                evm_tx = tx.code.get_evm_transaction()
                evm_tx.shard_size = self.branch.get_shard_size()
                apply_transaction(evm_state, evm_tx, tx.get_hash())
            for r in evm_state.receipts:  # type: Receipt
                for log in r.logs:  # type: Log
                    # empty recipient means no filtering
                    if self.recipients and log.address not in self.recipients:
                        continue
                    if self._log_topics_match(log):
                        ret.append(log)
        return ret

    def _log_topics_match(self, log: Log) -> bool:
        """Whether a log matches given criteria in constructor. Position / order matters."""
        if len(self.topics) != len(log.topics):
            return False
        for criteria, log_topic in zip(self.topics, log.topics):
            if not criteria:
                continue
            if isinstance(criteria, list):  # list of str "0x..."
                if not any(int(c[2:], 16) == log_topic for c in criteria):
                    return False
            else:  # str
                if int(criteria[2:], 16) != log_topic:
                    return False
        return True

    def run(self) -> List[Log]:
        candidate_blocks = self._get_block_candidates()
        logs = self._get_logs(candidate_blocks)
        return logs
