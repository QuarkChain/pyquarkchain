from typing import List, Optional, Union

from quarkchain.cluster.shard_db_operator import ShardDbOperator
from quarkchain.core import Address, Log, MinorBlock
from quarkchain.evm.bloom import bloom
from quarkchain.utils import Logger


class Filter:
    """
    Filter class for logs, blocks, pending tx, etc.
    TODO: For now only supports filtering logs.
    """

    def __init__(
        self,
        db: ShardDbOperator,
        addresses: List[Address],
        topics: List[Optional[Union[str, List[str]]]],
        start_block: int,
        end_block: int,
        block_hash: Optional[str] = None,
    ):
        self.db = db
        # if `addresses` present, should be in the same shard
        self.recipients = [addr.recipient for addr in addresses]
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
        self.topics = []  # type: List[Optional[Union[byte, List[bytes]]]]
        for tp in topics:
            if not tp:
                # regard as wildcard
                self.topics.append(tp)
                continue
            if isinstance(tp, list):  # list of str "0x..."
                bloom_list, topic_list = [], []
                for sub_tp in tp:
                    bs = bytes.fromhex(sub_tp[2:])
                    bloom_list.append(bloom(bs))
                    topic_list.append(int(sub_tp[2:], 16).to_bytes(32, "big"))
                self.bloom_bits.append(bloom_list)
                self.topics.append(topic_list)
            else:  # str
                self.bloom_bits.append([bloom(bytes.fromhex(tp[2:]))])
                self.topics.append(int(tp[2:], 16).to_bytes(32, "big"))

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
            for i in range(len(block.tx_list or [])):
                r = block.get_receipt(self.db.db, i)  # type: Receipt
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
            if isinstance(criteria, list):  # list of bytes
                if not any(c == log_topic for c in criteria):
                    return False
            else:  # single criteria as bytes
                if criteria != log_topic:
                    return False
        return True

    def run(self) -> List[Log]:
        candidate_blocks = self._get_block_candidates()
        logs = self._get_logs(candidate_blocks)
        return logs
