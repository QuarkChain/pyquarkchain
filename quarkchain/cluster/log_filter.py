import time
from typing import List, Optional

from quarkchain.cluster.shard_db_operator import ShardDbOperator
from quarkchain.core import Address, Log, MinorBlock, MinorBlockHeader
from quarkchain.evm.bloom import bloom
from quarkchain.utils import Logger


class LogFilter:
    """
    Filter class for logs, blocks, pending tx, etc.
    TODO: For now only supports filtering logs.
    """

    TIMEOUT = 10  # seconds

    def __init__(
        self,
        db: ShardDbOperator,
        addresses: List[Address],
        topics: List[List[bytes]],
        end_block_header: MinorBlockHeader,
        size: int,
        candidate_blocks: Optional[List[MinorBlock]] = None,
        block_hash: Optional[str] = None,
    ):
        """
        `topics` is a list of lists where each one expresses the OR semantics,
        while the whole list itself is connected by AND. For details check the
        Ethereum JSONRPC spec.
        """
        self.db = db
        # if `addresses` present, should be in the same shard
        self.recipients = [addr.recipient for addr in addresses]
        self.end_block_header = end_block_header
        self.size = size
        self.candidate_blocks = candidate_blocks
        if candidate_blocks is not None and (end_block_header is not None or size != 0):
            raise ValueError(
                "Should pass in either candidate blocks or end block header and size"
            )
        if candidate_blocks is not None:
            candidate_blocks.sort(key=lambda x: x.header.height)
        self.block_hash = block_hash  # TODO: not supported yet
        # construct bloom bits:
        # innermost: an integer with 3 bits set
        # outer: a list of those integers are connected by OR operator
        # outermost: a list of those lists are connected by AND operator
        self.bloom_bits = []  # type: List[List[int]]
        for r in self.recipients:
            b = bloom(r)
            self.bloom_bits.append([b])
        self.topics = topics
        for tp_list in topics:
            if not tp_list:
                # regard as wildcard
                continue
            bloom_list = []
            for tp in tp_list:
                bloom_list.append(bloom(tp))
            self.bloom_bits.append(bloom_list)
        # a timestamp to control timeout. will be set upon running
        self.start_ts = None

    def _get_block_candidates(self) -> List[MinorBlock]:
        """Use given criteria to generate potential blocks matching the bloom."""

        def should_skip(block: MinorBlock) -> bool:
            should_skip_block = False
            # same byte order as in bloom.py
            header_bloom = block.header.bloom
            for bit_list in self.bloom_bits:
                if not any((header_bloom & i) == i for i in bit_list):
                    should_skip_block = True
                    break
            return should_skip_block

        ret = []
        if self.candidate_blocks is not None:
            ret = [b for b in self.candidate_blocks if not should_skip(b)]
        else:
            end_block_hash = self.end_block_header.get_hash()
            for i in range(self.size):
                block = self.db.get_minor_block_by_hash(end_block_hash)
                if not block:
                    Logger.error(
                        "No block found for height {} at shard {}".format(
                            i, self.db.branch.get_full_shard_id()
                        )
                    )
                    continue

                if not should_skip(block):
                    ret.append(block)

                end_block_hash = block.header.hash_prev_block
            ret.reverse()

        return ret

    def _get_logs(self, blocks: List[MinorBlock]) -> List[Log]:
        """Given potential blocks, re-run tx to find exact matches."""
        ret = []
        for b_i, block in enumerate(blocks):
            tx_list_len = len(block.tx_list)
            deposit_hlist = self.db.get_xshard_deposit_hash_list(
                block.header.get_hash()
            )
            deposit_hlist_len = 0 if not deposit_hlist else len(deposit_hlist.hlist)
            for i in range(tx_list_len + deposit_hlist_len):
                # only provide deposit list when needed
                r = block.get_receipt(
                    self.db.db, i, None if i < tx_list_len else deposit_hlist
                )
                for log in r.logs:
                    # empty recipient means no filtering
                    if self.recipients and log.recipient not in self.recipients:
                        continue
                    if self._log_topics_match(log):
                        ret.append(log)
            if (1 + b_i) % 100 == 0 and time.time() - self.start_ts > LogFilter.TIMEOUT:
                raise Exception("Filter timeout")
        return ret

    def _log_topics_match(self, log: Log) -> bool:
        """Whether a log matches given criteria in constructor. Position / order matters."""
        # https://github.com/ethereum/wiki/wiki/JSON-RPC#a-note-on-specifying-topic-filters
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
        self.start_ts = time.time()
        candidate_blocks = self._get_block_candidates()
        logs = self._get_logs(candidate_blocks)
        return logs
