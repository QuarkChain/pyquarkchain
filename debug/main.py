from quarkchain.db import PersistentDb
from quarkchain.cluster.log_filter import LogFilter
from typing import List

if __name__ == "__main__":
    db = PersistentDb("", clean=False)
    addrs = []
    topics = List[List[bytes]]
    filter = LogFilter.create_from_end_block_header(db, addrs, topics, None, 100)

    logs = filter.run()
    for log in logs:
        print(log.block_number, log.block_hash, log.recipient, log.tx_hash)