"""
Mostly constants related to consensus, or p2p connection that are not suitable to be in config
"""

# blocks bearing a timestamp that is slightly larger than current epoch will be broadcasted
ALLOWED_FUTURE_BLOCKS_TIME_BROADCAST = 15

# blocks bearing a timestamp that is slightly larger than current epoch will be considered valid
ALLOWED_FUTURE_BLOCKS_TIME_VALIDATION = 15

# Current minor block size is up to 6M gas / 4 (zero-byte gas) = 1.5M
# Per-command size is now 128M so 128M / 1.5M = 85
MINOR_BLOCK_BATCH_SIZE = 50

MINOR_BLOCK_HEADER_LIST_LIMIT = 100

# max number of transactions from NEW_TRANSACTION_LIST command
NEW_TRANSACTION_LIST_LIMIT = 1000

ROOT_BLOCK_BATCH_SIZE = 100

ROOT_BLOCK_HEADER_LIST_LIMIT = 500

SYNC_TIMEOUT = 10
