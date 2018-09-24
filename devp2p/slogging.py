import logging

from quarkchain.evm.slogging import get_logger, configure_logging

"""
slogging module used by ethereum is configured via a comman-separated string,
and each named logger will receive custom level (defaults to INFO)
examples:
':info'
':info,p2p.discovery:debug'

because of the way that configure_logging() is written, we cannot call configure_logging() after cluster_config is loaded;
so the best way to configure slogging is to change SLOGGING_CONFIGURATION here
"""
SLOGGING_CONFIGURATION = ":info"
configure_logging(SLOGGING_CONFIGURATION)

if __name__ == "__main__":
    logging.basicConfig()
    log = get_logger("test")
    log.warn("miner.new_block", block_hash="abcdef123", nonce=2234231)
