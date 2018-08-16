import logging

from quarkchain.evm.slogging import get_logger, configure, configure_logging, getLogger
configure_logging()

if __name__ == '__main__':
    logging.basicConfig()
    log = get_logger('test')
    log.warn('miner.new_block', block_hash='abcdef123', nonce=2234231)
