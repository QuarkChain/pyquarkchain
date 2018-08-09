import asyncio
import ctypes
import logging
import time
import traceback

from eth_utils import keccak


def int_left_most_bit(v):
    """ Could be replaced by better raw implementation
    """
    b = 0
    while v != 0:
        v //= 2
        b += 1
    return b


def masks_have_overlap(m1, m2):
    '''
    0b101, 0b11 -> True
    0b101, 0b10 -> False
    '''
    check(m1 > 0 and m2 > 0)

    i1 = int_left_most_bit(m1)
    i2 = int_left_most_bit(m2)
    bitMask = (1 << ((min(i1, i2) - 1))) - 1
    return (m1 & bitMask) == (m2 & bitMask)


def is_p2(v):
    return (v & (v - 1)) == 0


def sha3_256(x):
    if isinstance(x, bytearray):
        x = bytes(x)
    if not isinstance(x, bytes):
        raise RuntimeError("sha3_256 only accepts bytes or bytearray")
    # TODO: keccak accepts more types than bytes
    return keccak(x)


def check(condition):
    """ Unlike assert, which can be optimized out,
    check will always check whether condition is satisfied or throw AssertionError if not
    """
    if not condition:
        raise AssertionError()


def crash():
    """ Crash python interpreter """
    p = ctypes.pointer(ctypes.c_char.from_address(5))
    p[0] = b'x'


def call_async(coro):
    future = asyncio.ensure_future(coro)
    asyncio.get_event_loop().run_until_complete(future)
    return future.result()


def assert_true_with_timeout(f, duration=1):
        async def d():
            deadline = time.time() + duration
            while not f() and time.time() < deadline:
                await asyncio.sleep(0.001)
            assert(f())

        asyncio.get_event_loop().run_until_complete(d())


class Logger:
    lastDebugTimeMap = dict()
    lastInfoTimeMap = dict()
    lastWarningTimeMap = dict()
    lastErrorTimeMap = dict()
    logger = logging.getLogger()

    @classmethod
    def isEnableForDebug(cls):
        return cls.logger.isEnabledFor(logging.DEBUG)

    @classmethod
    def isEnableForInfo(cls):
        return cls.logger.isEnabledFor(logging.INFO)

    @classmethod
    def isEnableForWarning(cls):
        return cls.logger.isEnabledFor(logging.WARNING)

    @staticmethod
    def debug(msg, *args, **kwargs):
        logging.debug(msg, *args, **kwargs)

    @classmethod
    def debugEverySec(cls, msg, duration):
        stackList = traceback.format_stack()
        if len(stackList) <= 1:
            Logger.debug(msg)
            return
        key = stackList[-2]

        if key not in cls.lastDebugTimeMap or time.time() - cls.lastDebugTimeMap[key] > duration:
            Logger.debug(msg)
            cls.lastDebugTimeMap[key] = time.time()

    @staticmethod
    def info(msg):
        logging.info(msg)

    @classmethod
    def infoEverySec(cls, msg, duration):
        stackList = traceback.format_stack()
        if len(stackList) <= 1:
            Logger.info(msg)
            return
        key = stackList[-2]

        if key not in cls.lastInfoTimeMap or time.time() - cls.lastInfoTimeMap[key] > duration:
            Logger.info(msg)
            cls.lastInfoTimeMap[key] = time.time()

    @staticmethod
    def warning(msg):
        logging.warning(msg)

    @classmethod
    def warningEverySec(cls, msg, duration):
        stackList = traceback.format_stack()
        if len(stackList) <= 1:
            Logger.warning(msg)
            return
        key = stackList[-2]

        if key not in cls.lastWarningTimeMap or time.time() - cls.lastWarningTimeMap[key] > duration:
            Logger.warning(msg)
            cls.lastWarningTimeMap[key] = time.time()

    @staticmethod
    def error(msg):
        logging.error(msg)

    @classmethod
    def errorEverySec(cls, msg, duration):
        stackList = traceback.format_stack()
        if len(stackList) <= 1:
            Logger.error(msg)
            return
        key = stackList[-2]

        if key not in cls.lastErrorTimeMap or time.time() - cls.lastErrorTimeMap[key] > duration:
            Logger.error(msg)
            cls.lastErrorTimeMap[key] = time.time()

    @staticmethod
    def errorException():
        Logger.error(traceback.format_exc())

    @staticmethod
    def logException():
        Logger.errorException()

    @classmethod
    def errorExceptionEverySec(cls, duration):
        stackList = traceback.format_stack()
        if len(stackList) <= 1:
            cls.errorException()
            return
        key = stackList[-2]

        if key not in cls.lastErrorTimeMap or time.time() - cls.lastErrorTimeMap[key] > duration:
            cls.errorException()
            cls.lastErrorTimeMap[key] = time.time()

    @staticmethod
    def debugException():
        Logger.debug(traceback.format_exc())

    @staticmethod
    def fatal(msg):
        logging.critical(msg)
        crash()

    @staticmethod
    def fatalException(msg):
        Logger.fatal(traceback.format_exc())


'''
# Color reference
BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
RESET_SEQ = "\033[1;0m"
COLOR_SEQ = "\033[1;3%dm"
BOLD_SEQ = "\033[1m"
'''


def set_logging_level(level):
    levelMap = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    level = level.upper()
    if level not in levelMap:
        raise RuntimeError("invalid level {}".format(level))

    logging.addLevelName(logging.DEBUG, "\033[1;33m%s\033[1;0m" % logging.getLevelName(logging.DEBUG))
    logging.addLevelName(logging.WARNING, "\033[1;35m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
    logging.addLevelName(logging.ERROR, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
    logging.addLevelName(logging.CRITICAL, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.CRITICAL))

    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=levelMap[level])


def main():
    set_logging_level("debug")

    for i in range(100):
        Logger.debugEverySec("log every 1s", 1)
        Logger.infoEverySec("log every 2s", 2)
        Logger.warningEverySec("log every 3s", 3)
        Logger.errorEverySec("log every 4s", 4)
        time.sleep(0.1)


if __name__ == '__main__':
    main()
