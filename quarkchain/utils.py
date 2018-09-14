import asyncio
import ctypes
import time
import traceback
import logging

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
    """
    0b101, 0b11 -> True
    0b101, 0b10 -> False
    """
    check(m1 > 0 and m2 > 0)

    i1 = int_left_most_bit(m1)
    i2 = int_left_most_bit(m2)
    bit_mask = (1 << ((min(i1, i2) - 1))) - 1
    return (m1 & bit_mask) == (m2 & bit_mask)


def is_p2(v):
    return (v & (v - 1)) == 0


def sha3_256(x):
    if isinstance(x, bytearray):
        x = bytes(x)
    if not isinstance(x, bytes):
        raise RuntimeError("sha3_256 only accepts bytes or bytearray")
    # TODO: keccak accepts more types than bytes
    return keccak(x)


def check(condition, msg=""):
    """ Unlike assert, which can be optimized out,
    check will always check whether condition is satisfied or throw AssertionError if not
    """
    if not condition:
        raise AssertionError(msg)


def crash():
    """ Crash python interpreter """
    p = ctypes.pointer(ctypes.c_char.from_address(5))
    p[0] = b"x"


def call_async(coro):
    future = asyncio.ensure_future(coro)
    asyncio.get_event_loop().run_until_complete(future)
    return future.result()


def assert_true_with_timeout(f, duration=1):
    async def d():
        deadline = time.time() + duration
        while not f() and time.time() < deadline:
            await asyncio.sleep(0.001)
        assert f()

    asyncio.get_event_loop().run_until_complete(d())


class Logger:
    last_debug_time_map = dict()
    last_info_time_map = dict()
    last_warning_time_map = dict()
    last_error_time_map = dict()
    logger = logging.getLogger()

    @classmethod
    def is_enable_for_debug(cls):
        return cls.logger.is_enabled_for(logging.DEBUG)

    @classmethod
    def is_enable_for_info(cls):
        return cls.logger.is_enabled_for(logging.INFO)

    @classmethod
    def is_enable_for_warning(cls):
        return cls.logger.is_enabled_for(logging.WARNING)

    @staticmethod
    def debug(msg, *args, **kwargs):
        logging.debug(msg, *args, **kwargs)

    @classmethod
    def debug_every_sec(cls, msg, duration):
        stack_list = traceback.format_stack()
        if len(stack_list) <= 1:
            Logger.debug(msg)
            return
        key = stack_list[-2]

        if (
            key not in cls.last_debug_time_map
            or time.time() - cls.last_debug_time_map[key] > duration
        ):
            Logger.debug(msg)
            cls.last_debug_time_map[key] = time.time()

    @staticmethod
    def info(msg):
        logging.info(msg)

    @classmethod
    def info_every_sec(cls, msg, duration):
        stack_list = traceback.format_stack()
        if len(stack_list) <= 1:
            Logger.info(msg)
            return
        key = stack_list[-2]

        if (
            key not in cls.last_info_time_map
            or time.time() - cls.last_info_time_map[key] > duration
        ):
            Logger.info(msg)
            cls.last_info_time_map[key] = time.time()

    @staticmethod
    def warning(msg):
        logging.warning(msg)

    @classmethod
    def warning_every_sec(cls, msg, duration):
        stack_list = traceback.format_stack()
        if len(stack_list) <= 1:
            Logger.warning(msg)
            return
        key = stack_list[-2]

        if (
            key not in cls.last_warning_time_map
            or time.time() - cls.last_warning_time_map[key] > duration
        ):
            Logger.warning(msg)
            cls.last_warning_time_map[key] = time.time()

    @staticmethod
    def error(msg):
        logging.error(msg)

    @classmethod
    def error_every_sec(cls, msg, duration):
        stack_list = traceback.format_stack()
        if len(stack_list) <= 1:
            Logger.error(msg)
            return
        key = stack_list[-2]

        if (
            key not in cls.last_error_time_map
            or time.time() - cls.last_error_time_map[key] > duration
        ):
            Logger.error(msg)
            cls.last_error_time_map[key] = time.time()

    @staticmethod
    def error_exception():
        Logger.error(traceback.format_exc())

    @staticmethod
    def log_exception():
        Logger.error_exception()

    @classmethod
    def error_exception_every_sec(cls, duration):
        stack_list = traceback.format_stack()
        if len(stack_list) <= 1:
            cls.error_exception()
            return
        key = stack_list[-2]

        if (
            key not in cls.last_error_time_map
            or time.time() - cls.last_error_time_map[key] > duration
        ):
            cls.error_exception()
            cls.last_error_time_map[key] = time.time()

    @staticmethod
    def debug_exception():
        Logger.debug(traceback.format_exc())

    @staticmethod
    def fatal(msg):
        logging.critical(msg)
        crash()

    @staticmethod
    def fatal_exception(msg):
        Logger.fatal(traceback.format_exc())


"""
# Color reference
BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
RESET_SEQ = "\033[1;0m"
COLOR_SEQ = "\033[1;3%dm"
BOLD_SEQ = "\033[1m"
"""


def set_logging_level(level):
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    level = level.upper()
    if level not in level_map:
        raise RuntimeError("invalid level {}".format(level))

    logging.addLevelName(
        logging.DEBUG, "\033[1;33m%s\033[1;0m" % logging.getLevelName(logging.DEBUG)
    )
    logging.addLevelName(
        logging.WARNING, "\033[1;35m%s\033[1;0m" % logging.getLevelName(logging.WARNING)
    )
    logging.addLevelName(
        logging.ERROR, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.ERROR)
    )
    logging.addLevelName(
        logging.CRITICAL,
        "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.CRITICAL),
    )

    logging.basicConfig(
        format="%(asctime)s:%(levelname)s:%(message)s", level=level_map[level]
    )


class QKCLogFormatter(logging.Formatter):
    def format(self, record):
        prefix = get_qkc_log_prefix(record)
        return prefix + super(QKCLogFormatter, self).format(record)


def get_colored_initial_for_level(level: int):
    mapping = {
        logging.CRITICAL: "\033[1;41mC\033[1;0m",
        logging.ERROR: "\033[1;31mE\033[1;0m",
        logging.WARNING: "\033[1;35mW\033[1;0m",
        logging.INFO: "I",
        logging.DEBUG: "\033[1;33mD\033[1;0m",
        logging.NOTSET: "?",
    }
    return mapping[level]


def get_qkc_log_prefix(record: logging.LogRecord):
    """Returns the absl log prefix for the log record.
  Args:
    record: logging.LogRecord, the record to get prefix for.
  """
    created_tuple = time.localtime(record.created)
    created_microsecond = int(record.created % 1.0 * 1e6)

    level = record.levelno
    severity = get_colored_initial_for_level(level)

    return "%s%02d%02d %02d:%02d:%02d.%06d %s:%d] " % (
        severity,
        created_tuple.tm_mon,
        created_tuple.tm_mday,
        created_tuple.tm_hour,
        created_tuple.tm_min,
        created_tuple.tm_sec,
        created_microsecond,
        record.filename,
        record.lineno,
    )


def time_ms():
    """currently pypy only has Python 3.5.3, but a new nice feature added by Python 3.7 is time.time_ns()
    this function provides the same behavior but there is no precision gain
    see https://www.python.org/dev/peps/pep-0564/
    """
    return int(time.time() * 1e3)


def main():
    set_logging_level("debug")

    for i in range(100):
        Logger.debug_every_sec("log every 1s", 1)
        Logger.info_every_sec("log every 2s", 2)
        Logger.warning_every_sec("log every 3s", 3)
        Logger.error_every_sec("log every 4s", 4)
        time.sleep(0.1)


if __name__ == "__main__":
    main()
