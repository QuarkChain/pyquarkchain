import asyncio
import ctypes
import io
import logging
import os
import sys
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


_LOGGING_FILE_PREFIX = os.path.join("logging", "__init__.")


class QKCLogger(logging.getLoggerClass()):
    """https://github.com/abseil/abseil-py/blob/master/absl/logging/__init__.py
    refer to ABSLLogger
    """

    def findCaller(self, stack_info=False):
        frame = sys._getframe(2)
        f_to_skip = {func for func in dir(Logger) if callable(getattr(Logger, func))}.union({func for func in dir(QKCLogger) if callable(getattr(QKCLogger, func))})

        while frame:
            code = frame.f_code
            if _LOGGING_FILE_PREFIX not in code.co_filename and (
                "utils.py" not in code.co_filename or code.co_name not in f_to_skip
            ):
                if not stack_info:
                    return (code.co_filename, frame.f_lineno, code.co_name, "")
                else:
                    sinfo = None
                    if stack_info:
                        out = io.StringIO()
                        out.write(u"Stack (most recent call last):\n")
                        traceback.print_stack(frame, file=out)
                        sinfo = out.getvalue().rstrip(u"\n")
                    return (code.co_filename, frame.f_lineno, code.co_name, sinfo)
            frame = frame.f_back

    def trace(self, msg: str, *args, **kwargs) -> None:
        """
        log as debug for now
        see https://github.com/ethereum/py-evm/blob/master/eth/tools/logging.py
        """
        self.debug(msg, *args, **kwargs)


class Logger:
    _count_map = dict()
    _last_debug_time_map = dict()
    _last_info_time_map = dict()
    _last_warning_time_map = dict()
    _last_error_time_map = dict()
    _qkc_logger = None
    _kafka_logger = None

    @classmethod
    def set_logging_level(cls, level):
        if cls._qkc_logger:
            Logger.warning("logging_level has already been set")
            return
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

        original_logger_class = logging.getLoggerClass()
        logging.setLoggerClass(QKCLogger)
        cls._qkc_logger = logging.getLogger("qkc")
        logging.setLoggerClass(original_logger_class)

        logging.root.setLevel(level_map[level])

        formatter = QKCLogFormatter()
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logging.root.addHandler(handler)

    @classmethod
    def set_kafka_logger(cls, kafka_logger):
        cls._kafka_logger = kafka_logger

    @classmethod
    def check_logger_set(cls):
        if not cls._qkc_logger:
            cls.set_logging_level("warning")
            Logger.warning(
                "Logger is called before set_logging_level, defaulting to warning level"
            )

    @classmethod
    def is_enable_for_debug(cls):
        return cls._qkc_logger.is_enabled_for(logging.DEBUG)

    @classmethod
    def is_enable_for_info(cls):
        return cls._qkc_logger.is_enabled_for(logging.INFO)

    @classmethod
    def is_enable_for_warning(cls):
        return cls._qkc_logger.is_enabled_for(logging.WARNING)

    @classmethod
    def check_count(cls, n):
        try:
            frame = sys._getframe(2)
        except ValueError:
            return True
        key = (frame.f_code.co_filename, frame.f_lineno)
        count = cls._count_map.get(key, 0)
        cls._count_map[key] = count + 1
        return count % n == 0

    @classmethod
    def debug(cls, msg, *args, **kwargs):
        cls.check_logger_set()
        cls._qkc_logger.debug(msg, *args, **kwargs)

    @classmethod
    def debug_every_sec(cls, msg, duration):
        stack_list = traceback.format_stack()
        if len(stack_list) <= 1:
            Logger.debug(msg)
            return
        key = stack_list[-2]

        if (
            key not in cls._last_debug_time_map
            or time.time() - cls._last_debug_time_map[key] > duration
        ):
            Logger.debug(msg)
            cls._last_debug_time_map[key] = time.time()

    @classmethod
    def debug_every_n(cls, msg, n):
        if cls.check_count(n):
            Logger.debug(msg)

    @classmethod
    def info(cls, msg):
        cls.check_logger_set()
        cls._qkc_logger.info(msg)

    @classmethod
    def info_every_sec(cls, msg, duration):
        stack_list = traceback.format_stack()
        if len(stack_list) <= 1:
            Logger.info(msg)
            return
        key = stack_list[-2]

        if (
            key not in cls._last_info_time_map
            or time.time() - cls._last_info_time_map[key] > duration
        ):
            Logger.info(msg)
            cls._last_info_time_map[key] = time.time()

    @classmethod
    def info_every_n(cls, msg, n):
        if cls.check_count(n):
            Logger.info(msg)

    @classmethod
    def warning(cls, msg):
        cls.check_logger_set()
        cls._qkc_logger.warning(msg)

    @classmethod
    def warning_every_sec(cls, msg, duration):
        stack_list = traceback.format_stack()
        if len(stack_list) <= 1:
            Logger.warning(msg)
            return
        key = stack_list[-2]

        if (
            key not in cls._last_warning_time_map
            or time.time() - cls._last_warning_time_map[key] > duration
        ):
            Logger.warning(msg)
            cls._last_warning_time_map[key] = time.time()

    @classmethod
    def warning_every_n(cls, msg, n):
        if cls.check_count(n):
            Logger.warning(msg)

    @classmethod
    def error(cls, msg):
        cls.check_logger_set()
        cls._qkc_logger.error(msg)
        cls.send_log_to_kafka("error", msg)

    @classmethod
    def error_every_sec(cls, msg, duration):
        stack_list = traceback.format_stack()
        if len(stack_list) <= 1:
            Logger.error(msg)
            return
        key = stack_list[-2]

        if (
            key not in cls._last_error_time_map
            or time.time() - cls._last_error_time_map[key] > duration
        ):
            Logger.error(msg)
            cls._last_error_time_map[key] = time.time()

    @classmethod
    def error_every_n(cls, msg, n):
        if cls.check_count(n):
            Logger.error(msg)

    @classmethod
    def exception(cls, msg):
        cls.check_logger_set()
        cls._qkc_logger.error(msg)
        cls.send_log_to_kafka("exception", msg)

    @staticmethod
    def error_exception():
        Logger.exception(traceback.format_exc())

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
            key not in cls._last_error_time_map
            or time.time() - cls._last_error_time_map[key] > duration
        ):
            cls.error_exception()
            cls._last_error_time_map[key] = time.time()

    @staticmethod
    def debug_exception():
        Logger.debug(traceback.format_exc())

    @classmethod
    def fatal(cls, msg):
        cls._qkc_logger.critical(msg)
        crash()

    @staticmethod
    def fatal_exception(msg):
        Logger.fatal(traceback.format_exc())

    @classmethod
    def send_log_to_kafka(cls, level_str, msg):
        if cls._kafka_logger:
            sample = {
                "time": time_ms() // 1000,
                "network": cls._kafka_logger.cluster_config.MONITORING.NETWORK_NAME,
                "cluster": cls._kafka_logger.cluster_config.MONITORING.CLUSTER_ID,
                "level": level_str,
                "message": msg,
            }
            asyncio.ensure_future(
                cls._kafka_logger.log_kafka_sample_async(
                    cls._kafka_logger.cluster_config.MONITORING.ERRORS, sample
                )
            )

    @classmethod
    def error_only(cls, msg):
        """used by KafkaSampleLogger to avoid circular calls
        """
        cls.check_logger_set()
        cls._qkc_logger.error(msg)

    @classmethod
    def error_only_every_n(cls, msg, n):
        if cls.check_count(n):
            cls.error_only(msg)


"""
# Color reference
BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
RESET_SEQ = "\033[1;0m"
COLOR_SEQ = "\033[1;3%dm"
BOLD_SEQ = "\033[1m"
"""


class QKCLogFormatter(logging.Formatter):
    def format(self, record):
        prefix = get_qkc_log_prefix(record)
        return prefix + super(QKCLogFormatter, self).format(record)


def get_colored_initial_for_level(level: int):
    mapping = {
        logging.CRITICAL: "\033[1;41mC",
        logging.ERROR: "\033[1;31mE",
        logging.WARNING: "\033[1;35mW",
        logging.INFO: "I",
        logging.DEBUG: "\033[1;33mD",
        logging.NOTSET: "?",
    }
    return mapping[level]


def get_end_color_for_level(level: int):
    mapping = {
        logging.CRITICAL: "\033[1;0m",
        logging.ERROR: "\033[1;0m",
        logging.WARNING: "\033[1;0m",
        logging.INFO: "",
        logging.DEBUG: "\033[1;0m",
        logging.NOTSET: "?",
    }
    return mapping[level]


def get_qkc_log_prefix(record: logging.LogRecord):
    """Returns the absl-like log prefix for the log record.
  Args:
    record: logging.LogRecord, the record to get prefix for.
  """
    created_tuple = time.localtime(record.created)
    created_microsecond = int(record.created % 1.0 * 1e6)

    level = record.levelno
    severity = get_colored_initial_for_level(level)
    end_severity = get_end_color_for_level(level)

    return "%s%02d%02d%s %02d:%02d:%02d.%06d %s:%d] " % (
        severity,
        created_tuple.tm_mon,
        created_tuple.tm_mday,
        end_severity,
        created_tuple.tm_hour,
        created_tuple.tm_min,
        created_tuple.tm_sec,
        created_microsecond,
        record.filename,
        record.lineno,
    )


def time_ms():
    """currently pypy only has Python 3.5.3, so we are missing Python 3.7's time.time_ns() with better precision
    see https://www.python.org/dev/peps/pep-0564/

    the function here is a convenience; you shall use `time.time_ns() // 1e6` if using >=Python 3.7
    """
    return int(time.time() * 1e3)


def main():
    Logger.set_logging_level("debug")

    for i in range(100):
        Logger.debug_every_n("log every 10", 10)
        Logger.info_every_n("log every 20", 20)
        Logger.warning_every_n("log every 30", 30)
        Logger.error_every_n("log every 40", 40)

    for i in range(100):
        Logger.debug_every_sec("log every 1s", 1)
        Logger.info_every_sec("log every 2s", 2)
        Logger.warning_every_sec("log every 3s", 3)
        Logger.error_every_sec("log every 4s", 4)
        time.sleep(0.1)


if __name__ == "__main__":
    main()
