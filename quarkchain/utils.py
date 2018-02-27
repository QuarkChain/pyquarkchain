import sha3
import logging
import time
import traceback
import sys


def int_left_most_bit(v):
    """ Could be replaced by better raw implementation
    """
    b = 0
    while v != 0:
        v //= 2
        b += 1
    return b


def is_p2(v):
    return (v & (v - 1)) == 0


def sha3_256(x):
    if isinstance(x, bytearray):
        x = bytes(x)
    if not isinstance(x, bytes):
        raise RuntimeError("sha3_256 only accepts bytes or bytearray")
    return sha3.keccak_256(x).digest()

def check(condition):
    """ Unlike assert, which can be optimized out,
    check will always check whether condition is satisfied or throw AssertionError if not
    """
    if not condition:
        raise AssertionError()


class Logger:
    lastInfoTimeMap = dict()
    lastWarningTimeMap = dict()
    lastErrorTimeMap = dict()

    @staticmethod
    def info(msg):
        logging.info(msg)

    @classmethod
    def infoEverySec(cls, msg, duration):
        stackList = traceback.format_stack()
        if len(stackList) <= 1:
            logging.info(msg)
            return
        key = stackList[-2]

        if key not in cls.lastInfoTimeMap or time.time() - cls.lastInfoTimeMap[key] > duration:
            logging.info(msg)
            cls.lastInfoTimeMap[key] = time.time()

    @staticmethod
    def warning(msg):
        logging.warning(msg)

    @classmethod
    def warningEverySec(cls, msg, duration):
        stackList = traceback.format_stack()
        if len(stackList) <= 1:
            logging.warning(msg)
            return
        key = stackList[-2]

        if key not in cls.lastWarningTimeMap or time.time() - cls.lastWarningTimeMap[key] > duration:
            logging.warning(msg)
            cls.lastWarningTimeMap[key] = time.time()

    @staticmethod
    def error(msg):
        logging.error(msg)

    @classmethod
    def errorEverySec(cls, msg, duration):
        stackList = traceback.format_stack()
        if len(stackList) <= 1:
            logging.error(msg)
            return
        key = stackList[-2]

        if key not in cls.lastErrorTimeMap or time.time() - cls.lastErrorTimeMap[key] > duration:
            logging.error(msg)
            cls.lastErrorTimeMap[key] = time.time()

    @staticmethod
    def errorException():
        traceback.print_exc(file=sys.stderr)

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


logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.INFO)


def main():
    for i in range(50):
        Logger.errorEverySec("log every 1s", 1)
        time.sleep(0.1)

    for i in range(50):
        Logger.errorEverySec("log every 2s", 2)
        time.sleep(0.1)


if __name__ == '__main__':
    main()
