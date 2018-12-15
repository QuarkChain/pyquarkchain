import datetime
from typing import Tuple

import rlp


def sxor(s1: bytes, s2: bytes) -> bytes:
    if len(s1) != len(s2):
        raise ValueError("Cannot sxor strings of different length")
    return bytes(x ^ y for x, y in zip(s1, s2))


def roundup_16(x: int) -> int:
    """Rounds up the given value to the next multiple of 16."""
    remainder = x % 16
    if remainder != 0:
        x += 16 - remainder
    return x


def get_devp2p_cmd_id(msg: bytes) -> int:
    """Return the cmd_id for the given devp2p msg.

    The cmd_id, also known as the payload type, is always the first entry of the RLP, interpreted
    as an integer.
    """
    return rlp.decode(msg[:1], sedes=rlp.sedes.big_endian_int)


def time_since(start_time: datetime.datetime) -> Tuple[int, int, int, int]:
    delta = datetime.datetime.now() - start_time
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return delta.days, hours, minutes, seconds


# colors from devp2p/utils.py
# ###### colors ###############

COLOR_FAIL = "\033[91m"
COLOR_BOLD = "\033[1m"
COLOR_UNDERLINE = "\033[4m"
COLOR_END = "\033[0m"

colors = ["\033[9%dm" % i for i in range(0, 7)]
colors += ["\033[4%dm" % i for i in range(1, 8)]


def cstr(num, txt):
    return "%s%s%s" % (colors[num % len(colors)], txt, COLOR_END)


def cprint(num, txt):
    print(cstr(num, txt))


if __name__ == "__main__":
    for i in range(len(colors)):
        cprint(i, "test")
