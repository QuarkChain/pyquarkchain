from __future__ import print_function
import struct
import rlp
from rlp.utils import encode_hex, decode_hex, str_to_bytes, bytes_to_str
from rlp.utils import safe_ord
import collections
import sys

ienc = int_to_big_endian = rlp.sedes.big_endian_int.serialize


def big_endian_to_int(s):
    return rlp.sedes.big_endian_int.deserialize(s.lstrip(b'\x00'))

idec = big_endian_to_int


def int_to_big_endian4(integer):
    ''' 4 bytes big endian integer'''
    return struct.pack('>I', integer)

ienc4 = int_to_big_endian4


node_uri_scheme = 'enode://'


def host_port_pubkey_from_uri(uri):
    b_node_uri_scheme = str_to_bytes(node_uri_scheme)
    b_uri = str_to_bytes(uri)
    assert b_uri.startswith(b_node_uri_scheme) and \
        b'@' in b_uri and b':' in b_uri, b_uri
    pubkey_hex, ip_port = b_uri[len(b_node_uri_scheme):].split(b'@')
    assert len(pubkey_hex) == 2 * 512 // 8
    ip, port = ip_port.split(b':')
    return ip, port, decode_hex(pubkey_hex)


def host_port_pubkey_to_uri(host, port, pubkey):
    assert len(pubkey) == 512 // 8
    uri = '{}{}@{}:{}'.format(node_uri_scheme,
                              bytes_to_str(encode_hex(pubkey)),
                              str(host), port)
    return str_to_bytes(uri)


PY3 = sys.version_info[0] >= 3


def remove_chars(s, chars):
    if PY3:
        d = {safe_ord(c): None for c in chars}
        return s.translate(d)
    else:
        return s.translate(None, chars)


# ###### config helpers ###############

def hex_decode_config(self):
    def _with_dict(d):
        "recursively search and decode hex encoded data"
        for k, v in d.items():
            if k.endswith('_hex'):
                d[k[:-len('_hex')]] = decode_hex(v)
            if isinstance(v, dict):
                _with_dict(v)
    _with_dict(self.config)


def update_config_with_defaults(config, default_config):
    for k, v in default_config.items():
        if isinstance(v, collections.Mapping):
            r = update_config_with_defaults(config.get(k, {}), v)
            config[k] = r
        elif k not in config:
            config[k] = default_config[k]
    return config


# ###### colors ###############

COLOR_FAIL = '\033[91m'
COLOR_BOLD = '\033[1m'
COLOR_UNDERLINE = '\033[4m'
COLOR_END = '\033[0m'

colors = ['\033[9%dm' % i for i in range(0, 7)]
colors += ['\033[4%dm' % i for i in range(1, 8)]


def cstr(num, txt):
    return '%s%s%s' % (colors[num % len(colors)], txt, COLOR_END)


def cprint(num, txt):
    print(cstr(num, txt))


def phx(x):
    return encode_hex(x)[:8]

if __name__ == '__main__':
    for i in range(len(colors)):
        cprint(i, 'test')
