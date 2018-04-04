import os
import json
import quarkchain.evm.trie as trie
from quarkchain.db import InMemoryDb
import itertools
from quarkchain.utils import Logger
import unittest

# customize VM log output to your needs
# hint: use 'py.test' with the '-s' option to dump logs to the console
# configure_logging(':trace')


def check_testdata(data_keys, expected_keys):
    assert set(data_keys) == set(expected_keys), \
        "test data changed, please adjust tests"


fixture_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'fixtures')


def load_tests_dict():
    fixture = {}
    testdir = os.path.join(fixture_path, 'TrieTests')
    for f in os.listdir(testdir):
        if f != 'trietest.json':
            continue
        sub_fixture = json.load(open(os.path.join(testdir, f)))
        for k, v in sub_fixture.items():
            fixture[f + "_" + k] = v
    return fixture


def load_tests(loader, tests, pattern):
    # python3 unittest interface
    suite = unittest.TestSuite()
    for key, pairs in load_tests_dict().items():
        test = unittest.FunctionTestCase((lambda key, pairs: lambda: run_test(key, pairs))(key, pairs), description=key)
        suite.addTests([test])
    return suite


def run_test(name, pairs):
    Logger.debug('testing %s' % name)

    def _dec(x):
        if isinstance(x, str) and x.startswith('0x'):
            return bytes.fromhex(str(x[2:]))
        if isinstance(x, str):
            return bytes(x, "ascii")
        return x

    pairs['in'] = [(_dec(k), _dec(v)) for k, v in pairs['in']]
    deletes = [(k, v) for k, v in pairs['in'] if v is None]

    N_PERMUTATIONS = 100
    for i, permut in enumerate(itertools.permutations(pairs['in'])):
        if i > N_PERMUTATIONS:
            break
        if pairs.get('nopermute', None) is not None and pairs['nopermute']:
            permut = pairs['in']
            N_PERMUTATIONS = 1
        t = trie.Trie(InMemoryDb())
        for k, v in permut:
            # logger.debug('updating with (%s, %s)' %(k, v))
            if v is not None:
                t.update(k, v)
            else:
                t.delete(k)
        # make sure we have deletes at the end
        for k, v in deletes:
            t.delete(k)
        if pairs['root'] != '0x' + t.root_hash.hex():
            raise Exception("Mismatch: %r %r %r %r" % (
                name, pairs['root'], '0x' + t.root_hash.hex(), (i, list(permut) + deletes)))


if __name__ == '__main__':
    for name, pairs in load_tests_dict().items():
        run_test(name, pairs)
