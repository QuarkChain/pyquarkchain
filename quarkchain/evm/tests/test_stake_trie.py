import os
import json
import quarkchain.evm.stake_trie as stake_trie
from quarkchain.db import InMemoryDb
import itertools
from quarkchain.utils import Logger
import unittest
import random

from rlp.sedes import big_endian_int
from quarkchain.evm.utils import int_to_big_endian, big_endian_to_int


# customize VM log output to your needs
# hint: use 'py.test' with the '-s' option to dump logs to the console
# configure_logging(':trace')


def check_testdata(data_keys, expected_keys):
    assert set(data_keys) == set(
        expected_keys
    ), "test data changed, please adjust tests"


fixture_path = os.path.join(os.path.dirname(__file__), "..", "..", "..", "fixtures")


def load_tests_dict():
    fixture = {}
    testdir = os.path.join(fixture_path, "TrieTests")
    for f in os.listdir(testdir):
        if f != "staketrietest.json":
            continue
        sub_fixture = json.load(open(os.path.join(testdir, f)))
        for k, v in sub_fixture.items():
            fixture[f + "_" + k] = v
    return fixture


def load_tests(loader, tests, pattern):
    # python3 unittest interface
    suite = unittest.TestSuite()
    for key, pairs in load_tests_dict().items():
        test = unittest.FunctionTestCase(
            (lambda key, pairs: lambda: run_test(key, pairs))(key, pairs),
            description=key,
        )
        suite.addTests([test])
    return suite


def run_test(name, pairs):
    Logger.debug("testing %s" % name)
    # random seed
    seed = "000000000000000000139dba4d2169f991f9867dd3e996f5e0a86f1ae985308f"
    random.seed(seed)

    def _dec(x):
        if isinstance(x, str) and x.startswith("0x"):
            return bytes.fromhex(str(x[2:]))

        if isinstance(x, int):
            return int_to_big_endian(x)

        if isinstance(x, str):
            return bytes(x, "ascii")
        return x

    pairs["in"] = [(_dec(k), _dec(v)) for k, v in pairs["in"]]
    stakerlist = [(k, v) for k, v in pairs["in"] if v is not None]
    deletes = [(k, v) for k, v in pairs["in"] if v is None]

    N_PERMUTATIONS = 100
    N_RANDOM = 300
    for i, permut in enumerate(itertools.permutations(pairs["in"])):
        if i > N_PERMUTATIONS:
            break
        if pairs.get("nopermute", None) is not None and pairs["nopermute"]:
            permut = pairs["in"]
            N_PERMUTATIONS = 1
        t = stake_trie.Stake_Trie(InMemoryDb())
        # insert the account
        for k, v in permut:
            # logger.debug('updating with (%s, %s)' %(k, v))
            if v is not None:
                t.update(k, v)

        # check the total account of stakes
        if t._check_total_tokens() == False:
            raise Exception(
                "Mismatch_total_stake: %r %r %r"
                % (name, t.root_node, t.get_total_stake())
            )

        if t.get_total_stake() != 75:
            raise Exception(
                "Mismatch_total_stake: %r %r %r"
                % (name, t.root_node, t.get_total_stake())
            )

        # check the root hash
        if pairs["root"] != "0x" + t.root_hash.hex():
            raise Exception(
                "Mismatch_root_hash: %r %r %r %r"
                % (
                    name,
                    pairs["root"],
                    "0x" + t.root_hash.hex(),
                    (i, list(permut) + deletes),
                )
            )

        for _ in range(N_RANDOM):
            # pseudo-randomly select a stake
            chosen_num = random.random() * t.get_total_stake()
            chosen = chosen_num

            for k, v in stakerlist:
                curr_val = big_endian_to_int(v)
                if chosen <= curr_val:
                    result = k
                    break
                chosen -= curr_val

            if _dec(t.select_staker(chosen_num)) != result:
                raise Exception(
                    "Mismatch_random_selection: %r %r %r %r %r"
                    % (
                        name,
                        t.root_node,
                        chosen_num,
                        t.select_staker(chosen_num),
                        result,
                    )
                )

        # delete the corresponding account
        for k, v in permut:
            # logger.debug('updating with (%s, %s)' %(k, v))
            if v is None:
                t.delete(k)

        # make sure we have deletes at the end
        for k, v in deletes:
            t.delete(k)

        # make sure we have deleted all the stakes
        if t.get_total_stake() != 0:
            raise Exception(
                "Mismatch: %r %r %r" % (name, t.root_node, t.get_total_stake())
            )


if __name__ == "__main__":
    for name, pairs in load_tests_dict().items():
        run_test(name, pairs)
