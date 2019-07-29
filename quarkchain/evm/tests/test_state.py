import argparse
import sys
from fractions import Fraction

from quarkchain.env import DEFAULT_ENV
from quarkchain.evm.tests import new_statetest_utils, testutils

from quarkchain.evm.slogging import get_logger, configure_logging

logger = get_logger()
# customize VM log output to your needs
# hint: use 'py.test' with the '-s' option to dump logs to the console
if "--trace" in sys.argv:  # not default
    configure_logging(":trace")
    sys.argv.remove("--trace")

checker = new_statetest_utils.verify_state_test
place_to_check = "GeneralStateTests"


def test_state(filename, testname, testdata):
    logger.debug("running test:%r in %r" % (testname, filename))
    try:
        checker(testdata)
    except new_statetest_utils.EnvNotFoundException:
        pass


def exclude_func(filename, _, __):
    return (
        "stQuadraticComplexityTest" in filename
        or "stMemoryStressTest" in filename  # Takes too long
        or "static_Call50000_sha256.json" in filename  # Too long
        or "MLOAD_Bounds.json" in filename  # We run out of memory
        # we know how to pass: force address 3 to get deleted. TODO confirm.
        or "failed_tx_xcf416c53" in filename
        # The test considers a "synthetic" scenario (the state described there can't
        # be arrived at using regular consensus rules).
        # * https://github.com/ethereum/py-evm/pull/1224#issuecomment-418775512
        # The result is in conflict with the yellow-paper:
        # * https://github.com/ethereum/py-evm/pull/1224#issuecomment-418800369
        or "RevertInCreateInInit.json" in filename
    )


def pytest_generate_tests(metafunc):
    testutils.generate_test_params(place_to_check, metafunc, exclude_func=exclude_func)


def main():
    global fixtures, filename, tests, testname, testdata

    parser = argparse.ArgumentParser()
    parser.add_argument("fixtures", type=str, help="fixture file path to run tests")
    args = parser.parse_args()

    qkc_env = DEFAULT_ENV.copy()
    # disable root chain tax
    qkc_env.quark_chain_config.REWARD_TAX_RATE = Fraction(0)

    # load fixtures from specified file or dir
    fixtures = testutils.get_tests_from_file_or_dir(args.fixtures)
    for filename, tests in list(fixtures.items()):
        for testname, testdata in list(tests.items()):
            if exclude_func(filename, None, None):
                print("Skipping: %s %s" % (filename, testname))
                continue
            print("Testing: %s %s" % (filename, testname))
            # hack qkc env into the test
            testdata["qkc"] = qkc_env
            testdata["qkcstate"] = "QuarkChainStateTests" in filename
            checker(testdata)


if __name__ == "__main__":
    main()
