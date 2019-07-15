import argparse
import json
import sys

from quarkchain.cluster.cluster_config import ClusterConfig
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
        or "MLOAD_Bounds.json" in filename  # We run out of memory
        or  # We run out of memory
        # we know how to pass: force address 3 to get deleted. TODO confer
        # with c++ best path foward.
        "failed_tx_xcf416c53" in filename
        or
        # we know how to pass: delete contract's code. Looks like c++
        # issue.
        "RevertDepthCreateAddressCollision.json" in filename
        or "pairingTest.json" in filename
        or "createJS_ExampleContract" in filename  # definitely a c++ issue
        or  # definitely a c++ issue
        # Existing failed tests in pyeth test (commit 69f55e86081)
        "static_CallEcrecoverR_prefixed0.json" in filename
        or "CallEcrecoverR_prefixed0.json" in filename
        or "CALLCODEEcrecoverR_prefixed0.json" in filename
        or "static_CallEcrecover80.json" in filename
        or "CallEcrecover80.json" in filename
        or "CALLCODEEcrecover80.json" in filename
    )


def pytest_generate_tests(metafunc):
    testutils.generate_test_params(place_to_check, metafunc, exclude_func=exclude_func)


def main():
    global fixtures, filename, tests, testname, testdata

    parser = argparse.ArgumentParser()
    parser.add_argument("fixtures", type=str, help="fixture file path to run tests")
    ClusterConfig.attach_arguments(parser)
    args = parser.parse_args()

    qkc_env = DEFAULT_ENV.copy()
    qkc_env.cluster_config = ClusterConfig.create_from_args(args)

    # load fixtures from specified file or dir
    try:
        fixtures = testutils.get_tests_from_file_or_dir(args.fixtures)
    except BaseException:
        fixtures = {"stdin": json.loads(sys.argv[1])}
    for filename, tests in list(fixtures.items()):
        for testname, testdata in list(tests.items()):
            if exclude_func(filename, None, None):
                print("Skipping: %s %s" % (filename, testname))
                continue
            print("Testing: %s %s" % (filename, testname))
            # hack qkc env into the test
            testdata["qkc"] = qkc_env
            checker(testdata)


if __name__ == "__main__":
    main()
