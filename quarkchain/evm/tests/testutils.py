import os
import json


fixture_path = os.path.join(os.path.dirname(__file__), '../..', 'fixtures')


def generate_test_params(testsource, metafunc,
                         skip_func=None, exclude_func=None):
    import pytest
    if ['filename', 'testname', 'testdata'] != metafunc.fixturenames:
        return

    fixtures = get_tests_from_file_or_dir(
        os.path.join(fixture_path, testsource))

    base_dir = os.path.dirname(os.path.dirname(__file__))
    params = []
    for filename, tests in fixtures.items():
        if isinstance(tests, dict):
            filename = os.path.relpath(filename, base_dir)
            for testname, testdata in tests.items():
                if exclude_func and exclude_func(filename, testname, testdata):
                    continue
                if skip_func:
                    skipif = pytest.mark.skipif(
                        skip_func(filename, testname, testdata),
                        reason="Excluded"
                    )
                    params.append(skipif((filename, testname, testdata)))
                else:
                    params.append((filename, testname, testdata))

    metafunc.parametrize(
        ('filename', 'testname', 'testdata'),
        params
    )
    return params[::-1]


def get_tests_from_file_or_dir(dname, json_only=False):
    if os.path.isfile(dname):
        if dname[-5:] == '.json' or not json_only:
            with open(dname) as f:
                return {dname: json.load(f)}
        else:
            return {}
    else:
        o = {}
        for f in os.listdir(dname):
            fullpath = os.path.join(dname, f)
            for k, v in list(get_tests_from_file_or_dir(
                    fullpath, True).items()):
                o[k] = v
        return o
