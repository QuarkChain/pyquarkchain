
from quarkchain.config import DEFAULT_ENV


def get_test_env():
    env = DEFAULT_ENV
    env.config.setShardSize(2)
    env.config.SKIP_MINOR_DIFFICULTY_CHECK = True
    env.config.SKIP_ROOT_DIFFICULTY_CHECK = True
    return env
