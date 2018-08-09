

class ConstMinorBlockRewardCalcultor:

    def __init__(self, env):
        self.env = env

    def get_block_reward(self, chain):
        return self.env.config.MINOR_BLOCK_DEFAULT_REWARD
