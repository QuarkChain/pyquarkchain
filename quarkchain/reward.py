class ConstMinorBlockRewardCalcultor:
    def __init__(self, env):
        self.env = env

    def get_block_reward(self):
        return self.env.quark_chain_config.MINOR_BLOCK_DEFAULT_REWARD
