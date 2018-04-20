

class ConstMinorBlockRewardCalcultor:

    def __init__(self, env):
        self.env = env

    def getBlockReward(self, chain):
        return self.env.config.MINOR_BLOCK_DEFAULT_REWARD
