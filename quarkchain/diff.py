from quarkchain.utils import check


class EthDifficultyCalculator:
    ''' Using metropolis or homestead algorithm (checkUncle=True or False)'''

    def __init__(self, cutoff, diffFactor, minimumDiff=1, checkUncle=False):
        self.cutoff = cutoff
        self.diffFactor = diffFactor
        self.minimumDiff = minimumDiff
        self.checkUncle = checkUncle

    def calculate_diff(self, chain, createTime=None):
        raise NotImplementedError()

    def calculate_diff_with_parent(self, parent, createTime):
        # TODO: support uncle
        check(not self.checkUncle)
        check(parent.createTime < createTime)
        sign = max(1 - (createTime - parent.createTime) // self.cutoff, -99)
        offset = parent.difficulty // self.diffFactor
        return int(max(parent.difficulty + offset * sign, self.minimumDiff))
