from quarkchain.utils import check


class MADifficultyCalculator:

    def __init__(self, maSamples=16, targetIntervalSec=1, bootstrapSamples=0, slideSize=1, minimumDiff=1):
        self.maSamples = maSamples
        self.targetIntervalSec = targetIntervalSec
        self.bootstrapSamples = bootstrapSamples
        self.slideSize = slideSize
        self.minimumDiff = minimumDiff

    def calculateDiff(self, chain, createTime=None):
        """ Obtain the difficulty required for the next block\
        """
        tip = chain.tip()

        if tip.height < self.bootstrapSamples:
            return chain.getBlockHeaderByHeight(0).difficulty

        if tip.height < self.maSamples:
            startHeader = chain.getBlockHeaderByHeight(0)
        else:
            startHeader = chain.getBlockHeaderByHeight(
                tip.height - self.maSamples)

        timeUsedSec = tip.createTime - startHeader.createTime
        totalDiff = 0
        for i in range(startHeader.height + 1, tip.height + 1):
            totalDiff += chain.getBlockHeaderByHeight(i).difficulty

        return max(totalDiff * self.targetIntervalSec // timeUsedSec, self.minimumDiff)

    def calculateDiffWithParent(self, parent, createTime):
        raise NotImplementedError()


class EthDifficultyCalculator:
    ''' Using metropolis or homestead algorithm (checkUncle=True or False)'''

    def __init__(self, cutoff, diffFactor, minimumDiff=1, checkUncle=False):
        self.cutoff = cutoff
        self.diffFactor = diffFactor
        self.minimumDiff = minimumDiff
        self.checkUncle = checkUncle

    def calculateDiff(self, chain, createTime=None):
        raise NotImplementedError()

    def calculateDiffWithParent(self, parent, createTime):
        # TODO: support uncle
        check(not self.checkUncle)
        check(parent.createTime < createTime)
        sign = max(1 - (createTime - parent.createTime) // self.cutoff, -99)
        offset = parent.difficulty // self.diffFactor
        return int(max(parent.difficulty + offset * sign, self.minimumDiff))
