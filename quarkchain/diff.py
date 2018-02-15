

class MADifficultyCalculator:

    def __init__(self, maSamples=16, targetIntervalSec=1, bootstrapSamples=0, slideSize=1):
        self.maSamples = maSamples
        self.targetIntervalSec = targetIntervalSec
        self.bootstrapSamples = bootstrapSamples
        self.slideSize = slideSize

    # Obtain the difficulty required for the next block
    def calculateDiff(self, chain, timeSec):
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

        return totalDiff * self.targetIntervalSec // timeUsedSec
