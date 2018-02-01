import unittest
import diff
import time
import proof_of_work
import statistics as stat


class Block:

    def __init__(self, nTime=0.0, requiredDiff=0.1):
        self.nTime = nTime
        self.requiredDiff = requiredDiff

    def getRequiredDiff(self):
        return self.requiredDiff

    def getCreateTimeSec(self):
        return self.nTime


class TestMADifficulty(unittest.TestCase):

    def testNoneSample(self):
        chain = [Block(0, 0.1)]
        diffCalc = diff.MADifficultyCalculator(
            maSamples=2, targetIntervalSec=5.0)
        self.assertEqual(diffCalc.calculateDiff(chain), 0.1)

    def testOneSample(self):
        chain = [Block(0, 0.1), Block(4.0, 0.1)]
        diffCalc = diff.MADifficultyCalculator(
            maSamples=2, targetIntervalSec=5.0)
        self.assertEqual(diffCalc.calculateDiff(chain), 0.08)

    def testTwoSample(self):
        chain = [Block(0, 0.1), Block(4.0, 0.1), Block(10, 0.08)]
        diffCalc = diff.MADifficultyCalculator(
            maSamples=2, targetIntervalSec=5.0)
        self.assertEqual(diffCalc.calculateDiff(chain), 1 / 11.25)


def main():
    targetIntervalSec = 5
    diffCalc = diff.MADifficultyCalculator(maSamples=32, targetIntervalSec=5)
    hashPower = 100

    cTime = 0.0
    chain = [Block(0, 0.002)]
    usedTimeList = []
    p = proof_of_work.PoW(hashPower)

    for i in range(1000):
        requiredDiff = diffCalc.calculateDiff(chain)
        cTime = cTime + p.mine(requiredDiff)
        block = Block(cTime, requiredDiff)
        usedTime = block.nTime - chain[-1].nTime
        chain.append(block)
        usedTimeList.append(usedTime)
        print("Time %.2f, block %d, requiredWork %.2f, usedTime %.2f" %
              (block.nTime, i + 1, 1 / block.requiredDiff, usedTime))

    print("Max: %.2f, min: %.2f, avg: %.2f, std: %.2f" % (max(usedTimeList), min(usedTimeList), stat.mean(usedTimeList), stat.stdev(usedTimeList)))



if __name__ == '__main__':
    main()
