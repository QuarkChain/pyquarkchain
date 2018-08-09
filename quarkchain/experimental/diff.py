#!/usr/bin/python3

# Simple moving average difficulty


class MADifficultyCalculator:

    def __init__(self, maSamples=16, targetIntervalSec=1, bootstrapSamples=0, slideSize=1):
        self.maSamples = maSamples
        self.targetIntervalSec = targetIntervalSec
        self.bootstrapSamples = bootstrapSamples
        self.slideSize = slideSize

    # Obtain the difficulty required for the next block
    def calculate_diff(self, chain):
        assert(len(chain) >= 1)
        gensisDiff = chain[0].get_required_diff()
        chain = chain[:len(chain) // self.slideSize * self.slideSize]
        if len(chain) <= self.bootstrapSamples + 1:
            return gensisDiff

        samples = self.maSamples
        if len(chain) < samples + 1:
            samples = len(chain) - 1

        workDone = 0
        for block in chain[-samples:]:
            workDone = workDone + 1 / block.get_required_diff()

        timeUsedSec = chain[-1].get_create_time_sec() - \
            chain[-1 - samples].get_create_time_sec()

        return timeUsedSec / self.targetIntervalSec / workDone


class FixedDifficultyCalculator:

    def __init__(self, diff):
        self.diff = diff

    def calculate_diff(self, chain):
        return self.diff
