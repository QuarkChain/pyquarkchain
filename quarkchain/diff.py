from quarkchain.utils import check


class EthDifficultyCalculator:
    """ Using metropolis or homestead algorithm (check_uncle=True or False)"""

    def __init__(self, cutoff, diff_factor, minimum_diff=1, check_uncle=False):
        self.cutoff = cutoff
        self.diff_factor = diff_factor
        self.minimum_diff = minimum_diff
        self.check_uncle = check_uncle

    def calculate_diff(self, chain, create_time=None):
        raise NotImplementedError()

    def calculate_diff_with_parent(self, parent, create_time):
        check(not self.check_uncle)
        check(parent.create_time < create_time)
        sign = max(1 - (create_time - parent.create_time) // self.cutoff, -99)
        offset = parent.difficulty // self.diff_factor
        return int(max(parent.difficulty + offset * sign, self.minimum_diff))
