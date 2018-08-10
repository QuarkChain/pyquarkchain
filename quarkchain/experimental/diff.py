#!/usr/bin/python3

# Simple moving average difficulty


class MADifficultyCalculator:
    def __init__(
        self, ma_samples=16, target_interval_sec=1, bootstrap_samples=0, slide_size=1
    ):
        self.ma_samples = ma_samples
        self.target_interval_sec = target_interval_sec
        self.bootstrap_samples = bootstrap_samples
        self.slide_size = slide_size

    # Obtain the difficulty required for the next block
    def calculate_diff(self, chain):
        assert len(chain) >= 1
        gensis_diff = chain[0].get_required_diff()
        chain = chain[: len(chain) // self.slide_size * self.slide_size]
        if len(chain) <= self.bootstrap_samples + 1:
            return gensis_diff

        samples = self.ma_samples
        if len(chain) < samples + 1:
            samples = len(chain) - 1

        work_done = 0
        for block in chain[-samples:]:
            work_done = work_done + 1 / block.get_required_diff()

        time_used_sec = (
            chain[-1].get_create_time_sec() - chain[-1 - samples].get_create_time_sec()
        )

        return time_used_sec / self.target_interval_sec / work_done


class FixedDifficultyCalculator:
    def __init__(self, diff):
        self.diff = diff

    def calculate_diff(self, chain):
        return self.diff
