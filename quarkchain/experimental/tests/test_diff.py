import unittest
import quarkchain.experimental.diff as diff
import quarkchain.experimental.proof_of_work as proof_of_work
import statistics as stat


class Block:
    def __init__(self, n_time=0.0, required_diff=0.1):
        self.n_time = n_time
        self.required_diff = required_diff

    def get_required_diff(self):
        return self.required_diff

    def get_create_time_sec(self):
        return self.n_time


class TestMADifficulty(unittest.TestCase):
    def test_none_sample(self):
        chain = [Block(0, 0.1)]
        diff_calc = diff.MADifficultyCalculator(ma_samples=2, target_interval_sec=5.0)
        self.assertEqual(diff_calc.calculate_diff(chain), 0.1)

    def test_one_sample(self):
        chain = [Block(0, 0.1), Block(4.0, 0.1)]
        diff_calc = diff.MADifficultyCalculator(ma_samples=2, target_interval_sec=5.0)
        self.assertEqual(diff_calc.calculate_diff(chain), 0.08)

    def test_two_sample(self):
        chain = [Block(0, 0.1), Block(4.0, 0.1), Block(10, 0.08)]
        diff_calc = diff.MADifficultyCalculator(ma_samples=2, target_interval_sec=5.0)
        self.assertEqual(diff_calc.calculate_diff(chain), 1 / 11.25)


def main():
    target_interval_sec = 5
    diff_calc = diff.MADifficultyCalculator(
        ma_samples=32, target_interval_sec=target_interval_sec
    )
    hash_power = 100

    cTime = 0.0
    chain = [Block(0, 0.002)]
    usedTimeList = []
    p = proof_of_work.PoW(hash_power)

    for i in range(1000):
        required_diff = diff_calc.calculate_diff(chain)
        cTime = cTime + p.mine(required_diff)
        block = Block(cTime, required_diff)
        used_time = block.n_time - chain[-1].n_time
        chain.append(block)
        usedTimeList.append(used_time)
        print(
            "Time %.2f, block %d, requiredWork %.2f, used_time %.2f"
            % (block.n_time, i + 1, 1 / block.required_diff, used_time)
        )

    print(
        "Max: %.2f, min: %.2f, avg: %.2f, std: %.2f"
        % (
            max(usedTimeList),
            min(usedTimeList),
            stat.mean(usedTimeList),
            stat.stdev(usedTimeList),
        )
    )


if __name__ == "__main__":
    main()
