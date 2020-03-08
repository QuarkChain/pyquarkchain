# A simulator of dual PoW with order statistics
import random
import heapq

HASH_MAX = (2 ** 64) - 1
hp_list = [40, 30, 20, 10]
# hp_list = [50, 30, 20]
COINBASE = 10 ** 20

# Number of statistics to reveal
K = 8


class Miner:
    def __init__(self, hp):
        self.hp = hp

    def mine_next(self):
        h = []
        for i in range(self.hp):
            v = random.randint(0, HASH_MAX)
            if len(h) < K:
                heapq.heappush(h, -v)
            elif v < -h[0]:
                heapq.heapreplace(h, -v)

        ret = [-i for i in h]
        ret.sort()
        return ret


def to_diff(v):
    """ Hash vaoue to difficulty
    """
    return HASH_MAX // v


N = 100000
miners = [Miner(hp) for hp in hp_list]
blocks = [0] * len(hp_list)
selfish_blocks = [0] * len(hp_list)
# miners = [MaliciousMiner(hp) for hp in hp_list]
rewards = [0] * len(hp_list)

for i in range(N):
    min_h = []
    min_j = 0
    min_hh = None
    for j in range(len(miners)):
        h = miners[j].mine_next()
        if len(min_h) == 0 or h[0] < min_h[0][0]:
            min_hh = h
            min_j = j
        for v in h:
            min_h.append((v, j))
        min_h.sort()
        min_h = min_h[0:K]

    blocks[min_j] += 1
    if [v[0] for v in min_h] == min_hh:
        selfish_blocks[min_j] += 1

    diff = [to_diff(v[0]) for v in min_h]
    total_diff = sum(diff)
    for j in range(len(min_h)):
        rewards[min_h[j][1]] += COINBASE * diff[j] // total_diff


print(blocks)
print([i / N for i in selfish_blocks])
total_rewards = sum(rewards)
print([v / total_rewards for v in rewards])
