# A simulator to demonstrate that selfish mining can be a serious attack on dual PoW algorithm.
import random

HASH_MAX = (2 ** 64) - 1
hp_list = [40, 30, 20, 10]


class Miner:
    def __init__(self, hp):
        self.hp = hp

    def mine_next(self):
        min_h = HASH_MAX
        for i in range(self.hp):
            min_h = min(min_h, random.randint(0, HASH_MAX))
        return min_h


class MaliciousMiner:
    def __init__(self, hp):
        self.hp = hp
        self.prev_min_h = HASH_MAX
        self.total_saved_hp = 0

    def mine_next(self):
        min_h = self.prev_min_h
        saved_hp = 0
        for i in range(self.hp):
            min_h = min(min_h, random.randint(0, HASH_MAX))
            if min_h < HASH_MAX // 100:
                saved_hp = self.hp - i - 1
                self.total_saved_hp += saved_hp
                print(min_h, i, saved_hp)
                saved_hp = 0
                # print(saved_hp)
                break
        self.prev_min_h = HASH_MAX
        for i in range(saved_hp):
            self.prev_min_h = min(self.prev_min_h, random.randint(0, HASH_MAX))
        return min_h


N = 100000
miners = [Miner(hp) for hp in hp_list]
blocks = [0] * len(hp_list)
miners[0] = MaliciousMiner(hp_list[0])
# miners = [MaliciousMiner(hp) for hp in hp_list]

for i in range(N):
    min_h = None
    min_j = 0
    for j in range(len(miners)):
        h = miners[j].mine_next()
        if min_h is None or h < min_h:
            min_h = h
            min_j = j
    blocks[min_j] += 1

print(blocks)
print(miners[0].total_saved_hp)
