import random


class Candidate:

    def __init__(self, key, score):
        self.key = key
        self.score = score


def get_whitelist(candidateList, n, seed):
    random.seed(seed)
    whiteList = []
    candidateList.sort(key=lambda p: p.key)
    for i in range(n):
        totalScore = sum([p.score for p in candidateList])
        chosen = random.randint(0, totalScore - 1)
        for idx, p in enumerate(candidateList):
            if chosen <= p.score:
                whiteList.append(p)
                candidateList.pop(idx)
                break
            chosen -= p.score
    return whiteList


def generate_person_list(n, seed):
    random.seed(seed)
    return [Candidate(key=random.random(), score=random.randint(1, 100)) for i in range(n)]


candidateList = generate_person_list(100, 123)
whiteList = get_whitelist(candidateList, 50, 321)
for p in whiteList:
    print(p.key, p.score)
