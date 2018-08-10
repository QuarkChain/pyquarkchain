import random


class Candidate:
    def __init__(self, key, score):
        self.key = key
        self.score = score


def get_whitelist(candidate_list, n, seed):
    random.seed(seed)
    white_list = []
    candidate_list.sort(key=lambda p: p.key)
    for i in range(n):
        total_score = sum([p.score for p in candidate_list])
        chosen = random.randint(0, total_score - 1)
        for idx, p in enumerate(candidate_list):
            if chosen <= p.score:
                white_list.append(p)
                candidate_list.pop(idx)
                break
            chosen -= p.score
    return white_list


def generate_person_list(n, seed):
    random.seed(seed)
    return [
        Candidate(key=random.random(), score=random.randint(1, 100)) for i in range(n)
    ]


candidate_list = generate_person_list(100, 123)
white_list = get_whitelist(candidate_list, 50, 321)
for p in white_list:
    print(p.key, p.score)
