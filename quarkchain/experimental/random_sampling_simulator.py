import random

committee_size = 150
shard_size = 1024
pool_size = 150 * 1024

# Percentage of attackers in pool
attacker_p = 0.15
attacker_n = int(attacker_p * pool_size)

# Attack threshold (a committee with t percent of attackers)
attacker_tn = int(committee_size / 3)

# Monte-carlo trials
trials = 100000

# Pool members 1 - attacker; 2 - honest validator
pool = [1 for i in range(attacker_n)]
pool.extend([0 for i in range(pool_size - attacker_n)])

attacked_trials = 0
for trial in range(trials):
    if trial != 0 and trial % 10 == 0:
        print("Trial %d, attack prob: %f" % (trial, attacked_trials / trial))
    random.shuffle(pool)
    for j in range(shard_size):
        if sum(pool[j * committee_size : (j + 1) * committee_size]) >= attacker_tn:
            attacked_trials += 1
            break

print("Attack prob: %f" % (attacked_trials / trials))
