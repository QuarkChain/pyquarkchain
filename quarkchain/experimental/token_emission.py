# Parameters
GENESIS_TIMESTAMP = 1556639999
GENESIS_SUPPLY = 6 * (10 ** 9) * (10 ** 18) # 6B

# block times are estimated based on block 2982930
ROOT_BLOCK_TIME = (1728506186 - GENESIS_TIMESTAMP) / 2982930  # ~ 57.61
ROOT_EPOCH_INTERVAL = 525600
ROOT_BLOCK_REWARD = 156000000000000000000

SHARD_BLOCK_TIME = (1728506186 - GENESIS_TIMESTAMP) / (17530850 + 17478840 + 17518941 + 17517331 + 17597426 + 17503472 + 17455360 + 17384838) * 8  # ~ 9.82
SHARD_EPOCH_INTERVAL = 3153600 # unit of blocks
SHARD_GENESIS_NUMBER = 8
SHARD_BLOCK_REWARD = 6500000000000000000


def get_chain_reward(bn, epoch_interval, block_reward):
    r = 0
    while bn != 0:
        b = min(bn, epoch_interval)
        bn -= b
        r += block_reward * b
        block_reward = block_reward * 88 // 100
    return r


def get_circulating_supply_by_bn(bns):
    cs = GENESIS_SUPPLY + get_chain_reward(bns[0], ROOT_EPOCH_INTERVAL, ROOT_BLOCK_REWARD)
    for i in range(1, 9):
        cs += get_chain_reward(bns[i], SHARD_EPOCH_INTERVAL, SHARD_BLOCK_REWARD)
    sbreward = SHARD_BLOCK_REWARD
    for i in range(9, len(bns)):
        sbreward = sbreward * 88 // 100
        cs += get_chain_reward(bns[i], SHARD_EPOCH_INTERVAL, sbreward) * 3 // 2 # 1 new shardds, and 0.5 will be root chains
    return cs


def get_circulating_supply(ts, with_new_shards=False):
    if ts < GENESIS_TIMESTAMP:
        return 0

    bns = [int((ts - GENESIS_TIMESTAMP) / ROOT_BLOCK_TIME)]
    bns += [int((ts - GENESIS_TIMESTAMP) / SHARD_BLOCK_TIME)] * 8
    if with_new_shards:
        # May add one shard per year, which will have the same emission as existin shard at the time of creation.
        # This means the genesis reward for the shard is already reduced.
        sbreward = SHARD_BLOCK_REWARD
        for i in range(int((ts - GENESIS_TIMESTAMP) / SHARD_BLOCK_TIME / SHARD_EPOCH_INTERVAL)):
            bns.append(int((ts - GENESIS_TIMESTAMP) / SHARD_BLOCK_TIME) - SHARD_EPOCH_INTERVAL * (i + 1))
    return get_circulating_supply_by_bn(bns)


def print_end_of_month_supply(with_new_shards=False):
    import calendar
    import datetime

    dt = datetime.datetime(2021, 12, 31, 0, 0)
    print("{}: {}".format(dt, get_circulating_supply(int(dt.timestamp()), with_new_shards) / 1e18))

    for year in range(2022, 2028):
        for month in range(1, 13):
            day = calendar.monthrange(year, month)[1]
            dt = datetime.datetime(year, month, day, 0, 0)
            print("{}: {}".format(dt, get_circulating_supply(int(dt.timestamp()), with_new_shards) / 1e18))


print(get_circulating_supply_by_bn([222898, 1285646, 1278508, 1279479, 1277021, 1282227, 1278981, 1275374, 1277279]) / 1e18) # 6101296435.5
print(get_circulating_supply_by_bn([1294545, 7600468, 7555507, 7562667, 7588454, 7595046, 7552602, 7584632, 7464263]) / 1e18) # 6542409279.3184
print(get_circulating_supply_by_bn([1419566, 8375518, 8340637, 8352809, 8412933, 8388349, 8330705, 8348813, 8230480]) / 1e18) # 6589106535.1807995
print(get_circulating_supply_by_bn([2762998, 16238568, 16185709, 16225668, 16273199, 16305257, 16211646, 16162554, 16061959]) / 1e18) # 6991264304.382

print(get_circulating_supply(1638686770) / 1e18) # 6577057566.592
print(get_circulating_supply(1638686770, with_new_shards=True) / 1e18) # 6632768875.2736
print(get_circulating_supply(100000000000, with_new_shards=False) / 1e18) # 8049840000.0 (max cap without new shards)
print(get_circulating_supply(100000000000, with_new_shards=True) / 1e18) # 9928860000.0 (max cap)

print_end_of_month_supply()
