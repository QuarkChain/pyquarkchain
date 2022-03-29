
REDUCTION_FACTOR = 0.88

ROOT_BLOCK_REWARD = 3.25 * 8 * 6
ROOT_BLOCK_REDUCTION_SIZE = 525600

MINOR_BLOCK_REWARD = 3.25 * 2  # split to root and miner block miners
MINOR_BLOCK_REDUCTION_SIZE = 3153600

#root_height = 1294545
#shard_heights = [7600468, 7555507, 7562667, 7588454, 7595046, 7552602, 7584632, 7464263]
root_height = 663132
shard_heights = [3872082, 3846760, 3844333, 3867437, 3876030, 3846766, 3893407, 3816372]

def get_root_reward(root_height):
    reward = 0
    block_reward = ROOT_BLOCK_REWARD
    while root_height != 0:
        height = root_height
        if height > ROOT_BLOCK_REDUCTION_SIZE:
            height = ROOT_BLOCK_REDUCTION_SIZE

        root_height -= height
        reward += height * block_reward
        block_reward = block_reward * REDUCTION_FACTOR
    return reward


def get_shard_reward(shard_height):
    reward = 0
    block_reward = MINOR_BLOCK_REWARD
    while shard_height != 0:
        height = shard_height
        if height > MINOR_BLOCK_REDUCTION_SIZE:
            height = MINOR_BLOCK_REDUCTION_SIZE

        shard_height -= height
        reward += height * block_reward
        block_reward = block_reward * REDUCTION_FACTOR
    return reward


def get_shards_reward(shard_heights):
    return sum([get_shard_reward(height) for height in shard_heights])


rreward = get_root_reward(root_height)
sreward = get_shards_reward(shard_heights)
print(rreward, sreward, rreward + sreward)
