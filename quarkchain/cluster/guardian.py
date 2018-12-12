class Guardian:
    @staticmethod
    def adjust_difficulty(original_difficulty: int, block_height: int):
        return original_difficulty // 1000
        # TODO: decide on the parameters for mainnet
        # if block_height < 1000:
        #     return original_difficulty // 1000
        # if block_height < 10000:
        #     return original_difficulty // 100
        # if block_height < 100000:
        #     return original_difficulty // 10
        # return original_difficulty
