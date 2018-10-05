class Guardian:
    @staticmethod
    def adjust_difficulty(original_difficulty: int, block_height: int):
        # TODO: decide on the parameters
        if block_height < 1000:
            return original_difficulty // 1000
        if block_height < 10000:
            return original_difficulty // 100
        if block_height < 100000:
            return original_difficulty // 10
        return original_difficulty
