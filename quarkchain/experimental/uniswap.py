class Exchange:
    def __init__(self, token_reserve, eth_pool, fee_rate=997):
        self.total_supply = 0
        self.create_liquidity(eth_pool, token_reserve)
        self.fee_rate = fee_rate

    def __get_input_price(self, input_amount, input_reserve, output_reserve):
        input_amount_with_fee = input_amount * self.fee_rate
        numerator = input_amount_with_fee * output_reserve
        denominator = input_reserve * 1000 + input_amount_with_fee
        return numerator // denominator

    def buy(self, eth_amount):
        self.eth_pool += eth_amount
        tokens_bought = self.__get_input_price(
            eth_amount, self.eth_pool - eth_amount, self.token_reserve
        )
        assert self.token_reserve - tokens_bought
        self.token_reserve -= tokens_bought
        return tokens_bought

    def sell(self, token_amount):
        eth_bought = self.__get_input_price(
            token_amount, self.token_reserve, self.eth_pool
        )
        self.eth_pool -= eth_bought
        self.token_reserve += token_amount
        return eth_bought

    def buy_print(self, eth_amount):
        tokens = self.buy(eth_amount)
        print(
            "Buy {} tokens with {} ETH at {}, ETH: {}, Token: {}".format(
                tokens, eth_amount, tokens / eth_amount, e.eth_pool, e.token_reserve
            )
        )
        return tokens

    def sell_print(self, token_amount):
        eth = self.sell(token_amount)
        print(
            "Sell {} tokens with {} ETH at {}, ETH: {}, Token: {}".format(
                token_amount, eth, token_amount / eth, e.eth_pool, e.token_reserve
            )
        )
        return eth

    def create_liquidity(self, eth_amount, token_amount):
        assert self.total_supply == 0
        initial_liquidity = eth_amount
        self.total_supply = initial_liquidity
        self.token_reserve = token_amount
        self.eth_pool = eth_amount
        return initial_liquidity

    def add_liquidity(self, eth_amount, token_amount):
        assert self.total_supply != 0
        total_liquidity = self.total_supply
        eth_reserve = self.eth_pool
        token_reserve = self.token_reserve
        token_amount = eth_amount * token_reserve // eth_reserve + 1
        liquidity_minted = eth_amount * total_liquidity // eth_reserve
        self.eth_reserve += eth_amount
        self.token_reserve += token_amount
        self.total_supply = total_liquidity + liquidity_minted
        return liquidity_minted

    def remove_liquidity(self, amount):
        total_liquidity = self.total_supply
        eth_amount = amount * self.eth_pool // total_liquidity
        token_amount = amount * self.token_reserve // total_liquidity
        self.total_supply -= amount
        self.eth_pool -= eth_amount
        self.token_reserve -= token_amount
        return eth_amount, token_amount


e = Exchange(1000000, 100000, fee_rate=1000)
print("ETH: {}, Token: {}".format(e.eth_pool, e.token_reserve))
token = e.buy_print(10000)
e.sell_print(token)
token = e.buy_print(10000)
e.sell_print(token)

e = Exchange(1000000, 100000, fee_rate=1000)
s1 = e.buy_print(10000)
s2 = e.buy_print(10000)
print(s1 + s2)
e = Exchange(1000000, 100000, fee_rate=1000)
e.buy_print(20000)

e.remove_liquidity(50000)
print("ETH: {}, Token: {}".format(e.eth_pool, e.token_reserve))
