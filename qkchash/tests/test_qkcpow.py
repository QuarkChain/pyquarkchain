import unittest

from qkchash.qkcpow import QkchashMiner, check_pow


class TestQkcpow(unittest.TestCase):
    def test_pow(self):
        header = (2 ** 256 - 1234567890).to_bytes(32, "big")
        diff = 10
        height = 10
        miner = QkchashMiner(height, diff, header)
        nonce, mixhash = miner.mine()

        self.assertIsNotNone(nonce)
        self.assertIsNotNone(mixhash)

        # wrong nonce, mixhash order
        self.assertFalse(check_pow(height, header, nonce, mixhash, diff))

        self.assertTrue(check_pow(height, header, mixhash, nonce, diff))

        # In the same epoch
        height = 3000 
        self.assertTrue(check_pow(height, header, mixhash, nonce, diff))

        # wrong epoch
        height = 30001
        self.assertFalse(check_pow(height, header, mixhash, nonce, diff))
