import unittest

from qkchash.qkcpow import QkchashMiner, check_pow


class TestQkcpow(unittest.TestCase):
    def test_pow(self):
        header = (2 ** 256 - 1234567890).to_bytes(32, "big")
        diff = 10
        miner = QkchashMiner(diff, header)
        nonce, mixhash = miner.mine()
        self.assertIsNotNone(nonce)
        self.assertIsNotNone(mixhash)

        # wrong nonce, mixhash order
        self.assertFalse(check_pow(header, nonce, mixhash, diff))

        self.assertTrue(check_pow(header, mixhash, nonce, diff))
