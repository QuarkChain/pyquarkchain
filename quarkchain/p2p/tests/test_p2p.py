#!/usr/bin/python3

import unittest
from quarkchain.p2p.p2p_network import parse_additional_bootstraps


class TestBootstrap(unittest.TestCase):
    def test_additional_bootstrap(self):
        self.assertEqual(parse_additional_bootstraps(""), [])
        self.assertEqual(
            parse_additional_bootstraps("172.31.19.215:29000"),
            [
                b"enode://59d33772ddc829d1a434e51755ca9c9d69280d58e088678088721bec162ece98fad370d198528f1d2301eac167aaee4ce6b5b2917e644cd02028bfbff1bdbfc0@172.31.19.215:29000"
            ],
        )
        self.assertEqual(
            parse_additional_bootstraps("172.31.19.215:29000,0.0.0.0:29000"),
            [
                b"enode://59d33772ddc829d1a434e51755ca9c9d69280d58e088678088721bec162ece98fad370d198528f1d2301eac167aaee4ce6b5b2917e644cd02028bfbff1bdbfc0@172.31.19.215:29000",
                b"enode://c118d844bd0587b61f92b6061caee4f85743bba831d70a5954196b5cce4d5a456d020f340f8bfe599ffa9d2028202949cbb1eeb4d94404046742244bf4ce3cd3@0.0.0.0:29000",
            ],
        )
