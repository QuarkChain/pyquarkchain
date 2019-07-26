import unittest

from quarkchain.evm.specials import proc_current_mnt_id
from quarkchain.evm.vm import Message


class TestPrecompiledContracts(unittest.TestCase):
    def test_current_mnt_id(self):
        addr = b"\x00" * 20
        msg = Message(addr, addr, transfer_token_id=1234)
        # test case 1: not enough gas
        msg.gas = 2
        result, gas_remained, data = proc_current_mnt_id(None, msg)
        self.assertListEqual([result, gas_remained, data], [0, 0, []])

        # test case 2: normal case
        msg.gas = 4
        msg.transfer_token_id = 2 ** 64 - 1
        result, gas_remained, data = proc_current_mnt_id(None, msg)
        self.assertListEqual([result, gas_remained], [1, 4 - 3])
        self.assertEqual(len(data), 32)
        self.assertEqual(int.from_bytes(data, byteorder="big"), 2 ** 64 - 1)
