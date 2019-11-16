import unittest

from quarkchain.evm.specials import proc_current_mnt_id, proc_mint_mnt
from quarkchain.evm.state import State
from quarkchain.evm.vm import Message, VmExtBase
from quarkchain.evm.utils import decode_hex
from quarkchain.core import Address


class TestPrecompiledContracts(unittest.TestCase):
    def test_current_mnt_id(self):
        addr = b"\x00" * 20
        msg = Message(addr, addr, transfer_token_id=1234)
        # test case 1: not enough gas
        msg.gas = 2
        result, gas_remained, data = proc_current_mnt_id(VmExtBase(), msg)
        self.assertListEqual([result, gas_remained, data], [0, 0, []])

        # test case 2: normal case
        msg.gas = 4
        msg.transfer_token_id = 2 ** 64 - 1
        result, gas_remained, data = proc_current_mnt_id(VmExtBase(), msg)
        self.assertListEqual([result, gas_remained], [1, 4 - 3])
        self.assertEqual(len(data), 32)
        self.assertEqual(int.from_bytes(data, byteorder="big"), 2 ** 64 - 1)

    def test_proc_mint_mnt(self):
        sys_contract_addr = decode_hex(b"514b430000000000000000000000000000000002")
        random_addr = Address.create_random_account().recipient
        testcases = [(sys_contract_addr, True), (random_addr, False)]

        minter = b"\x00" * 19 + b"\x34"
        token_id = b"\x00" * 28 + b"\x11" * 4
        amount = b"\x00" * 30 + b"\x22" * 2
        data = b"\x00" * 12 + minter + token_id + amount

        for addr, expect_success in testcases:
            msg = Message(addr, addr, gas=34001, data=data)
            state = State()
            result, gas_remained, ret = proc_mint_mnt(VmExtBase(state), msg)
            balance = state.get_balance(
                minter, int.from_bytes(token_id, byteorder="big")
            )

            if expect_success:
                self.assertListEqual([result, gas_remained], [1, 34001 - 34000])
                self.assertEqual(len(ret), 32)
                self.assertEqual(int.from_bytes(ret, byteorder="big"), 1)
                self.assertEqual(balance, int.from_bytes(amount, byteorder="big"))

                # Mint again with exactly the same parameters
                result, gas_remained, ret = proc_mint_mnt(VmExtBase(state), msg)
                balance = state.get_balance(
                    minter, int.from_bytes(token_id, byteorder="big")
                )
                self.assertListEqual([result, gas_remained], [1, 34001 - 9000])
                self.assertEqual(len(ret), 32)
                self.assertEqual(int.from_bytes(ret, byteorder="big"), 1)
                self.assertEqual(balance, 2 * int.from_bytes(amount, byteorder="big"))
            else:
                self.assertListEqual([result, gas_remained], [0, 0])
                self.assertEqual(len(ret), 0)
                self.assertEqual(balance, 0)
