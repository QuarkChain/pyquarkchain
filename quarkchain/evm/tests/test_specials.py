import unittest

from quarkchain.evm.specials import proc_current_mnt_id, proc_mint_mnt, proc_balance_mnt
from quarkchain.evm.state import State
from quarkchain.evm.vm import Message, VmExtBase
from quarkchain.evm.utils import decode_hex, encode_int32
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

    @staticmethod
    def __mint(state, minter, token, amount):
        sys_contract_addr = decode_hex(b"514b430000000000000000000000000000000002")
        data = b"\x00" * 12 + minter + token + amount
        msg = Message(sys_contract_addr, bytes(20), gas=34000, data=data)
        return proc_mint_mnt(VmExtBase(state), msg)

    def test_proc_balance_mnt(self):
        default_addr = b"\x00" * 19 + b"\x34"
        token_id = 1234567
        token_id_bytes = token_id.to_bytes(32, byteorder="big")
        state = State()

        self.__mint(state, default_addr, token_id_bytes, encode_int32(2020))
        balance = state.get_balance(default_addr, token_id)
        self.assertEqual(balance, 2020)

        data = b"\x00" * 12 + default_addr + token_id_bytes
        # Gas not enough
        msg = Message(default_addr, default_addr, gas=399, data=data)
        ret_tuple = proc_balance_mnt(VmExtBase(state), msg)
        self.assertEqual(ret_tuple, (0, 0, []))

        # Success case
        testcases = [
            (default_addr, token_id, 2020),  # Balance already set
            (default_addr, 54321, 0),  # Non-existent token
            (Address.create_random_account(0).recipient, token_id, 0),  # Blank
        ]
        for addr, tid, bal in testcases:
            data = b"\x00" * 12 + addr + tid.to_bytes(32, byteorder="big")
            msg = Message(addr, addr, gas=500, data=data)
            result, gas_remained, ret = proc_balance_mnt(VmExtBase(state), msg)
            ret_int = int.from_bytes(ret, byteorder="big")
            self.assertEqual(result, 1)
            self.assertEqual(gas_remained, 500 - 400)
            self.assertEqual(ret_int, bal)
