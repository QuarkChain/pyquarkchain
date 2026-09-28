import unittest

from quarkchain.evm import vm
from quarkchain.evm.vm import Message, VmExtBase


class _Ext(VmExtBase):
    """Minimal VM external interface for exercising vm_execute directly.

    The target account has no balance and no code; create() reports the child
    frame as failed (mirrors a child that immediately runs out of gas), which is
    enough to drive the parent frame's gas accounting.
    """

    def __init__(self):
        super().__init__()
        self.get_balance = lambda addr, token_id=0: 0
        self.get_code = lambda addr: b""
        self.account_exists = lambda addr: True
        self.create = lambda msg, salt=None: (0, 0, b"")
        self.log = lambda addr, topics, data: None


# PUSH1 0 PUSH1 0 MSTORE            -> pre-pay for 32 bytes of memory
# PUSH1 salt PUSH1 32 PUSH1 0 PUSH1 0 CREATE2
# CREATE2 is the last opcode, so the frame exits right after it. The init-code
# region [0, 32) is already allocated, so mem_extend is a no-op and cannot be
# relied on to charge for the CREATE2 per-word hashing fee.
_CREATE2_AT_END = bytes(
    [
        0x60, 0x00, 0x60, 0x00, 0x52,  # MSTORE(0, 0)
        0x60, 0x01,                    # salt
        0x60, 0x20,                    # init-code size = 32
        0x60, 0x00,                    # init-code offset = 0
        0x60, 0x00,                    # value = 0
        0xF5,                          # CREATE2
    ]
)

# PUSH1 0 PUSH1 0 MSTORE            -> pre-pay for 32 bytes of memory
# PUSH1 32 PUSH1 0 LOG0
# LOG0 is the last opcode. The data region [0, 32) is already allocated, so
# mem_extend is a no-op and cannot be relied on to charge for the LOG per-byte
# data fee.
_LOG0_AT_END = bytes(
    [
        0x60, 0x00, 0x60, 0x00, 0x52,  # MSTORE(0, 0)
        0x60, 0x20,                    # data size = 32
        0x60, 0x00,                    # data offset = 0
        0xA0,                          # LOG0
    ]
)


def _sweep_gas_never_negative(testcase, code, gas_range):
    """Run `code` across a range of start-gas values and assert the frame never
    leaves a negative amount of gas. Also assert the range straddles both the
    out-of-gas and the success side of the fee boundary.
    """
    saw_out_of_gas = False
    saw_success = False
    for start_gas in gas_range:
        msg = Message(b"\x00" * 20, b"\x11" * 20, 0, start_gas, b"")
        result, gas_remained, _ = vm.vm_execute(_Ext(), msg, code)
        testcase.assertGreaterEqual(
            gas_remained,
            0,
            "gas_remained went negative at start_gas=%d" % start_gas,
        )
        if result == 0:
            saw_out_of_gas = True
        else:
            saw_success = True
    testcase.assertTrue(saw_out_of_gas, "range did not cover the out-of-gas case")
    testcase.assertTrue(saw_success, "range did not cover the success case")


class TestDynamicGasNeverNegative(unittest.TestCase):
    def test_create2_word_fee_never_yields_negative_gas(self):
        _sweep_gas_never_negative(self, _CREATE2_AT_END, range(32000, 32060))

    def test_log_data_fee_never_yields_negative_gas(self):
        _sweep_gas_never_negative(self, _LOG0_AT_END, range(360, 720))


if __name__ == "__main__":
    unittest.main()
