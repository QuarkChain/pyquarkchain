#!/usr/bin/python3

import os
import unittest

from quarkchain.accounts import Account
from quarkchain.tools.keys import privtoaddr

PRIVATE_KEY = "7a28b5ba57c53603b0b07b56bba752f7784bf506fa95edc395f5cf6c7514fe9d"
ADDRESS = "008aeeda4d805471df9b2a5b0f38a0c3bcba786b"

class TestAccount(unittest.TestCase):

    def test_create_account_with_key(self):
        account = Account.new("a-super-secure-password", key=PRIVATE_KEY)
        assert account.privkey_as_hex == PRIVATE_KEY
        assert account.address_as_hex == ADDRESS

    def test_create_account(self):
        account = Account.new("a-super-secure-password")
        assert len(account.privkey_as_hex) == 64
        assert len(account.address_as_hex) == 40
        assert account.address == privtoaddr(account.privkey)

    def test_write_and_load_keystore(self):
        keystore_file_path = "/tmp/tmp.key"
        account = Account.new("a-super-secure-password", path=keystore_file_path)
        account.dump(write=True)

        loaded = Account.load(keystore_file_path, password="a-super-secure-password")
        assert account.privkey_as_hex == loaded.privkey_as_hex
        assert account.address_as_hex == loaded.address_as_hex
        assert account.pubkey_as_hex == loaded.pubkey_as_hex

        # cleanup
        os.remove("/tmp/tmp.key")
