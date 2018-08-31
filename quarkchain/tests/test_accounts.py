#!/usr/bin/python3

import os
import unittest

from quarkchain.accounts import Account
from quarkchain.core import Address, Identity

PRIVATE_KEY = "7a28b5ba57c53603b0b07b56bba752f7784bf506fa95edc395f5cf6c7514fe9d"
ADDRESS = "008aeeda4d805471df9b2a5b0f38a0c3bcba786b00802ac3"


class TestAccount(unittest.TestCase):

    def test_create_account_with_key(self):
        account = Account.new(key=PRIVATE_KEY)
        assert account.privkey == PRIVATE_KEY
        assert account.address == ADDRESS
        # check integer version of full shard id matches
        assert account.qkc_address.full_shard_id == int(ADDRESS[40:], 16)

    def test_create_random_account(self):
        account = Account.new()
        assert len(account.privkey) == 64
        assert len(account.qkc_address.recipient.hex()) == 40
        assert len(account.address) == 48
        identity = Identity.create_from_key(account.identity.key)
        assert account.address == Address.create_from_identity(identity).to_hex()

    def test_write_and_load_keystore(self):
        tmp_dir = "/tmp"
        account = Account.new()
        account.dump("a-super-secure-password", write=True, directory=tmp_dir)

        file_path = "{0}/{1}.json".format(tmp_dir, account.uuid)
        loaded = Account.load(file_path, password="a-super-secure-password")
        assert account.privkey == loaded.privkey
        assert account.address == loaded.address

        # cleanup
        os.remove(file_path)
