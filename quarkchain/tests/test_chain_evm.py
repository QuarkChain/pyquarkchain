import unittest
from quarkchain.core import Identity, Address, Transaction, TransactionInput, Code
from quarkchain.tests.test_utils import get_test_env
from quarkchain.chain import QuarkChainState
from quarkchain.evm.transactions import Transaction as EvmTransaction
from ethereum.utils import mk_contract_address


class TestEvm(unittest.TestCase):

    def testSimpleEvm(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1).addressInShard(0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisQuarkash=0,
            genesisMinorQuarkash=100000)
        qcState = QuarkChainState(env)
        b1 = qcState \
            .getGenesisMinorBlock(0) \
            .createBlockToAppend(quarkash=0)

        evmTx = EvmTransaction(
            branchValue=b1.header.branch.value,
            nonce=0,
            gasprice=1,
            startgas=21000,
            to=acc1.recipient,
            value=0,
            data=bytes(0),
            withdraw=100000,
            withdrawSign=0)
        evmTx.sign(
            key=id1.getKey(),
            network_id=env.config.NETWORK_ID)

        b1.addTx(
            Transaction(
                inList=[TransactionInput(
                    hash=qcState.getGenesisMinorBlock(0).txList[0].getHash(),
                    index=0)],
                code=Code.createEvmCode(evmTx),
                outList=[]).sign([id1.getKey()])).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))
        self.assertEqual(qcState.getBalance(id1.getRecipient()), 100000 * 2 - 21000)

    def testIncorrectBlockReward(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1).addressInShard(0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=100000)
        env.config.SKIP_MINOR_COINBASE_CHECK = False
        qcState = QuarkChainState(env)
        b1 = qcState \
            .getGenesisMinorBlock(0) \
            .createBlockToAppend(quarkash=env.config.MINOR_BLOCK_DEFAULT_REWARD + 1)

        evmTx = EvmTransaction(
            branchValue=b1.header.branch.value,
            nonce=0,
            gasprice=1,
            startgas=21000,
            to=acc1.recipient,
            value=0,
            data=bytes(0),
            withdraw=100000,
            withdrawSign=0)
        evmTx.sign(
            key=id1.getKey(),
            network_id=env.config.NETWORK_ID)

        b1.addTx(
            Transaction(
                inList=[TransactionInput(
                    hash=qcState.getGenesisMinorBlock(0).txList[0].getHash(),
                    index=0)],
                code=Code.createEvmCode(evmTx),
                outList=[]).sign([id1.getKey()])).finalizeMerkleRoot()
        self.assertIsNotNone(qcState.appendMinorBlock(b1))

    def testSimpleSmartContract(self):
        id1 = Identity.createRandomIdentity()
        acc1 = Address.createFromIdentity(id1).addressInShard(0)

        env = get_test_env(
            genesisAccount=acc1,
            genesisMinorQuarkash=1000000)
        env.config.SKIP_MINOR_COINBASE_CHECK = False
        qcState = QuarkChainState(env)
        b1 = qcState \
            .getGenesisMinorBlock(0) \
            .createBlockToAppend(quarkash=env.config.MINOR_BLOCK_DEFAULT_REWARD)

        # Simple setter and getter sc, see
        # http://solidity.readthedocs.io/en/v0.4.21/introduction-to-smart-contracts.html
        evmTx = EvmTransaction(
            branchValue=b1.header.branch.value,
            nonce=0,
            gasprice=1,
            startgas=1000000,
            to=b'',             # Create smart contract
            value=0,
            data=bytes.fromhex(
                '6060604052341561000f57600080fd5b60d38061001d6000396000f3006060604052600436106049576000357c01'
                '00000000000000000000000000000000000000000000000000000000900463ffffffff16806360fe47b114604e57'
                '80636d4ce63c14606e575b600080fd5b3415605857600080fd5b606c60048080359060200190919050506094565b'
                '005b3415607857600080fd5b607e609e565b6040518082815260200191505060405180910390f35b806000819055'
                '5050565b600080549050905600a165627a7a72305820e170013eadb8debdf58398ee9834aa86cf08db2eee5c9094'
                '7c1bcf6c18e3eeff0029'),
            withdraw=1000000,
            withdrawSign=0)
        evmTx.sign(
            key=id1.getKey(),
            network_id=env.config.NETWORK_ID)

        scAddr = mk_contract_address(
            sender=id1.recipient,
            nonce=0)

        b1.addTx(
            Transaction(
                inList=[TransactionInput(
                    hash=qcState.getGenesisMinorBlock(0).txList[0].getHash(),
                    index=0)],
                code=Code.createEvmCode(evmTx),
                outList=[]).sign([id1.getKey()])).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b1))

        # Set the value  (123 or 0x7b)
        b2 = b1.createBlockToAppend()
        evmTx1 = EvmTransaction(
            branchValue=b1.header.branch.value,
            nonce=1,
            gasprice=1,
            startgas=50000,
            to=scAddr,
            value=0,
            data=bytes.fromhex('60fe47b1000000000000000000000000000000000000000000000000000000000000007b'),
            withdraw=0,
            withdrawSign=1)
        evmTx1.sign(
            key=id1.getKey(),
            network_id=env.config.NETWORK_ID)

        b2.addTx(
            Transaction(
                inList=[],
                code=Code.createEvmCode(evmTx1),
                outList=[]).sign([id1.getKey()])).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b2))

        b3 = b2.createBlockToAppend()
        evmTx2 = EvmTransaction(
            branchValue=b1.header.branch.value,
            nonce=2,
            gasprice=1,
            startgas=80000,
            to=scAddr,
            value=0,
            data=bytes.fromhex('6d4ce63c'),
            withdraw=0,
            withdrawSign=1)
        evmTx2.sign(
            key=id1.getKey(),
            network_id=env.config.NETWORK_ID)

        b3.addTx(
            Transaction(
                inList=[],
                code=Code.createEvmCode(evmTx2),
                outList=[]).sign([id1.getKey()])).finalizeMerkleRoot()
        self.assertIsNone(qcState.appendMinorBlock(b3))
