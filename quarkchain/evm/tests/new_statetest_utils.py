from quarkchain.evm.state import State
from quarkchain.evm.common import FakeHeader
from quarkchain.evm.utils import (
    decode_hex,
    parse_int_or_hex,
    sha3,
    to_string,
    remove_0x_head,
    encode_hex,
    big_endian_to_int,
)
from quarkchain.evm.config import default_config, Env
from quarkchain.config import get_default_evm_config
from quarkchain.evm.exceptions import InvalidTransaction
import quarkchain.evm.transactions as transactions
from quarkchain.evm.messages import apply_transaction
import copy
import os
from quarkchain.db import InMemoryDb
from quarkchain.utils import token_id_encode

config_string = ":info,eth.vm.log:trace,eth.vm.op:trace,eth.vm.stack:trace,eth.vm.exit:trace,eth.pb.msg:trace,eth.pb.tx:debug"

konfig = copy.copy(default_config)

# configure_logging(config_string=config_string)

fixture_path = os.path.join(os.path.dirname(__file__), "../..", "fixtures")

fake_headers = {}


def mk_fake_header(blknum):
    if blknum not in fake_headers:
        fake_headers[blknum] = FakeHeader(sha3(to_string(blknum)))
    return fake_headers[blknum]


basic_env = {
    "currentCoinbase": "0x2adc25665018aa1fe0e6bc666dac8fc2697ff9ba",
    "currentDifficulty": "0x020000",
    "currentGasLimit": "0x7fffffffffffffff",
    "currentNumber": "0x01",
    "currentTimestamp": "0x03e8",
    "previousHash": "0x5e20a0453cecd065ea59c37ac63e079ee08998b6045136a8ce6635c7912ec0b6",
}


evm_config = get_default_evm_config()
network_to_test = {"ConstantinopleFix"}

# Makes a diff between a prev and post state


def mk_state_diff(prev, post):
    o = {}
    for k in prev.keys():
        if k not in post:
            o[k] = ["-", prev[k]]
    for k in post.keys():
        if k not in prev:
            o[k] = ["+", post[k]]
        elif prev[k] != post[k]:
            ok = {}
            for key in ("nonce", "token_balances", "code"):
                if prev[k][key] != post[k][key]:
                    ok[key] = [prev[k][key], "->", post[k][key]]
            if prev[k]["storage"] != post[k]["storage"]:
                ok["storage"] = {}
                for sk in prev[k]["storage"].keys():
                    if sk not in post[k]["storage"]:
                        ok["storage"][sk] = ["-", prev[k]["storage"][sk]]
                for sk in post[k]["storage"].keys():
                    if sk not in prev[k]["storage"]:
                        ok["storage"][sk] = ["+", post[k]["storage"][sk]]
                    else:
                        ok["storage"][sk] = [
                            prev[k]["storage"][sk],
                            "->",
                            post[k]["storage"][sk],
                        ]
            o[k] = ok
    return o


# Compute a single unit of a state test


def compute_state_test_unit(state, txdata, indices, konfig, is_qkc_state, qkc_env=None):
    state.env.config = konfig
    s = state.snapshot()
    if "transferTokenId" in txdata:
        transfer_token_id = parse_int_or_hex(
            txdata["transferTokenId"][indices["transferTokenId"]]
        )
    else:
        transfer_token_id = token_id_encode("QKC")
    try:
        # Create the transaction
        tx = transactions.Transaction(
            nonce=parse_int_or_hex(txdata["nonce"] or b"0"),
            gasprice=parse_int_or_hex(txdata["gasPrice"] or b"0"),
            startgas=parse_int_or_hex(txdata["gasLimit"][indices["gas"]] or b"0"),
            to=decode_hex(remove_0x_head(txdata["to"])),
            value=parse_int_or_hex(txdata["value"][indices["value"]] or b"0"),
            data=decode_hex(remove_0x_head(txdata["data"][indices["data"]])),
            gas_token_id=token_id_encode("QKC"),
            transfer_token_id=transfer_token_id,
            # Should not set testing flag if testing QuarkChain state
            is_testing=not is_qkc_state,
        )
        tx.set_quark_chain_config(qkc_env.quark_chain_config)
        if "secretKey" in txdata:
            tx.sign(decode_hex(remove_0x_head(txdata["secretKey"])))
        else:
            tx._in_mutable_context = True
            tx.v = parse_int_or_hex(txdata["v"])
            tx._in_mutable_context = False
        # Run it
        prev = state.to_dict()
        success, output = apply_transaction(state, tx, tx_wrapper_hash=bytes(32))
        print("Applied tx")
    except InvalidTransaction as e:
        print("Exception: %r" % e)
        success, output = False, b""
    # touch coinbase, make behavior consistent with go-ethereum
    state.delta_token_balance(state.block_coinbase, token_id_encode("QKC"), 0)
    state.commit()
    post = state.to_dict()
    output_decl = {
        "hash": "0x" + encode_hex(state.trie.root_hash),
        "indexes": indices,
        "diff": mk_state_diff(prev, post),
    }
    state.revert(s)
    return output_decl


# Initialize the state for state tests
def init_state(env, pre, is_qkc_state, qkc_env=None):
    # Setup env
    db = InMemoryDb()
    state_env = Env(config=konfig)
    state_env.db = db
    state = State(
        env=state_env,
        block_prevhash=decode_hex(remove_0x_head(env["previousHash"])),
        prev_headers=[
            mk_fake_header(i)
            for i in range(
                parse_int_or_hex(env["currentNumber"]) - 1,
                max(-1, parse_int_or_hex(env["currentNumber"]) - 257),
                -1,
            )
        ],
        block_number=parse_int_or_hex(env["currentNumber"]),
        block_coinbase=decode_hex(remove_0x_head(env["currentCoinbase"])),
        block_difficulty=parse_int_or_hex(env["currentDifficulty"]),
        gas_limit=parse_int_or_hex(env["currentGasLimit"]),
        timestamp=parse_int_or_hex(env["currentTimestamp"]),
        qkc_config=qkc_env.quark_chain_config,
        # If testing QuarkChain states, should not use mock account
        use_mock_evm_account=not is_qkc_state,
    )

    seen_token_ids = set()
    # Fill up pre
    for address, h in list(pre.items()):
        assert len(address) in (40, 42)
        address = decode_hex(remove_0x_head(address))
        state.set_nonce(address, parse_int_or_hex(h["nonce"]))
        if is_qkc_state and "balances" in h:
            # In QuarkChain state tests, can either specify balance map or single balance
            for token_id, balance in h["balances"].items():
                parsed_token_id = parse_int_or_hex(token_id)
                state.set_token_balance(
                    address, parsed_token_id, parse_int_or_hex(balance)
                )
                seen_token_ids.add(parsed_token_id)
        else:
            state.set_balance(address, parse_int_or_hex(h["balance"]))
        state.set_code(address, decode_hex(remove_0x_head(h["code"])))
        for k, v in h["storage"].items():
            state.set_storage_data(
                address,
                big_endian_to_int(decode_hex(k[2:])),
                big_endian_to_int(decode_hex(v[2:])),
            )

    # Update allowed token IDs
    if seen_token_ids:
        state.qkc_config._allowed_token_ids = seen_token_ids

    state.commit(allow_empties=True)
    return state


class EnvNotFoundException(Exception):
    pass


def verify_state_test(test):
    print("Verifying state test")
    if "env" not in test:
        raise EnvNotFoundException("Env not found")
    _state = init_state(test["env"], test["pre"], test["qkcstate"], qkc_env=test["qkc"])
    for config_name, results in test["post"].items():
        # Old protocol versions may not be supported
        if config_name not in network_to_test:
            continue
        print("Testing for %s" % config_name)
        for result in results:
            data = test["transaction"]["data"][result["indexes"]["data"]]
            if len(data) > 2000:
                data = "data<%d>" % (len(data) // 2 - 1)
            print(
                "Checking for values: g %d v %d d %s (indexes g %d v %d d %d)"
                % (
                    parse_int_or_hex(
                        test["transaction"]["gasLimit"][result["indexes"]["gas"]]
                    ),
                    parse_int_or_hex(
                        test["transaction"]["value"][result["indexes"]["value"]]
                    ),
                    data,
                    result["indexes"]["gas"],
                    result["indexes"]["value"],
                    result["indexes"]["data"],
                )
            )
            computed = compute_state_test_unit(
                _state,
                test["transaction"],
                result["indexes"],
                evm_config,
                test["qkcstate"],
                qkc_env=test["qkc"],
            )
            if computed["hash"][-64:] != result["hash"][-64:]:
                for k in computed["diff"]:
                    print(k, computed["diff"][k])
                raise Exception(
                    "Hash mismatch, computed: %s, supplied: %s"
                    % (computed["hash"], result["hash"])
                )
            else:
                for k in computed["diff"]:
                    print(k, computed["diff"][k])
                print("Hash matched!: %s" % computed["hash"])
    return True
