#!/usr/bin/env python3
"""Scan a local QuarkChain database for contract-creation state cases.

This scanner replays each shard's canonical minor blocks against the state root
stored by that block's parent. It checks every contract-creation transaction:

1. Before CREATE: target has QKC balance > 0, nonce == 0, empty code, and a
   non-empty storage trie.
2. After a failed CREATE: the target's previously non-empty storage trie became
   empty.

The database is opened read-only in practice: EVM execution uses an OverlayDb,
so temporary state changes and code writes do not go to the persistent shard
database. Run from the repository root, for example:

    .venv/bin/python quarkchain/tools/scan_create_contract_cases.py \
        --db-root ./quarkchain/cluster/qkc-data/mainnet \
        --cluster-config ./mainnet/singularity/cluster_config_template.json \
        --output ./create-contract-cases.jsonl

Use --end-height for a bounded smoke test. The output is JSON Lines.
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# When invoked as ``python quarkchain/tools/<script>.py``, Python puts the
# tools directory on sys.path rather than the repository root.
REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import rlp

from quarkchain.cluster.cluster_config import ClusterConfig
from quarkchain.cluster.shard_db_operator import ShardDbOperator
from quarkchain.cluster.shard_state import ShardState
from quarkchain.db import PersistentDb
from quarkchain.env import DEFAULT_ENV
from quarkchain.evm import utils
from quarkchain.evm.messages import apply_transaction
from quarkchain.evm.state import BLANK_ROOT


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db-root",
        default="./quarkchain/cluster/qkc-data/mainnet",
        help="directory containing shard-<full_shard_id>.db",
    )
    parser.add_argument(
        "--cluster-config",
        default="./mainnet/singularity/cluster_config_template.json",
    )
    parser.add_argument("--start-height", type=int, default=1)
    parser.add_argument("--end-height", type=int)
    parser.add_argument("--shards", default="", help="comma-separated full shard IDs")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--progress-every", type=int, default=10000)
    parser.add_argument("--output", default="-")
    return parser.parse_args()


def load_env(config_path):
    with open(config_path, encoding="utf-8") as config_file:
        cluster_config = ClusterConfig.from_json(config_file.read())
    env = DEFAULT_ENV.copy()
    env.cluster_config = cluster_config
    return env


def get_tip_height(db):
    """Find the highest canonical minor-block height in a shard DB."""
    if db.get(b"mi_0") is None:
        return -1
    lo, hi = 0, 1
    while db.get(("mi_{}".format(hi)).encode()) is not None:
        hi *= 2
    while lo < hi - 1:
        mid = (lo + hi) // 2
        if db.get(("mi_{}".format(mid)).encode()) is None:
            hi = mid
        else:
            lo = mid
    return lo


def account_snapshot(state, address):
    """Read account fields and the complete storage trie at this state."""
    account = state.get_and_cache_account(address)
    balances = account.token_balances.to_dict()
    storage = account.storage_trie.to_dict()
    return {
        "qkc_balance": balances.get(state.shard_config.default_chain_token, 0),
        "nonce": account.nonce,
        "code_size": len(account.code),
        "storage_root": account.storage_trie.root_hash.hex(),
        "storage_entries": len(storage),
    }


def create_target(evm_tx):
    """Match create_contract's normal CREATE address derivation."""
    if evm_tx.to or evm_tx.to_full_shard_key is None:
        return None
    encoded = [
        utils.normalize_address(evm_tx.sender),
        evm_tx.to_full_shard_key,
        utils.encode_int(evm_tx.nonce),
    ]
    return utils.sha3(rlp.encode(encoded))[12:]


def is_candidate(before):
    return (
        before["qkc_balance"] > 0
        and before["nonce"] == 0
        and before["code_size"] == 0
        and before["storage_root"] != BLANK_ROOT.hex()
    )


def prepare_block_state(shard_state, block):
    """Mirror the state preparation in ShardState.run_block."""
    # __validate_tx and _is_neighbor consult the shard's current root tip.
    # This scanner does not call init_from_root_block(), so set the tip to the
    # root block referenced by the minor block before replaying it.
    shard_state.root_tip = shard_state.db.get_root_block_header_by_hash(
        block.header.hash_prev_root_block
    )
    if shard_state.root_tip is None:
        raise RuntimeError(
            "missing root block {} for minor block {}".format(
                block.header.hash_prev_root_block.hex(), block.header.height
            )
        )
    evm_state = shard_state._get_evm_state_for_new_block(block, ephemeral=True)
    _, evm_state.xshard_tx_cursor_info = (
        shard_state._ShardState__run_cross_shard_tx_with_cursor(
            evm_state=evm_state, mblock=block
        )
    )
    if evm_state.gas_used < block.meta.evm_xshard_gas_limit:
        evm_state.gas_limit -= block.meta.evm_xshard_gas_limit - evm_state.gas_used
    return evm_state


def scan_shard(env, full_shard_id, args):
    db_path = os.path.join(args.db_root, "shard-{}.db".format(full_shard_id))
    raw_db = PersistentDb(db_path)
    try:
        shard_state = ShardState(env, full_shard_id, db=raw_db)
        db = ShardDbOperator(raw_db, env, shard_state.branch)
        tip = get_tip_height(raw_db)
        end_height = tip if args.end_height is None else min(args.end_height, tip)
        if end_height < args.start_height:
            return []

        results = []
        started = time.time()
        for height in range(args.start_height, end_height + 1):
            block = db.get_minor_block_by_height(height)
            if block is None:
                continue
            evm_state = prepare_block_state(shard_state, block)
            for index, typed_tx in enumerate(block.tx_list):
                evm_tx = shard_state._ShardState__validate_tx(
                    typed_tx,
                    evm_state,
                    xshard_gas_limit=block.meta.evm_xshard_gas_limit,
                )
                evm_tx.set_quark_chain_config(env.quark_chain_config)
                target = create_target(evm_tx)
                before = account_snapshot(evm_state, target) if target else None

                success, _ = apply_transaction(
                    evm_state, evm_tx, typed_tx.get_hash()
                )

                if target and is_candidate(before):
                    after = account_snapshot(evm_state, target)
                    storage_cleared = (
                        not success
                        and before["storage_root"] != BLANK_ROOT.hex()
                        and after["storage_root"] == BLANK_ROOT.hex()
                    )
                    results.append(
                        {
                            "shard": full_shard_id,
                            "height": height,
                            "tx_index": index,
                            "tx_hash": typed_tx.get_hash().hex(),
                            "sender": evm_tx.sender.hex(),
                            "target": target.hex(),
                            "success": bool(success),
                            "before": before,
                            "after": after,
                            "storage_cleared_after_failed_create": storage_cleared,
                        }
                    )

            if (
                args.progress_every
                and (height - args.start_height + 1) % args.progress_every == 0
            ):
                elapsed = time.time() - started
                print(
                    "shard {}: {}/{} blocks, {} matching candidates, {:.1f}s".format(
                        full_shard_id,
                        height - args.start_height + 1,
                        end_height - args.start_height + 1,
                        len(results),
                        elapsed,
                    ),
                    file=sys.stderr,
                )
        print(
            "shard {}: scanned {}-{}, {} matching candidates".format(
                full_shard_id, args.start_height, end_height, len(results)
            ),
            file=sys.stderr,
        )
        return results
    finally:
        raw_db.close()


def main():
    args = parse_args()
    env = load_env(args.cluster_config)
    if args.shards:
        shard_ids = [int(value, 0) for value in args.shards.split(",")]
    else:
        shard_ids = sorted(env.quark_chain_config.shards)

    output = (
        open(args.output, "w", encoding="utf-8")
        if args.output != "-"
        else sys.stdout
    )
    totals = {"candidates": 0, "failed_storage_clears": 0}
    errors = []
    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(scan_shard, env.copy(), shard_id, args): shard_id
                for shard_id in shard_ids
            }
            for future in as_completed(futures):
                shard_id = futures[future]
                try:
                    results = future.result()
                except Exception as exc:
                    print("shard {} failed: {}".format(shard_id, exc), file=sys.stderr)
                    errors.append((shard_id, exc))
                    continue
                for result in results:
                    totals["candidates"] += 1
                    if result["storage_cleared_after_failed_create"]:
                        totals["failed_storage_clears"] += 1
                    output.write(json.dumps(result, sort_keys=True) + "\n")
                output.flush()
    finally:
        if output is not sys.stdout:
            output.close()
    print(
        "finished: {} matching candidates, {} failed-create storage clears".format(
            totals["candidates"], totals["failed_storage_clears"]
        ),
        file=sys.stderr,
    )
    if errors:
        raise SystemExit("{} shard scans failed".format(len(errors)))


if __name__ == "__main__":
    main()
