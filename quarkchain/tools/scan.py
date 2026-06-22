#!/usr/bin/env python3
"""
scan.py - QuarkChain DB Scanner

Requires Python 3.6+.

Scans shard RocksDB databases for a time range and outputs TX count and
active user metrics in Markdown format.

Usage:
    python scan.py --db-path /data/qkc --start "2026-01-01 00:00:00" --end "2026-05-01 00:00:00"
"""

import argparse
import glob
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

if sys.version_info < (3, 6):
    raise RuntimeError("This script requires Python 3.6 or higher")

# Allow running from any directory
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "../.."))

from quarkchain.db import PersistentDb
from quarkchain.core import MinorBlock


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _block_key(height: int) -> bytes:
    return b"mi_%d" % height


def _get_tip_height(db: PersistentDb) -> int:
    """Exponential + binary search for the canonical tip block height."""
    if db.get(_block_key(0)) is None:
        return -1
    hi = 1
    while db.get(_block_key(hi)) is not None:
        hi *= 2
    lo = hi // 2
    while lo < hi - 1:
        mid = (lo + hi) // 2
        if db.get(_block_key(mid)) is not None:
            lo = mid
        else:
            hi = mid
    return lo


def _block_time(db: PersistentDb, height: int):
    """Return the create_time (unix ts) of the canonical block at *height*, or None."""
    h = db.get(_block_key(height))
    if h is None:
        return None
    raw = db.get(b"mblock_" + h)
    if raw is None:
        return None
    return MinorBlock.deserialize(raw).header.create_time


def _height_for_ts(db: PersistentDb, target_ts: int, lo: int, hi: int, find_first: bool) -> int:
    """
    Binary search for block height.
    find_first=True  → smallest height with create_time >= target_ts
    find_first=False → largest  height with create_time <= target_ts
    """
    result = hi if find_first else lo
    while lo <= hi:
        mid = (lo + hi) // 2
        t = _block_time(db, mid)
        if t is None:
            # gap in the chain; shrink toward the direction we're searching
            if find_first:
                hi = mid - 1
            else:
                lo = mid + 1
            continue
        if find_first:
            if t >= target_ts:
                result = mid
                hi = mid - 1
            else:
                lo = mid + 1
        else:
            if t <= target_ts:
                result = mid
                lo = mid + 1
            else:
                hi = mid - 1
    return result


# ---------------------------------------------------------------------------
# Shard scanner
# ---------------------------------------------------------------------------

def scan_shard(db: PersistentDb, start_ts: int, end_ts: int, shard_label: str):
    """
    Iterate over all canonical blocks in [start_ts, end_ts) for one shard.

    Returns list of (create_time, tx_count, sender_set).
    Only senders are tracked as "active users" (avoids double-counting with
    contract addresses and cross-shard recipients).
    """
    tip = _get_tip_height(db)
    if tip < 0:
        print(f"  [{shard_label}] empty db, skipping", file=sys.stderr)
        return []

    start_h = _height_for_ts(db, start_ts, 0, tip, find_first=True)
    end_h   = _height_for_ts(db, end_ts - 1, 0, tip, find_first=False)

    if start_h > end_h:
        print(f"  [{shard_label}] no blocks in range", file=sys.stderr)
        return []

    print(f"  [{shard_label}] heights {start_h}–{end_h} ({end_h - start_h + 1} blocks)",
          file=sys.stderr)

    records = []
    for height in range(start_h, end_h + 1):
        h = db.get(_block_key(height))
        if h is None:
            continue
        raw = db.get(b"mblock_" + h)
        if raw is None:
            continue
        block = MinorBlock.deserialize(raw)
        ts = block.header.create_time
        if ts < start_ts or ts >= end_ts:
            continue

        senders = set()
        for tx in block.tx_list:
            evm_tx = tx.tx.to_evm_tx()
            senders.add(evm_tx.sender)

        records.append((ts, len(block.tx_list), senders))

    return records


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def _aggregate(records, key_fn):
    tx_counts  = defaultdict(int)
    user_sets  = defaultdict(set)
    for ts, tx_count, senders in records:
        k = key_fn(ts)
        tx_counts[k]  += tx_count
        user_sets[k].update(senders)
    return tx_counts, user_sets


def _day_key(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _month_key(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m")


def _quarter_key(ts):
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    q  = (dt.month - 1) // 3 + 1
    return f"{dt.year}-Q{q}"


def _month_offset(ym: str, delta: int) -> str:
    """Shift a 'YYYY-MM' string by *delta* months (positive or negative)."""
    year, month = int(ym[:4]), int(ym[5:])
    month += delta
    year  += (month - 1) // 12
    month  = (month - 1) % 12 + 1
    return f"{year:04d}-{month:02d}"


# ---------------------------------------------------------------------------
# Markdown output
# ---------------------------------------------------------------------------

def _md_table(title, headers, rows):
    sep = "| " + " | ".join("---" for _ in headers) + " |"
    out = [f"## {title}", "",
           "| " + " | ".join(headers) + " |", sep]
    for row in rows:
        out.append("| " + " | ".join(str(v) for v in row) + " |")
    out.append("")
    return "\n".join(out)


def render_report(args, shard_count, all_records):
    tx_day,  usr_day  = _aggregate(all_records, _day_key)
    tx_mon,  usr_mon  = _aggregate(all_records, _month_key)
    tx_qtr,  usr_qtr  = _aggregate(all_records, _quarter_key)

    total_tx    = sum(tx_day.values())
    total_users = len(set().union(*[s for _, _, s in all_records])) if all_records else 0

    lines = [
        "# QuarkChain Stats Report",
        "",
        f"| Field | Value |",
        f"| --- | --- |",
        f"| Period | {args.start} UTC → {args.end} UTC |",
        f"| Shards scanned | {shard_count} |",
        f"| Total TXs | {total_tx:,} |",
        f"| Total unique active users | {total_users:,} |",
        "",
    ]

    # Daily
    days = sorted(tx_day)
    lines.append(_md_table(
        "Daily",
        ["Date", "TX Count", "Active Users"],
        [(d, f"{tx_day[d]:,}", f"{len(usr_day[d]):,}") for d in days],
    ))

    # Monthly
    months = sorted(tx_mon)
    lines.append(_md_table(
        "Monthly",
        ["Month", "TX Count", "Active Users"],
        [(m, f"{tx_mon[m]:,}", f"{len(usr_mon[m]):,}") for m in months],
    ))

    # Quarterly (calendar Q1/Q2/Q3/Q4)
    quarters = sorted(tx_qtr)
    lines.append(_md_table(
        "3-Month (Quarter)",
        ["Quarter", "TX Count", "Active Users"],
        [(q, f"{tx_qtr[q]:,}", f"{len(usr_qtr[q]):,}") for q in quarters],
    ))

    # Rolling 3-month: for each month M in the data, sum M-2 + M-1 + M
    rolling_rows = []
    for m in sorted(tx_mon):
        prev2, prev1 = _month_offset(m, -2), _month_offset(m, -1)
        tx_3m = tx_mon.get(prev2, 0) + tx_mon.get(prev1, 0) + tx_mon[m]
        usr_3m = (usr_mon.get(prev2, set())
                  | usr_mon.get(prev1, set())
                  | usr_mon[m])
        label = f"{prev2} ~ {m}"
        rolling_rows.append((label, f"{tx_3m:,}", f"{len(usr_3m):,}"))
    lines.append(_md_table(
        "Rolling 3-Month",
        ["Window (end month)", "TX Count", "Active Users"],
        rolling_rows,
    ))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Scan QuarkChain shard DBs and output TX/active-user stats in Markdown."
    )
    p.add_argument(
        "--db-path", required=True,
        help="Directory that contains shard-*.db sub-directories (e.g. /data/qkc or ./data)",
    )
    p.add_argument(
        "--start", required=True,
        metavar="DATETIME",
        help="Start of range, UTC, inclusive (e.g. '2026-01-01 00:00:00')",
    )
    p.add_argument(
        "--end", required=True,
        metavar="DATETIME",
        help="End of range, UTC, exclusive (e.g. '2026-05-01 00:00:00')",
    )
    p.add_argument(
        "--output", default="-",
        help="Write Markdown to this file (default: stdout)",
    )
    return p.parse_args()


def main():
    args = parse_args()

    try:
        start_dt = datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
        end_dt   = datetime.strptime(args.end,   "%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        print(f"Error parsing dates: {e}", file=sys.stderr)
        sys.exit(1)

    start_ts = int(start_dt.replace(tzinfo=timezone.utc).timestamp())
    end_ts   = int(end_dt.replace(tzinfo=timezone.utc).timestamp())

    if start_ts >= end_ts:
        print("Error: --start must be before --end", file=sys.stderr)
        sys.exit(1)

    shard_paths = sorted(glob.glob(os.path.join(args.db_path, "shard-*.db")))
    if not shard_paths:
        print(f"No shard-*.db directories found under {args.db_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(shard_paths)} shard database(s)", file=sys.stderr)

    all_records = []
    for path in shard_paths:
        label = os.path.basename(path)
        print(f"Scanning {path} ...", file=sys.stderr)
        db = PersistentDb(path)
        try:
            records = scan_shard(db, start_ts, end_ts, label)
        finally:
            db.close()
        print(f"  → {len(records)} blocks with data", file=sys.stderr)
        all_records.extend(records)

    report = render_report(args, len(shard_paths), all_records)

    if args.output == "-":
        print(report)
    else:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"Report written to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
