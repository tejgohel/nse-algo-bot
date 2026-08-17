"""
seed_state.py  —  One-time bootstrap for indicator_state DB table.

Run this ONCE before using the incremental updater in production:

    python tools/seed_state.py

For every stock in the watchlist it:
  1. Loads the full 4H history from SQLite
  2. Runs every indicator (parallel, no DB writes during compute)
  3. Calls extract_state() → 16-value incremental state dict
  4. Collects all results in memory, then writes ALL in a single batch transaction

After seeding, main.py's incremental_updater only needs to apply the
2 new candles per day (O(1)) instead of recomputing 2500 rows per stock.
"""

import json
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

import config
import db
import watchlist
from indicators import (
    calculate_indicator_1,
    calculate_indicator_2,
    calculate_indicator_3,
    calculate_emas,
    extract_state,
)

# ─── Per-stock compute (no DB writes here) ────────────────────────────────────

def _compute_one(stock: dict) -> tuple[str, str, dict | None, str | None]:
    """
    Full compute → extract_state for one stock.
    Returns (sid, status, state_dict_or_None, last_date_or_None).
    Does NOT write to DB — caller batches all writes.
    """
    sid = stock["security_id"]

    df = db.get_candles(sid)
    if df is None or df.empty:
        return sid, "no_data", None, None

    df = df[df["date"] <= pd.Timestamp.now()].copy()

    min_req = max(config.INDICATOR_1_LENGTH, config.INDICATOR_3_LENGTH) * 3
    if len(df) < min_req:
        return sid, "insufficient", None, None

    # Keyword names match the signatures in indicators.py exactly. They used to
    # drift (multiplier=, _today_override=), which meant a rule written against
    # the documented signature blew up here with an unexpected-keyword error.
    df = calculate_indicator_1(df, length=config.INDICATOR_1_LENGTH, mult=config.INDICATOR_1_MULT)
    df = calculate_indicator_2(df, length=config.INDICATOR_2_LENGTH, mult=config.INDICATOR_2_MULT)
    df = calculate_indicator_3(df, length=config.INDICATOR_3_LENGTH, mult=config.INDICATOR_3_MULT)
    df = calculate_emas(df)

    state     = extract_state(df)
    last_date = str(df["date"].iloc[-1])
    return sid, "ok", state, last_date


# ─── Batch DB write (single transaction) ──────────────────────────────────────

def _batch_write(rows: list[tuple[str, str, str]]) -> None:
    """
    Write all (security_id, last_date, state_json) rows in ONE transaction.
    ~100x faster than 2123 individual connections.
    """
    con = sqlite3.connect(config.DB_PATH, timeout=30)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS indicator_state (
                security_id  TEXT PRIMARY KEY,
                last_date    TEXT,
                state_json   TEXT
            )
        """)
        con.executemany(
            "INSERT OR REPLACE INTO indicator_state (security_id, last_date, state_json) "
            "VALUES (?, ?, ?)",
            rows
        )
        con.commit()
    finally:
        con.close()


# ─── Main seeder ──────────────────────────────────────────────────────────────

def seed_all(workers: int = 8) -> None:
    stocks = watchlist.load_watchlist()
    total  = len(stocks)

    print(f"\n{'═'*64}")
    print(f"  🌱 SEED INDICATOR STATE  ({total} stocks, {workers} workers)")
    print(f"  DB: {config.DB_PATH}")
    print(f"{'═'*64}\n")

    t0      = time.time()
    ok      = 0
    no_data = 0
    insuf   = 0
    done    = 0
    batch   = []   # (security_id, last_date, state_json) tuples

    print(f"  ⚙️  Computing indicators (parallel)...")

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_compute_one, s): s for s in stocks}
        for fut in as_completed(futs):
            sid, status, state, last_date = fut.result()
            done += 1

            if status == "ok":
                ok += 1
                batch.append((str(sid), str(last_date), json.dumps(state)))
            elif status == "no_data":
                no_data += 1
            else:
                insuf += 1

            if done % 200 == 0 or done == total:
                pct     = done / total * 100
                bar     = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
                elapsed = time.time() - t0
                eta     = elapsed / done * (total - done) if done else 0
                print(f"  [{done:>4}/{total}] [{bar}] {pct:3.0f}%  "
                      f"✅ {ok}  ❌ {no_data + insuf}  ETA: {int(eta)}s")

    compute_elapsed = time.time() - t0
    print(f"\n  ✅ Compute done in {compute_elapsed:.1f}s")

    # ── Single batch write ────────────────────────────────────────────────────
    print(f"  💾 Writing {len(batch)} rows to DB (single transaction)...")
    t_write = time.time()
    _batch_write(batch)
    write_elapsed = time.time() - t_write

    elapsed = time.time() - t0
    print(f"\n{'═'*64}")
    print(f"  ✅ SEED COMPLETE  ({elapsed:.1f}s total  |  write: {write_elapsed:.2f}s)")
    print(f"  Seeded       : {ok}")
    print(f"  No data      : {no_data}")
    print(f"  Insufficient : {insuf}")
    print(f"{'═'*64}\n")


if __name__ == "__main__":
    workers = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    seed_all(workers=workers)
