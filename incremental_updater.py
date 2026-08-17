"""
incremental_updater.py  —  Fast O(K) indicator state update.

Called from main.py AFTER db.update_all() and BEFORE scanner.precompute_all():

    import incremental_updater
    incremental_updater.run(stocks)

For each stock it:
  1. Loads the existing state from indicator_state DB table
  2. Gets last candle date with a CHEAP single-row query (no full load)
  3. If already current → returns immediately (no further DB work)
  4. If new candles exist (≤ MAX_INCREMENTAL_BARS): loads only NEW bars,
     applies increment_state() for each, saves updated state
  5. If too many missing bars → marks stale (precompute will full-recompute)

Statuses:
  updated   — new candles found and applied          (typical morning case)
  current   — state matches last candle already       (no new data fetched)
  no_state  — no state in DB yet (run seed_state.py)  (falls back to full precompute)
  stale     — too many missing bars                   (falls back to full precompute)
  no_data   — no candles in DB for this stock         (skip)
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import db
from indicators import increment_state

MAX_INCREMENTAL_BARS = 20   # more than this → skip (full precompute will handle)


# ─── Per-stock update ─────────────────────────────────────────────────────────

def _update_one(stock: dict) -> tuple[str, str]:
    """Apply incremental state update for one stock. Returns (sid, status)."""
    sid = stock["security_id"]
    try:
        existing = db.load_indicator_state(sid)
        if existing is None:
            return sid, "no_state"

        state_dict, last_date_str = existing

        # ── CHEAP DATE CHECK: single-row query, no full candle load ──────────────
        live_session_str = db.get_current_session_start_str()
        last_completed = db.get_last_candle_date(
            sid, before=live_session_str if live_session_str else None
        )

        if last_completed is None:
            return sid, "no_data"

        if last_completed == last_date_str:
            return sid, "current"   # already up-to-date — skip everything

        # ── LOAD ONLY NEW BARS (not full history) ─────────────────────────────────
        df_new = db.get_candles_since(sid, last_date_str)

        if df_new is None or df_new.empty:
            return sid, "current"

        # Strip live (currently-forming) session bar if it appears in new bars
        if live_session_str:
            import pandas as _pd
            df_new = df_new[df_new["date"] < _pd.Timestamp(live_session_str)].copy()

        if df_new.empty:
            return sid, "current"

        if len(df_new) > MAX_INCREMENTAL_BARS:
            return sid, "stale"

        # Apply increment_state for each new completed candle
        for _, row in df_new.iterrows():
            state_dict = increment_state(state_dict, row.to_dict())

        db.save_indicator_state(sid, state_dict, last_completed)
        return sid, "updated"

    except Exception:
        return sid, "no_state"   # any unexpected error → fall back to full precompute


# ─── Public entry point ───────────────────────────────────────────────────────

def run(stocks: list[dict], workers: int = 8) -> dict:
    """
    Update indicator_state for all stocks incrementally.

    Called between db.update_all() and scanner.precompute_all() in main.py.
    Stocks that are 'no_state' or 'stale' are noted — precompute_state() will
    fall back to full recompute for those (usual O(N) path).

    Returns a summary counts dict.
    """
    t0     = time.time()
    total  = len(stocks)
    counts = {"updated": 0, "current": 0, "no_state": 0, "stale": 0, "no_data": 0}

    print(f"\n{'─'*64}")
    print(f"  INCREMENTAL STATE UPDATE  ({total} stocks, {workers} workers)")

    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_update_one, s): s for s in stocks}
        for fut in as_completed(futs):
            try:
                _, status = fut.result()
            except Exception:
                status = "no_state"
            counts[status] = counts.get(status, 0) + 1
            done += 1
            if done % 500 == 0:
                elapsed = time.time() - t0
                print(f"  [{done:>4}/{total}]  updated={counts['updated']}  "
                      f"current={counts['current']}  stale={counts['stale']}  "
                      f"({elapsed:.1f}s)")

    elapsed     = time.time() - t0
    needs_full  = counts["no_state"] + counts["stale"]

    print(
        f"  Updated: {counts['updated']:>4}  "
        f"Current: {counts['current']:>4}  "
        f"Needs full precompute: {needs_full:>4}  "
        f"No data: {counts['no_data']}  "
        f"({elapsed:.1f}s)"
    )
    if counts["no_state"] > 0:
        print(f"  {counts['no_state']} stocks have no state — run  python tools/seed_state.py  once to seed them.")
    print(f"{'─'*64}\n")

    return counts
