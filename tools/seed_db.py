# ─────────────────────────────────────────────────────────────────────────────
#  seed_db.py  —  Historical Data Seeder for nse-algo-bot  (1H + 2H + 4H candles)
#
#  RUN THIS ONCE before using main.py for the first time:
#      python tools/seed_db.py
#
#  WHAT IT DOES:
#    STEP 0: Purges all old daily-format candle data from the DB
#            (rows where date = "YYYY-MM-DD" — 10 chars, no time component)
#    STEP 1: For each stock NOT yet in the DB (sequential loop):
#              Fetches 5 years of 1H intraday data in 89-day chunks
#              Stores raw 1H candles  → candles_1h_{id}
#              Aggregates 1H → 2H    → candles_2h_{id}
#              Aggregates 1H → 4H    → candles_{id}  (TradingView session style)
#    STEP 2: For stocks ALREADY seeded with all 3 tables: skips them
#
#  TIMEFRAME SESSIONS (Indian Market 09:15–15:30):
#    1H : every 1-hour bar  (09:15, 10:15, 11:15, 12:15, 13:15, 14:15, 15:15)
#    2H : 09:15–11:15 | 11:15–13:15 | 13:15–15:30  (3 bars/day)
#    4H : 09:15–13:15 | 13:15–15:30                (2 bars/day, TradingView style)
#
#  API CALLS: ~20 per stock (5 years ÷ 89 days)
#  RESUMABLE: Ctrl+C and restart — already-seeded stocks are skipped
# ─────────────────────────────────────────────────────────────────────────────

import sys
import time
import sqlite3

import db
import config
import watchlist
import auto_login
import login as _login_mod


def _purge_daily_data(stocks: list[dict]) -> int:
    """
    Deletes all old daily-format rows (date = "YYYY-MM-DD", length ≤ 10 chars)
    from every stock table in the DB.
    """
    purged_total = 0
    try:
        with sqlite3.connect(config.DB_PATH) as con:
            for s in stocks:
                table = f"candles_{s['security_id']}"
                try:
                    res = con.execute(
                        f"DELETE FROM {table} WHERE LENGTH(date) <= 10"
                    )
                    purged_total += res.rowcount
                except Exception:
                    pass   # Table doesn't exist yet — that's fine
            con.commit()
    except Exception as e:
        print(f"  ⚠️  Purge error: {e}")
    return purged_total


def main():
    print("\n" + "═"*64)
    print("  nse-algo-bot  —  HISTORICAL DATA SEEDER  (1H + 2H + 4H)")
    print("  Fetches 5 years of 1H data → stores 1H, aggregates 2H & 4H")
    print("  Safe to restart — already-seeded stocks are skipped.")
    print("═"*64 + "\n")

    # ── Login — auto_login first, config.py as fallback ────────────────────────
    print("  🔐 Generating fresh Dhan access token...")
    token = auto_login.generate_token()
    if token:
        auto_login.save_token_to_config(token)
        config.ACCESS_TOKEN = token
    else:
        fallback = getattr(config, "ACCESS_TOKEN", "").strip()
        if fallback:
            print("  ⚠️  Auto-login failed — using token from config.py as fallback")
        else:
            print("  ❌ No valid token available. Exiting.")
            return
    _login_mod.reload_dhan()
    print("  ✅ Token ready\n")

    stocks = watchlist.load_watchlist()
    if not stocks:
        print("  ❌ No stocks in watchlist. Check config.WATCHLIST_SYMBOLS.\n")
        return

    # ── STEP 0: Purge old daily-format data ──────────────────────────────────
    print("  🗑️  Step 0: Purging old daily candle data...")
    print("       (Removes 'YYYY-MM-DD' format rows — not compatible with 4H)\n")
    purged = _purge_daily_data(stocks)
    if purged > 0:
        print(f"  ✅ Purged {purged:,} old daily rows\n")
    else:
        print("  ✅ No old daily rows found (DB is clean)\n")

    print("  " + "─"*62)

    # ── STEP 1: Determine what needs seeding ──────────────────────────────────
    needs_seed = [s for s in stocks if not db.is_seeded(s["security_id"])]
    already    = len(stocks) - len(needs_seed)

    print(f"\n  📊 Watchlist        : {len(stocks)} stocks")
    print(f"  ✅ Already seeded   : {already}  (skipping)")
    print(f"  🌱 Need seeding     : {len(needs_seed)}")
    print(f"  📅 Data range       : last 5 years (89-day chunks per stock)")
    print(f"  🗄️  Tables per stock : candles_1h_{{id}}, candles_2h_{{id}}, candles_{{id}}\n")

    if not needs_seed:
        print("\n  🎉 All stocks already seeded! Nothing to do.")
        print("  Run  python main.py  to start the algo.\n")
        return

    print("  " + "─"*62)

    total_candles = 0
    failed        = []
    t_start       = time.time()

    try:
        for i, s in enumerate(needs_seed, 1):
            symbol      = s["symbol"]
            security_id = s["security_id"]
            listing     = s["listing_date"]

            try:
                n = db.seed_stock(security_id, listing, symbol)
            except Exception as e:
                print(f"  ❌ [{symbol:<12}] Error: {e}  [{i}/{len(needs_seed)}]")
                failed.append(symbol)
                continue

            elapsed = time.time() - t_start
            eta     = (elapsed / i) * (len(needs_seed) - i) if i > 0 else 0
            pct     = (i / len(needs_seed)) * 100

            if n == 0:
                print(f"  ⚠️  [{symbol:<12}] 0 candles (pre-IPO or bad symbol?)  [{i}/{len(needs_seed)}]")
                failed.append(symbol)
            else:
                total_candles += n
                print(
                    f"  ✅ [{symbol:<12}] {n:>5,} 4H candles (+1H +2H)  "
                    f"[{i}/{len(needs_seed)}  {pct:.0f}%  ETA {eta/60:.1f}m]"
                )

    except KeyboardInterrupt:
        print(f"\n\n  ⚠️  Interrupted — {i}/{len(needs_seed)} done.")
        print(f"  Progress saved. Re-run seed_db.py to continue.\n")
        sys.exit(0)

    elapsed_min = (time.time() - t_start) / 60
    print("\n" + "═"*64)
    print(f"  ✅ SEEDING COMPLETE  ({elapsed_min:.1f} minutes)")
    print(f"  Total 4H candles stored : {total_candles:,}  (1H & 2H also stored)")
    print(f"  Stocks seeded           : {len(needs_seed) - len(failed)}")
    if failed:
        print(f"  Failed / 0 candles      : {failed}")
    print(f"\n  Run  python main.py  to start the algo.")
    print("═"*64 + "\n")


if __name__ == "__main__":
    main()
