# ─────────────────────────────────────────────────────────────────────────────
#  seed_daily_db.py  —  Historical DAILY Data Seeder  (nse-algo-bot)
#
#  RUN THIS ONCE before using the daily OB gap filter:
#      python tools/seed_daily_db.py
#
#  WHAT IT DOES:
#    STEP 1: For each stock NOT yet in candles_daily.db:
#              Fetches ~2 years of EOD daily candles from Dhan API
#              Stores in SQLite (candles_daily.db) with date = "YYYY-MM-DD"
#    STEP 2: After seeding each stock → precomputes Bear OBs from daily candles
#              Stored in bear_obs_{security_id} table for fast O(1) lookup
#    STEP 3: Stocks ALREADY seeded → skipped (safe to restart after Ctrl+C)
#
#  WHY ~2 YEARS?
#    Indicators need enough history to warm up before they mean anything
#    breaks and build reliable OBs.  2 years ≈ 500 trading days — plenty of
#    history without bloating the DB (vs 5 years for 4H candles).
#
#  API CALLS: ~2 per stock (2 × 1-year chunks)
#  PARALLEL:  4 workers + 2 calls/sec global rate limiter
#  RESUMABLE: Ctrl+C and restart — already-seeded stocks are skipped
#
#  NEXT STEP: After seeding, run  python main.py  normally.
#             main.py will call daily_db.update_all_daily() every morning
#             to keep candles_daily.db fresh and OBs recomputed.
# ─────────────────────────────────────────────────────────────────────────────

import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import daily_db
import watchlist
import auto_login
import config


# ── Semaphore: max 4 simultaneous API threads ─────────────────────────────────
_API_SEMAPHORE = threading.Semaphore(4)


def _seed_one(s: dict) -> tuple[str, int, "str | None"]:
    """
    Seeds a single stock with ~2 years of daily candles + precomputes Bear OBs.
    Returns (symbol, candles_inserted, error_or_None).
    """
    symbol      = s["symbol"]
    security_id = s["security_id"]
    listing     = s["listing_date"]

    try:
        with _API_SEMAPHORE:
            n = daily_db.seed_stock_daily(security_id, listing, symbol)
        return symbol, n, None
    except Exception as e:
        return symbol, 0, str(e)


def main():
    print("\n" + "="*64)
    print("  nse-algo-bot  —  DAILY DB SEEDER")
    print("  Fetches ~2 years of EOD daily candles + precomputes Bear OBs")
    print(f"  Output DB : candles_daily.db")
    print("  Safe to restart — already-seeded stocks are skipped.")
    print("=" *64 + "\n")

    # ── Step 0: Login — auto_login first, config.py as fallback ────────────────
    print("  Generating fresh Dhan access token...")
    token = auto_login.generate_token()

    if token:
        auto_login.save_token_to_config(token)
    else:
        fallback = getattr(config, "ACCESS_TOKEN", "").strip()
        if fallback:
            print("  ⚠️  Auto-login failed — using token from config.py as fallback")
            token = fallback
        else:
            print("  ❌ No valid token available. Exiting.")
            return

    config.ACCESS_TOKEN = token
    import login as _login_mod
    _login_mod.reload_dhan()
    print("  Token ready. Starting seed...\n")

    stocks = watchlist.load_watchlist()
    if not stocks:
        print("  ❌ No stocks in watchlist. Check config.WATCHLIST_SYMBOLS.\n")
        return

    # ── Determine what needs seeding ──────────────────────────────────────────
    needs_seed = [s for s in stocks if not daily_db.is_daily_seeded(s["security_id"])]
    already    = len(stocks) - len(needs_seed)

    print(f"  📊 Watchlist          : {len(stocks)} stocks")
    print(f"  ✅ Already seeded     : {already}  (daily data fresh, skipping)")
    print(f"  🌱 Need seeding       : {len(needs_seed)}")
    print(f"  📅 History per stock  : ~2 years EOD candles (2 API calls each)")
    print(f"  🔢 Total API calls    : ~{len(needs_seed) * 2} (throttled at 2/sec)\n")

    if not needs_seed:
        print("  🎉 All stocks already seeded with daily data! Nothing to do.")
        print("  Precomputed Bear OBs are ready for fast gap checking.\n")
        print("  Run  python main.py  to start the algo.\n")
        return

    WORKERS = 4
    print(f"  Starting parallel seed with {WORKERS} workers...\n")
    print("  " + "─"*62)

    total_candles = 0
    failed        = []
    done          = 0
    t_start       = time.time()

    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {pool.submit(_seed_one, s): s for s in needs_seed}

            for fut in as_completed(futures):
                symbol, n, err = fut.result()
                done    += 1
                elapsed  = time.time() - t_start
                eta      = (elapsed / done) * (len(needs_seed) - done) if done > 0 else 0
                pct      = (done / len(needs_seed)) * 100

                if err:
                    print(f"  ❌ [{symbol:<12}] Error: {err}  [{done}/{len(needs_seed)}]")
                    failed.append(symbol)
                elif n == 0:
                    print(f"  ⚠️  [{symbol:<12}] 0 candles (pre-IPO or bad symbol?)  [{done}/{len(needs_seed)}]")
                    failed.append(symbol)
                else:
                    total_candles += n
                    print(
                        f"  ✅ [{symbol:<12}] {n:>4,} daily bars  "
                        f"[{done}/{len(needs_seed)}  {pct:.0f}%  ETA {eta/60:.1f}m]"
                    )

    except KeyboardInterrupt:
        print(f"\n\n  ⚠️  Interrupted — {done}/{len(needs_seed)} done.")
        print(f"  Progress saved. Re-run seed_daily_db.py to continue.\n")
        sys.exit(0)

    elapsed_min = (time.time() - t_start) / 60
    print("\n" + "═"*64)
    print(f"  ✅ DAILY SEEDING COMPLETE  ({elapsed_min:.1f} minutes)")
    print(f"  Total daily candles stored : {total_candles:,}")
    print(f"  Stocks seeded              : {len(needs_seed) - len(failed)}")
    print(f"  Bear OBs precomputed       : ✅ (stored in bear_obs_* tables)")
    if failed:
        print(f"  Failed / 0 candles         : {failed}")
    print(f"\n  Run  python main.py  to start the algo.")
    print("═"*64 + "\n")


if __name__ == "__main__":
    main()
