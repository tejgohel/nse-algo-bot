"""
refresh_db.py  —  Smart DB refresh: seed missing + update outdated stocks.

Logic per stock:
  MISSING  (no table / 0 rows)  →  seed_stock from listing date  (full 5-year fill)
  OUTDATED (last date < today's last trading day)
                                →  seed_stock from (last_stored_date - 2 days)
                                   so only the GAP is fetched — efficient, no dupes
  CURRENT  (already up to date) →  skip entirely (no API call)

INSERT OR IGNORE used throughout — 100% safe to re-run anytime.
Ctrl+C safe — re-run to resume from where it stopped.

Run:
    python refresh_db.py
"""

import sys, os, sqlite3, time, threading
from pathlib import Path
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# tools/ lives one level below the project root, and importing config &
# friends needs that root on sys.path. Derive it from this file so the
# script works from any checkout and any working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config
import auto_login
import login as _login_mod
import db
import watchlist
from nse_holidays import last_trading_day as _nse_last_trading_day

WORKERS = 4

print("\n" + "═"*64)
print("  REFRESH DB  —  Seed missing + Update outdated stocks")
print("  4H + 2H + 1H candles  |  INSERT OR IGNORE (no duplicates)")
print("═"*64)

# ─── Step 1: Load watchlist ───────────────────────────────────────────────────
stocks = watchlist.load_watchlist()
print(f"  Watchlist: {len(stocks)} stocks\n")

# ─── Step 2: Batch-query MAX(date) per stock from SQLite  (no API) ────────────
print("  Checking DB status for all stocks...")

last_dates: dict[str, str | None] = {}
with sqlite3.connect(config.DB_PATH, timeout=30) as con:
    for s in stocks:
        sid = s["security_id"]
        try:
            row = con.execute(
                f"SELECT MAX(date) FROM candles_{sid}"
            ).fetchone()
            last_dates[sid] = row[0] if row and row[0] else None
        except Exception:
            last_dates[sid] = None   # table doesn't exist

# ─── Step 3: Classify stocks ──────────────────────────────────────────────────
today     = datetime.now().date()
last_biz  = _nse_last_trading_day(today)
last_biz_str = last_biz.strftime("%Y-%m-%d")

missing  = [s for s in stocks if last_dates[s["security_id"]] is None]
outdated = [s for s in stocks
            if last_dates[s["security_id"]] is not None
            and last_dates[s["security_id"]][:10] < last_biz_str]
current  = [s for s in stocks
            if last_dates[s["security_id"]] is not None
            and last_dates[s["security_id"]][:10] >= last_biz_str]

print(f"\n  📅 Last trading day : {last_biz_str}")
print(f"  ✅ Already current  : {len(current)} stocks  (skipping)")
print(f"  📥 Missing          : {len(missing)} stocks  → full seed from listing date")
print(f"  🔄 Outdated         : {len(outdated)} stocks  → gap-fill from last stored date")

to_process = missing + outdated

if not to_process:
    print(f"\n  🎉 All {len(stocks)} stocks are up to date! Nothing to do.\n")
    sys.exit(0)

confirm = input(f"\n  ⚠️  Process {len(to_process)} stocks? [y/N]: ").strip().lower()
if confirm != "y":
    print("  Cancelled.\n")
    sys.exit(0)

# ─── Step 4: Login — auto_login first, config.py as fallback ─────────────────
print()
_tok = auto_login.generate_token()
if _tok:
    auto_login.save_token_to_config(_tok)
    config.ACCESS_TOKEN = _tok
    print("  ✅ Auto-login successful")
else:
    _fallback = getattr(config, "ACCESS_TOKEN", "").strip()
    if _fallback:
        print("  ⚠️  Auto-login failed — using token from config.py as fallback")
    else:
        print("  ❌ No valid token. Exiting.")
        sys.exit(1)
_login_mod.reload_dhan()
print(f"  ✅ Token ready\n{'─'*64}\n")

# ─── Step 5: Process in parallel ──────────────────────────────────────────────
_sem = threading.Semaphore(WORKERS)

def _process_one(s: dict) -> tuple[str, int, str]:
    sid     = s["security_id"]
    symbol  = s.get("symbol", sid)
    listing = s.get("listing_date", "2000-01-01")
    last    = last_dates[sid]

    if last is None:
        # MISSING: seed from listing date (full 5-year history)
        action    = "SEED"
        from_date = listing
    else:
        # OUTDATED: only fetch the gap → pass (last_date - 2 days) as the start
        # seed_stock uses max(from_date, 5_years_ago), so passing a recent date
        # means it starts there instead of 5 years back → efficient gap-fill.
        action    = "UPDATE"
        gap_start = datetime.strptime(last[:10], "%Y-%m-%d") - timedelta(days=2)
        from_date = gap_start.strftime("%Y-%m-%d")

    try:
        with _sem:
            n = db.seed_stock(sid, from_date, symbol)
        return (symbol, n, action)
    except Exception as e:
        return (symbol, -1, action)


total   = len(to_process)
done    = 0
ok      = 0
failed  = []
t_start = time.time()

try:
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_process_one, s): s for s in to_process}

        for fut in as_completed(futures):
            symbol, n, action = fut.result()
            done += 1
            elapsed = time.time() - t_start
            eta     = (elapsed / done) * (total - done) if done else 0
            pct     = done / total * 100

            if n < 0:
                failed.append(symbol)
                tag = "❌ ERROR"
            else:
                ok += 1
                tag = f"✅ +{n:>4,} bars  [{action}]"

            print(
                f"  [{done:>4}/{total}  {pct:4.0f}%]  {symbol:<15} {tag}"
                f"   ETA {int(eta//60)}m{int(eta%60):02d}s",
                flush=True,
            )

except KeyboardInterrupt:
    print(f"\n\n  ⚠️  Interrupted — {done}/{total} done.")
    print(f"  Re-run refresh_db.py to resume (done stocks won't be re-fetched).\n")
    sys.exit(0)

# ─── Summary ──────────────────────────────────────────────────────────────────
elapsed_min = (time.time() - t_start) / 60
print(f"\n  {'═'*64}")
print(f"  ✅ Done in {elapsed_min:.1f} minutes")
print(f"  Processed : {ok} / {total}")
print(f"  Failed    : {len(failed)}")
if failed:
    print(f"  Failed    : {failed}")
    print(f"\n  Re-run refresh_db.py to retry failed stocks.")
print()
