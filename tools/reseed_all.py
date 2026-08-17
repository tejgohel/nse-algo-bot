"""
reseed_all.py  —  Drop all daily candles tables and reseed with correct dates.

FIX: daily_db.py treated toDate as exclusive. The whole DB needs reseeding.

FLOW:
  1. load symbol → listing_date from nse_listing_dates_sorted.txt
  2. drop every candles_* table in candles_daily.db
  3. reseed in parallel on 4 threads (using the config token)
  4. Ctrl+C safe — already-seeded stocks are skipped on re-run

Run:
    python reseed_all.py
"""

import sys, os, sqlite3, time, threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# tools/ lives one level below the project root, and importing config &
# friends needs that root on sys.path. Derive it from this file so the
# script works from any checkout and any working directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config
import auto_login
import login as _login_mod
import daily_db
import watchlist

DB            = daily_db.DAILY_DB_PATH
LISTING_FILE  = os.path.join(os.path.dirname(__file__), "nse_listing_dates_sorted.txt")
DEFAULT_FROM  = "2000-01-01"   # fallback if symbol not in listing file
TO_DATE_FIXED = "2026-05-16"   # toDate (exclusive) → gets data up to 2026-05-15
WORKERS       = 4

# ─── Step 1: Parse listing dates from txt file ────────────────────────────────
print("\n" + "═"*64)
print("  STEP 1: Loading listing dates from nse_listing_dates_sorted.txt")
print("═"*64)

listing_map: dict[str, str] = {}   # symbol → "YYYY-MM-DD"

with open(LISTING_FILE, encoding="utf-8", errors="ignore") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 3:
            continue
        symbol = parts[0].strip()
        date_str = parts[2].strip()   # e.g. "28-Aug-2006"
        try:
            dt = datetime.strptime(date_str, "%d-%b-%Y")
            listing_map[symbol] = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

print(f"  Loaded {len(listing_map)} listing dates\n")

# ─── Step 2: Load watchlist ───────────────────────────────────────────────────
print("═"*64)
print("  STEP 2: Loading watchlist stocks")
print("═"*64)

stocks = watchlist.load_watchlist()
print(f"  Total stocks in watchlist: {len(stocks)}\n")

# Build from_date for each stock using listing file
for s in stocks:
    sym = s.get("symbol", "")
    s["from_date"] = listing_map.get(sym, DEFAULT_FROM)

# Show sample
print(f"  Sample listing dates:")
for s in stocks[:5]:
    print(f"    {s['symbol']:<15} → {s['from_date']}")
print(f"    ...")
print()

# ─── Step 3: Drop all candle tables ──────────────────────────────────────────
print("═"*64)
print("  STEP 3: Dropping all candles_* tables from candles_daily.db")
print("═"*64)

con = sqlite3.connect(DB)
tables = [r[0] for r in con.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
).fetchall()]
candle_tables = [t for t in tables if t.startswith("candles_")]
other_tables  = [t for t in tables if not t.startswith("candles_")]

print(f"\n  Candle tables : {len(candle_tables)}  → will be DROPPED")
print(f"  Other tables  : {len(other_tables)}  → kept (OBs etc.)\n")

confirm = input("  ⚠️  Proceed? [y/N]: ").strip().lower()
if confirm != "y":
    print("  Cancelled.\n")
    sys.exit(0)

for t in candle_tables:
    con.execute(f"DROP TABLE IF EXISTS [{t}]")
con.commit()
con.close()
print(f"\n  ✅ Dropped {len(candle_tables)} tables — DB cleared\n")

# ─── Step 4: Login — auto_login first, config.py as fallback ────────────────
print("═"*64)
print("  STEP 4: Login")
print("═"*64)

_tok = auto_login.generate_token()
if _tok:
    auto_login.save_token_to_config(_tok)
    config.ACCESS_TOKEN = _tok
    print(f"  ✅ Auto-login successful")
else:
    _fallback = getattr(config, "ACCESS_TOKEN", "").strip()
    if _fallback:
        print(f"  ⚠️  Auto-login failed — using token from config.py as fallback")
    else:
        print(f"  ❌ No valid token. Exiting.")
        sys.exit(1)

_login_mod.reload_dhan()
print(f"  ✅ Token ready (Client: {_login_mod.mask_client_id()})\n")

# ─── Step 5: Reseed in parallel ───────────────────────────────────────────────
print("═"*64)
print(f"  STEP 5: Reseeding {len(stocks)} stocks ({WORKERS} threads)")
print("  Ctrl+C is safe — re-run to resume from checkpoint")
print("═"*64 + "\n")

_sem = threading.Semaphore(WORKERS)

def _reseed_one(s: dict) -> tuple[str, int]:
    """Returns (symbol, rows_inserted). -1 = error."""
    sid       = s["security_id"]
    symbol    = s.get("symbol", sid)
    from_date = s["from_date"]

    # Skip if already seeded (for resume after Ctrl+C)
    if daily_db.is_seeded(sid):
        return (symbol, 0)

    try:
        with _sem:
            n = daily_db.seed_stock(sid, from_date, symbol)
        return (symbol, n)
    except Exception as e:
        return (symbol, -1)


total   = len(stocks)
done    = 0
seeded  = 0
skipped = 0
failed  = []
t_start = time.time()

try:
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_reseed_one, s): s for s in stocks}

        for fut in as_completed(futures):
            symbol, n = fut.result()
            done += 1
            elapsed = time.time() - t_start
            eta     = (elapsed / done) * (total - done) if done else 0
            pct     = done / total * 100

            if n < 0:
                failed.append(symbol)
                tag = "❌ ERROR"
            elif n == 0:
                skipped += 1
                tag = "⏭  SKIP (already seeded)"
            else:
                seeded += 1
                tag = f"✅ {n:>4,} bars"

            print(f"  [{done:>4}/{total}  {pct:4.0f}%]  {symbol:<15} {tag}"
                  f"   ETA {int(eta//60)}m{int(eta%60):02d}s", flush=True)

except KeyboardInterrupt:
    print(f"\n\n  ⚠️  Interrupted — {done}/{total} done.")
    print(f"  Re-run this script to resume (already-seeded stocks will be skipped).\n")
    sys.exit(0)

# ─── Summary ──────────────────────────────────────────────────────────────────
elapsed_min = (time.time() - t_start) / 60
print(f"\n  {'═'*64}")
print(f"  ✅ Done in {elapsed_min:.1f} minutes")
print(f"  Freshly seeded : {seeded}")
print(f"  Skipped        : {skipped}  (already had data)")
print(f"  Failed         : {len(failed)}")
if failed:
    print(f"  Failed symbols : {failed}")
    print(f"\n  Re-run this script to retry failed stocks.")
print()
