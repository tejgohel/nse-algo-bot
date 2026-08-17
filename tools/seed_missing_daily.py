"""
seed_missing_daily.py  —  Seed only the daily candles MISSING from candles_daily.db.

Same logic as reseed_all.py — EXACT same threading pattern.
Only difference: skips stocks that already have a table in candles_daily.db.

Run:
    python tools/seed_missing_daily.py
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

DB           = daily_db.DAILY_DB_PATH
LISTING_FILE = os.path.join(os.path.dirname(__file__), "nse_listing_dates_sorted.txt")
DEFAULT_FROM = "2000-01-01"
WORKERS      = 4

# ─── Step 1: Load listing dates ───────────────────────────────────────────────
print("\n" + "="*64)
print("  STEP 1: Loading listing dates")
print("="*64)

listing_map: dict[str, str] = {}
with open(LISTING_FILE, encoding="utf-8", errors="ignore") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 3:
            continue
        symbol   = parts[0].strip()
        date_str = parts[2].strip()
        try:
            dt = datetime.strptime(date_str, "%d-%b-%Y")
            listing_map[symbol] = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

print(f"  Loaded {len(listing_map)} listing dates\n")

# ─── Step 2: Load watchlist ───────────────────────────────────────────────────
print("="*64)
print("  STEP 2: Loading watchlist")
print("="*64)

stocks = watchlist.load_watchlist()
print(f"  Total stocks: {len(stocks)}\n")

for s in stocks:
    s["from_date"] = listing_map.get(s.get("symbol", ""), DEFAULT_FROM)

# ─── Step 3: Find missing stocks (NO table in candles_daily.db) ───────────────
print("="*64)
print("  STEP 3: Checking candles_daily.db")
print("="*64)

with sqlite3.connect(DB) as con:
    existing = {
        r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'candles_%'"
        ).fetchall()
    }

to_seed  = [s for s in stocks if f"candles_{s['security_id']}" not in existing]
skipped  = len(stocks) - len(to_seed)

print(f"\n  Already in DB : {skipped}  (skipping)")
print(f"  Need seeding  : {len(to_seed)}\n")

if not to_seed:
    print("  Nothing to do — all stocks already have daily data!\n")
    sys.exit(0)

confirm = input(f"  Seed {len(to_seed)} missing stocks? [y/N]: ").strip().lower()
if confirm != "y":
    print("  Cancelled.\n")
    sys.exit(0)

# ─── Step 4: Login — auto_login first, config.py as fallback ─────────────────
print()
print("="*64)
print("  STEP 4: Login")
print("="*64)

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
print(f"  Token ready (Client: {config.CLIENT_ID})\n")

# ─── Step 5: Seed in parallel — EXACT same pattern as reseed_all.py ───────────
print("="*64)
print(f"  STEP 5: Seeding {len(to_seed)} stocks  ({WORKERS} threads)")
print("  Ctrl+C safe — re-run skips already-done stocks")
print("="*64 + "\n")

_sem = threading.Semaphore(WORKERS)

def _seed_one(s: dict) -> tuple[str, int]:
    sid      = s["security_id"]
    symbol   = s.get("symbol", sid)
    from_dt  = s["from_date"]

    if daily_db.is_seeded(sid):
        return (symbol, 0)

    try:
        with _sem:
            n = daily_db.seed_stock(sid, from_dt, symbol)
        return (symbol, n)
    except Exception:
        return (symbol, -1)


total   = len(to_seed)
done    = 0
seeded  = 0
skipped_n = 0
failed  = []
t_start = time.time()

try:
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(_seed_one, s): s for s in to_seed}

        for fut in as_completed(futures):
            symbol, n = fut.result()
            done += 1
            elapsed = time.time() - t_start
            eta     = (elapsed / done) * (total - done) if done else 0
            pct     = done / total * 100

            if n < 0:
                failed.append(symbol)
                tag = "ERROR"
            elif n == 0:
                skipped_n += 1
                tag = "SKIP (already seeded)"
            else:
                seeded += 1
                tag = f"+{n:>4,} bars"

            print(
                f"  [{done:>3}/{total}  {pct:4.0f}%]  {symbol:<20} {tag}"
                f"   ETA {int(eta//60)}m{int(eta%60):02d}s",
                flush=True
            )

except KeyboardInterrupt:
    print(f"\n  Interrupted — {done}/{total} done. Re-run to resume.\n")
    sys.exit(0)

# ─── Summary ──────────────────────────────────────────────────────────────────
elapsed_min = (time.time() - t_start) / 60
print(f"\n  {'='*60}")
print(f"  Done in {elapsed_min:.1f} minutes")
print(f"  Seeded  : {seeded}")
print(f"  Skipped : {skipped_n}")
print(f"  Failed  : {len(failed)}")
if failed:
    print(f"  Failed symbols: {failed}")
    print("  Re-run to retry.")
print()
