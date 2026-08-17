# ─────────────────────────────────────────────────────────────────────────────
#  daily_db.py  —  SQLite Daily OHLCV Database Manager
#
#  DATA FLOW:
#    Dhan 1H intraday API → aggregate to 4H (TradingView sessions) → SQLite
#
#  4H SESSION DEFINITION (Indian Market — NSE 09:15–15:30):
#    Session 1 : 09:15 → 13:15  (candle key: "YYYY-MM-DD 09:15:00")
#    Session 2 : 13:15 → 15:30  (candle key: "YYYY-MM-DD 13:15:00")
#
#  WHY 2 SESSIONS?
#    TradingView 4H for Indian market creates exactly 2 bars per day:
#      Bar 1 → 09:15, 10:15, 11:15, 12:15  (4 one-hour candles combined)
#      Bar 2 → 13:15, 14:15, 15:15         (3 one-hour candles combined)
#    Our 4H candles exactly match TradingView's chart.
#
#  DB SCHEMA (one table per security — SAME COLUMN NAMES as before):
#    candles_{security_id} (
#        date    TEXT PRIMARY KEY,   -- "YYYY-MM-DD HH:MM:SS" (session open time)
#        open    REAL,
#        high    REAL,
#        low     REAL,
#        close   REAL,
#        volume  INTEGER
#    )
#
#  WHY SAME COLUMN NAMES?
#    All downstream code (indicators.py, strategy.py) uses df["date"], df["open"]
#    etc. Keeping identical column names = zero changes in dependent code.
#
#  API: Dhan /v2/charts/intraday  (interval=60, max 90 days per call)
#  DATA: Last 5 years fetched in 89-day chunks (~20 API calls per stock)
# ─────────────────────────────────────────────────────────────────────────────

import sqlite3
import time
import threading
import requests
from datetime import datetime, timedelta, date
from collections import Counter, OrderedDict

import config
import login as _login
from nse_holidays import last_trading_day as _nse_last_trading_day, is_trading_day as _is_trading_day

_DB_LOCK = threading.Lock()   # SQLite writes must be serialized across threads


# ── Weekend / Holiday Helper ──────────────────────────────────────────────────

def _last_biz_day(ref: date | None = None) -> date:
    """
    Returns the last NSE trading day STRICTLY BEFORE `ref`.
    Skips weekends AND NSE public holidays (loaded from nse_holidays.py).

    If `ref` is not given, uses today.

    Examples:
      Run on Friday  24-Apr-2026  → 2026-04-23  (Thursday)
      Run on Monday  27-Apr-2026  → 2026-04-24  (Friday — skips Sat+Sun) ✅
      Run on Saturday 25-Apr-2026 → 2026-04-24  (Friday)
      Run on Sunday  26-Apr-2026  → 2026-04-24  (Friday)
      Run on Tue     29-May-2026  → 2026-05-27  (Wed — skips Thu Bakri Id holiday)
    """
    d = ref if ref is not None else datetime.now().date()
    return _nse_last_trading_day(d)


# ── Global Rate Limiter ───────────────────────────────────────────────────────

class _GlobalRateLimiter:
    """
    Token-bucket rate limiter shared across all threads.
    Ensures at most `calls_per_sec` API calls per second globally.
    """
    def __init__(self, calls_per_sec: float = 2.0):
        self._lock     = threading.Lock()
        self._interval = 1.0 / calls_per_sec
        self._last     = 0.0

    def wait(self):
        with self._lock:
            now     = time.monotonic()
            elapsed = now - self._last
            if elapsed < self._interval:
                time.sleep(self._interval - elapsed)
            self._last = time.monotonic()


# 1.5 API calls/sec = 90/min — Dhan historical endpoint limit
_RL = _GlobalRateLimiter(calls_per_sec=1.5)


import os as _os
DAILY_DB_PATH = _os.path.join(
    _os.path.dirname(_os.path.abspath(__file__)),
    "candles_daily.db"
)

# ── 4H Session Definitions ────────────────────────────────────────────────────
#
#  Indian NSE market: 09:15–15:30
#  TradingView 4H splits = 2 sessions per trading day
#
#  [ 09:15 → 13:15 )   Session 1  →  label "09:15:00"
#  [ 13:15 → 15:31 )   Session 2  →  label "13:15:00"
#

_S1_MINS = 9  * 60 + 15   # 555  (09:15)
_S2_MINS = 13 * 60 + 15   # 795  (13:15)
_ME_MINS = 15 * 60 + 31   # 931  (15:31 — exclusive upper bound)


def _get_session_key(hour: int, minute: int) -> str | None:
    """
    Map an IST hour:minute to its 4H session start string.
    Returns "09:15:00", "13:15:00", or None (pre/post-market).
    """
    t = hour * 60 + minute
    if _S1_MINS <= t < _S2_MINS:
        return "09:15:00"
    elif _S2_MINS <= t < _ME_MINS:
        return "13:15:00"
    return None


def get_current_session_start_str() -> str | None:
    """
    Returns the session-start datetime string for the CURRENT live 4H bar.
    E.g., "2024-01-15 09:15:00"  or  "2024-01-15 13:15:00"  or  None (market closed).
    Used by strategy.py to identify which 4H bar is currently forming.
    """
    now = datetime.now()
    key = _get_session_key(now.hour, now.minute)
    if key is None:
        return None
    return f"{now.strftime('%Y-%m-%d')} {key}"


# ── SQLite Helpers ────────────────────────────────────────────────────────────

def _conn() -> sqlite3.Connection:
    """Open a connection to the daily SQLite database (candles_daily.db)."""
    return sqlite3.connect(DAILY_DB_PATH)


def _table(security_id: str) -> str:
    return f"candles_{security_id}"


def _ensure_table(cur: sqlite3.Cursor, security_id: str):
    """
    Create the candle table (if not exists).
    Schema is identical to the old daily schema — only the date values differ.
    For 4H data, `date` = "YYYY-MM-DD HH:MM:SS" (session open time).
    """
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_table(security_id)} (
            date    TEXT PRIMARY KEY,
            open    REAL,
            high    REAL,
            low     REAL,
            close   REAL,
            volume  INTEGER
        )
    """)


def _insert_rows(cur: sqlite3.Cursor, security_id: str, rows: list[dict]):
    """INSERT OR IGNORE — safe to re-run during seed, no duplicates."""
    _ensure_table(cur, security_id)
    cur.executemany(
        f"INSERT OR IGNORE INTO {_table(security_id)} "
        f"(date, open, high, low, close, volume) VALUES (?,?,?,?,?,?)",
        [(r["date"], r["open"], r["high"], r["low"], r["close"], r["volume"]) for r in rows]
    )


def _upsert_rows(cur: sqlite3.Cursor, security_id: str, rows: list[dict]):
    """
    INSERT OR REPLACE — used by update_stock() to OVERWRITE existing rows.

    WHY: On the day of seeding, the last 4H candle (13:15 bar) might have been
    fetched mid-day (partial 15:15 candle). Next morning, Dhan's API returns
    the fully-settled close. REPLACE ensures the correct EOD close overwrites
    any partially-captured value.

    Example: seed at 18:00 → 15:15 candle close = 1423.3 (partial)
             update at 09:00 next day → same candle close = 1425.5 (final EOD)
             REPLACE fixes it. INSERT OR IGNORE would leave the stale value.
    """
    _ensure_table(cur, security_id)
    cur.executemany(
        f"INSERT OR REPLACE INTO {_table(security_id)} "
        f"(date, open, high, low, close, volume) VALUES (?,?,?,?,?,?)",
        [(r["date"], r["open"], r["high"], r["low"], r["close"], r["volume"]) for r in rows]
    )


# ── Dhan EOD API Fetch ────────────────────────────────────────────────────────

def _fetch_candles_range(security_id: str, from_date: str, to_date: str) -> list[dict]:
    """
    Fetch EOD daily candles from Dhan POST /v2/charts/historical API.

    Request format:
        {"securityId": "1333", "exchangeSegment": "NSE_EQ",
         "instrument": "EQUITY", "expiryCode": 0, "oi": false,
         "fromDate": "2022-01-08", "toDate": "2022-02-08"}

    Returns list of dicts: [{date: "YYYY-MM-DD", open, high, low, close, volume}]
    Returns [] on failure or no data.
    """
    payload = {
        "securityId"      : str(security_id),
        "exchangeSegment" : "NSE_EQ",
        "instrument"      : "EQUITY",
        "expiryCode"      : 0,
        "oi"              : False,
        "fromDate"        : from_date,
        "toDate"          : to_date,
    }

    for attempt in range(3):
        try:
            _RL.wait()   # global rate limiter
            r = requests.post(
                "https://api.dhan.co/v2/charts/historical",
                json    = payload,
                headers = _login.get_headers(),
                timeout = 20,
            )

            if r.status_code == 429:
                wait_s = 10 * (attempt + 1)
                print(f"  ⏳ Rate limit — waiting {wait_s}s...")
                time.sleep(wait_s)
                continue

            if r.status_code in (400, 422):
                return []   # Pre-IPO range or invalid params — normal, skip

            if r.status_code != 200:
                print(f"  ❌ HTTP {r.status_code} ({security_id}) [{from_date}→{to_date}]")
                return []

            data = r.json()
            required = ("open", "high", "low", "close", "volume", "timestamp")
            for key in required:
                if key not in data or not data[key]:
                    return []

            rows = []
            for i, ts in enumerate(data["timestamp"]):
                # Dhan EOD candle timestamp → direct date conversion (IST local)
                # Note: +timedelta(days=1) workaround removed — Dhan API now
                # returns the correct trading day timestamp directly.
                dt = datetime.fromtimestamp(ts)
                d  = dt.date()
                date_str = d.strftime("%Y-%m-%d")
                rows.append({
                    "date"   : date_str,
                    "open"   : float(data["open"][i]),
                    "high"   : float(data["high"][i]),
                    "low"    : float(data["low"][i]),
                    "close"  : float(data["close"][i]),
                    "volume" : int(data["volume"][i]),
                })
            return rows

        except Exception as e:
            print(f"  ❌ Fetch exception ({security_id}): {e}")
            return []

    return []


def _fetch_today_from_1h(security_id: str) -> "dict | None":
    """
    Build today's daily OHLCV from 1H candles.

    Priority:
      1. candles.db (candles_1h_{id}) — zero API calls, instant
      2. Dhan intraday API            — fallback if candles.db has no data for today

    Aggregation:
        open   = first 1H candle open  (09:15 bar)
        high   = max of all 1H highs
        low    = min of all 1H lows
        close  = last 1H candle close  (15:15 bar)
        volume = sum of all 1H volumes
    """
    today_str = datetime.now().strftime("%Y-%m-%d")

    # ── Step 1: try candles.db first ─────────────────────────────────────────
    table = f"candles_1h_{security_id}"
    try:
        with sqlite3.connect(config.DB_PATH, timeout=10) as con:
            rows = con.execute(
                f"SELECT open, high, low, close, volume FROM {table} "
                f"WHERE date LIKE ? ORDER BY date ASC",
                (f"{today_str}%",)
            ).fetchall()
        if rows:
            return {
                "date"   : today_str,
                "open"   : float(rows[0][0]),
                "high"   : float(max(r[1] for r in rows)),
                "low"    : float(min(r[2] for r in rows)),
                "close"  : float(rows[-1][3]),
                "volume" : int(sum(r[4] for r in rows)),
            }
    except Exception:
        pass   # table missing or DB locked — fall through to API

    # ── Step 2: fallback to Dhan intraday API ────────────────────────────────
    payload = {
        "securityId"      : str(security_id),
        "exchangeSegment" : "NSE_EQ",
        "instrument"      : "EQUITY",
        "interval"        : "60",
        "oi"              : False,
        "fromDate"        : f"{today_str} 09:00:00",
        "toDate"          : f"{today_str} 16:00:00",
    }
    try:
        _RL.wait()
        r = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            json    = payload,
            headers = _login.get_headers(),
            timeout = 20,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if not data.get("timestamp") or not data.get("open"):
            return None
        return {
            "date"   : today_str,
            "open"   : float(data["open"][0]),
            "high"   : float(max(data["high"])),
            "low"    : float(min(data["low"])),
            "close"  : float(data["close"][-1]),
            "volume" : int(sum(data["volume"])),
        }
    except Exception:
        return None


def _aggregate_to_4h(rows_1h: list[dict]) -> list[dict]:
    """
    Aggregate 1H candles into 4H OHLCV candles — TradingView Indian market style.

    Session 1: 09:15–13:15 → label "YYYY-MM-DD 09:15:00"  (4 one-hour candles)
    Session 2: 13:15–15:30 → label "YYYY-MM-DD 13:15:00"  (3 one-hour candles)

    4H OHLCV rules:
      open   = first 1H open
      high   = max of all 1H highs
      low    = min of all 1H lows
      close  = last 1H close
      volume = sum of all 1H volumes

    Returns list of 4H dicts: [{date, open, high, low, close, volume}]
    `date` = "YYYY-MM-DD HH:MM:SS"  -- used as PRIMARY KEY in SQLite.
    """
    pass  # kept for compatibility; not used by daily_db


# ── Public API ────────────────────────────────────────────────────────────────

def seed_stock(security_id: str, listing_date: str, symbol: str = "") -> int:
    """
    One-time seed: fetch 2 years of EOD daily candles in 365-day chunks
    and store in candles_daily.db.
    Safe to re-run — INSERT OR IGNORE means no duplicates.
    Returns total daily candles inserted.

    NOTE: Dhan API toDate is EXCLUSIVE — to get data UP TO date D,
    we must pass toDate = D + 1 day.
    """
    from_dt = datetime.strptime(listing_date, "%Y-%m-%d")
    to_dt   = datetime.now()
    label   = symbol or security_id
    total_inserted = 0
    chunk_start = from_dt

    while chunk_start < to_dt:
        chunk_end = min(chunk_start + timedelta(days=364), to_dt)
        from_str  = chunk_start.strftime("%Y-%m-%d")
        # +1 day because Dhan toDate is exclusive
        to_str    = (chunk_end + timedelta(days=1)).strftime("%Y-%m-%d")

        rows = _fetch_candles_range(security_id, from_str, to_str)
        if rows:
            with _DB_LOCK:
                with _conn() as con:
                    cur = con.cursor()
                    _insert_rows(cur, security_id, rows)
            total_inserted += len(rows)

        chunk_start = chunk_end + timedelta(days=1)

    return total_inserted


def _market_closed_today() -> bool:
    """
    Returns True if today is a trading day AND market has already closed
    (i.e. current IST time is past 15:30).
    """
    now = datetime.now()
    today = now.date()
    if not _is_trading_day(today):
        return False
    market_close_mins = 15 * 60 + 30   # 15:30 IST
    return (now.hour * 60 + now.minute) >= market_close_mins


def update_stock(security_id: str, symbol: str = "") -> int:
    """
    Incremental update: fetches candles from last stored date up to the
    most recent completed NSE trading day (never fetches today/weekends/holidays).

    POST-MARKET BEHAVIOUR (run after 15:30 on a trading day):
      Today's EOD candle is fully settled — it is included in the fetch.
      e.g. running at 9 PM on 28-Apr will add 28-Apr EOD candle to DB.

    Returns the number of new candles inserted.
    """
    label = symbol or security_id

    with _conn() as con:
        cur = con.cursor()
        _ensure_table(cur, security_id)
        row = cur.execute(
            f"SELECT MAX(date) FROM {_table(security_id)}"
        ).fetchone()
        last_date = row[0] if row and row[0] else None

    if last_date is None:
        print(f"  WARNING [{label}] DB empty — run seed_stock() first")
        return 0

    today = datetime.now().date()

    # ── Determine the effective last completed trading day ──────────────────────
    # After 15:30 on a trading day → today's EOD candle is complete.
    # Before 15:30 (or on weekend/holiday) → use the previous trading day.
    if _market_closed_today():
        last_biz = today          # today IS the last completed trading day
    else:
        last_biz = _last_biz_day(today)

    # If DB already has data up to the last trading day, nothing to do
    if last_date >= last_biz.strftime("%Y-%m-%d"):
        return 0

    # Use last_date as from_str basis — normalize to a valid trading day.
    from_d = datetime.strptime(last_date, "%Y-%m-%d").date()
    while not _is_trading_day(from_d):
        from_d -= timedelta(days=1)
    from_str = from_d.strftime("%Y-%m-%d")

    to_str   = (last_biz + timedelta(days=1)).strftime("%Y-%m-%d")  # +1: Dhan toDate is exclusive

    rows = _fetch_candles_range(security_id, from_str, to_str)
    if not rows:
        print(f"  WARNING [{label}] No data fetched ({from_str} to {to_str})")
        return 0

    new_rows = [r for r in rows if r["date"] > last_date]

    # Dhan historical API lags same-day EOD — if today's candle is missing,
    # build it from 1H intraday data instead.
    today_str = today.strftime("%Y-%m-%d")
    if (_market_closed_today()
            and last_biz.strftime("%Y-%m-%d") == today_str
            and not any(r["date"] == today_str for r in new_rows)):
        intraday_row = _fetch_today_from_1h(security_id)
        if intraday_row:
            new_rows.append(intraday_row)

    if not new_rows:
        return 0

    with _conn() as con:
        cur = con.cursor()
        _insert_rows(cur, security_id, new_rows)

    n = len(new_rows)
    print(f"  [{label}] +{n} new candle(s)  (last was {last_date}  ->  {new_rows[-1]['date']})")
    return n



def get_candles(security_id: str) -> "pd.DataFrame | None":
    """
    Returns full 4H OHLCV history for a security as a pandas DataFrame,
    sorted by date ascending. Returns None if the table is empty or missing.

    Column `date` contains pd.Timestamp values (datetime precision).
    All other columns (open, high, low, close, volume) unchanged.
    """
    import pandas as pd

    with _conn() as con:
        try:
            df = pd.read_sql_query(
                f"SELECT date, open, high, low, close, volume "
                f"FROM {_table(security_id)} ORDER BY date ASC",
                con
            )
        except Exception:
            return None

    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    df = df.reset_index(drop=True)
    return df


def is_seeded(security_id: str) -> bool:
    """
    Returns True if this stock has been seeded with recent 4H data.
    Logic: has ≥1 row AND max(date) is within the last 10 calendar days.
    """
    with _conn() as con:
        cur = con.cursor()
        try:
            row = cur.execute(
                f"SELECT COUNT(*), MAX(date) FROM {_table(security_id)}"
            ).fetchone()
            count    = row[0] if row else 0
            max_date = row[1] if row else None

            if count == 0 or max_date is None:
                return False

            # Handle both "YYYY-MM-DD HH:MM:SS" and legacy "YYYY-MM-DD"
            try:
                last_dt = datetime.strptime(max_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                last_dt = datetime.strptime(max_date, "%Y-%m-%d")

            cutoff = datetime.now() - timedelta(days=10)
            return last_dt >= cutoff

        except Exception:
            return False   # Table doesn't exist yet


def update_all(stocks: list[dict]) -> int:
    """
    DAILY INCREMENTAL UPDATE — called by main.py every day at startup.

    FAST ALGORITHM (avoids unnecessary API calls):
      1. Batch-query MAX(date) for all stocks in SQLite (< 1 second, no API)
      2. Find 'consensus_date' = most common max date = last 4H candle seen by all
      3. Make ONE test API call to see if new 4H candles exist
         - Yes → update ALL seeded stocks (new trading session)
         - No  → only update stocks that are behind consensus (stragglers)
      4. Unseeded stocks → ⚠️ skip, ask user to run seed_db.py

    consensus_date is now "YYYY-MM-DD HH:MM:SS" (4H candle datetime).
    """
    print(f"  DAILY EOD UPDATE  --  {len(stocks)} stocks  (candles_daily.db)")
    print(f"{'='*64}\n")

    today        = datetime.now().date()
    today_str    = today.strftime("%Y-%m-%d")
    last_biz     = _last_biz_day(today)          # last NSE trading day (skips weekends)
    yesterday    = last_biz.strftime("%Y-%m-%d")
    # ⚠️ We NEVER fetch today's candle — it's live/incomplete.
    # DB only stores fully-completed historical candles.
    # Today's bar is built from WS ticks in check_signal_on_tick.

    # ── Phase 1: batch-query all MAX dates from SQLite (no API) ──────────────
    last_dates: dict[str, str | None] = {}

    with _conn() as con:
        for s in stocks:
            try:
                row = con.execute(
                    f"SELECT MAX(date) FROM {_table(s['security_id'])}"
                ).fetchone()
                last_dates[s["security_id"]] = row[0] if row and row[0] else None
            except Exception:
                last_dates[s["security_id"]] = None

    # ── Classify stocks ───────────────────────────────────────────────────────
    unseeded = [s for s in stocks if last_dates[s["security_id"]] is None]
    seeded   = [s for s in stocks if last_dates[s["security_id"]] is not None]

    if unseeded:
        syms = ", ".join(s["symbol"] for s in unseeded[:10])
        more = f" ... +{len(unseeded)-10} more" if len(unseeded) > 10 else ""
        print(f"  ⚠️  {len(unseeded)} stock(s) not seeded yet — run  python tools/seed_daily_db.py  first")
        print(f"     {syms}{more}\n")

    if not seeded:
        print("  ℹ️  No seeded stocks — nothing to update.\n")
        return len(unseeded)

    # ── Phase 2: find consensus last 4H candle datetime ───────────────────────
    filled_dates   = [d for d in last_dates.values() if d]
    consensus_date = Counter(filled_dates).most_common(1)[0][0]   # e.g. "2024-01-15 13:15:00"

    needs_behind = [s for s in seeded if last_dates[s["security_id"]] < consensus_date]

    print(f"  📅 Consensus last 4H candle : {consensus_date}")

    # ── Phase 3: ONE test API call to detect a new trading session ────────────
    needs_new_day: list[dict] = []

    consensus_date_only = consensus_date[:10]   # "YYYY-MM-DD"

    print(f"  Last trading day : {yesterday}")
    print(f"  Consensus in DB  : {consensus_date_only}")

    # ── Phase 3: Build update list using pre-fetched last_dates ───────────────
    # Determine effective last completed trading day (post-market = today)
    if _market_closed_today():
        target_date = today_str          # 28-Apr should be in DB by 9 PM
    else:
        target_date = yesterday          # yesterday is the last settled day

    needs_api = [
        s for s in seeded
        if last_dates[s["security_id"]] is not None
        and last_dates[s["security_id"]] < target_date
    ]

    if not needs_api:
        print(f"  All {len(seeded)} seeded stocks already up-to-date for {target_date}")
        print(f"\n  DB update complete.  ({len(seeded)} checked, {len(unseeded)} skipped)\n")
        return len(unseeded)

    print(f"  {len(needs_api)} stock(s) need update  ({len(seeded) - len(needs_api)} already up-to-date)")
    print(f"  Updating in parallel (8 workers)...\n")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    done = 0
    total = len(needs_api)

    def _do_update(s):
        return update_stock(s["security_id"], s["symbol"])

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(_do_update, s): s for s in needs_api}
        for fut in as_completed(futures):
            done += 1
            if done % 100 == 0 or done == total:
                pct = done * 100 // total
                bar = "#" * (pct // 5) + "." * (20 - pct // 5)
                print(f"  [{bar}] {done}/{total} ({pct}%)", flush=True)


    print(f"\n  ✅ DB update complete.  ({len(seeded)} checked, {len(unseeded)} skipped)\n")
    return len(unseeded)


# ── Backward-compatible aliases (main.py uses old names) ─────────────────────
update_all_daily  = update_all
get_daily_candles = get_candles
is_daily_seeded   = is_seeded
seed_stock_daily  = seed_stock
update_stock_daily = update_stock


def get_current_4h_ohlcv(security_id: str) -> dict | None:
    """
    Fetch the CURRENT live 4H session's OHLCV from Dhan intraday API.
    Used by strategy.py to build the live 4H bar for signal calculation.

    How it works:
      1. Determine which 4H session we're in right now (09:15 or 13:15)
      2. Fetch all 1H candles for today from Dhan API
      3. Filter to only the current session's 1H candles
      4. Aggregate those 1H candles → single {open, high, low, close, volume}

    Returns:
        {open, high, low, close, volume} for current 4H session,
        or None if market is closed or no data yet.
    """
    now = datetime.now()
    session_key = _get_session_key(now.hour, now.minute)
    if session_key is None:
        return None   # Market is closed

    today_str = now.strftime("%Y-%m-%d")
    rows_1h   = _fetch_candles_range(security_id, today_str, today_str)
    if not rows_1h:
        return None

    return {
        "open"   : rows_1h[0]["open"],
        "high"   : max(r["high"]   for r in rows_1h),
        "low"    : min(r["low"]    for r in rows_1h),
        "close"  : rows_1h[-1]["close"],
        "volume" : sum(r["volume"] for r in rows_1h),
    }



# Backward-compatible alias for any code still calling get_today_ohlcv()
get_today_ohlcv = get_current_4h_ohlcv


def get_prev_5min_candle(security_id: str) -> dict | None:
    """
    Returns the OHLCV of the most recently COMPLETED 5-min candle.

    Example:
      Called at 11:26 → returns the 11:20 candle (11:20–11:24:59).
      Called at 11:30 → returns the 11:25 candle (11:25–11:29:59).

    Used to set SL = prev_5min LOW (for LONG entry).

    Returns:
        {"open": x, "high": x, "low": x, "close": x, "time": "HH:MM"}
        None on API error or before market open.
    """
    now       = datetime.now()
    today_str = now.strftime("%Y-%m-%d")

    # Find start of the most recently completed 5-min bar.
    # E.g. 11:26 → last completed bar started at 11:20.
    total_mins = now.hour * 60 + now.minute
    # Market opens at 09:15; 5-min bars: 09:15, 09:20, 09:25, ...
    mins_since_open = total_mins - (9 * 60 + 15)
    if mins_since_open < 5:
        return None   # first bar not yet complete

    # Start of last completed bar relative to 09:15
    completed_bar_offset = (mins_since_open // 5 - 1) * 5
    bar_start_mins       = 9 * 60 + 15 + completed_bar_offset
    bar_h, bar_m         = divmod(bar_start_mins, 60)
    bar_time_str         = f"{bar_h:02d}:{bar_m:02d}"

    payload = {
        "securityId"      : str(security_id),
        "exchangeSegment" : "NSE_EQ",
        "instrument"      : "EQUITY",
        "interval"        : "5",               # 5-minute candles
        "oi"              : False,
        "fromDate"        : f"{today_str} 09:00:00",
        "toDate"          : f"{today_str} {now.strftime('%H:%M:%S')}",
    }

    try:
        _RL.wait()
        r = requests.post(
            "https://api.dhan.co/v2/charts/intraday",
            json=payload,
            headers=_login.get_headers(),
            timeout=10,
        )
        if r.status_code != 200:
            return None

        data = r.json()
        if not data.get("timestamp"):
            return None

        # Find the candle whose timestamp matches bar_start_mins
        target_mins = bar_start_mins
        best_idx    = None
        best_diff   = 999

        for i, ts in enumerate(data["timestamp"]):
            dt   = datetime.fromtimestamp(ts)
            diff = abs((dt.hour * 60 + dt.minute) - target_mins)
            if diff < best_diff:
                best_diff = diff
                best_idx  = i

        if best_idx is None or best_diff > 2:   # allow 2-min tolerance
            return None

        return {
            "open"  : round(float(data["open"][best_idx]),   2),
            "high"  : round(float(data["high"][best_idx]),   2),
            "low"   : round(float(data["low"][best_idx]),    2),
            "close" : round(float(data["close"][best_idx]),  2),
            "time"  : bar_time_str,
        }

    except Exception:
        return None




def get_recent_intraday_candles(
    security_id  : str,
    interval_min : int,
    days         : int = 90,
) -> "pd.DataFrame | None":
    """
    Fetch intraday OHLCV candles from Dhan API for the last `days` calendar days.

    Args:
        security_id  : Dhan security ID
        interval_min : candle size in minutes — 1, 5, 15, 25, 60, etc.
        days         : how many calendar days of history to fetch (max 90 per Dhan)

    Returns:
        pd.DataFrame with columns: date(Timestamp), open, high, low, close, volume
        Rows sorted ascending. Today's INCOMPLETE candles are stripped.
        Returns None on API error or empty response.

    Used for:
        5min  → 5min OB gap filter  (Condition 12)
        15min → resampled from 5min (no extra API call)
        60min → 1H OB gap filter    (Condition 12)
    """
    import pandas as pd

    today    = datetime.now().date()
    from_dt  = today - timedelta(days=min(days, 90))   # Dhan limit: 90 days
    from_str = from_dt.strftime("%Y-%m-%d")
    to_str   = today.strftime("%Y-%m-%d")

    payload = {
        "securityId"      : str(security_id),
        "exchangeSegment" : "NSE_EQ",
        "instrument"      : "EQUITY",
        "interval"        : str(interval_min),
        "oi"              : False,
        "fromDate"        : f"{from_str} 09:00:00",
        "toDate"          : f"{to_str} 16:00:00",
    }

    for attempt in range(3):
        try:
            _RL.wait()
            r = requests.post(
                "https://api.dhan.co/v2/charts/historical",
                json=payload,
                headers=_login.get_headers(),
                timeout=20,
            )
            if r.status_code == 429:
                time.sleep(10 * (attempt + 1))
                continue
            if r.status_code in (400, 422):
                return None
            if r.status_code != 200:
                return None

            data = r.json()
            if not data.get("timestamp"):
                return None

            rows = []
            for i, ts in enumerate(data["timestamp"]):
                dt_ist = datetime.fromtimestamp(ts)
                rows.append({
                    "date"   : dt_ist,
                    "open"   : float(data["open"][i]),
                    "high"   : float(data["high"][i]),
                    "low"    : float(data["low"][i]),
                    "close"  : float(data["close"][i]),
                    "volume" : int(data["volume"][i]),
                })

            if not rows:
                return None

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)

            # Strip today's incomplete candles (market still open)
            df = df[df["date"].dt.date < today].copy()

            return df if not df.empty else None

        except Exception:
            return None

    return None


def get_recent_1h_candles(security_id: str, days: int = 90) -> "pd.DataFrame | None":
    """Backward-compatible wrapper — fetches real 60-min candles."""
    return get_recent_intraday_candles(security_id, interval_min=60, days=days)




if __name__ == "__main__":
    import pandas as pd

    test_sid = "1333"
    test_sym = "HDFCBANK"

    print(f"\n{'═'*60}")
    print(f"  DB 4H Test  |  {test_sym}  ({test_sid})")
    print(f"{'═'*60}\n")

    print(f"  Current session : {get_current_session_start_str() or 'Market closed'}\n")

    if not is_seeded(test_sid):
        print("  Seeding last 5 years of 1H → 4H data...")
        n = seed_stock(test_sid, "2000-01-01", test_sym)
        print(f"  ✅ Inserted {n} 4H candles\n")
    else:
        print("  Already seeded — running incremental update...")
        update_stock(test_sid, test_sym)

    df = get_candles(test_sid)
    if df is not None:
        print(f"\n  Total 4H candles : {len(df)}")
        print(f"  Date range       : {df['date'].iloc[0]} → {df['date'].iloc[-1]}")
        print(f"\n  Last 6 candles (should see 09:15 and 13:15 timestamps):")
        print(df.tail(6).to_string(index=False))
    else:
        print("  ❌ No data found in DB")
