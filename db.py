# ─────────────────────────────────────────────────────────────────────────────
#  db.py  —  SQLite Multi-Timeframe OHLCV Database Manager
#
#  DATA FLOW:
#    Dhan 1H intraday API → store as 1H → aggregate to 2H → aggregate to 4H → SQLite
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

_DB_LOCK      = threading.Lock()    # Serializes write transactions across threads
_thread_local = threading.local()   # Thread-local connection cache (1 conn per thread)


def _conn() -> sqlite3.Connection:
    """
    Return a thread-local SQLite connection, creating it on first use.
    Reusing one connection per thread eliminates the ~60ms open-cost per query.
    WAL mode allows concurrent readers without blocking each other.
    """
    conn = getattr(_thread_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(config.DB_PATH, timeout=30, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")   # safe: WAL guarantees durability
        _thread_local.conn = conn
    return conn


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


# 2 API calls/sec globally = 120/min — safely under Dhan's rate limit
_RL = _GlobalRateLimiter(calls_per_sec=2.0)


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

# ── 2H Session Definitions ────────────────────────────────────────────────────
#  Indian NSE market: 09:15–15:30  →  3 two-hour blocks per day
#  Block 1: 09:15–11:15 → label "09:15:00"  (2 one-hour candles)
#  Block 2: 11:15–13:15 → label "11:15:00"  (2 one-hour candles)
#  Block 3: 13:15–15:30 → label "13:15:00"  (2-3 one-hour candles)
_2H_B1_MINS = 9  * 60 + 15   # 555  (09:15)
_2H_B2_MINS = 11 * 60 + 15   # 675  (11:15)
_2H_B3_MINS = 13 * 60 + 15   # 795  (13:15)
_2H_END_MINS = 15 * 60 + 31  # 931  (15:31 — exclusive upper bound)


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


def _get_2h_session_key(hour: int, minute: int) -> str | None:
    """
    Map an IST hour:minute to its 2H session start string.
    Returns "09:15:00", "11:15:00", "13:15:00", or None.
    """
    t = hour * 60 + minute
    if _2H_B1_MINS <= t < _2H_B2_MINS:
        return "09:15:00"
    elif _2H_B2_MINS <= t < _2H_B3_MINS:
        return "11:15:00"
    elif _2H_B3_MINS <= t < _2H_END_MINS:
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

def _table(security_id: str) -> str:
    return f"candles_{security_id}"


def _table_1h(security_id: str) -> str:
    return f"candles_1h_{security_id}"


def _table_2h(security_id: str) -> str:
    return f"candles_2h_{security_id}"


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


def _ensure_table_1h(cur: sqlite3.Cursor, security_id: str):
    """Create the 1H candle table (if not exists)."""
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_table_1h(security_id)} (
            date    TEXT PRIMARY KEY,
            open    REAL,
            high    REAL,
            low     REAL,
            close   REAL,
            volume  INTEGER
        )
    """)


def _ensure_table_2h(cur: sqlite3.Cursor, security_id: str):
    """Create the 2H candle table (if not exists)."""
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_table_2h(security_id)} (
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


def _insert_rows_1h(cur: sqlite3.Cursor, security_id: str, rows: list[dict]):
    """INSERT OR IGNORE 1H rows."""
    _ensure_table_1h(cur, security_id)
    cur.executemany(
        f"INSERT OR IGNORE INTO {_table_1h(security_id)} "
        f"(date, open, high, low, close, volume) VALUES (?,?,?,?,?,?)",
        [(r["date"], r["open"], r["high"], r["low"], r["close"], r["volume"]) for r in rows]
    )


def _insert_rows_2h(cur: sqlite3.Cursor, security_id: str, rows: list[dict]):
    """INSERT OR IGNORE 2H rows."""
    _ensure_table_2h(cur, security_id)
    cur.executemany(
        f"INSERT OR IGNORE INTO {_table_2h(security_id)} "
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


def _upsert_rows_1h(cur: sqlite3.Cursor, security_id: str, rows: list[dict]):
    """INSERT OR REPLACE 1H rows — fixes stale closes."""
    _ensure_table_1h(cur, security_id)
    cur.executemany(
        f"INSERT OR REPLACE INTO {_table_1h(security_id)} "
        f"(date, open, high, low, close, volume) VALUES (?,?,?,?,?,?)",
        [(r["date"], r["open"], r["high"], r["low"], r["close"], r["volume"]) for r in rows]
    )


def _upsert_rows_2h(cur: sqlite3.Cursor, security_id: str, rows: list[dict]):
    """INSERT OR REPLACE 2H rows — fixes stale closes."""
    _ensure_table_2h(cur, security_id)
    cur.executemany(
        f"INSERT OR REPLACE INTO {_table_2h(security_id)} "
        f"(date, open, high, low, close, volume) VALUES (?,?,?,?,?,?)",
        [(r["date"], r["open"], r["high"], r["low"], r["close"], r["volume"]) for r in rows]
    )


# ── Dhan 1H API Fetch ─────────────────────────────────────────────────────────

def _fetch_1h_candles_range(security_id: str, from_date: str, to_date: str) -> list[dict]:
    """
    Fetch 60-minute (1H) intraday candles from Dhan API.

    from_date / to_date: "YYYY-MM-DD" strings (max 90 days apart — Dhan API limit).
    Returns list of dicts: [{_dt: datetime(IST), open, high, low, close, volume}]
    Returns [] on failure or no data.
    """
    payload = {
        "securityId"      : str(security_id),
        "exchangeSegment" : "NSE_EQ",
        "instrument"      : "EQUITY",
        "interval"        : "60",          # 60-minute candles
        "oi"              : False,
        "fromDate"        : f"{from_date} 09:00:00",
        "toDate"          : f"{to_date} 16:00:00",
    }

    for attempt in range(3):
        try:
            _RL.wait()   # global rate limiter
            r = requests.post(
                "https://api.dhan.co/v2/charts/intraday",
                json=payload,
                headers=_login.get_headers(),
                timeout=20,
            )

            if r.status_code == 429:
                wait_s = 10 * (attempt + 1)
                print(f"  ⏳ Rate limit — waiting {wait_s}s...")
                time.sleep(wait_s)
                continue

            if r.status_code in (400, 422):
                # Pre-IPO range or invalid params — normal, skip silently
                return []

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
                # Convert epoch → IST datetime.
                # datetime.fromtimestamp() uses the OS local timezone.
                # On this machine (IST = UTC+5:30) this gives correct IST times.
                dt_ist = datetime.fromtimestamp(ts)
                rows.append({
                    "_dt"    : dt_ist,
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


# ── 1H → 4H Aggregation ───────────────────────────────────────────────────────

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
    `date` = "YYYY-MM-DD HH:MM:SS"  → used as PRIMARY KEY in SQLite.
    """
    sessions: dict[str, list[dict]] = OrderedDict()

    for row in rows_1h:
        dt  = row["_dt"]
        key = _get_session_key(dt.hour, dt.minute)
        if key is None:
            continue   # pre/post-market candle — skip

        full_key = f"{dt.strftime('%Y-%m-%d')} {key}"
        if full_key not in sessions:
            sessions[full_key] = []
        sessions[full_key].append(row)

    result = []
    for session_dt_str, candles in sessions.items():
        if not candles:
            continue
        result.append({
            "date"   : session_dt_str,
            "open"   : candles[0]["open"],
            "high"   : max(c["high"]   for c in candles),
            "low"    : min(c["low"]    for c in candles),
            "close"  : candles[-1]["close"],
            "volume" : sum(c["volume"] for c in candles),
        })

    return result


def _aggregate_to_1h(rows_1h: list[dict]) -> list[dict]:
    """
    Format raw 1H candles for storage.
    Only keeps candles within NSE market hours (09:15–15:30).
    Returns list of dicts: [{date, open, high, low, close, volume}]
    `date` = "YYYY-MM-DD HH:MM:SS"
    """
    result = []
    for row in rows_1h:
        dt  = row["_dt"]
        t   = dt.hour * 60 + dt.minute
        if _S1_MINS <= t < _ME_MINS:
            result.append({
                "date"   : dt.strftime("%Y-%m-%d %H:%M:%S"),
                "open"   : row["open"],
                "high"   : row["high"],
                "low"    : row["low"],
                "close"  : row["close"],
                "volume" : row["volume"],
            })
    return result


def _aggregate_to_2h(rows_1h: list[dict]) -> list[dict]:
    """
    Aggregate 1H candles into 2H OHLCV candles.

    Block 1: 09:15–11:15 → label "YYYY-MM-DD 09:15:00"  (2 candles)
    Block 2: 11:15–13:15 → label "YYYY-MM-DD 11:15:00"  (2 candles)
    Block 3: 13:15–15:30 → label "YYYY-MM-DD 13:15:00"  (2-3 candles)

    OHLCV rules: open=first, high=max, low=min, close=last, volume=sum
    Returns list of 2H dicts: [{date, open, high, low, close, volume}]
    """
    sessions: dict[str, list[dict]] = OrderedDict()

    for row in rows_1h:
        dt  = row["_dt"]
        key = _get_2h_session_key(dt.hour, dt.minute)
        if key is None:
            continue
        full_key = f"{dt.strftime('%Y-%m-%d')} {key}"
        if full_key not in sessions:
            sessions[full_key] = []
        sessions[full_key].append(row)

    result = []
    for session_dt_str, candles in sessions.items():
        if not candles:
            continue
        result.append({
            "date"   : session_dt_str,
            "open"   : candles[0]["open"],
            "high"   : max(c["high"]   for c in candles),
            "low"    : min(c["low"]    for c in candles),
            "close"  : candles[-1]["close"],
            "volume" : sum(c["volume"] for c in candles),
        })
    return result


# ── Public API ────────────────────────────────────────────────────────────────

def seed_stock(security_id: str, listing_date: str, symbol: str = "") -> int:
    """
    One-time seed: fetch last 5 years of 1H data in 89-day chunks,
    aggregate each chunk to 4H candles, and store in SQLite.

    Uses INSERT OR IGNORE — completely safe to re-run (no duplicates).
    Returns total 4H candles inserted.

    NOTE: Old daily-format rows (date = "YYYY-MM-DD") should be purged BEFORE
    calling this. seed_db.py handles that automatically.
    """
    to_dt   = datetime.now()
    from_dt = to_dt - timedelta(days=5 * 365)   # 5 years back

    # Respect the stock's listing date if it's more recent
    try:
        listing_dt = datetime.strptime(listing_date, "%Y-%m-%d")
        if listing_dt > from_dt:
            from_dt = listing_dt
    except Exception:
        pass

    total_inserted = 0
    chunk_start    = from_dt

    while chunk_start < to_dt:
        chunk_end = min(chunk_start + timedelta(days=89), to_dt)   # ≤90 days per Dhan API call
        from_str  = chunk_start.strftime("%Y-%m-%d")
        to_str    = chunk_end.strftime("%Y-%m-%d")

        rows_1h = _fetch_1h_candles_range(security_id, from_str, to_str)
        if rows_1h:
            rows_1h_fmt = _aggregate_to_1h(rows_1h)
            rows_2h     = _aggregate_to_2h(rows_1h)
            rows_4h     = _aggregate_to_4h(rows_1h)
            with _DB_LOCK:
                with _conn() as con:
                    cur = con.cursor()
                    if rows_1h_fmt:
                        _insert_rows_1h(cur, security_id, rows_1h_fmt)
                    if rows_2h:
                        _insert_rows_2h(cur, security_id, rows_2h)
                    if rows_4h:
                        _insert_rows(cur, security_id, rows_4h)
            total_inserted += len(rows_4h)

        chunk_start = chunk_end + timedelta(days=1)

    return total_inserted


def _market_closed_today() -> bool:
    """
    Returns True if today is a trading day AND market has already closed
    (i.e. current IST time is past 15:30).
    When True, today's candles are fully complete and should be stored.
    """
    now = datetime.now()
    today = now.date()
    if not _is_trading_day(today):
        return False   # weekend / holiday — no candles today
    market_close_mins = 15 * 60 + 30   # 15:30 IST
    return (now.hour * 60 + now.minute) >= market_close_mins


def update_stock(security_id: str, symbol: str = "") -> int:
    """
    Incremental update: re-fetches the last 2 calendar days from Dhan API
    and UPSERTS (INSERT OR REPLACE) into SQLite.

    WHY LAST 2 DAYS (not just 'since last stored date'):
      On the day of seeding, the last 4H candle (13:15 bar) is often fetched
      mid-afternoon when the 15:15 candle is still partially formed.
      Dhan's API settles the final EOD close overnight. On the next morning,
      re-fetching those 2 days with REPLACE corrects the stale close value
      so our DB matches TradingView exactly.

    POST-MARKET BEHAVIOUR (run after 15:30 on a trading day):
      Today's candles are fully complete — they are included in the update.
      The 'strip today' filter is skipped so 28-Apr data is stored when
      main.py is launched at e.g. 9 PM after market close.

    Returns number of new 4H candles inserted (net new, after upsert).
    """
    label = symbol or security_id

    with _conn() as con:
        cur = con.cursor()
        _ensure_table(cur, security_id)
        row = cur.execute(
            f"SELECT MAX(date) FROM {_table(security_id)}"
        ).fetchone()
        last_date_str = row[0] if row and row[0] else None

    if last_date_str is None:
        print(f"  WARNING [{label}] DB empty — run seed_stock() first")
        return 0

    today     = datetime.now().date()
    today_str = today.strftime("%Y-%m-%d")

    # ── Determine the effective "last completed trading day" ──────────────────
    # After 15:30 on a trading day → today's candles are fully complete.
    # Before 15:30 (or on weekend/holiday) → use previous trading day.
    post_market = _market_closed_today()
    if post_market:
        last_biz = today                          # today IS the last completed day
        to_str   = today_str                      # include today in API range
    else:
        last_biz = _last_biz_day(today)           # last NSE trading day before today
        # Dhan quirk: set toDate = today (not last_biz) so Dhan treats last_biz
        # as fully historical and returns its data.
        to_str   = today_str

    # WHY last 2 biz days: The previous day's 15:15 candle may have been
    # captured partially mid-day. Re-fetching with REPLACE fixes stale closes.
    from_dt = last_biz - timedelta(days=2)
    while not _is_trading_day(from_dt):
        from_dt -= timedelta(days=1)
    from_str = from_dt.strftime("%Y-%m-%d")

    # ── Skip if DB is already up-to-date ─────────────────────────────────────
    last_date_only = last_date_str[:10] if last_date_str else None
    if last_date_only and last_date_only >= last_biz.strftime("%Y-%m-%d"):
        return 0

    rows_1h = _fetch_1h_candles_range(security_id, from_str, to_str)
    if not rows_1h:
        print(f"  WARNING [{label}] No data fetched ({from_str} to {to_str})")
        return 0

    rows_1h_fmt = _aggregate_to_1h(rows_1h)
    rows_2h     = _aggregate_to_2h(rows_1h)
    rows_4h     = _aggregate_to_4h(rows_1h)
    if not rows_4h:
        print(f"  WARNING [{label}] Aggregation returned 0 bars")
        return 0

    # ── Strip today's rows only if market is still OPEN (incomplete candles) ──
    # Post-market: keep today's rows (fully formed candles).
    # Pre-market / intraday: strip today to avoid storing partial bars.
    if not post_market:
        rows_1h_fmt = [r for r in rows_1h_fmt if not r["date"].startswith(today_str)]
        rows_2h     = [r for r in rows_2h     if not r["date"].startswith(today_str)]
        rows_4h     = [r for r in rows_4h     if not r["date"].startswith(today_str)]
    if not rows_4h:
        return 0

    # Count new candles = those strictly newer than last stored date
    new_rows = [r for r in rows_4h if r["date"] > last_date_str]

    with _DB_LOCK:
        with _conn() as con:
            cur = con.cursor()
            if rows_1h_fmt:
                _upsert_rows_1h(cur, security_id, rows_1h_fmt)
            if rows_2h:
                _upsert_rows_2h(cur, security_id, rows_2h)
            # UPSERT all fetched rows — fixes stale 15:15 close AND adds new dates
            _upsert_rows(cur, security_id, rows_4h)

    # Count new 1H / 2H bars too (for logging)
    new_1h = [r for r in rows_1h_fmt if r["date"] > last_date_str]
    new_2h = [r for r in rows_2h     if r["date"] > last_date_str]

    if new_rows:
        print(f"  [{label}] +{len(new_rows)} new 4H bar(s)  "
              f"(last: {last_date_str} -> {rows_4h[-1]['date']})  "
              f"[1H +{len(new_1h)}  2H +{len(new_2h)}]")
    else:
        print(f"  [{label}] DB refreshed (last: {last_date_str})  "
              f"[{len(rows_4h)} bars re-verified via REPLACE]")

    return len(new_rows)



def get_last_candle_date(security_id: str, before: str | None = None) -> "str | None":
    """
    Returns the last 4H candle date string for a security.
    If `before` is given (ISO string), returns last date strictly before it.
    O(1) — reads only one row. Used by incremental_updater to skip full load.
    """
    table = _table(security_id)
    try:
        with _conn() as con:
            if before:
                row = con.execute(
                    f"SELECT date FROM {table} WHERE date < ? ORDER BY date DESC LIMIT 1",
                    (before,)
                ).fetchone()
            else:
                row = con.execute(
                    f"SELECT date FROM {table} ORDER BY date DESC LIMIT 1"
                ).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def get_candles_since(security_id: str, since: str) -> "pd.DataFrame | None":
    """
    Returns 4H candles with date STRICTLY AFTER `since` (ISO string), ascending.
    Much cheaper than get_candles() when only new bars are needed.
    """
    import pandas as pd
    table = _table(security_id)
    try:
        with _conn() as con:
            df = pd.read_sql_query(
                f"SELECT date, open, high, low, close, volume "
                f"FROM {table} WHERE date > ? ORDER BY date ASC",
                con, params=(since,)
            )
    except Exception:
        return None
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    return df.reset_index(drop=True)


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


def get_candles_1h(security_id: str) -> "pd.DataFrame | None":
    """
    Returns full 1H OHLCV history for a security as a pandas DataFrame,
    sorted by date ascending. Returns None if the table is empty or missing.
    """
    import pandas as pd

    with _conn() as con:
        try:
            df = pd.read_sql_query(
                f"SELECT date, open, high, low, close, volume "
                f"FROM {_table_1h(security_id)} ORDER BY date ASC",
                con
            )
        except Exception:
            return None

    if df.empty:
        return None

    df["date"] = pd.to_datetime(df["date"])
    df = df.reset_index(drop=True)
    return df


def get_candles_2h(security_id: str) -> "pd.DataFrame | None":
    """
    Returns full 2H OHLCV history for a security as a pandas DataFrame,
    sorted by date ascending. Returns None if the table is empty or missing.
    """
    import pandas as pd

    with _conn() as con:
        try:
            df = pd.read_sql_query(
                f"SELECT date, open, high, low, close, volume "
                f"FROM {_table_2h(security_id)} ORDER BY date ASC",
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
    Returns True if ALL THREE tables (1H, 2H, 4H) have been seeded with recent data.

    WHY check all three:
      Old stocks may have 4H data but no 1H/2H tables (seeded before multi-TF support).
      Returning False for them forces seed_stock() to re-run and fill missing tables.
      INSERT OR IGNORE in seed_stock() means 4H rows won't be duplicated.

    Logic: ALL tables have ≥1 row AND 4H max(date) is within the last 10 calendar days.
    """
    with _conn() as con:
        cur = con.cursor()
        try:
            # ── Check 1H table has data ───────────────────────────────────────
            row_1h = cur.execute(
                f"SELECT COUNT(*) FROM {_table_1h(security_id)}"
            ).fetchone()
            if not row_1h or row_1h[0] == 0:
                return False

            # ── Check 2H table has data ───────────────────────────────────────
            row_2h = cur.execute(
                f"SELECT COUNT(*) FROM {_table_2h(security_id)}"
            ).fetchone()
            if not row_2h or row_2h[0] == 0:
                return False

            # ── Check 4H table has recent data ────────────────────────────────
            row_4h = cur.execute(
                f"SELECT COUNT(*), MAX(date) FROM {_table(security_id)}"
            ).fetchone()
            count    = row_4h[0] if row_4h else 0
            max_date = row_4h[1] if row_4h else None

            if count == 0 or max_date is None:
                return False

            try:
                last_dt = datetime.strptime(max_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                last_dt = datetime.strptime(max_date, "%Y-%m-%d")

            cutoff = datetime.now() - timedelta(days=10)
            return last_dt >= cutoff

        except Exception:
            return False   # Any table doesn't exist yet


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
    print(f"\n{'═'*64}")
    print(f"  📦 DB INCREMENTAL UPDATE  —  {len(stocks)} stocks  (4H candles)")
    print(f"{'═'*64}\n")

    today        = datetime.now().date()
    today_str    = today.strftime("%Y-%m-%d")
    last_biz     = _last_biz_day(today)
    yesterday    = last_biz.strftime("%Y-%m-%d")

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
        print(f"  ⚠️  {len(unseeded)} stock(s) not seeded yet — run  python tools/seed_db.py  first")
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

    # Effective target date: post-market → today, otherwise → yesterday
    post_market = _market_closed_today()
    target_date = today_str if post_market else yesterday

    consensus_date_only = consensus_date[:10]   # "YYYY-MM-DD"

    if consensus_date_only < target_date and seeded:
        try:
            next_day_str = (
                datetime.strptime(consensus_date_only, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")

            # Post-market: probe up to today; otherwise only up to yesterday
            test_1h = _fetch_1h_candles_range(seeded[0]["security_id"], next_day_str, target_date)
            test_4h = _aggregate_to_4h(test_1h)
            # Strip incomplete rows only if NOT post-market
            if not post_market:
                test_4h = [r for r in test_4h if not r["date"].startswith(today_str)]
            truly_new = [r for r in test_4h if r["date"] > consensus_date]

            if truly_new:
                new_date = truly_new[-1]["date"]
                print(f"  🆕 New 4H candle detected : {new_date} — updating all stocks!")
                needs_new_day = seeded   # update everyone
        except Exception:
            pass

    needs_api = list({s["security_id"]: s for s in (needs_behind + needs_new_day)}.values())

    if needs_api:
        print(f"  🔄 Updating {len(needs_api)} stock(s)...  (4 parallel workers)")
        print()
        from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

        done_n  = [0]
        total_n = len(needs_api)
        _lock   = threading.Lock()

        def _do_update(s):
            try:
                update_stock(s["security_id"], s["symbol"])
            except Exception as _e:
                print(f"  ⚠️  [{s['symbol']}] update error: {_e}", flush=True)
            with _lock:
                done_n[0] += 1
                n = done_n[0]
            if n % 25 == 0 or n == total_n:
                pct = n * 100 // total_n
                print(f"  ⏳ 4H update: {n}/{total_n} ({pct}%)", flush=True)

        with ThreadPoolExecutor(max_workers=4) as ex:
            futures = [ex.submit(_do_update, s) for s in needs_api]
            for fut in _as_completed(futures):
                try:
                    fut.result()
                except Exception:
                    pass
    else:
        already_ok = len(seeded) - len(needs_behind)
        print(f"  ✅ All {already_ok} seeded stocks up-to-date for {target_date} — 0 API calls needed!")

    print(f"\n  ✅ DB update complete.  ({len(seeded)} checked, {len(unseeded)} skipped)\n")
    return len(unseeded)


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
    rows_1h   = _fetch_1h_candles_range(security_id, today_str, today_str)
    if not rows_1h:
        return None

    # Filter to ONLY current session's 1H candles
    session_rows = [
        r for r in rows_1h
        if _get_session_key(r["_dt"].hour, r["_dt"].minute) == session_key
    ]

    if not session_rows:
        return None

    return {
        "open"   : session_rows[0]["open"],
        "high"   : max(r["high"]   for r in session_rows),
        "low"    : min(r["low"]    for r in session_rows),
        "close"  : session_rows[-1]["close"],
        "volume" : sum(r["volume"] for r in session_rows),
    }


# Backward-compatible alias for any code still calling get_today_ohlcv()
get_today_ohlcv = get_current_4h_ohlcv


def get_first_15min_hi_lo(security_id: str) -> dict | None:
    """
    Fetch the first-15-minute High and Low of today's trading session
    via Dhan 1-minute intraday candles (09:15 → 09:29 inclusive).

    Returns:
        {"high": float, "low": float}  after 09:30 when window is complete.
        None  — before 09:30 (window still forming) or on API error.

    WHY 1-min interval:
        We need exact 09:15–09:29 data. Dhan's /v2/charts/intraday
        supports interval=1. Three 5-min candles would also work but
        1-min is more precise for the exact 15-min boundary.
    """
    now = datetime.now()
    # Only meaningful after the first 15-min window closes (09:30)
    if now.hour * 60 + now.minute < 9 * 60 + 30:
        return None

    today_str = now.strftime("%Y-%m-%d")
    payload = {
        "securityId"      : str(security_id),
        "exchangeSegment" : "NSE_EQ",
        "instrument"      : "EQUITY",
        "interval"        : "1",                              # 1-minute candles
        "oi"              : False,
        "fromDate"        : f"{today_str} 09:00:00",
        "toDate"          : f"{today_str} 09:35:00",
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

        highs, lows = [], []
        for i, ts in enumerate(data["timestamp"]):
            dt = datetime.fromtimestamp(ts)
            # Keep only 09:15:00 → 09:29:59
            if dt.hour == 9 and 15 <= dt.minute < 30:
                highs.append(float(data["high"][i]))
                lows.append(float(data["low"][i]))

        if not highs:
            return None

        return {"high": round(max(highs), 2), "low": round(min(lows), 2)}

    except Exception:
        return None


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
                "https://api.dhan.co/v2/charts/intraday",
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


# ── Incremental indicator State ──────────────────────────────────────────────

def save_indicator_state(security_id: str, state: dict, last_date_str: str) -> None:
    """
    Persist the incremental indicator state (16 values) to SQLite.
    Called after each precompute_state() to avoid full recompute on next run.

    Schema: indicator_state (security_id PK, last_date TEXT, state_json TEXT)
    """
    import json
    with _DB_LOCK:
        with _conn() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS indicator_state (
                    security_id  TEXT PRIMARY KEY,
                    last_date    TEXT,
                    state_json   TEXT
                )
            """)
            con.execute(
                "INSERT OR REPLACE INTO indicator_state (security_id, last_date, state_json) "
                "VALUES (?, ?, ?)",
                (str(security_id), str(last_date_str), json.dumps(state))
            )


def load_indicator_state(security_id: str) -> "tuple[dict, str] | None":
    """
    Load persisted indicator state from SQLite.
    Returns (state_dict, last_date_str) or None if not found.
    """
    import json
    try:
        with _conn() as con:
            row = con.execute(
                "SELECT state_json, last_date FROM indicator_state WHERE security_id = ?",
                (str(security_id),)
            ).fetchone()
        if row is None:
            return None
        state = json.loads(row[0])
        return state, row[1]
    except Exception:
        return None

