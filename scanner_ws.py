# ─────────────────────────────────────────────────────────────────────────────
#  scanner_ws.py  —  nse-algo-bot Real-Time WebSocket Signal Scanner
#
#  ARCHITECTURE:
#    1. precompute_all()  — loads DB + builds indicator state for all stocks
#                          (parallel, ~20s with 4 workers)
#    2. scan()           — subscribes all stocks via WebSocket (Ticker mode)
#                          On each LTP tick:
#                           a. Track current 4H session's running O/H/L per stock
#                           b. Reset O/H/L at 13:15 (new TradingView 4H session)
#                           c. Debounce: max 1 signal check/sec per stock
#                           d. check_signal_on_tick() — fast, no DB/API call
#                           e. Signal found → stop WS → return signal dict
#
#  4H SESSION TRACKING:
#    Session 1: 09:15–13:15  → today_ohlc tracks Session 1 OHLC
#    Session 2: 13:15–15:30  → today_ohlc RESETS at 13:15, tracks Session 2 OHLC
#    This ensures the live 4H bar passed to strategy exactly matches TradingView.
#
#  Dhan WS limits:
#    • 5000 instruments per connection
#    • Max 100 instruments per subscribe JSON message (we split into chunks of 100)
# ─────────────────────────────────────────────────────────────────────────────

import struct
import json
import threading
import time
import websocket
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import config
import strategy
import db   # for get_current_session_start_str()


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


# Dhan WS protocol constants
REQUEST_SUBSCRIBE = 15   # subscribe to market feed
REQUEST_UNSUB     = 12   # disconnect / unsubscribe
RESP_TICKER       = 2    # ticker packet: LTP + LTT
RESP_PREV_CLOSE   = 6    # prev close packet (ignored here)
RESP_DISCONNECT   = 50   # server-side disconnect
NSE_EQ_CODE       = "NSE_EQ"

# Max stocks per subscribe message (Dhan docs: 100)
CHUNK_SIZE = 100

# Debounce: only recheck a stock's signal if it hasn't been checked in N seconds
# Prevents CPU overload when many ticks arrive simultaneously
DEBOUNCE_SEC = 0.5

# Debug: print tick counter every N seconds
DEBUG_TICK_INTERVAL = 30   # seconds

# Reconnect delay (seconds) after a WS drop
RECONNECT_DELAY = 3

# Keepalive: send a Dhan heartbeat every N seconds to prevent 60s server timeout
KEEPALIVE_INTERVAL = 25


class ScannerWS:
    """
    Real-time WebSocket-based indicator signal scanner.

    Usage:
        scanner = ScannerWS(stocks)
        scanner.precompute_all(workers=4)   # ~20 seconds
        result = scanner.scan()             # blocks until signal or MAX_ENTRY_TIME
        # result = {"symbol", "security_id", "signal", "ltp", "indicator_state"}
        # or None if no signal before MAX_ENTRY_TIME
    """

    def __init__(self, stocks: list[dict]):
        self.stocks         = stocks
        self._id_to_stock   = {s["security_id"]: s for s in stocks}
        # leverage lookup: security_id → True (EQ) / False (T2T)
        self._leverage_map  = {s["security_id"]: s.get("intraday_leverage", True) for s in stocks}
        self._cache         = {}       # security_id → precompute_state() result
        self._today_ohlc    = {}       # security_id → {open, high, low, close} for CURRENT 4H session
        self._last_checked  = {}       # security_id → timestamp (debounce)
        self._ws            = None
        self._stop_event    = threading.Event()
        self._signal_result = None     # set when signal found
        self._lock          = threading.Lock()
        self._precomputed   = False
        # Symbols already traded today — ticks from these are silently ignored
        self.skip_symbols:  set[str] = set()
        # Debug counters
        self._tick_count     = 0
        self._text_msg_count = 0
        self._last_debug_ts  = time.time()
        self._first_msg_done = False
        self._total_checks   = 0        # total signal evaluations
        self._stocks_seen    = set()    # security_ids that sent at least 1 tick
        self._last_symbol    = ""       # last stock ticked (for status line)
        # 4H session tracking — detect session boundary (13:15) and reset ohlc
        # _current_session: "YYYY-MM-DD HH:MM:SS" or None (market closed)
        self._current_session = db.get_current_session_start_str()
        # First-15-min H/L is stored directly in self._cache[sid]["first_15min_high/low"]
        # (None = not yet collected, float = collected & ready)

    # ── Phase 1: parallel precompute ─────────────────────────────────────────

    def precompute_all(self, workers: int = 4) -> int:
        """
        Load DB + compute indicator for all stocks in parallel.
        Caches a tail of each stock's pre-computed state for fast per-tick checks.

        Returns: number of stocks successfully cached.
        """
        print(f"\n  ⚡ Pre-computing indicator state for {len(self.stocks)} stocks "
              f"({workers} workers)...")
        print(f"     ⏳ (This runs once — subsequent ticks will be instant)\n")

        t0      = time.time()
        cached  = 0
        failed  = 0
        unwritten = 0          # failed because strategy.py is still a scaffold
        total   = len(self.stocks)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {
                ex.submit(strategy.precompute_state, s["security_id"], s.get("symbol", "")): s
                for s in self.stocks
            }
            done = 0
            for fut in as_completed(futures):
                s = futures[fut]
                done += 1
                try:
                    state = fut.result()
                    if state:
                        with self._lock:
                            self._cache[s["security_id"]] = state
                        cached += 1
                    else:
                        failed += 1
                except NotImplementedError:
                    # Expected on a fresh clone: strategy.py ships as a
                    # scaffold. Counted apart from real failures so the summary
                    # can say WHY nothing cached instead of just "failed: 25".
                    unwritten += 1
                    failed += 1
                except Exception:
                    failed += 1

                if done % 50 == 0 or done == total:
                    elapsed = round(time.time() - t0, 1)
                    pct = int(done / total * 100)
                    bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                    # An unwritten scaffold is not a failure, and calling it one
                    # sends people hunting for a bug that is not there.
                    tail = f"❌ failed: {failed - unwritten}"
                    if unwritten:
                        tail = f"⏸ awaiting rule: {unwritten}  " + tail
                    print(f"  📊 [{elapsed}s] [{bar}] {done}/{total} ({pct}%) "
                          f"— ✅ cached: {cached}  {tail}")

        elapsed = round(time.time() - t0, 1)
        summary = f"{cached} ✅ cached, {failed - unwritten} ⚠️ skipped"
        if unwritten:
            summary += f", {unwritten} ⏸ awaiting rule"
        print(f"\n  ✅ Pre-compute done in {elapsed}s  ({summary})\n")

        if unwritten:
            print(f"  ℹ️  {unwritten} of those raised NotImplementedError — "
                  f"indicators.py and strategy.py")
            print(f"     are still the empty scaffolds this repository ships. "
                  f"Write them and")
            print(f"     these stocks will cache normally. Nothing below this "
                  f"can produce a signal\n     until then.\n")

        self._precomputed = True
        return cached

    # ── Phase 2: WebSocket scan ───────────────────────────────────────────────

    def scan(self) -> dict | None:
        """
        Subscribe all stocks via WebSocket and watch for indicator signals.
        Blocks until:
          • A LONG or SHORT signal is detected  → returns signal dict
          • MAX_ENTRY_TIME is reached           → returns None

        Signal dict: {"symbol", "security_id", "signal", "ltp", "indicator_state"}
          signal = "LONG" or "SHORT"
        """
        if not self._precomputed:
            raise RuntimeError("Call precompute_all() before scan()")

        # ── Reset per-scan state so this object can be reused across rounds ───
        self._signal_result  = None
        self._stop_event     = threading.Event()
        self._first_msg_done = False
        self._text_msg_count = 0
        self._tick_count     = 0
        self._total_checks   = 0
        self._last_debug_ts  = time.time()
        # Refresh session — important if scan() is called near 13:15
        self._current_session = db.get_current_session_start_str()

        max_time = datetime.strptime(config.MAX_ENTRY_TIME, "%H:%M").replace(
            year=datetime.now().year,
            month=datetime.now().month,
            day=datetime.now().day,
        )

        active_count = len(self._cache) - len(self.skip_symbols)
        print(f"{'='*64}")
        print(f"  📡 REAL-TIME SCANNER  [{_now()}]")
        print(f"  👁️  Watching {active_count} stocks  "
              f"({len(self.skip_symbols)} ✔️ skipped — already traded today)")
        print(f"  ⏰ Auto-stop at {config.MAX_ENTRY_TIME}")
        print(f"  🗓️  4H Session: {self._current_session or 'Market closed'}")
        if True:
            #  Started more than 5 min into a session? Then this session's open
            #  is already history and the first tick we see is not it.
            _sess_start = (datetime.strptime(self._current_session, "%Y-%m-%d %H:%M:%S")
                           if self._current_session else None)
            if _sess_start and datetime.now() - _sess_start > timedelta(minutes=5):
                _known = sum(1 for c in self._cache.values() if c.get("day_open") is not None)
                print(f"  ⚠️  Session already under way — this session's OPEN is taken "
                      f"from the first tick seen, not from the exchange open.")
                print(f"     {_known}/{len(self._cache)} stocks got their day-open from "
                      f"history; the rest are approximated.")
        print(f"{'='*64}\n")

        # ── Phase 0: First-15-min H/L data acquisition ───────────────────────
        # Path A (before 09:30): WS connects early, ticks accumulate f15 into cached dict.
        # Path B (after  09:30): Started late — fetch from Dhan 1-min API in bg.
        F15_DONE = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
        now_dt   = datetime.now()

        if not config.TRACK_OPENING_RANGE:
            # ── Filter OFF: no first-15-min collection; scan straight from 09:15 ─
            print(f"  ⛔ Opening-range tracking off (config.TRACK_OPENING_RANGE=False)")
            print(f"  🚀 [{_now()}] Signal scanning from 09:15 — skipping first-15-min collection.\n")
        elif now_dt < F15_DONE:
            # ── Path A: collect from live WS ticks ────────────────────────────
            print(f"  ⏳ First-15-min collection phase (09:15 → 09:30)")
            print(f"  🔌 Connecting WS early to collect H/L from live ticks...")
            self._ws_done = threading.Event()
            self._connect()

            while datetime.now() < F15_DONE:
                remaining_secs = int((F15_DONE - datetime.now()).total_seconds())
                mins, secs = divmod(max(remaining_secs, 0), 60)
                with self._lock:
                    f15_count = sum(1 for c in self._cache.values() if c.get("first_15min_high") is not None)
                print(f"  ⌛ [{_now()}] Collecting first-15-min data... "
                      f"{mins:02d}:{secs:02d} left | "
                      f"{f15_count} stocks tracked so far",
                      end="\r", flush=True)
                time.sleep(1)

            with self._lock:
                f15_count = sum(1 for c in self._cache.values() if c.get("first_15min_high") is not None)
            print(f"\n  ✅ [{_now()}] First-15-min window closed! "
                  f"{f15_count} stocks tracked.")
            print(f"  🚀 Signal scanning begins NOW!\n")

        else:
            # ── Path B: started after 09:30 (e.g. 10:16 test run or Session 2) ─
            # If cached already has f15 for all stocks (e.g. Session 2 rescan after
            # Session 1 collected it via WS ticks), skip the API fetch entirely.
            # Only fetch for stocks that are genuinely missing.
            total_stocks = len(self._cache)
            already_ready = sum(
                1 for c in self._cache.values()
                if c.get("first_15min_high") is not None
            )
            need_fetch = total_stocks - already_ready

            if need_fetch == 0:
                print(f"  ✅ [{_now()}] f15 already collected for all {total_stocks} stocks"
                      f" — skipping API fetch.")
            else:
                print(f"  ⚡ [{_now()}] Started after 09:30 — "
                      f"fetching first-15-min H/L from API "
                      f"({need_fetch} stocks missing, {already_ready} already ready)")
                print(f"     4 background workers | Cond 9 & 10 activate progressively")
            print(f"  🚀 Signal scanning starts NOW!\n")

            def _fetch_worker(sids_chunk):
                for sid in sids_chunk:
                    if self._stop_event.is_set():
                        return
                    with self._lock:
                        cached_item = self._cache.get(sid)
                        if cached_item is None or cached_item.get("first_15min_high") is not None:
                            continue   # not in cache or already collected
                    data = db.get_first_15min_hi_lo(sid)
                    if data:
                        with self._lock:
                            cached_item = self._cache.get(sid)
                            if cached_item is not None:
                                cached_item["first_15min_high"] = data["high"]
                                cached_item["first_15min_low"]  = data["low"]

            def _prefetch_all():
                sids    = list(self._cache.keys())
                n       = len(sids)
                chunks  = [sids[i::4] for i in range(4)]
                threads = [threading.Thread(target=_fetch_worker,
                                            args=(c,), daemon=True)
                           for c in chunks]
                for t in threads:
                    t.start()
                for t in threads:
                    t.join()
                if not self._stop_event.is_set():
                    with self._lock:
                        done = sum(1 for c in self._cache.values() if c.get("first_15min_high") is not None)
                    print(f"\n  ✅ [{_now()}] f15 pre-fetch complete: "
                          f"{done}/{n} stocks loaded — Cond 9 & 10 fully active!\n")

            threading.Thread(target=_prefetch_all, daemon=True).start()



        # ── Main loop: connect → watch → auto-reconnect on drop ───────────────
        while True:
            # Time expired?
            if datetime.now() >= max_time:
                print(f"\n  ⏰ [{_now()}] ⏹️ MAX_ENTRY_TIME ({config.MAX_ENTRY_TIME})"
                      f" — stopping scanner.")
                self.stop()
                break

            # Signal already found?
            if self._signal_result is not None:
                self._stop_event.set()
                break

            # Connect (or reconnect) WebSocket in a background thread
            # (if WS already connected from Phase 0 and still alive, this is a no-op
            # because _ws_done won't be set yet)
            if not hasattr(self, '_ws_done') or self._ws_done.is_set():
                self._ws_done = threading.Event()
                self._connect()

            # Wait until WS closes OR signal found OR time exceeded
            while not self._ws_done.is_set():
                if datetime.now() >= max_time:
                    self.stop()
                    break
                if self._signal_result is not None:
                    self.stop()
                    break
                time.sleep(0.5)

            # If we have a result or were explicitly stopped, exit
            if self._signal_result is not None or self._stop_event.is_set():
                break

            # WS dropped without a signal — reconnect after short delay
            print(f"  ⚠️ [{_now()}] WS dropped — 🔄 reconnecting in {RECONNECT_DELAY}s...")
            time.sleep(RECONNECT_DELAY)

        return self._signal_result

    def stop(self):
        self._stop_event.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass

    # ── WebSocket internals ───────────────────────────────────────────────────

    def _connect(self):
        ws = websocket.WebSocketApp(
            config.WS_URL,
            on_open    = self._on_open,
            on_message = self._on_message,
            on_error   = self._on_error,
            on_close   = self._on_close,
        )
        self._ws = ws
        t = threading.Thread(
            target=ws.run_forever,
            # ping_interval=0 disables websocket-client's built-in ping.
            # Dhan's server does NOT respond to standard WS ping frames,
            # which causes "ping/pong timed out" and drops the connection.
            kwargs={"ping_interval": 0},
            daemon=True
        )
        t.start()

        # Start Dhan-specific keepalive thread
        ka = threading.Thread(target=self._keepalive_loop, args=(ws,), daemon=True)
        ka.start()

    def _keepalive_loop(self, ws):
        """
        Sends a Dhan-compatible heartbeat every KEEPALIVE_INTERVAL seconds.
        Dhan WS server closes the connection after ~60s of silence.
        NOTE: Do NOT check ws.sock.connected — it's unreliable on VPS/Windows.
        Just try to send and catch exception if WS is already closed.
        """
        import time as _time
        heartbeat = json.dumps({
            "RequestCode"    : REQUEST_SUBSCRIBE,
            "InstrumentCount": 0,
            "InstrumentList" : [],
        })
        while not self._stop_event.is_set():
            _time.sleep(KEEPALIVE_INTERVAL)
            if self._stop_event.is_set():
                break
            try:
                ws.send(heartbeat)
            except Exception:
                break  # WS already closed — outer loop will reconnect

    def _on_open(self, ws):
        print(f"  📶 [{_now()}] Scanner WS connected — subscribing stocks...")

        sids   = list(self._cache.keys())
        chunks = [sids[i:i+CHUNK_SIZE] for i in range(0, len(sids), CHUNK_SIZE)]

        for chunk in chunks:
            msg = {
                "RequestCode"    : REQUEST_SUBSCRIBE,
                "InstrumentCount": len(chunk),
                "InstrumentList" : [
                    {"ExchangeSegment": NSE_EQ_CODE, "SecurityId": str(sid)}
                    for sid in chunk
                ],
            }
            ws.send(json.dumps(msg))

        print(f"  ✅ Subscribed {len(sids)} stocks "
              f"({len(chunks)} messages of ≤{CHUNK_SIZE})\n")
        print(f"  👀 Watching for indicator LONG or SHORT signal in real-time...")
        print(f"  ⚡ Any signal will be reported instantly!")
        print(f"  🔍 DEBUG: Waiting for first WS message...\n")

    def _on_message(self, ws, message):
        # ── DEBUG: show first message and periodic stats ──────────────────────
        if not self._first_msg_done:
            self._first_msg_done = True
            if isinstance(message, (bytes, bytearray)):
                print(f"  DEBUG [{_now()}] First msg = BINARY, len={len(message)}, "
                      f"bytes[0:4]={list(message[:4])}")
            else:
                print(f"  DEBUG [{_now()}] First msg = TEXT: {str(message)[:200]}")

        now = time.time()
        if now - self._last_debug_ts >= DEBUG_TICK_INTERVAL:
            self._last_debug_ts = now
            pct_seen = round(len(self._stocks_seen) / max(len(self._cache), 1) * 100, 1)
            print(f"  📶 [{_now()}] 🟢 Scanner alive — "
                  f"{self._tick_count} ticks | "
                  f"{len(self._stocks_seen)}/{len(self._cache)} stocks seen ({pct_seen}%) | "
                  f"{self._total_checks} checks | "
                  f"last: {self._last_symbol}")
            self._tick_count = 0

        # ── TEXT messages (Dhan may send JSON ack/error as text) ─────────────
        if not isinstance(message, (bytes, bytearray)):
            self._text_msg_count += 1
            if self._text_msg_count <= 5:
                print(f"  DEBUG [{_now()}] TEXT msg #{self._text_msg_count}: "
                      f"{str(message)[:300]}")
            return

        if len(message) < 8:
            return

        feed_code   = message[0]
        security_id = str(struct.unpack_from("<i", message, 4)[0])

        if feed_code == RESP_TICKER and len(message) >= 13:
            ltp = round(float(struct.unpack_from("<f", message, 8)[0]), 2)
            self._tick_count += 1
            self._on_ltp(security_id, ltp)
        elif feed_code == RESP_DISCONNECT:
            code = struct.unpack_from("<h", message, 8)[0] if len(message) >= 10 else -1
            print(f"  [{_now()}] Scanner WS disconnect (code={code})")
        else:
            if self._text_msg_count <= 3:
                self._text_msg_count += 1
                print(f"  DEBUG [{_now()}] Unknown feed_code={feed_code}, "
                      f"len={len(message)}, bytes={list(message[:8])}")

    def _on_error(self, ws, error):
        print(f"  [{_now()}] Scanner WS error: {error}")

    def _on_close(self, ws, code, msg):
        print(f"  [{_now()}] Scanner WS closed (code={code})")
        # Signal the inner wait-loop that this WS session ended.
        # Do NOT set _stop_event here — that would kill the scanner permanently.
        # The outer scan() loop will decide whether to reconnect or stop.
        if hasattr(self, '_ws_done'):
            self._ws_done.set()

    def _on_ltp(self, security_id: str, ltp: float):
        """
        Called on every LTP tick.
        Updates the current 4H session's running O/H/L and checks for signal.
        """
        # Already found a signal — ignore all further ticks
        if self._stop_event.is_set():
            return

        # ── Skip stocks already traded today ──────────────────────────────────
        stock_info = self._id_to_stock.get(security_id, {})
        sym_check  = stock_info.get("symbol", "")
        if sym_check in self.skip_symbols:
            return

        # Track stock activity
        self._stocks_seen.add(security_id)

        # ── 4H SESSION BOUNDARY CHECK ──────────────────────────────────────────
        # At 13:15, TradingView starts a NEW 4H candle. Before clearing today_ohlc
        # we MUST inject the completed Session 1 candle into each stock's df_tail.
        # Without this, condition 8 (prev candle close>open) would check yesterday's
        # last candle instead of today's Session 1 — a silent but critical bug.
        new_session = db.get_current_session_start_str()
        if new_session and new_session != self._current_session:
            with self._lock:
                if new_session != self._current_session:   # double-check inside lock
                    old_session = self._current_session
                    print(f"\n  🔄 [{_now()}] 4H session boundary: "
                          f"{old_session} → {new_session}")

                    if old_session is None:
                        # Market just opened (None → 09:15) — no completed candle to inject.
                        # Just start tracking Session 1 from scratch.
                        #
                        # Anything already in today_ohlc came from a pre-open
                        # tick (Dhan streams the 09:00-09:08 auction), and its
                        # "open" is NOT the 09:15 open. Drop it — a rule that
                        # keys off the session open would be reading an auction
                        # price for the rest of the day.
                        print(f"  ℹ️  Market opened — Session 1 tracking starts now.")
                        if self._today_ohlc:
                            print(f"  🧹 Discarding {len(self._today_ohlc)} pre-open O/H/L "
                                  f"— opens will come from the 09:15 ticks.")
                            self._today_ohlc.clear()
                        for cached_state in self._cache.values():
                            cached_state["day_open"] = None
                            cached_state["session_open"] = None
                    else:
                        # True session rollover (09:15 → 13:15): inject completed S1 candle
                        # into df_tail AND advance indicator_state so S2 incremental calc starts
                        # from S1 (not from yesterday's S2 — would silently skip S1's ATR/EMA).
                        from indicators import increment_state as _inc
                        import pandas as _pd
                        s1_ts    = _pd.Timestamp(old_session)
                        appended = 0
                        for sid, ohlc in self._today_ohlc.items():
                            cached_state = self._cache.get(sid)
                            if cached_state is None:
                                continue
                            cached_state["session_open"] = None   # S2 opens on its first tick
                            s1_row = _pd.DataFrame([{
                                "date"  : s1_ts,
                                "open"  : ohlc["open"],
                                "high"  : ohlc["high"],
                                "low"   : ohlc["low"],
                                "close" : ohlc["close"],
                                "volume": 0,
                            }])
                            cached_state["df_tail"] = _pd.concat(
                                [cached_state["df_tail"], s1_row], ignore_index=True
                            )
                            if "indicator_state" in cached_state:
                                cached_state["indicator_state"] = _inc(
                                    cached_state["indicator_state"],
                                    {"open": ohlc["open"], "high": ohlc["high"],
                                     "low": ohlc["low"],   "close": ohlc["close"]},
                                )
                            appended += 1
                        print(f"  Session 1 candle committed — indicator state "
                              f"advanced for {appended} stocks")
                        print(f"  🧹 Resetting O/H/L — Session 2 starts.")
                        self._today_ohlc.clear()

                    self._current_session = new_session
                    # Refresh cached session str so check_signal_on_tick uses new date
                    for sid, cached_state in self._cache.items():
                        cached_state["live_session_str"] = new_session

        # ── Debounce: max 1 signal check per stock per DEBOUNCE_SEC ───────────
        now = time.time()
        if now - self._last_checked.get(security_id, 0) < DEBOUNCE_SEC:
            # Still update the running high/low even while debounced
            with self._lock:
                ohlc = self._today_ohlc.get(security_id)
                if ohlc:
                    ohlc["high"]  = max(ohlc["high"], ltp)
                    ohlc["low"]   = min(ohlc["low"],  ltp)
                    ohlc["close"] = ltp
            return

        self._last_checked[security_id] = now

        # ── Update current 4H session's running O/H/L ─────────────────────────
        # This dict represents the live 4H candle building in real-time.
        # Resets at 13:15 session boundary (see above).
        with self._lock:
            if security_id not in self._today_ohlc:
                # First tick of this 4H session for this stock → set open
                self._today_ohlc[security_id] = {
                    "open" : ltp,
                    "high" : ltp,
                    "low"  : ltp,
                    "close": ltp,
                }
            else:
                ohlc = self._today_ohlc[security_id]
                ohlc["high"]  = max(ohlc["high"], ltp)
                ohlc["low"]   = min(ohlc["low"],  ltp)
                ohlc["close"] = ltp

            cached     = self._cache.get(security_id)
            today_ohlc = dict(self._today_ohlc.get(security_id, {}))

            # ── Period opens, captured live ──────────────────────────────────
            #  A rule that compares price against the day's or the session's
            #  OPEN needs that open, and the broker does not send it: the first
            #  tick a stock prints in a period IS its open for that period. So
            #  it is captured here, once, and left in the cache for strategy.py.
            if cached is not None and today_ohlc:
                first_px = today_ohlc["open"]
                if cached.get("session_open") is None:
                    cached["session_open"] = first_px
                if cached.get("day_open") is None:
                    cached["day_open"] = first_px

        if not cached:
            return  # Stock not pre-computed (insufficient history)

        # ── Time gate: before 09:30 only collect first-15-min H/L, skip signals ─
        # Filter ON  → collect first-15-min H/L (09:15–09:30) and start signals @09:30.
        # Filter OFF → no collection; signals are evaluated from 09:15 onwards.
        now_obj  = datetime.now()
        now_mins = now_obj.hour * 60 + now_obj.minute
        F15_START, F15_END = 9 * 60 + 15, 9 * 60 + 30   # 555, 570

        if config.TRACK_OPENING_RANGE:
            # Accumulate first-15-min H/L directly in cached dict (09:15–09:30 only)
            if F15_START <= now_mins < F15_END and cached is not None:
                with self._lock:
                    cur_h = cached.get("first_15min_high")
                    if cur_h is None:
                        cached["first_15min_high"] = ltp
                        cached["first_15min_low"]  = ltp
                    else:
                        if ltp > cur_h:
                            cached["first_15min_high"] = ltp
                        if ltp < cached["first_15min_low"]:
                            cached["first_15min_low"]  = ltp

            # Before 09:30 — skip signal evaluation entirely
            if now_mins < F15_END:
                return

        # ── Signal check (after 09:30) ───────────────────────────────────────
        try:
            stock  = self._id_to_stock.get(security_id, {})
            symbol = stock.get("symbol", security_id)
            self._last_symbol   = symbol
            self._total_checks += 1

            signal, state = strategy.check_signal_on_tick(
                cached, ltp, today_ohlc,
                first_15min_high=cached.get("first_15min_high"),
                first_15min_low=cached.get("first_15min_low"),
            )
        except Exception as e:
            print(f"  ❌ [{_now()}] check_signal_on_tick ERROR for {security_id}: {e}")
            return

        if signal in ("LONG", "SHORT"):
            stock  = self._id_to_stock.get(security_id, {})
            symbol = stock.get("symbol", security_id)

            is_long = (signal == "LONG")
            sig_icon = "🟢" if is_long else "🔴"
            sl_label = f"Prev 4H {'Low' if is_long else 'High'}"
            sl_price = state.get("prev_4h_low") if is_long else state.get("prev_4h_high")

            print(f"\n{'='*64}")
            print(f"  🔔 REAL-TIME SIGNAL DETECTED!  [{_now()}]")
            print(f"  {sig_icon} {signal}  {symbol:<14}  LTP: {ltp:.2f}")
            print(f"{'─'*64}")

            #  Whatever strategy.check_signal_on_tick() put in `state` is what
            #  gets shown — so a rule stays explainable without editing this file.
            for _k, _v in state.items():
                if _k in ("prev_4h_low", "prev_4h_high"):
                    continue
                print(f"  {_k:<22} {_v}")
            print(f"  SL ({sl_label}): {sl_price}")
            print(f"{'='*64}\n")

            with self._lock:
                if self._signal_result is None:   # only take first signal
                    self._signal_result = {
                        "symbol"            : symbol,
                        "security_id"       : security_id,
                        "signal"            : signal,
                        "ltp"               : ltp,
                        "indicator_state"          : state,
                        "raw_indicator_state"      : dict(cached.get("indicator_state", {})),
                        "intraday_leverage" : self._leverage_map.get(security_id, True),
                    }
            self.stop()
