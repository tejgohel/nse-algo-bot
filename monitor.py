# ─────────────────────────────────────────────────────────────────────────────
#  monitor.py  —  nse-algo-bot WebSocket Live Monitor (indicator-based)
#
#  On each live tick:
#  1. increment_state()  → advance your indicator state by this tick, then read
#     the optional reverse-exit levels out of it (O(1), no DataFrame recompute)
#  2. SL check  (initial SL = prev 4H candle LOW for buy, HIGH for sell)
#  3. Trailing SL — fires ONCE: once price moves TRAIL_STEP_PCT% in favour,
#     SL is moved to breakeven (entry price) and never moves again.
#  4. No fixed target — hold until 15:15 (MARKET_EXIT_TIME) force-exit
# ─────────────────────────────────────────────────────────────────────────────

import csv
import os
import struct
import json
import threading
import time
import websocket
from datetime import datetime

import config
import broker
import telegram_notify
import copy_trading
from indicators import increment_state

# Module-level list — all trades closed during this session (used by main.py for daily summary)
_closed_trades_today: list[dict] = []


# ─── TRADE LOG ────────────────────────────────────────────────────────────────
#  One CSV per trading day, all stored in the logs/ subfolder:
#    logs/trades_2026-04-06.csv
#    logs/trades_2026-04-07.csv  ← next day auto-creates new file
#    ...

_LOGS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
_log_lock = threading.Lock()


def _get_log_file() -> str:
    """Return today's CSV path, creating the logs/ folder if needed."""
    os.makedirs(_LOGS_DIR, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    return os.path.join(_LOGS_DIR, f"trades_{date_str}.csv")


# Convenience alias used in main.py for the end-of-session print
LOG_FILE = _get_log_file()


def _write_trade_log(symbol: str, entry_price: float, exit_price: float,
                     pnl: float, indicator_state: dict = None, entry_time: str = None):
    """Append one completed trade row to today's date-stamped CSV.
    entry_time: timestamp when the trade was entered (HH:MM:SS)
    """
    now      = datetime.now()
    log_path = _get_log_file()          # always today's file
    is_new   = not os.path.exists(log_path)
    row = {
        "Date"        : now.strftime("%Y-%m-%d"),
        "Entry Time"  : entry_time or "",
        "Exit Time"   : now.strftime("%H:%M:%S"),
        "Stock"       : symbol,
        "Entry Price" : round(entry_price, 2),
        "Exit Price"  : round(exit_price,  2),
        "PnL"         : round(pnl,          2),
    }
    with _log_lock:
        with open(log_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(row.keys()))
            if is_new:
                writer.writeheader()
            writer.writerow(row)
    print(
        f"  📝 Trade logged  → {symbol}  "
        f"Entry: ₹{entry_price:.2f}  Exit: ₹{exit_price:.2f}  "
        f"PnL: ₹{pnl:+.2f}  [{log_path}]"
    )


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


REQUEST_TICKER  = 15
RESP_TICKER     = 2
RESP_DISCONNECT = 50
NSE_EQ_CODE     = "NSE_EQ"


class IndicatorMonitor:
    """
    WebSocket-based monitor for indicator positions.

    Each position dict must contain:
        symbol, security_id, signal, entry_price,
        quantity, sl_price, indicator_state, today_ohlcv

    SL Logic:
        Initial SL  = prev 4H candle LOW  (BUY) / prev 4H candle HIGH (SELL)
        Trailing SL = fires ONCE: once LTP moves TRAIL_STEP_PCT% from entry,
                      SL is moved to entry + BREAKEVEN_BUFFER_PCT
                      (small cushion to cover brokerage/slippage).
                      ONCE TRAILED → LOCKED (never moves again).

    Capital-based exits:
        (Tiered early profit locks — TEMPORARILY DISABLED, search "TIER" below)
        PROFIT_LOCK_PCT (10%) of DEPLOYED_CAPITAL → main lock SL at that profit level
            e.g. capital=₹50K → PnL≥₹5,500 → SL shifted to lock ₹5K minimum profit
        TP_PCT (20%) of DEPLOYED_CAPITAL → hard close immediately
            e.g. capital=₹50K → PnL≥₹10K → exit NOW, do not wait for time exit

    Exits on: Hard TP | SL hit (initial/early-locked/profit-locked) | indicator reverse | Time exit
    """

    def __init__(self, positions: list[dict], on_all_closed=None):
        self.positions      = positions
        self.on_all_closed  = on_all_closed
        self._closed_trades: list[dict] = []   # accumulates closed position info
        self._ws           = None
        self._running      = True
        self._lock         = threading.Lock()
        self._done_event   = threading.Event()
        self._exited       = set()

    def start(self):
        threading.Thread(target=self._run_ws, daemon=True).start()
        if not config.PAPER_TRADING:
            threading.Thread(target=self._manual_exit_poller, daemon=True).start()

    def stop(self):
        self._running = False
        if self._ws:
            self._ws.close()
        self._done_event.set()

    def wait(self):
        self._done_event.wait()

    def update_position_entry(self, security_id: str, actual_fill_price: float):
        """Called after order fill to lock the actual entry price & recalc SL."""
        with self._lock:
            for p in self.positions:
                if p["security_id"] == security_id:
                    p["entry_price"]      = actual_fill_price
                    p["trail_high_water"] = actual_fill_price   # reset trail anchor
                    p["trail_low_water"]  = actual_fill_price   # reset trail anchor (SHORT)
                    p["fill_confirmed"]   = True                # unlock P&L-based triggers
                    # Use prev 4H candle low/high (already stored in position dict)
                    # so SL stays at the market-structure level, not re-derived from fill
                    sl = broker.get_initial_sl(
                        p["signal"],
                        actual_fill_price,
                        p["quantity"],
                        prev_candle_low  = p.get("prev_4h_low"),
                        prev_candle_high = p.get("prev_4h_high"),
                    )
                    p["sl_price"] = sl
                    sl_gap = round(abs(actual_fill_price - sl), 2)
                    sl_label = f"Prev 4H {'Low' if p['signal'] == 'LONG' else 'High'}"
                    print(f"  🔄 [{p['symbol']}] Fill confirmed @ ₹{actual_fill_price}")
                    print(f"     Initial SL : {sl_label} → ₹{sl}  (₹{sl_gap}/share × {p['quantity']} = ₹{sl_gap * p['quantity']:,.0f} risk)")
                    print(f"     Target     : time exit  (trailing SL active)")
                    break

    def force_exit_all(self, reason: str = "15:15 TIME EXIT"):
        with self._lock:
            for p in list(self.positions):
                sid = p["security_id"]
                if sid not in self._exited:
                    self._exited.add(sid)
                    ltp    = p.get("last_ltp", p["entry_price"])
                    entry  = p["entry_price"]
                    qty    = p["quantity"]
                    signal = p["signal"]

                    # Recompute PnL from actual exit LTP (not stale cached tick)
                    if signal == "LONG":
                        pnl = round((ltp - entry) * qty, 2)
                    else:
                        pnl = round((entry - ltp) * qty, 2)

                    print(f"\n  ⏰ [{_now()}] [{p['symbol']}] {reason}  |  Est. PnL ≈ ₹{pnl:+.2f}")
                    broker.place_exit_order(
                        p["security_id"], p["signal"], p["quantity"], reason
                    )
                    copy_trading.replicate_exit(p["security_id"], reason)
                    p["status"] = "CLOSED"
                    _write_trade_log(
                        symbol      = p["symbol"],
                        entry_price = entry,
                        exit_price  = ltp,
                        pnl         = pnl,
                        indicator_state    = p.get("indicator_state", {}),
                        entry_time  = p.get("entry_time", ""),
                    )
                    # ── Telegram: Time-exit notification with full PnL details ────
                    telegram_notify.notify_time_exit(
                        symbol      = p["symbol"],
                        signal      = signal,
                        entry_price = entry,
                        exit_price  = ltp,
                        qty         = qty,
                        pnl         = pnl,
                        entry_time  = p.get("entry_time", ""),
                        exit_time   = _now(),
                        reason      = reason,
                    )
                    # Add to BOTH instance list AND module-level accumulator
                    # so daily summary includes time exits alongside SL/TP exits
                    closed_snapshot               = dict(p)
                    closed_snapshot["gross_pnl"]  = pnl
                    closed_snapshot["exit_reason"] = reason
                    self._closed_trades.append(closed_snapshot)
                    _closed_trades_today.append(closed_snapshot)   # ← daily summary
        self.stop()

    def _run_ws(self):
        retry_delay = 3
        while self._running and self.positions:
            self._ws = websocket.WebSocketApp(
                config.WS_URL,
                on_open    = self._on_open,
                on_message = self._on_message,
                on_error   = self._on_error,
                on_close   = self._on_close,
            )
            try:
                self._ws.run_forever(ping_interval=0)  # Dhan doesn't respond to WS pings
            except Exception as e:
                print(f"  ⚠️  WS exception: {e}")

            if self._running and self.positions:
                print(f"  🔄 [{_now()}] WS dropped — reconnecting in {retry_delay}s...")
                time.sleep(retry_delay)

        self._done_event.set()

    def _on_open(self, ws):
        print(f"  📡 [{_now()}] WebSocket connected — subscribing to live ticks...")
        with self._lock:
            instruments = [
                {"ExchangeSegment": NSE_EQ_CODE, "SecurityId": p["security_id"]}
                for p in self.positions
            ]
        msg = {
            "RequestCode"     : REQUEST_TICKER,
            "InstrumentCount" : len(instruments),
            "InstrumentList"  : instruments,
        }
        ws.send(json.dumps(msg))
        print(f"  ✅ Subscribed to {len(instruments)} security(ies)\n")

    def _on_message(self, ws, message):
        if not isinstance(message, (bytes, bytearray)) or len(message) < 8:
            return
        feed_code   = message[0]
        security_id = str(struct.unpack_from("<i", message, 4)[0])
        if feed_code == RESP_TICKER:
            ltp = struct.unpack_from("<f", message, 8)[0]
            self._on_tick(security_id, round(float(ltp), 2))
        elif feed_code == RESP_DISCONNECT:
            code = struct.unpack_from("<h", message, 8)[0] if len(message) >= 10 else -1
            print(f"  ⚠️  [{_now()}] WS server disconnect code: {code}")

    def _on_error(self, ws, error):
        print(f"  ⚠️  [{_now()}] WS error: {error}")

    def _on_close(self, ws, code, msg):
        print(f"  📴 [{_now()}] WS closed (code={code})")

    def _on_tick(self, security_id: str, ltp: float):
        with self._lock:
            position = None
            for p in self.positions:
                if p["security_id"] == security_id:
                    position = p
                    break

            if position is None or security_id in self._exited:
                return

            position["last_ltp"] = ltp
            entry  = position["entry_price"]
            qty    = position["quantity"]
            signal = position["signal"]
            symbol = position["symbol"]

            if signal == "LONG":
                position["pnl"] = round((ltp - entry) * qty, 2)
            else:
                position["pnl"] = round((entry - ltp) * qty, 2)

            # ── 1. Trailing SL update ─────────────────────────────────────────
            self._update_trail(position, ltp)

            sl_price = position["sl_price"]
            pnl      = position["pnl"]

            # ── Update today's running OHLCV + advance the indicator state ──────
            _ohlcv = position["today_ohlcv"]
            _ohlcv["high"]  = max(_ohlcv["high"],  ltp)
            _ohlcv["low"]   = min(_ohlcv["low"],   ltp)
            _ohlcv["close"] = ltp
            if not position.get("indicator_state"):
                # No indicator state for this position — a strategy that does
                # not build one is fine. NaN leaves every price-based exit below
                # intact (stop, trailing stop, profit lock, hard target and the
                # 15:15 force exit) and skips only the two reverse-exit checks.
                _exit_long_below = _exit_short_above = float("nan")
            else:
                try:
                    _ns = increment_state(position["indicator_state"], {
                        "high"  : _ohlcv["high"],
                        "low"   : _ohlcv["low"],
                        "close" : ltp,
                        "open"  : _ohlcv["open"],
                    })
                    # OPTIONAL contract. Put these two keys in the dict your
                    # extract_state() / increment_state() return and a long is
                    # closed when price loses `exit_long_below`, a short when it
                    # takes out `exit_short_above` — a trailing-indicator exit
                    # without this file knowing what the indicator is. Leave them
                    # out and only the price-based exits apply. Read with .get()
                    # precisely so an unknown state shape cannot KeyError here.
                    _exit_long_below  = float(_ns.get("exit_long_below", "nan"))
                    _exit_short_above = float(_ns.get("exit_short_above", "nan"))
                except Exception as _st_err:
                    # indicator values unavailable — SL/TP checks still run below,
                    # only the reverse exit is disabled for this tick.
                    print(f"  ⚠️  [{symbol}] indicator state update failed: {_st_err}"
                          f" — reverse exit disabled this tick")
                    _exit_long_below  = float("nan")
                    _exit_short_above = float("nan")

            # ── Fill not confirmed yet — skip all P&L-based triggers ──────────
            # SL and reverse-exit checks below still run (price-based, not P&L)
            if not position.get("fill_confirmed"):
                if signal == "LONG" and ltp <= sl_price:
                    self._trigger_exit(position, ltp, f"🛑 SL HIT ₹{sl_price}")
                elif signal == "SHORT" and ltp >= sl_price:
                    self._trigger_exit(position, ltp, f"🛑 SL HIT ₹{sl_price}")
                return

            # ─── Derived thresholds from capital ─────────────────────────────
            capital            = config.DEPLOYED_CAPITAL
            lock_sl_thresh     = round(capital * config.PROFIT_LOCK_PCT               / 100, 2)  # ₹5,000
            lock_trig_thresh   = round(capital * config.PROFIT_LOCK_TRIGGER_PCT       / 100, 2)  # ₹5,500
            tp_thresh          = round(capital * config.TP_PCT                        / 100, 2)  # ₹10,000

            # ── 2. Hard TP — 20% capital profit ──────────────────────────────
            if pnl >= tp_thresh:
                self._trigger_exit(
                    position, ltp,
                    f"🎯 HARD TP HIT  (PnL ₹{pnl:+.2f} >= ₹{tp_thresh:.0f} target)"
                )
                return

            # ── 3/4/5. Tiered early profit locks — TEMPORARILY DISABLED ─────
            # To re-enable: uncomment EARLY_/TIER2_/TIER3_PROFIT_LOCK_* in
            # config.py, restore their threshold lines above, then uncomment
            # the block below.
            #
            # if (pnl >= early_trig_thresh
            #         and not position.get("early_lock_active")
            #         and not position.get("profit_lock_active")):
            #     _sl = self._calc_lock_sl(position, early_lock_thresh)
            #     if (signal == "LONG" and _sl > position["sl_price"]) or \
            #        (signal == "SHORT" and _sl < position["sl_price"]):
            #         position["sl_price"]          = _sl
            #         position["early_lock_active"] = True
            #         print(f"\n  🔐 [{_now()}] [{symbol}] TIER 1 LOCK (5%→1%)"
            #               f"  PnL:₹{pnl:+.2f}  →  SL:₹{_sl}  (protects ≥ ₹{early_lock_thresh:.0f})")
            #
            # if (pnl >= t2_trig_thresh
            #         and not position.get("t2_lock_active")
            #         and not position.get("profit_lock_active")):
            #     _sl = self._calc_lock_sl(position, t2_lock_thresh)
            #     if (signal == "LONG" and _sl > position["sl_price"]) or \
            #        (signal == "SHORT" and _sl < position["sl_price"]):
            #         position["sl_price"]       = _sl
            #         position["t2_lock_active"] = True
            #         print(f"\n  🔐 [{_now()}] [{symbol}] TIER 2 LOCK (6%→3%)"
            #               f"  PnL:₹{pnl:+.2f}  →  SL:₹{_sl}  (protects ≥ ₹{t2_lock_thresh:.0f})")
            #
            # if (pnl >= t3_trig_thresh
            #         and not position.get("t3_lock_active")
            #         and not position.get("profit_lock_active")):
            #     _sl = self._calc_lock_sl(position, t3_lock_thresh)
            #     if (signal == "LONG" and _sl > position["sl_price"]) or \
            #        (signal == "SHORT" and _sl < position["sl_price"]):
            #         position["sl_price"]       = _sl
            #         position["t3_lock_active"] = True
            #         print(f"\n  🔐 [{_now()}] [{symbol}] TIER 3 LOCK (8%→4%)"
            #               f"  PnL:₹{pnl:+.2f}  →  SL:₹{_sl}  (protects ≥ ₹{t3_lock_thresh:.0f})")

            # ── 6. Main Profit Lock — 11% trigger → 10% lock (hard freeze) ──
            if pnl >= lock_trig_thresh and not position.get("profit_lock_active"):
                _sl = self._calc_lock_sl(position, lock_sl_thresh)
                if (signal == "LONG" and _sl > position["sl_price"]) or \
                   (signal == "SHORT" and _sl < position["sl_price"]):
                    position["sl_price"]           = _sl
                    position["profit_lock_active"] = True
                    position["trail_active"]        = True   # freeze all further movement
                    print(f"\n  🔒 [{_now()}] [{symbol}] MAIN LOCK (11%→10%)"
                          f"  PnL:₹{pnl:+.2f}  →  SL:₹{_sl}  (protects ≥ ₹{lock_sl_thresh:.0f})")

            # Refresh — main lock above may have just moved the SL this tick
            sl_price = position["sl_price"]

            # ── 7. SL check ───────────────────────────────────────────────────
            if signal == "LONG" and ltp <= sl_price:
                self._trigger_exit(position, ltp, "🛑 SL HIT")
                return
            if signal == "SHORT" and ltp >= sl_price:
                self._trigger_exit(position, ltp, "🛑 SL HIT")
                return

            # ── 8. reverse exit, if the state supplied a level ────────────────
            if signal == "LONG" and ltp < _exit_long_below:
                reason = "🔁 REVERSE EXIT (price lost the level your state set)"
                print(f"\n  {reason}  [{symbol}]  LTP: ₹{ltp}")
                self._trigger_exit(position, ltp, reason)
                return
            if signal == "SHORT" and ltp > _exit_short_above:
                reason = "🔁 REVERSE EXIT (price took out the level your state set)"
                print(f"\n  {reason}  [{symbol}]  LTP: ₹{ltp}")
                self._trigger_exit(position, ltp, reason)
                return

            # ── 9. Print live status ──────────────────────────────────────────
            paper_tag  = " [PAPER]" if config.PAPER_TRADING else ""
            pnl_str    = f"₹{pnl:+.2f}"
            sl_dist    = round(abs(ltp - sl_price), 2)
            if signal == "SHORT":
                sup_str = f"  exit>₹{round(_exit_short_above, 2)}" if _exit_short_above == _exit_short_above else ""
            else:
                sup_str = f"  exit<₹{round(_exit_long_below, 2)}" if _exit_long_below == _exit_long_below else ""
            if position.get("profit_lock_active"):
                sl_tag = f"  🔒 MainLock SL: ₹{sl_price}"
            elif position.get("trail_active"):
                sl_tag = f"  Trail SL: ₹{sl_price}"
            else:
                sl_tag = f"  Initial SL: ₹{sl_price}"
            tp_left = round(tp_thresh - pnl, 2)
            print(
                f"  📊{paper_tag} [{_now()}] {symbol:<12} LTP: ₹{ltp:<8.2f} "
                f"PnL: {pnl_str:<10} "
                f"{sl_tag} ({sl_dist} away)"
                f"  TP in: ₹{tp_left}"
                f"{sup_str}"
            )

    def _manual_exit_poller(self):
        """
        Polls Dhan /v2/positions every 15s to detect manual exits.
        If net qty = 0 for an open position, it was manually closed —
        mark it exited, log it, and trigger the on_all_closed callback
        so main.py rescans (same stock skipped via traded_symbols).
        Skipped in PAPER mode (no real positions to check).

        Only counts a qty=0 read against a position once that position has
        been seen genuinely open on the broker at least once (see
        `_ever_confirmed_open` below) — otherwise a slow-filling entry order
        on an illiquid scrip looks identical to a manual close and the bot
        would abandon a position before it even existed on Dhan's side.
        """
        POLL_INTERVAL = 15   # seconds between checks
        while self._running:
            time.sleep(POLL_INTERVAL)
            if not self._running:
                break

            with self._lock:
                active = [
                    p for p in list(self.positions)
                    if p["security_id"] not in self._exited
                ]

            for p in active:
                qty = broker.get_net_position_qty(p["security_id"])
                if qty is None:
                    continue   # paper mode or API error — skip

                # Position still open on Dhan
                if (p["signal"] == "LONG"  and qty > 0) or \
                   (p["signal"] == "SHORT" and qty < 0):
                    p["_ever_confirmed_open"] = True   # broker has genuinely seen this live
                    p["_zero_qty_strikes"] = 0          # reset — confirmed still open
                    continue

                # qty == 0 (or wrong sign). Two distinct cases look identical here:
                #   (a) a real manual close
                #   (b) the entry order simply hasn't filled yet (slow/illiquid scrip —
                #       e.g. a MARKET order that takes >30s to execute), so Dhan's
                #       /v2/positions never showed a position to begin with.
                # We can only tell these apart once we've actually seen the position
                # open on the broker at least once. Until then, qty=0 is the EXPECTED
                # state, not evidence of a manual exit — so don't strike it.
                if not p.get("_ever_confirmed_open"):
                    continue

                # Confirmed-open position now reading 0 — could be a real manual
                # close, or a stale/glitched read. Require 2 consecutive misses
                # 15s apart before acting, so one bad read can't make the bot
                # abandon a position that is actually still live.
                p["_zero_qty_strikes"] = p.get("_zero_qty_strikes", 0) + 1
                if p["_zero_qty_strikes"] < 2:
                    continue

                # confirmed manually closed
                ltp    = p.get("last_ltp", p["entry_price"])
                entry  = p["entry_price"]
                q      = p["quantity"]
                signal = p["signal"]
                pnl    = round((ltp - entry) * q if signal == "LONG"
                               else (entry - ltp) * q, 2)
                p["pnl"] = pnl

                p["manual_exit"] = True   # skip place_exit_order in _do_exit
                print(f"\n  ✂️  [{_now()}] [{p['symbol']}] MANUAL EXIT DETECTED"
                      f"  |  Est. PnL ≈ ₹{pnl:+.2f}")
                telegram_notify.notify_manual_exit_detected(p["symbol"], signal, pnl)
                self._trigger_exit(p, ltp, "✂️ MANUAL EXIT")

    def _calc_lock_sl(self, position: dict, profit_target: float) -> float:
        entry  = position["entry_price"]
        qty    = position["quantity"]
        signal = position["signal"]
        if qty <= 0:
            return position["sl_price"]
        if signal == "SHORT":
            return round(entry - profit_target / qty, 2)
        return round(entry + profit_target / qty, 2)

    def _update_trail(self, position: dict, ltp: float):
        """
        Trailing SL — fires ONCE: once LTP moves TRAIL_STEP_PCT% in favour,
        SL is moved to entry + BREAKEVEN_BUFFER_PCT (small cushion above
        entry to cover brokerage/slippage) and never moves again until the
        main profit lock takes over.
        """
        # Already trailed (or profit-locked) — do nothing
        if position.get("trail_active") or position.get("profit_lock_active"):
            return

        entry        = position["entry_price"]
        signal       = position["signal"]
        step_price   = entry * config.TRAIL_STEP_PCT / 100.0
        buffer_price = entry * config.BREAKEVEN_BUFFER_PCT / 100.0

        if signal == "LONG":
            new_sl = round(entry + buffer_price, 2)
            if ltp >= entry + step_price and new_sl > position["sl_price"]:
                position["sl_price"]     = new_sl
                position["trail_active"] = True
                print(
                    f"  📈 [{_now()}] [{position['symbol']}] "
                    f"Trail SL → Breakeven+ ₹{new_sl}  (LTP: ₹{ltp})"
                )

        else:  # SHORT
            new_sl = round(entry - buffer_price, 2)
            if ltp <= entry - step_price and new_sl < position["sl_price"]:
                position["sl_price"]     = new_sl
                position["trail_active"] = True
                print(
                    f"  📉 [{_now()}] [{position['symbol']}] "
                    f"Trail SL → Breakeven+ ₹{new_sl}  (LTP: ₹{ltp})"
                )

    def _trigger_exit(self, position: dict, ltp: float, reason: str):
        sid = position["security_id"]
        self._exited.add(sid)
        pnl = position["pnl"]
        print(f"\n  {reason}  [{position['symbol']}]  LTP: ₹{ltp}  |  Est. PnL ≈ ₹{pnl:+.2f}")
        threading.Thread(
            target=self._do_exit,
            args=(position, reason),
            daemon=True
        ).start()

    def _do_exit(self, position: dict, reason: str):
        # Guard: if another _do_exit thread already claimed this position (race between
        # _on_tick and _manual_exit_poller), skip to avoid duplicate log + double callback.
        with self._lock:
            if position.get("_exit_in_progress"):
                return
            position["_exit_in_progress"] = True

        ltp_at_exit = position.get("last_ltp", position["entry_price"])

        # Place exit order and get order_id for fill confirmation
        exit_order_id = None
        if not position.get("manual_exit"):
            exit_order_id = broker.place_exit_order(
                position["security_id"],
                position["signal"],
                position["quantity"],
                reason
            )

        # ── Copy Trading: followers' positions are real even if the master's
        # was closed manually — always mirror the exit on every follower.
        copy_trading.replicate_exit(position["security_id"], reason)

        # Fetch actual exit fill price (same logic as entry fill)
        if exit_order_id and exit_order_id != "PAPER_EXIT":
            actual_exit = broker.get_order_fill_price(exit_order_id, ltp_fallback=ltp_at_exit)
        else:
            actual_exit = ltp_at_exit   # paper trading or manual exit

        exit_price = actual_exit if actual_exit else ltp_at_exit

        # Recalculate final P&L using actual fill prices
        entry  = position["entry_price"]
        qty    = position["quantity"]
        signal = position["signal"]
        if signal == "LONG":
            pnl = round((exit_price - entry) * qty, 2)
        else:
            pnl = round((entry - exit_price) * qty, 2)

        print(f"  💰 [{position['symbol']}] Exit fill @ ₹{exit_price}  |  Final PnL: ₹{pnl:+.2f}")

        all_closed = False
        with self._lock:
            _write_trade_log(
                symbol      = position["symbol"],
                entry_price = entry,
                exit_price  = exit_price,
                pnl         = pnl,
                indicator_state    = position.get("indicator_state", {}),
                entry_time  = position.get("entry_time", ""),
            )

            closed_snapshot = dict(position)
            closed_snapshot["gross_pnl"]    = pnl
            closed_snapshot["exit_price"]   = exit_price
            closed_snapshot["exit_reason"]  = reason
            self._closed_trades.append(closed_snapshot)
            _closed_trades_today.append(closed_snapshot)   # module-level accumulator

            if position in self.positions:
                self.positions.remove(position)
            if not self.positions:
                all_closed = True
                self._running = False
                if self._ws:
                    self._ws.close()

        # ── Outside lock: Telegram (5 s HTTP — must not hold _lock) ──────────
        telegram_notify.notify_exit(
            symbol      = position["symbol"],
            signal      = signal,
            entry_price = entry,
            exit_price  = exit_price,
            qty         = qty,
            pnl         = pnl,
            reason      = reason,
        )

        # ── on_all_closed must finish (balance update) BEFORE main thread ─────
        # wakes from monitor.wait() — so set _done_event only after callback.
        if all_closed:
            print(f"\n  ✅ [{_now()}] All positions closed.\n")
            try:
                if self.on_all_closed:
                    self.on_all_closed(self._closed_trades)
            finally:
                self._done_event.set()   # always unblock main thread
