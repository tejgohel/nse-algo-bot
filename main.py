
# ─────────────────────────────────────────────────────────────────────────────
#  main.py  —  nse-algo-bot — Orchestrator
#
#  Run:  python main.py
#
#  FULL FLOW:
#    1. Script starts early (e.g. 8:12) — data collection runs IMMEDIATELY
#         • New stocks   → seed from listing date (one-time, fetches full history)
#         • Known stocks → fetch only candles since last stored date
#    2. Precompute indicator state for all stocks (parallel ~20s) — right after fetch
#    3. Wait until DATA_COLLECT_WAIT_TIME (09:11) — then hand off to scanner
#    4. Scan-Trade loop — scanner.scan() called before 09:30 so WS PATH A runs:
#         • WS connects at 09:11, collects exact 09:15-09:30 tick H/L per stock
#         • At 09:30 opening range frozen, signal scanning begins immediately
#    5. Scan-Trade loop repeats until MAX_ENTRY_TIME:
#         a. Build remaining list (exclude already-traded stocks)
#         b. ⚡ FIRST SIGNAL found → enter trade, start monitoring
#         c. When trade closes (SL / reverse exit / 15:15) → auto-rescan
#         d. If no signal found → done for the day
#         e. Past MAX_ENTRY_TIME → stop loop
#    6. All trade exits logged to trades_log.csv
#       (Date, Time, Stock, Entry Price, Exit Price, PnL)
# ─────────────────────────────────────────────────────────────────────────────

import os
import time
import threading
from datetime import datetime, date

import config
import login as _login
import broker


import watchlist
import db
import daily_db
import auto_login
import telegram_notify
import incremental_updater
import copy_trading
from scanner_ws import ScannerWS
from monitor import IndicatorMonitor
import frontend as _fe
import signal_store


active_positions: list[dict] = []
traded_symbols:  set[str]   = set()   # stocks already traded today (skip in rescan)
_rescan_event = threading.Event()      # fired when a trade closes → trigger rescan

           
# ─── HELPERS ──────────────────────────────────────────────────────────────────

def _now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _open_charts_side_by_side(symbol: str):
    """Open TradingView 4H chart in a new Chrome tab on entry."""
    import subprocess
    tv_url = f"https://www.tradingview.com/chart/?symbol=NSE:{symbol}&interval=240"
    try:
        subprocess.Popen(f'start chrome "{tv_url}"', shell=True)
        print(f"  🌐 [{symbol}] TradingView 4H chart opened in Chrome")
    except Exception as e:
        print(f"  ⚠️  [{symbol}] Could not open chart: {e}")

def _current_time() -> datetime:
    return datetime.now()

def _parse_time(t_str: str) -> datetime:
    t = datetime.strptime(t_str, "%H:%M")
    return _current_time().replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)


# ─── SCANNER-MODE DASHBOARD HELPERS ──────────────────────────────────────────

def _keep_dashboard_alive():
    """Keep the Flask dashboard serving until the user quits (Ctrl+C)."""
    print("  🖥️  Dashboard live — press Ctrl+C to exit.\n")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("  🛑 Exiting.\n")


def _maybe_show_stored_signals() -> bool:
    """
    SCANNER_MODE only. If we're POST-MARKET on a trading day, or on a
    non-trading day (weekend/holiday), show the LIVE signals that were stored
    for the relevant trading day on the dashboard — instead of scanning.

    Returns True if it handled the session (caller should return), else False
    to continue into the normal live scan flow.
    """
    from nse_holidays import is_trading_day, last_trading_day

    today       = date.today()
    post_market = is_trading_day(today) and _current_time() >= _parse_time(config.MAX_ENTRY_TIME)
    non_trading = not is_trading_day(today)

    if not (post_market or non_trading):
        return False   # live market hours → run the normal scan flow

    day = today if post_market else last_trading_day(today)

    if not signal_store.exists(day):
        msg = f"No live signals stored for {day} — run the scanner live to collect them."
        print(f"  ℹ️  {msg}")
        _fe.set_status(msg, scanning=False)
        _keep_dashboard_alive()
        return True

    sigs = signal_store.load(day)
    print(f"  🖥️  Scanner mode — showing {len(sigs)} stored LIVE signal(s) from {day}:")
    _fe.set_status(
        f"Live signals — {day.strftime('%d %b (%a)')} ({len(sigs)} signal(s))",
        scanning=False,
    )
    for entry in sigs:
        _fe.add_signal(entry)
        print(f"     {entry.get('time',''):<9} {entry.get('direction',''):<4} "
              f"{entry.get('symbol','')}  ₹{entry.get('ltp')}")
    _keep_dashboard_alive()
    return True


# ─── WAIT FOR START TIME ──────────────────────────────────────────────────────

def wait_for_start():
    target = _parse_time(config.ALGO_START_TIME)
    now    = _current_time()

    if now >= target:
        print(f"  ⏰ Already past {config.ALGO_START_TIME} — starting immediately.")
        return

    wait_secs = (target - now).total_seconds()
    print(f"\n  ⏳ Waiting until {config.ALGO_START_TIME}...")
    print(f"     Now     : {now.strftime('%H:%M:%S')}")
    print(f"     Waiting : {int(wait_secs // 60)}m {int(wait_secs % 60)}s\n")

    while _current_time() < target:
        time.sleep(5)

    print(f"  🟢 [{_now_str()}] Start time reached.\n")


# ─── WAIT FOR DATA-COLLECT DONE (post-DB-update wait until 09:11) ─────────────

def wait_for_data_collect_done():
    """
    After DB update finishes (which may complete around 8:45),
    wait until DATA_COLLECT_WAIT_TIME (09:11) before doing anything else.
    """
    target = _parse_time(config.DATA_COLLECT_WAIT_TIME)
    now    = _current_time()

    if now >= target:
        print(f"  ⏰ Already past {config.DATA_COLLECT_WAIT_TIME} — no need to wait.")
        return

    wait_secs = (target - now).total_seconds()
    print(f"\n  ⏳ DB update done. Now waiting until {config.DATA_COLLECT_WAIT_TIME} "
          f"before opening-range collection starts...")
    print(f"     Now     : {now.strftime('%H:%M:%S')}")
    print(f"     Waiting : {int(wait_secs // 60)}m {int(wait_secs % 60)}s\n")

    while _current_time() < target:
        time.sleep(5)

    print(f"  🟢 [{_now_str()}] Reached {config.DATA_COLLECT_WAIT_TIME} — "
          f"handing off to scanner (WS will collect 09:15-09:30 opening range).\n")


# ─── PHASE 2: ENTER POSITIONS ─────────────────────────────────────────────────

def run_entries(signals: list[dict], on_closed=None) -> IndicatorMonitor | None:
    """
    Enter positions for all signals.
    on_closed: optional callback(closed_trades: list) called when ALL positions exit.
    """
    if not signals:
        return None

    if _current_time() >= _parse_time(config.MAX_ENTRY_TIME):
        print(f"  ⚠️  Already past MAX_ENTRY_TIME ({config.MAX_ENTRY_TIME}) — no entries.")
        return None

    print(f"\n{'═'*64}")
    print(f"  🚀 ENTRY PHASE  [{_now_str()}]")
    print(f"  💼 Placing market order...")
    print(f"{'═'*64}\n")

    batch_positions: list[dict] = []

    for sig in signals:
        symbol      = sig["symbol"]
        security_id = sig["security_id"]
        signal      = sig["signal"]
        ltp         = sig["ltp"]
        indicator_state    = sig.get("indicator_state", {})
        raw_indicator_state = sig.get("raw_indicator_state") or {}

        if _current_time() >= _parse_time(config.MAX_ENTRY_TIME):
            print(f"  ⚠️  [{symbol}] MAX_ENTRY_TIME reached — no more entries.")
            break

        # ── Check actual intraday leverage from Dhan margincalculator API ───────
        print(f"  🔍 [{symbol}] Checking intraday leverage availability...")
        actual_lev = broker.get_intraday_leverage(security_id, ltp)
        print(f"  ⚡ [{symbol}] Dhan leverage: {actual_lev}x  (required: {config.MIN_LEVERAGE_REQUIRED}x)")

        if actual_lev < config.MIN_LEVERAGE_REQUIRED:
            print(f"  ⚠️  [{symbol}] Leverage {actual_lev}x < required {config.MIN_LEVERAGE_REQUIRED}x "
                  f"(ASM/GSM/T2T) — skipping trade.\n")
            continue

        qty           = broker.calculate_quantity(ltp, leverage=actual_lev)
        prev_4h_low   = indicator_state.get("prev_4h_low")
        prev_4h_high  = indicator_state.get("prev_4h_high")
        sl_price      = broker.get_initial_sl(signal, ltp, qty,
                                              prev_candle_low  = prev_4h_low,
                                              prev_candle_high = prev_4h_high)
        sl_gap   = round(abs(ltp - sl_price), 2)
        sl_pct   = round(abs(ltp - sl_price) / ltp * 100, 2)

        print(f"  📌 [{symbol}] Signal: {signal}  |  {actual_lev}x leverage")
        print(f"  💰 [{symbol}] LTP         : ₹{ltp}")
        print(f"  🔢 [{symbol}] Quantity    : {qty}  (₹{qty * ltp:,.0f} exposure)")
        sl_label = f"Prev 4H {'Low' if signal == 'LONG' else 'High'}: ₹{sl_price}"
        print(f"  🛑 [{symbol}] Initial SL  : {sl_label}  "
              f"({sl_pct:.2f}% away | ₹{sl_gap}/share × {qty} = ₹{sl_gap * qty:,.0f} risk)")
        print(f"  🎯 [{symbol}] Target      : TIME EXIT  "
              f"(trail SL: {config.TRAIL_STEP_PCT}% step, fires once then locks)")

        order_id = broker.place_market_order(security_id, signal, qty)
        if not order_id:
            print(f"  ❌ [{symbol}] Entry order failed.\n")
            continue

        # ── Copy Trading: mirror entry onto follower accounts ────────────────
        copy_trading.replicate_entry(security_id, signal, ltp, leverage=actual_lev)

        # ── Telegram: Entry notification — sent only after order is confirmed ──
        telegram_notify.notify_entry(
            symbol = symbol,
            signal = signal,
            qty    = qty,
            sl     = sl_price,
        )

        # ── Auto-open Dhan + TradingView 4H side-by-side on entry ───────────────
        _open_charts_side_by_side(symbol)

        _today_4h = db.get_current_4h_ohlcv(security_id) or {}
        _today_ohlcv = {
            "open"  : _today_4h.get("open",  ltp),
            "high"  : _today_4h.get("high",  ltp),
            "low"   : _today_4h.get("low",   ltp),
            "close" : ltp,
        }

        position = {
            "symbol"           : symbol,
            "security_id"      : security_id,
            "order_id"         : order_id,
            "signal"           : signal,
            "entry_price"      : ltp,
            "entry_time"       : datetime.now().strftime("%H:%M:%S"),
            "quantity"         : qty,
            "sl_price"         : sl_price,
            "prev_4h_low"      : indicator_state.get("prev_4h_low"),
            "prev_4h_high"     : indicator_state.get("prev_4h_high"),
            "trail_high_water" : ltp,
            "trail_low_water"  : ltp,
            "trail_active"     : False,
            "last_ltp"         : ltp,
            "pnl"              : 0.0,
            "fill_confirmed"   : False,
            "status"           : "FILLED",
            "indicator_state"         : raw_indicator_state,
            "today_ohlcv"      : _today_ohlcv,
        }
        batch_positions.append(position)
        active_positions.append(position)
        print()

    if not batch_positions:
        print("  ⚠️  No positions entered.\n")
        return None

    print(f"  ✅ {len(batch_positions)} position(s) entered.")

    monitor = IndicatorMonitor(batch_positions, on_all_closed=on_closed)

    # Confirm actual fill prices in background — monitor starts immediately
    # so no ticks/TP/SL are missed while waiting for fill confirmation
    def _confirm_fills():
        time.sleep(0.5)   # brief wait for order to settle on exchange
        for p in batch_positions:
            fill_price = broker.get_order_fill_price(p["order_id"], ltp_fallback=p["entry_price"])
            if fill_price:
                monitor.update_position_entry(p["security_id"], fill_price)
            else:
                print(f"  ⚠️  [{p['symbol']}] Fill not confirmed — keeping signal LTP ₹{p['entry_price']}")

    threading.Thread(target=_confirm_fills, daemon=True).start()

    return monitor


# ─── PHASE 3: MONITOR LIVE ────────────────────────────────────────────────────

def run_monitor(monitor: IndicatorMonitor):
    print(f"\n{'═'*64}")
    print(f"  👀 MONITOR PHASE  [{_now_str()}]")
    print(f"  📡 WebSocket live — indicator reverse + SL/TP exits")
    print(f"{'═'*64}\n")

    monitor.start()

    exit_time = _parse_time(config.MARKET_EXIT_TIME)

    while not monitor._done_event.is_set():
        if _current_time() >= exit_time:
            print(f"\n  ⏰ [{_now_str()}] {config.MARKET_EXIT_TIME} — ⚡ force-exiting all positions!")
            monitor.force_exit_all()
            break
        time.sleep(5)

    monitor.wait()
    print(f"\n  ✅ [{_now_str()}] Algo complete.\n")


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    # ── Scanner dashboard (SCANNER_MODE only) ────────────────────────────────
    #  Open the browser dashboard FIRST. If we're post-market / non-trading,
    #  just replay today's stored LIVE signals and keep the dashboard alive —
    #  no login / DB / scan needed.
    if config.SCANNER_MODE:
        _fe.start_server(port=5001)
        _fe.open_browser(port=5001)
        _fe.set_status("Starting up...", scanning=False)
        print("\n  🖥️  Scanner dashboard        →  http://127.0.0.1:5001")
        print(f"  🌐 From another PC (WiFi/LAN) →  http://{_fe.lan_ip()}:5001\n")

    # ── Step 0: Login — auto_login first, config.py as fallback ──────────────
    print("\n" + "═"*64)
    print("  🔐 AUTO LOGIN — Generating fresh Dhan access token...")

    token = auto_login.generate_token()

    if token:
        # Save fresh token to config.py on disk → next day's run picks it up automatically
        auto_login.save_token_to_config(token)
    else:
        # Auto-login failed → fall back to token already saved in config.py
        # Condition: fallback token must stay valid through 15:30 (market close)
        fallback = getattr(config, "ACCESS_TOKEN", "").strip()
        if fallback and auto_login.is_token_valid_until_market_close(fallback):
            print("  ⚠️  Auto-login failed — config.py token valid till 15:30, using it.")
            token = fallback
        elif fallback:
            print("  ❌ Auto-login failed, and the config.py token expires before 15:30.")
            print("  ❌ Not safe to trade on it — stopping.")
            telegram_notify.send(
                "❌ <b>ALGO STOPPED — Token Problem</b>\n"
                "Could not generate a fresh token.\n"
                "The one in config.py expires before 15:30.\n"
                "Re-run main.py, or check the network."
            )
            return
        else:
            print("  ❌ Auto-login failed and config.py has no token to fall back on.")
            telegram_notify.send("❌ <b>ALGO FAILED TO START</b>\nDhan auto-login failed and config.py token is empty.")
            return

    # ── Patch live token into config so WS_URL uses the fresh token ───────────
    config.ACCESS_TOKEN = token
    config.WS_URL = (
        "wss://api-feed.dhan.co"
        "?version=2"
        f"&token={token}"
        f"&clientId={config.CLIENT_ID}"
        "&authType=2"
    )
    _login.reload_dhan()   # rebuild dhanhq SDK client with fresh token
    print("  ✅ Token ready — WebSocket + dhanhq SDK ready.")
    print()

    # ── Copy Trading: log in follower accounts (no-op if disabled) ───────────
    copy_trading.login_followers()

    # ── Live mode: fetch real account balance from Dhan ─────────────────────────
    if not config.PAPER_TRADING:
        print("  💰 Fetching live account balance from Dhan...")
        fetched_balance = broker.fetch_account_balance()
        if fetched_balance and fetched_balance > 0:
            reserve = int(fetched_balance * config.CAPITAL_RESERVE_PCT / 100)
            config.DEPLOYED_CAPITAL = min(int(fetched_balance) - reserve, 10_000)  # TEMP: cap at ₹10k
            print(f"  ✅ Account balance   : ₹{int(fetched_balance):,}")
            print(f"  🔒 Reserve ({config.CAPITAL_RESERVE_PCT}% slippage): ₹{reserve:,}")
            print(f"  💰 Deployed capital  : ₹{config.DEPLOYED_CAPITAL:,} (capped at ₹10,000)")
            print(f"  ⚡ Buying power (5x) : ₹{config.DEPLOYED_CAPITAL * config.LEVERAGE:,}")
        else:
            print(f"  ⚠️  Could not fetch balance — using config default: ₹{config.DEPLOYED_CAPITAL:,}")
        print()

    print("\n" + "═"*64)
    mode_tag = "  📋 [PAPER TRADING — NO REAL ORDERS]" if config.PAPER_TRADING else "  🚀  LIVE TRADING MODE"
    print(f"  🤖 NSE ALGO BOT  —  live scan + execution")
    print(mode_tag)
    print("═"*64)
    print(f"  💵 Capital         : ₹{config.DEPLOYED_CAPITAL:,}")
    print(f"  ⚡ Leverage        : {config.LEVERAGE}x")
    print(f"  💼 Buying power    : ₹{config.DEPLOYED_CAPITAL * config.LEVERAGE:,}")
    print(f"  📐 Indicators      : EMA / ATR / RSI — whatever you write in indicators.py")
    print(f"     slot params     : #1({config.INDICATOR_1_LENGTH},{config.INDICATOR_1_MULT})  "
          f"#2({config.INDICATOR_2_LENGTH},{config.INDICATOR_2_MULT})  "
          f"#3({config.INDICATOR_3_LENGTH},{config.INDICATOR_3_MULT})")
    print(f"  🎯 Signal rule     : strategy.check_signal_on_tick()  "
          f"— edit strategy.py to plug in your own")
    print(f"  📋 Watchlist       : {len(config.WATCHLIST_SYMBOLS)} symbols")
    print(f"  🛑 Initial SL      : Previous candle Low (LONG) / High (SHORT)  [fallback: {config.INITIAL_SL_PCT}% capital]")
    print(f"  📉 Trailing SL     : {config.TRAIL_STEP_PCT}% step, fires ONCE then locks (never moves again)")
    lock_rs = config.DEPLOYED_CAPITAL * config.PROFIT_LOCK_PCT / 100
    tp_rs   = config.DEPLOYED_CAPITAL * config.TP_PCT          / 100
    print(f"  🔒 Profit Lock     : ₹{lock_rs:,.0f} PnL ({config.PROFIT_LOCK_PCT}% of capital) → SL locked at lock price")
    print(f"  🎯 Hard TP         : ₹{tp_rs:,.0f} PnL ({config.TP_PCT}% of capital) → trade closed immediately")
    print(f"  🕐 Entry window    : {config.ALGO_START_TIME} → {config.MAX_ENTRY_TIME}")
    print(f"  🚪 Force exit at   : {config.MARKET_EXIT_TIME}")
    print(f"  🗄️  DB              : {config.DB_PATH}")
    print("═"*64 + "\n")

    # ── Telegram: Algo started notification ───────────────────────────────────
    mode_label = "📋 PAPER" if config.PAPER_TRADING else "🚀 LIVE"
    lock_rs    = config.DEPLOYED_CAPITAL * config.PROFIT_LOCK_PCT / 100
    tp_rs      = config.DEPLOYED_CAPITAL * config.TP_PCT          / 100
    telegram_notify.send(
        f"<b>🤖 ALGO STARTED — nse-algo-bot [{mode_label}]</b>\n"
        f"Capital   : ₹{config.DEPLOYED_CAPITAL:,}   Leverage: {config.LEVERAGE}x\n"
        f"Window    : {config.ALGO_START_TIME} → {config.MAX_ENTRY_TIME}\n"
        f"Force Exit: {config.MARKET_EXIT_TIME}\n"
        f"Profit Lock: ₹{lock_rs:,.0f}   Hard TP: ₹{tp_rs:,.0f}"
    )

    # ─ PHASE 1: Load watchlist + collect historical data IMMEDIATELY ───────────
    # (Script starts at e.g. 8:12 — data collection runs right away)
    stocks = watchlist.load_watchlist()
    if not stocks:
        print("  ℹ️  No stocks in watchlist.\n")
        return

    # Seed / update SQLite 4H DB with yesterday's + recent candles
    db.update_all(stocks)

    # Update Daily DB with EOD candles
    print("\n" + "─"*64)
    print("  DAILY DB UPDATE  (candles_daily.db)")
    print("─"*64)
    try:
        daily_db.update_all_daily(stocks)
    except Exception as _e:
        print(f"  [ERROR] daily_db.update_all_daily crashed: {_e}")
        import traceback; traceback.print_exc()

    # ─ PHASE 2a: Incremental state update — O(2 bars/stock) instead of O(N) ──
    # Updates indicator_state DB for all stocks that got new candles.
    # Stocks with no state yet (first run) will be handled by full precompute below.
    #
    # Bring each stock's saved indicator state up to the latest completed
    # candle, so precompute_all() below only has to replay what is new.
    incremental_updater.run(stocks)

    # ─ PHASE 2b: Precompute indicator state — uses incremental state if current ─
    # Fast path skips full 2500-row compute; falls back to full recompute only
    # for stocks that are missing state or have > 20 new bars.
    scanner = ScannerWS(stocks)
    scanner.precompute_all(workers=4)

    # ── SCANNER_MODE post-market / holiday → show stored LIVE signals ─────────
    #  Runs AFTER the full pipeline (login, DB store, precompute) so all that
    #  terminal output still appears exactly like before. If we're past
    #  MAX_ENTRY_TIME today, or on a non-trading day, replay that day's stored
    #  live signals on the dashboard instead of scanning.
    if config.SCANNER_MODE and _maybe_show_stored_signals():
        return

    # ─ PHASE 3: Wait until 09:11 — buffer before scan phase ────────────────
    # scanner.scan() is called right after, before 09:30.
    # scanner detects it's pre-09:30 → connects WS early (PATH A) → collects
    # exact 09:15-09:30 H/L from live WS ticks → starts signal scan at 09:30.
    # No separate LTP-polling or API calls needed for opening range.
    wait_for_data_collect_done()


    # ── Restore traded_symbols from today's log (survives restarts) ──────────
    # If the bot was restarted mid-day, any stock already logged in today's
    # trades CSV is added to traded_symbols so it won't be traded again.
    try:
        import csv as _csv
        from monitor import _get_log_file as _log_path
        _lf = _log_path()
        if os.path.exists(_lf):
            with open(_lf, newline="") as _f:
                for _row in _csv.DictReader(_f):
                    _sym = _row.get("Stock", "").strip()
                    if _sym:
                        traded_symbols.add(_sym)
            if traded_symbols:
                print(f"  📋 Restored {len(traded_symbols)} already-traded symbol(s) "
                      f"from today's log: {', '.join(sorted(traded_symbols))}")
    except Exception as _e:
        print(f"  ⚠️  Could not restore traded_symbols from log: {_e}")

    # ── Scan-Trade loop ────────────────────────────────────────────────────────
    #  Runs until MAX_ENTRY_TIME or all stocks traded.
    #  After each trade closes, the loop restarts automatically with the
    #  remaining (un-traded) stocks.
    # ──────────────────────────────────────────────────────────────────────────
    # Fresh signal store for this live session (persisted for post-market re-runs)
    signal_store.reset(date.today())

    while _current_time() < _parse_time(config.MAX_ENTRY_TIME):

        remaining = [s for s in stocks if s["symbol"] not in traded_symbols]

        if not remaining:
            print(f"  ℹ️  [{_now_str()}] All watchlist stocks already traded today. Stopping.\n")
            break

        print(f"\n  🔄 [{_now_str()}] Scanning {len(remaining)} stocks  "
              f"({len(traded_symbols)} already traded today — skipped 🙅)")

        if config.SCANNER_MODE:
            _fe.set_status(
                f"Scanning {len(remaining)} stocks | {len(traded_symbols)} already signaled",
                scanning=True,
            )

        # Point scanner at remaining stocks only
        # Also update skip_symbols so the WS tick handler ignores already-traded
        # stocks at the source — prevents re-entry on same stock after any exit
        scanner.stocks       = remaining
        scanner.skip_symbols = set(traded_symbols)   # copy; thread-safe snapshot
        _rescan_event.clear()

        signal_result = scanner.scan()   # blocks until 1st signal or MAX_ENTRY_TIME

        if not signal_result:
            print(f"  ⚪ [{_now_str()}] No indicator signal before "
                  f"{config.MAX_ENTRY_TIME}. Done for today.\n")
            telegram_notify.notify_no_trade(
                f"No indicator signal found before {config.MAX_ENTRY_TIME}.\n"
                f"Scanned {len(remaining)} stocks."
            )
            break

        # ── Signal found ────────────────────────────────────────────────────
        sym        = signal_result["symbol"]
        _sig       = signal_result.get("signal", "LONG")
        _sig_icon  = "🟢" if _sig == "LONG" else "🔴"
        _tail      = " — signal detected!\n" if config.SCANNER_MODE else " — entering trade!\n"
        print(f"  ✅ [{_now_str()}] {_sig_icon} {_sig} on {sym} "
              f"@ ₹{signal_result['ltp']}{_tail}")

        # ── Push to dashboard + persist to signal_store (both modes) ──────────
        _direction = "BUY" if _sig == "LONG" else "SELL"
        _entry = {
            "time"       : _now_str(),
            "direction"  : _direction,
            "symbol"     : sym,
            "security_id": signal_result["security_id"],
            "ltp"        : signal_result["ltp"],
        }
        _fe.add_signal(_entry)
        signal_store.append(date.today(), _entry)

        traded_symbols.add(sym)   # mark as done; won't be scanned again

        # ── SCANNER MODE: signal-only — no trade. Telegram + resume scanning. ──
        if config.SCANNER_MODE:
            telegram_notify.notify_signal(sym, _direction, signal_result["ltp"],
                                          time_str=_entry["time"])
            _fe.set_status(
                f"{len(traded_symbols)} signal(s) so far | scanning for more...",
                scanning=True,
            )
            print(f"  🔔 [{_now_str()}] Scanner mode — pushed to dashboard + Telegram. "
                  f"Resuming scan...\n")
            continue

        def _on_trade_closed(closed_trades: list, _sym=sym):
            """Called by monitor when ALL positions from this batch are exited."""
            print(f"\n  ✅ [{_now_str()}] Trade on {_sym} closed. "
                  f"🔄 Resuming scan for remaining stocks...\n")
            _rescan_event.set()   # rescan immediately; refresh the balance in the background

            def _update_balance():
                if config.PAPER_TRADING:
                    return
                print("  💰 Re-fetching account balance for next trade...")
                new_balance = broker.fetch_account_balance()
                if new_balance and new_balance > 0:
                    reserve = int(new_balance * config.CAPITAL_RESERVE_PCT / 100)
                    config.DEPLOYED_CAPITAL = min(int(new_balance) - reserve, 10_000)  # TEMP: cap at ₹10k
                    print(f"  ✅ Account balance     : ₹{int(new_balance):,}")
                    print(f"  🔒 Reserve ({config.CAPITAL_RESERVE_PCT}% slippage)  : ₹{reserve:,}")
                    print(f"  💰 Updated capital     : ₹{config.DEPLOYED_CAPITAL:,} (capped at ₹10,000)")
                    print(f"  ⚡ New buying power    : ₹{config.DEPLOYED_CAPITAL * config.LEVERAGE:,}\n")
                else:
                    print(f"  ⚠️  Balance fetch failed — keeping current: ₹{config.DEPLOYED_CAPITAL:,}\n")

            threading.Thread(target=_update_balance, daemon=True).start()

        monitor = run_entries([signal_result], on_closed=_on_trade_closed)

        if monitor is None:
            print("  ℹ️  No positions entered. Continuing scan.\n")
            continue

        run_monitor(monitor)   # blocks until trade is fully exited
        # After run_monitor returns, the while-loop restarts with updated remaining

    # ── Session summary ────────────────────────────────────────────────────────
    if config.SCANNER_MODE:
        print(f"  🏁 [{_now_str()}] Scanner session done.  "
              f"Total signals today: {len(traded_symbols)}.\n")
        _fe.set_status(
            f"Session complete — {len(traded_symbols)} signal(s) today",
            scanning=False,
        )
        _keep_dashboard_alive()
        return

    print(f"  🏁 [{_now_str()}] Session done.  "
          f"Total trades taken today: {len(traded_symbols)}.\n")
    if traded_symbols:
        from monitor import _get_log_file
        print(f"  📝 Trade log saved → {_get_log_file()}\n")

    # ── Telegram: Daily summary ────────────────────────────────────────────────
    import monitor as _monitor_mod
    telegram_notify.notify_daily_summary(_monitor_mod._closed_trades_today)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n  🛑 [{datetime.now().strftime('%H:%M:%S')}] Algo stopped by user (Ctrl+C).\n")
