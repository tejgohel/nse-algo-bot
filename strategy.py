# ─────────────────────────────────────────────────────────────────────────────
#  strategy.py  —  YOUR ENTRY RULE GOES HERE
#
#  ⚠️  THIS FILE IS INTENTIONALLY EMPTY.
#
#  `indicators.py` computes numbers. This file turns them into a decision:
#  LONG, SHORT, or nothing. It is the only place in the repository where that
#  judgement is made, and it ships empty on purpose — a live trading rule is
#  not something anyone publishes, and a rule that has not been measured on
#  your own data is worth nothing anyway.
#
#  Everything around it is complete: candle databases, the live feed, order
#  placement, position monitoring, the dashboard and the backtest harness all
#  work as soon as these two functions return real values.
#
#
#  ── THE TWO FUNCTIONS ───────────────────────────────────────────────────────
#
#    precompute_state(security_id, symbol)      once per stock, before the open
#        Load that stock's history, run your indicators over it, and return a
#        small cache dict. The scanner calls this in parallel at startup and
#        then never touches the database again during the session.
#
#    check_signal_on_tick(cached, ltp, today_ohlc, ...)     on every tick
#        Apply the forming candle to that cache and decide. No database, no
#        API calls, no pandas if you can avoid it — this runs hundreds of
#        times a second across the whole watchlist.
#
#
#  ── WHAT THE CACHE MUST CONTAIN ─────────────────────────────────────────────
#
#  The bot reads these keys out of whatever precompute_state() returns, so
#  they need to be there. Everything else in the dict is yours.
#
#      security_id     str    the stock this cache belongs to
#      last_close      float  previous session's close — used for the move filter
#      last_date              date of the last completed candle
#      prev_4h_low     float  previous candle's low   → initial stop for a LONG
#      prev_4h_high    float  previous candle's high  → initial stop for a SHORT
#
#  The scanner also writes into this dict during the session, so do not freeze
#  it: session_open, day_open, first_15min_high, first_15min_low.
#
#
#  ── WHAT check_signal_on_tick MUST RETURN ───────────────────────────────────
#
#      ("LONG",  state)   open a long
#      ("SHORT", state)   open a short
#      (None,    state)   do nothing            ← the common case
#
#  `state` is a plain dict and it is yours to shape. Whatever you put in it is
#  printed to the console when a signal fires and pushed to the dashboard, so
#  put your condition flags in there — that is what makes a fill explainable
#  three weeks later. Two keys are read by the bot itself:
#
#      prev_4h_low / prev_4h_high    used to place the initial stop
#
#
#  ── A MINIMAL WORKING EXAMPLE ───────────────────────────────────────────────
#
#      import db, indicators
#
#      def precompute_state(security_id, symbol=""):
#          df = db.get_candles(security_id)
#          if df is None or len(df) < 60:
#              return None
#          df = indicators.calculate_indicator_1(df)
#          df = indicators.add_entry_signal(df)
#          last, prev = df.iloc[-1], df.iloc[-2]
#          return {
#              "security_id": security_id,
#              "symbol":      symbol,
#              "last_close":  float(last["close"]),
#              "last_date":   last["date"],
#              "prev_4h_low":  float(prev["low"]),
#              "prev_4h_high": float(prev["high"]),
#              "indicator_state":    indicators.extract_state(df),
#          }
#
#      def check_signal_on_tick(cached, ltp, today_ohlc,
#                               first_15min_high=None, first_15min_low=None):
#          st = cached["indicator_state"]
#          state = {"ltp": ltp,
#                   "band_up": st["band_up"],
#                   "prev_4h_low":  cached["prev_4h_low"],
#                   "prev_4h_high": cached["prev_4h_high"]}
#          if ltp > st["band_up"]:
#              state["reason"] = "close above band"
#              return "LONG", state
#          return None, state
#
#
#  ── BEFORE YOU TRADE IT ─────────────────────────────────────────────────────
#
#  Run backtest.py over a meaningful sample first, and keep PAPER_TRADING on
#  in config.py until the live signals match what the backtest said they would
#  be. The bot will place real orders the moment you turn it off.
# ─────────────────────────────────────────────────────────────────────────────

import pandas as pd

#  A sentinel far-future timestamp. The scanner uses it to mark the forming
#  candle, which has no close time yet, so it always sorts last.
_FUTURE_TS = pd.Timestamp("2099-01-01")

_NOT_IMPLEMENTED = (
    "strategy.{name}() is not implemented — this repository ships without a "
    "trading rule on purpose. Write yours in strategy.py (see the notes at "
    "the top of the file)."
)


def precompute_state(security_id: str, symbol: str = "") -> "dict | None":
    """
    Build one stock's cache before the market opens.

    Called once per stock, in parallel, at startup. Load history from db.py,
    run your indicators over it, and return the cache dict described at the
    top of this file — or None to skip this stock for the session.
    """
    raise NotImplementedError(_NOT_IMPLEMENTED.format(name="precompute_state"))


def check_signal_on_tick(
    cached          : dict,
    ltp             : float,
    today_ohlc      : dict,
    first_15min_high: "float | None" = None,
    first_15min_low : "float | None" = None,
) -> "tuple[str | None, dict]":
    """
    Decide, on one tick, whether to open a position.

    cached           the dict precompute_state() returned for this stock
    ltp              last traded price
    today_ohlc       the forming candle: open / high / low / close so far
    first_15min_*    the opening range, if your rule wants it (may be None
                     before 09:30, so handle that)

    →  ("LONG" | "SHORT" | None, state_dict)

    Runs on every tick for every stock in the watchlist, so keep it cheap:
    read from `cached`, do arithmetic, return. No database, no API calls.
    """
    raise NotImplementedError(_NOT_IMPLEMENTED.format(name="check_signal_on_tick"))
