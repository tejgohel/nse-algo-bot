# ─────────────────────────────────────────────────────────────────────────────
#  indicators.py  —  YOUR INDICATORS GO HERE
#
#  ⚠️  THIS FILE IS INTENTIONALLY EMPTY.
#
#  Everything else in this repository is the machinery around a trading rule:
#  candle databases, a live WebSocket feed, order placement, position
#  monitoring, a dashboard, a backtest harness and the scripts that keep them
#  fed. None of it decides anything on its own.
#
#  This file is where the numbers get computed, and `strategy.py` is where
#  those numbers become a BUY or a SELL. Both ship empty on purpose. Bring
#  whatever you have tested: moving-average bands, ATR channels, breakout
#  levels, an oscillator, a model score — the bot does not care what you
#  compute, only that these functions return what it expects.
#
#
#  ── HOW THE BOT USES THIS FILE ──────────────────────────────────────────────
#
#  There are two paths through here, and they must agree with each other.
#
#  1. THE BATCH PATH — used at startup and in backtests
#
#         calculate_indicator_1(df)     each adds its columns to `df`
#         calculate_indicator_2(df)     and returns it
#         calculate_indicator_3(df)
#         calculate_emas(df)
#         add_entry_signal(df)              adds a long-entry boolean column
#         add_short_signal(df)              adds a short-entry boolean column
#
#     These run over a whole DataFrame of completed candles. Slow is fine here
#     — it happens once per instrument, before the market opens.
#
#  2. THE LIVE PATH — used on every tick, for hundreds of instruments
#
#         extract_state(df)          fold the whole history into a small dict
#         increment_state(state, row)  advance that dict by ONE candle
#
#     A tick cannot afford to recompute history. So the batch path runs once,
#     `extract_state` captures whatever the recursive parts need to continue
#     (previous band values, previous trend direction, running averages), and
#     from then on each new candle only calls `increment_state`.
#
#     THE CONTRACT THAT MATTERS: walking a series with `increment_state` must
#     produce the same values as running the batch functions over that same
#     series. If the two drift apart, the bot will trade one thing and your
#     backtest will report another. Test this explicitly.
#
#
#  ── WHAT THE CANDLES LOOK LIKE ──────────────────────────────────────────────
#
#      df : pandas DataFrame, oldest row first, columns
#           date (Timestamp) · open · high · low · close · volume
#
#      The project's default timeframe is 4-hour bars built from 1-hour data,
#      two per NSE session (09:15 and 13:15). See db.py.
#
#
#  ── A MINIMAL WORKING EXAMPLE ───────────────────────────────────────────────
#
#      import numpy as np
#
#      def calculate_indicator_1(df, length=20, mult=2.0):
#          hl2 = (df["high"] + df["low"]) / 2
#          atr = (df["high"] - df["low"]).rolling(length).mean()
#          df["band_up"]   = hl2 + mult * atr
#          df["band_down"] = hl2 - mult * atr
#          return df
#
#      def add_entry_signal(df):
#          df["entry_long"] = df["close"] > df["band_up"].shift(1)
#          return df
#
#      def extract_state(df):
#          last = df.iloc[-1]
#          return {"band_up": float(last["band_up"]),
#                  "band_down": float(last["band_down"]),
#                  "close": float(last["close"])}
#
#      def increment_state(state, row):
#          # advance by one completed candle, returning the new state
#          hl2 = (row["high"] + row["low"]) / 2
#          ...
#          return state
#
#  Fill these in, then fill in strategy.py, and the bot runs.
# ─────────────────────────────────────────────────────────────────────────────

_NOT_IMPLEMENTED = (
    "indicators.{name}() is not implemented — this repository ships without "
    "indicators on purpose. Add your own in indicators.py (see the notes at "
    "the top of the file)."
)


def _todo(name):
    raise NotImplementedError(_NOT_IMPLEMENTED.format(name=name))


# ─────────────────────────────────────────────────────────────────────────────
#  Batch path — run once over a full history of completed candles
# ─────────────────────────────────────────────────────────────────────────────

def calculate_indicator_1(df, length: int = 20, mult: float = 2.0):
    """
    Indicator slot 1 — an EMA, an ATR channel, an RSI, anything you like.

    df  : DataFrame with date / open / high / low / close / volume
    →   : the same DataFrame with your columns added

    Whatever column names you add here are the ones extract_state() must
    capture and strategy.py can read. `mult` is passed through for indicators
    that need a band width; ignore it for the ones that do not.
    """
    _todo("calculate_indicator_1")


def calculate_indicator_2(df, length: int = 10, mult: float = 3.0):
    """Indicator slot 2. Same contract as above."""
    _todo("calculate_indicator_2")


def calculate_indicator_3(df, length: int = 30, mult: float = 1.0):
    """Indicator slot 3. Same contract as above."""
    _todo("calculate_indicator_3")


def calculate_emas(df):
    """Any plain moving averages your rule needs. Same contract as above."""
    _todo("calculate_emas")


def add_entry_signal(df):
    """
    Add the LONG entry column.

    Called on a full DataFrame after the calculate_* functions have run.
    Add a boolean column and return the DataFrame; strategy.py and the
    backtest both read it.
    """
    _todo("add_entry_signal")


def add_short_signal(df):
    """Add the SHORT entry column. Same contract as add_entry_signal."""
    _todo("add_short_signal")


# ─────────────────────────────────────────────────────────────────────────────
#  Live path — a small dict that advances one candle at a time
# ─────────────────────────────────────────────────────────────────────────────

def extract_state(df) -> dict:
    """
    Fold a full history into the smallest dict that can be advanced forward.

    Called once per instrument at startup, and persisted to SQLite by
    tools/seed_state.py so the next run does not repeat the work.

    →  a JSON-serialisable dict holding whatever increment_state() needs:
       previous indicator values, the current direction, running averages,
       and a short rolling window if your indicator needs one.

    TWO OPTIONAL KEYS, if you want a trailing-indicator exit:

        exit_long_below    close the long once price trades under this
        exit_short_above   close the short once price trades over this

    monitor.py reads them with .get() on every tick, so they are entirely
    optional — omit them and only the price-based exits (stop, trail, profit
    lock, target, force close) apply. Include them and your indicator gets to
    close the trade without monitor.py knowing what the indicator is.
    """
    _todo("extract_state")


def increment_state(state: dict, row: dict) -> dict:
    """
    Advance `state` by ONE completed candle and return the new state.

    state : whatever extract_state() produced
    row   : dict with open / high / low / close / volume for the new candle

    Must match the batch path exactly — see the contract note at the top.
    """
    _todo("increment_state")
