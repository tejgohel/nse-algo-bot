# ─────────────────────────────────────────────────────────────────────────────
#  config.py  —  every setting in one place
#
#  ⚠️  NO SECRETS IN THIS FILE.
#      Credentials are read from environment variables (or a local .env, which
#      is git-ignored). Copy .env.example → .env and fill it in.
# ─────────────────────────────────────────────────────────────────────────────

import os as _os
import sys as _sys

_HERE = _os.path.dirname(_os.path.abspath(__file__))

# Windows consoles default to cp1252 and cannot encode the box-drawing and
# emoji characters used throughout the output.
for _stream in (_sys.stdout, _sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")
    except Exception:
        pass


def _load_dotenv() -> None:
    path = _os.path.join(_HERE, ".env")
    if not _os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            _os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))


_load_dotenv()


def _flag(name: str, default: bool) -> bool:
    return _os.getenv(name, "1" if default else "0").strip().lower() in ("1", "true", "yes", "on")


# ═════════════════════════════════════════════════════════════════════════════
#  👉  YOUR BROKER CREDENTIALS GO HERE — but NOT in this file.
#
#      Every value below is read from an environment variable so that nothing
#      secret is ever committed. To connect your own Dhan account:
#
#        1. copy  .env.example  →  .env
#        2. fill in your own values:
#
#             DHAN_CLIENT_ID     your Dhan client ID     (e.g. 11XXXXXXXX)
#             DHAN_ACCESS_TOKEN  your API access token   (long JWT string)
#             DHAN_PIN           your Dhan login PIN     (optional, for TOTP)
#             DHAN_TOTP_SECRET   your base32 2FA seed    (optional, for TOTP)
#
#      Where to get them:
#        Client ID + Access Token → https://dhanhq.co → My Profile
#                                   → DhanHQ Trading APIs → Generate Token
#        TOTP secret              → the text behind the QR code when you turn
#                                   on 2FA; with it, auto_login.py mints a
#                                   fresh token on every run
#
#      ⚠️  Do NOT paste real values below. `.env` is in .gitignore; config.py
#          is NOT — anything hardcoded here WILL be published when you push.
# ═════════════════════════════════════════════════════════════════════════════

CLIENT_ID    = _os.getenv("DHAN_CLIENT_ID",    "")   # ← set in .env
ACCESS_TOKEN = _os.getenv("DHAN_ACCESS_TOKEN", "")   # ← set in .env
DHAN_PIN         = _os.getenv("DHAN_PIN",         "")   # ← set in .env
DHAN_TOTP_SECRET = _os.getenv("DHAN_TOTP_SECRET", "")   # ← set in .env


# ═════════════════════════════════════════════════════════════════════════════
#  ⛔  THE SAFETY SWITCH — read this before you change it
#
#      True  → PAPER. Signals are found, sized and logged. No order is ever
#              sent. This is the default and it is where you should stay until
#              live signals match what your backtest said they would be.
#
#      False → LIVE. Real orders, real money, no confirmation prompt.
#
#      This repository ships without a trading rule (see strategy.py), so with
#      the defaults it cannot place an order even if you flip this. Once you
#      write a rule, this flag is the only thing between it and your account.
# ═════════════════════════════════════════════════════════════════════════════
PAPER_TRADING = _flag("PAPER_TRADING", True)

#  True  → SCANNER ONLY. Signals go to the dashboard and Telegram, and the
#          scan loop immediately resumes looking for the next one. No trades.
#  False → FULL TRADING. Signal → leverage check → size → order → monitor.
SCANNER_MODE = _flag("SCANNER_MODE", True)

#  Take short trades as well as long ones.
ENABLE_SHORT_TRADES = _flag("ENABLE_SHORT_TRADES", True)


# ─────────────────────────────────────────────────────────────────────────────
#  Capital and sizing
# ─────────────────────────────────────────────────────────────────────────────
DEPLOYED_CAPITAL      = int(_os.getenv("DEPLOYED_CAPITAL", "50000"))  # ₹ you intend to deploy
CAPITAL_RESERVE_PCT   = 15     # % kept aside as a slippage buffer
LEVERAGE              = 5      # intraday leverage you expect
MIN_LEVERAGE_REQUIRED = 5.0    # skip the trade if the broker offers less (ASM/GSM/T2T)


# ─────────────────────────────────────────────────────────────────────────────
#  Indicator parameters  (see indicators.py — that file ships empty)
#
#  Three unnamed slots, passed straight into calculate_indicator_1/2/3(). They
#  carry no meaning until you give them one: make slot 1 an EMA and ignore its
#  mult, make slot 2 an ATR channel, make slot 3 an RSI — the scanner never
#  looks inside. The defaults below are round numbers to start from, not advice.
# ─────────────────────────────────────────────────────────────────────────────
INDICATOR_1_LENGTH = 20
INDICATOR_1_MULT   = 2.0

INDICATOR_2_LENGTH = 10
INDICATOR_2_MULT   = 3.0

INDICATOR_3_LENGTH = 30
INDICATOR_3_MULT   = 1.0


# ─────────────────────────────────────────────────────────────────────────────
#  Risk — stop, trail and target, all as a % of DEPLOYED_CAPITAL
#
#  Expressing risk in capital rather than in price is deliberate: a 1% move on
#  a ₹200 stock and on a ₹4,000 stock are not the same risk, and sizing off
#  price alone is how a "small" loss turns out not to be.
# ─────────────────────────────────────────────────────────────────────────────
INITIAL_SL_PCT       = 5     # max rupee risk on entry, as % of capital
TRAIL_STEP_PCT       = 0.5   # move from entry that pulls the stop to breakeven
BREAKEVEN_BUFFER_PCT = 0.1   # a hair beyond entry, so breakeven covers costs

PROFIT_LOCK_PCT         = 10   # once triggered, never give back more than this
PROFIT_LOCK_TRIGGER_PCT = 11   # ...and it arms here
TP_PCT                  = 20   # hard exit, no questions

MAX_MOVE_PCT = 5.0    # skip an entry if the stock has already run this far today


# ─────────────────────────────────────────────────────────────────────────────
#  Liquidity filter
# ─────────────────────────────────────────────────────────────────────────────
MIN_VOLUME          = 100_000   # minimum average candle volume
MIN_VOLUME_LOOKBACK = 5         # averaged over this many completed candles


# ─────────────────────────────────────────────────────────────────────────────
#  Timing  (IST, 24-hour)
# ─────────────────────────────────────────────────────────────────────────────
DATA_COLLECT_WAIT_TIME = "09:11"   # wait here after the DB update, before connecting
OPENING_RANGE_START    = "09:15"
OPENING_RANGE_END      = "09:30"   # opening range done; scanning begins
ALGO_START_TIME        = OPENING_RANGE_END
MAX_ENTRY_TIME         = "14:30"   # no new entries after this
MARKET_EXIT_TIME       = "15:15"   # force-close everything by this time
RESCAN_INTERVAL        = 60        # seconds between rescans when nothing fires

#  Track the first-15-minute high/low and hand it to strategy.py. Turn it off
#  if your rule does not use an opening range.
TRACK_OPENING_RANGE = _flag("TRACK_OPENING_RANGE", True)


# ─────────────────────────────────────────────────────────────────────────────
#  Copy trading — mirror every entry and exit onto follower accounts
#
#  Each follower logs in independently with the same TOTP flow and is sized off
#  its OWN capital, not as a multiple of the master's quantity. The master is
#  CLIENT_ID above — do not list it again here.
#
#  ⚠️  These are full account credentials. Keep them in .env, never here.
#      Format (JSON, one line):
#        FOLLOWER_ACCOUNTS=[{"name":"...","client_id":"...","pin":"...",
#                            "totp_secret":"...","deployed_capital":100000,
#                            "leverage":5,"enabled":true}]
# ─────────────────────────────────────────────────────────────────────────────
COPY_TRADING_ENABLED = _flag("COPY_TRADING_ENABLED", False)

import json as _json
try:
    FOLLOWER_ACCOUNTS = _json.loads(_os.getenv("FOLLOWER_ACCOUNTS", "[]"))
except Exception:
    print("[WARN] FOLLOWER_ACCOUNTS is not valid JSON — copy trading disabled.")
    FOLLOWER_ACCOUNTS = []


# ─────────────────────────────────────────────────────────────────────────────
#  Data
# ─────────────────────────────────────────────────────────────────────────────
DB_PATH           = _os.path.join(_HERE, "candles.db")
DAILY_DB_PATH     = _os.path.join(_HERE, "candles_daily.db")
SECURITY_CSV_PATH = _os.path.join(_HERE, "security_id_list.csv")


# ─────────────────────────────────────────────────────────────────────────────
#  Market feed WebSocket — built lazily so it always uses the CURRENT token
#  (auto_login patches ACCESS_TOKEN in memory at startup).
# ─────────────────────────────────────────────────────────────────────────────
def ws_url() -> str:
    return (
        "wss://api-feed.dhan.co"
        "?version=2"
        f"&token={ACCESS_TOKEN}"
        f"&clientId={CLIENT_ID}"
        "&authType=2"
    )

WS_URL = ws_url()   # kept for callers that read it as a plain attribute


# ─────────────────────────────────────────────────────────────────────────────
#  Watchlist — NSE symbols to scan
#
#  Replace this with whatever universe you want. watchlist.py resolves each
#  name to its broker security_id and listing date from the instrument master,
#  so only the symbol text matters here.
#
#  These 25 large caps are a starting point, not a recommendation.
# ─────────────────────────────────────────────────────────────────────────────
WATCHLIST_SYMBOLS = [
    "ADANIPORTS", "ASIANPAINT", "AXISBANK", "BAJFINANCE", "BHARTIARTL",
    "HCLTECH",    "HDFCBANK",   "HINDUNILVR", "ICICIBANK", "INFY",
    "ITC",        "KOTAKBANK",  "LT",        "MARUTI",     "NTPC",
    "ONGC",       "POWERGRID",  "RELIANCE",  "SBIN",       "SUNPHARMA",
    "TATASTEEL",  "TCS",        "TITAN",     "ULTRACEMCO", "WIPRO",
]
