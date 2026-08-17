"""
fetch_15min.py  —  Fetch today's first 15-min candle (the 09:15 bar) for ICICIAMC

Usage:
    python fetch_15min.py

Token : taken from config.ACCESS_TOKEN automatically
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from dhanhq import dhanhq
import config

# ── Config ────────────────────────────────────────────────────────────────────
SECURITY_ID = "760407"          # ICICIAMC
SYMBOL      = "ICICIAMC"
TODAY       = datetime.now().strftime("%Y-%m-%d")

dhan = dhanhq(config.CLIENT_ID, config.ACCESS_TOKEN)

# ── API Call ──────────────────────────────────────────────────────────────────
print(f"\n{'='*55}")
print(f"  Dhan Intraday API — {SYMBOL}  ({TODAY})")
print(f"  Fetching 09:15 first 15-min candle...")
print(f"{'='*55}\n")

result = dhan.intraday_minute_data(
    security_id    = SECURITY_ID,
    exchange_segment = dhan.NSE,
    instrument_type  = "EQUITY",
    interval         = 15,
    from_date        = TODAY,
    to_date          = TODAY,
)

if result.get("status") != "success":
    print(f"  ERROR: {result.get('remarks', result)}")
    sys.exit(1)

data = result["data"]

# ── Parse candles ─────────────────────────────────────────────────────────────
timestamps = data.get("timestamp", [])
opens      = data.get("open",      [])
highs      = data.get("high",      [])
lows       = data.get("low",       [])
closes     = data.get("close",     [])
volumes    = data.get("volume",    [])

if not timestamps:
    print("  No data returned. Market may not be open yet, or holiday.")
    sys.exit(0)

print(f"  Total candles returned : {len(timestamps)}\n")
print(f"  {'Time':<12} {'Open':>10} {'High':>10} {'Low':>10} {'Close':>10} {'Volume':>12}")
print(f"  {'-'*66}")

target_candle = None

for i, ts in enumerate(timestamps):
    dt   = datetime.fromtimestamp(ts)
    o, h, l, c, v = opens[i], highs[i], lows[i], closes[i], volumes[i]
    time_str = dt.strftime("%H:%M:%S")
    marker = "  <-- 09:15 bar (FIRST 15MIN)" if (dt.hour == 9 and dt.minute == 15) else ""
    print(f"  {time_str:<12} {o:>10.2f} {h:>10.2f} {l:>10.2f} {c:>10.2f} {v:>12,.0f}{marker}")
    if dt.hour == 9 and dt.minute == 15:
        target_candle = {"time": time_str, "open": o, "high": h, "low": l, "close": c, "volume": v}

print()

if target_candle:
    print(f"{'='*55}")
    print(f"  First 15-min Candle (09:15 bar)  —  {SYMBOL}")
    print(f"{'='*55}")
    print(f"  Open   : Rs{target_candle['open']:.2f}")
    print(f"  High   : Rs{target_candle['high']:.2f}")
    print(f"  Low    : Rs{target_candle['low']:.2f}")
    print(f"  Close  : Rs{target_candle['close']:.2f}")
    print(f"  Volume :  {target_candle['volume']:,.0f}")
else:
    dt = datetime.fromtimestamp(timestamps[0])
    print(f"  09:15 bar not found exactly — using first candle ({dt.strftime('%H:%M:%S')})")
    print(f"  Open   : Rs{opens[0]:.2f}")
    print(f"  High   : Rs{highs[0]:.2f}")
    print(f"  Low    : Rs{lows[0]:.2f}")
    print(f"  Close  : Rs{closes[0]:.2f}")
    print(f"  Volume :  {volumes[0]:,.0f}")

print(f"{'='*55}\n")
