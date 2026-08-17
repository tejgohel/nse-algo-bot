# ─────────────────────────────────────────────────────────────────────────────
#  broker.py  —  order operations
#  (Same logic as ema2050/broker.py — adapted for indicator algo)
# ─────────────────────────────────────────────────────────────────────────────

import time
import requests
import config
import login as _login


def fetch_account_balance() -> float | None:
    """
    Fetch available intraday balance from Dhan /v2/fundlimit API.
    Returns the usable cash balance, or None on failure.
    """
    try:
        r = requests.get(
            "https://api.dhan.co/v2/fundlimit",
            headers=_login.get_headers(),
            timeout=10,
        )
        if r.status_code != 200:
            print(f"  ⚠️  Fund limit API returned {r.status_code}")
            return None
        data = r.json()
        # Dhan's field name has a typo; try both variants plus sodLimit as fallback
        for field in ("availabelBalance", "availableBalance", "sodLimit"):
            val = data.get(field)
            if val is not None and float(val) > 0:
                return float(val)
        return None
    except Exception as e:
        print(f"  ⚠️  Balance fetch error: {e}")
        return None


def get_intraday_leverage(security_id: str, ltp: float) -> float:
    """
    Calls Dhan /v2/margincalculator to get the actual intraday leverage for a stock.
    Returns the leverage as a float (e.g. 5.0, 3.0, 1.0).
    Falls back to 1.0 (no leverage) on any API error.
    """
    payload = {
        "dhanClientId"    : config.CLIENT_ID,
        "exchangeSegment" : "NSE_EQ",
        "transactionType" : "BUY",
        "quantity"        : 1,
        "productType"     : "INTRADAY",
        "securityId"      : str(security_id),
        "price"           : float(ltp),
    }
    try:
        r = requests.post(
            "https://api.dhan.co/v2/margincalculator",
            json    = payload,
            headers = _login.get_headers(),
            timeout = 8,
        )
        if r.status_code != 200:
            print(f"  ⚠️  margincalculator API {r.status_code} — defaulting to 1x")
            return 1.0
        lev_str = str(r.json().get("leverage", "1")).upper().replace("X", "").strip()
        return float(lev_str)
    except Exception as e:
        print(f"  ⚠️  margincalculator error: {e} — defaulting to 1x")
        return 1.0


def calculate_quantity(ltp: float, leverage: float | None = None) -> int:
    lev          = leverage if leverage is not None else float(config.LEVERAGE)
    buying_power = config.DEPLOYED_CAPITAL * lev
    return max(int(buying_power // ltp), 1)


def get_initial_sl(signal: str, entry_price: float, qty: int,
                   prev_candle_low: float | None = None,
                   prev_candle_high: float | None = None) -> float:
    """
    Initial SL based on the previous completed 4H candle's Low (LONG) or High (SHORT).

      LONG  → SL price = previous 4H candle's Low
      SHORT → SL price = previous 4H candle's High

    This gives a natural market-structure stop-loss instead of an arbitrary
    fixed-rupee SL. The previous candle's extreme is a meaningful price level
    that, if broken, invalidates the trade setup.

    Fallback: if prev_candle_low / prev_candle_high is unavailable, falls back
    to the old INITIAL_SL_PCT-based calculation (5% of capital).
    """
    if signal == "LONG":
        if prev_candle_low is not None:
            return round(prev_candle_low, 2)
        # Fallback — fixed capital-based SL
        max_loss = config.DEPLOYED_CAPITAL * config.INITIAL_SL_PCT / 100
        return round(entry_price - max_loss / qty, 2)
    else:
        if prev_candle_high is not None:
            return round(prev_candle_high, 2)
        # Fallback — fixed capital-based SL
        max_loss = config.DEPLOYED_CAPITAL * config.INITIAL_SL_PCT / 100
        return round(entry_price + max_loss / qty, 2)



def get_ltp(security_id: str) -> float | None:
    try:
        r = requests.post(
            "https://api.dhan.co/v2/marketfeed/ltp",
            json={"NSE_EQ": [int(security_id)]},
            headers=_login.get_headers(),
            timeout=5
        )
        if r.status_code != 200:
            return None
        return float(r.json()["data"]["NSE_EQ"][str(security_id)]["last_price"])
    except Exception:
        return None


def get_ltp_batch(security_ids: list[str]) -> dict:
    """Fetch LTP for up to 100 security IDs in one API call."""
    try:
        r = requests.post(
            "https://api.dhan.co/v2/marketfeed/ltp",
            json={"NSE_EQ": [int(sid) for sid in security_ids]},
            headers=_login.get_headers(),
            timeout=8
        )
        if r.status_code != 200:
            return {}
        raw = r.json().get("data", {}).get("NSE_EQ", {})
        return {str(k): float(v["last_price"]) for k, v in raw.items()}
    except Exception:
        return {}


def get_ltp_all(security_ids: list[str], batch_size: int = 100) -> dict:
    """
    Fetches LTP for ALL security IDs in batches of `batch_size` (max 100).
    Returns {security_id_str: ltp_float} for all stocks in one shot.

    WHY: Instead of 499 separate API calls (8+ min), we make ~5 calls in ~5s.
    Dhan's /v2/marketfeed/ltp accepts up to 100 IDs per request.
    """
    all_ltps = {}
    chunks   = [security_ids[i:i+batch_size] for i in range(0, len(security_ids), batch_size)]

    for chunk in chunks:
        time.sleep(0.3)   # small pause between batches (be kind to the API)
        ltps = get_ltp_batch(chunk)
        all_ltps.update(ltps)

    return all_ltps


def place_market_order(security_id: str, signal: str, quantity: int) -> str | None:
    direction = "📈 LONG  BUY" if signal == "LONG" else "📉 SHORT SELL"

    if config.PAPER_TRADING:
        fake_id = f"PAPER-{int(time.time())}"
        print(f"  📋 [PAPER] Simulated entry | {direction} | Qty: {quantity}")
        print(f"     OrderID: {fake_id}")
        return fake_id

    txn_type = _login.dhan.BUY if signal == "LONG" else _login.dhan.SELL

    try:
        response = _login.dhan.place_order(
            security_id      = str(security_id),
            exchange_segment = _login.dhan.NSE,
            transaction_type = txn_type,
            quantity         = quantity,
            order_type       = _login.dhan.MARKET,
            product_type     = _login.dhan.INTRA,
            price            = 0,
        )
        if response.get("status") == "success":
            order_id = response["data"]["orderId"]
            print(f"  ✅ Market Order placed | {direction} | Qty: {quantity}")
            print(f"     OrderID: {order_id}")
            return order_id
        else:
            print(f"  ❌ Market Order failed: {response}")
            return None

    except Exception as e:
        print(f"  ❌ Market Order exception: {e}")
        return None


def get_order_fill_price(order_id: str, ltp_fallback: float = None) -> float | None:
    if config.PAPER_TRADING:
        if ltp_fallback:
            print(f"  📋 [PAPER] Simulated fill @ ₹{ltp_fallback}")
            return ltp_fallback
        return None

    TRADED_STATUSES = {"TRADED", "COMPLETE", "FILLED", "PART_TRADED"}

    for attempt in range(10):
        try:
            time.sleep(0.3)
            r = requests.get(
                "https://api.dhan.co/v2/orders",
                headers=_login.get_headers(),
                timeout=10
            )
            if r.status_code != 200:
                continue
            for order in r.json():
                if str(order.get("orderId")) == str(order_id):
                    status    = order.get("orderStatus", "").upper()
                    avg_price = float(order.get("averageTradedPrice") or 0)
                    if status in TRADED_STATUSES and avg_price > 0:
                        print(f"  ✅ Fill confirmed @ ₹{avg_price}  (attempt {attempt+1})")
                        return avg_price
        except Exception as e:
            print(f"  ⚠️  Fill price fetch error (attempt {attempt+1}): {e}")

    # Fallback — use signal LTP so P&L stays internally consistent (slippage not captured)
    if ltp_fallback:
        print(f"  ⚠️  Fill not confirmed after 10 attempts — falling back to signal LTP ₹{ltp_fallback}")
        return ltp_fallback
    return None


def get_net_position_qty(security_id: str) -> int | None:
    """
    Returns net INTRADAY qty for security_id from Dhan /v2/positions.
      > 0  = long open
      < 0  = short open
        0  = fully closed (manual or algo)
      None = API error
    Returns None in PAPER mode (no real positions to check).

    Filters on productType == "INTRADAY": Dhan returns a SEPARATE position
    row per (securityId, productType) — e.g. a stray/cancelled CNC or MARGIN
    order on the same security shows up as its own row with netQty=0. Without
    this filter, that unrelated 0-qty row could be matched first, falsely
    reporting the position as closed while the real INTRADAY position
    (the one this bot actually entered) is still open.
    """
    if config.PAPER_TRADING:
        return None
    try:
        r = requests.get(
            "https://api.dhan.co/v2/positions",
            headers=_login.get_headers(),
            timeout=10
        )
        if r.status_code != 200:
            return None
        for pos in r.json():
            if str(pos.get("securityId")) == str(security_id) and \
               pos.get("productType") == _login.dhan.INTRA:
                return int(pos.get("netQty", 0))
        return 0   # not in list = no open INTRADAY position
    except Exception:
        return None


def place_exit_order(security_id: str, signal: str, quantity: int, reason: str) -> str | None:
    """Returns order_id on success, None on failure."""
    if config.PAPER_TRADING:
        print(f"  📋 [PAPER] Simulated exit [{reason}] | Qty: {quantity}")
        return "PAPER_EXIT"

    exit_txn = _login.dhan.SELL if signal == "LONG" else _login.dhan.BUY

    try:
        response = _login.dhan.place_order(
            security_id      = str(security_id),
            exchange_segment = _login.dhan.NSE,
            transaction_type = exit_txn,
            quantity         = quantity,
            order_type       = _login.dhan.MARKET,
            product_type     = _login.dhan.INTRA,
            price            = 0,
        )
        if response.get("status") == "success":
            order_id = response["data"]["orderId"]
            print(f"  ✅ Exit placed [{reason}] | OrderID: {order_id}")
            return order_id
        else:
            print(f"  ❌ Exit order failed [{reason}]: {response}")
            return None
    except Exception as e:
        print(f"  ❌ Exit order exception [{reason}]: {e}")
        return None
