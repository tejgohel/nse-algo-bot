# ─────────────────────────────────────────────────────────────────────────────
#  watchlist.py  —  Resolves symbol names → security IDs + listing dates
#
#  Returns list of dicts:
#    [{"symbol": "RELIANCE", "security_id": "2885", "listing_date": "1995-01-03"}, ...]
#
#  listing_date is used by db.py to know how far back to seed historical data.
# ─────────────────────────────────────────────────────────────────────────────

import os
import pandas as pd
import config
import login as _login


def load_watchlist() -> list[dict]:
    """
    Loads security_id_list.csv, resolves config.WATCHLIST_SYMBOLS → security IDs
    and listing dates (SEM_LISTING_DATE).

    Returns a list of dicts:
        [
            {"symbol": "RELIANCE", "security_id": "2885", "listing_date": "1995-01-03"},
            ...
        ]
    """
    csv_path = config.SECURITY_CSV_PATH

    if not os.path.exists(csv_path):
        print(f"  📥 {csv_path} not found — downloading from Dhan...")
        _login.dhan.fetch_security_list("compact")
        print(f"  ✅ Security list downloaded.\n")

    df = pd.read_csv(csv_path, low_memory=False)

    nse_eq = df[
        (df["SEM_EXM_EXCH_ID"]    == "NSE") &
        (df["SEM_INSTRUMENT_NAME"] == "EQUITY")
    ]

    # Build lookups
    symbol_to_id      = dict(zip(nse_eq["SEM_TRADING_SYMBOL"], nse_eq["SEM_SMST_SECURITY_ID"]))
    symbol_to_listing = dict(zip(nse_eq["SEM_TRADING_SYMBOL"], nse_eq.get("SEM_LISTING_DATE", "")))

    eq_symbols = set(
        nse_eq[nse_eq["SEM_SERIES"] == "EQ"]["SEM_TRADING_SYMBOL"].tolist()
    )

    resolved    = []
    not_found   = []
    with_lev    = []   # EQ  — intraday leverage available
    without_lev = []   # BE/BZ — T2T, cash-only delivery

    for symbol in config.WATCHLIST_SYMBOLS:
        security_id = symbol_to_id.get(symbol)

        if security_id is None:
            not_found.append(symbol)
            continue

        # listing_date: try to parse, fall back to a safe default
        raw_date = symbol_to_listing.get(symbol, "")
        try:
            listing_date = pd.to_datetime(str(raw_date)).strftime("%Y-%m-%d")
        except Exception:
            listing_date = "1995-01-01"

        # EQ series → intraday leverage OK; BE/BZ (T2T) → delivery only, no leverage
        intraday_leverage = symbol in eq_symbols

        resolved.append({
            "symbol"            : symbol,
            "security_id"       : str(int(security_id)),
            "listing_date"      : listing_date,
            "intraday_leverage" : intraday_leverage,
        })

        if intraday_leverage:
            with_lev.append(symbol)
        else:
            without_lev.append(symbol)

    print(f"\n{'═'*64}")
    print(f"  WATCHLIST LOADED  —  {len(resolved)} stocks resolved")
    print(f"  ⚡ With intraday leverage (EQ)  : {len(with_lev)}")
    print(f"  📦 Cash-only / T2T (BE/BZ)     : {len(without_lev)}")
    if not_found:
        print(f"  ⚠️  Not found in CSV ({len(not_found)}): {not_found}")
    if without_lev:
        print(f"  ℹ️  T2T stocks (no SHORT allowed, 1x capital): {without_lev}")
    print(f"{'═'*64}\n")

    return resolved


if __name__ == "__main__":
    wl = load_watchlist()
    print(f"Total resolved: {len(wl)}")
    for item in wl[:5]:
        print(f"  {item['symbol']:<20}  →  {item['security_id']:<10}  listed: {item['listing_date']}")
