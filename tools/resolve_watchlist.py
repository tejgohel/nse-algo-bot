import os, sys
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import pandas as pd

with open(os.path.join(_ROOT, 'equity_watchlist.txt')) as f:
    user_symbols = [line.strip() for line in f if line.strip()]

df = pd.read_csv(os.path.join(_ROOT, 'security_id_list.csv'), low_memory=False)
nse_eq = df[(df['SEM_EXM_EXCH_ID'] == 'NSE') & (df['SEM_INSTRUMENT_NAME'] == 'EQUITY')]

symbol_to_id      = dict(zip(nse_eq['SEM_TRADING_SYMBOL'], nse_eq['SEM_SMST_SECURITY_ID']))
symbol_to_listing = dict(zip(nse_eq['SEM_TRADING_SYMBOL'], nse_eq.get('SEM_LISTING_DATE', '')))
eq_symbols        = set(nse_eq[nse_eq['SEM_SERIES'] == 'EQ']['SEM_TRADING_SYMBOL'].tolist())

resolved  = []
not_found = []
t2t_list  = []

for symbol in user_symbols:
    sid = symbol_to_id.get(symbol)
    if sid is None:
        not_found.append(symbol)
        continue

    try:
        listing_date = pd.to_datetime(str(symbol_to_listing.get(symbol, ''))).strftime('%Y-%m-%d')
    except Exception:
        listing_date = '1995-01-01'

    intraday = symbol in eq_symbols
    if not intraday:
        t2t_list.append(symbol)

    resolved.append({
        'symbol'      : symbol,
        'security_id' : str(int(sid)),
        'listing_date': listing_date,
        'intraday'    : intraday,
    })

out = os.path.join(_ROOT, 'equity_watchlist_resolved.txt')
with open(out, 'w') as f:
    f.write('symbol,security_id,listing_date,intraday_leverage\n')
    for r in resolved:
        f.write(f"{r['symbol']},{r['security_id']},{r['listing_date']},{r['intraday']}\n")

print(f"Total in txt        : {len(user_symbols)}")
print(f"Resolved with SID   : {len(resolved)}")
print(f"EQ (intraday OK)    : {len([r for r in resolved if r['intraday']])}")
print(f"T2T / BE / BZ       : {len(t2t_list)}")
print(f"Not found in CSV    : {len(not_found)}")
if not_found:
    print(f"Not found           : {not_found}")
print(f"Saved -> {out}")
