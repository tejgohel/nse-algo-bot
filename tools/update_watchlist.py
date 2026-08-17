import os, sys
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import pandas as pd
import re

# Read EQ-only symbols
df = pd.read_csv(os.path.join(_ROOT, 'equity_watchlist_resolved.txt'))
eq = df[df['intraday_leverage'] == True]['symbol'].tolist()

# Build new WATCHLIST_SYMBOLS block (50 per line)
lines = []
for i in range(0, len(eq), 50):
    chunk = eq[i:i+50]
    quoted = [f"'{s}'" for s in chunk]
    lines.append(','.join(quoted))

new_value = 'WATCHLIST_SYMBOLS = [' + ',\n'.join(lines) + ']\n'

# Read config.py
with open(os.path.join(_ROOT, 'config.py'), 'r', encoding='utf-8') as f:
    content = f.read()

# Replace WATCHLIST_SYMBOLS block (handles multi-line list)
new_content = re.sub(
    r'WATCHLIST_SYMBOLS\s*=\s*\[.*?\]',
    new_value.strip(),
    content,
    flags=re.DOTALL
)

with open(os.path.join(_ROOT, 'config.py'), 'w', encoding='utf-8') as f:
    f.write(new_content)

print(f"Done. {len(eq)} EQ symbols written to WATCHLIST_SYMBOLS in config.py")
