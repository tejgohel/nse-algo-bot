"""
delete_last_rows.py  —  Delete the last row of 5 random stocks from the DB

Purpose: verify that update_all() really does an incremental update.
         After this, run main.py → it should re-fetch and restore those rows.

Tables affected per stock:
  candles_{id}     (4H)
  candles_2h_{id}  (2H)
  candles_1h_{id}  (1H)
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import sqlite3
import random
import config

DB_PATH = config.DB_PATH
N = 5   # how many stocks to delete a row from

con = sqlite3.connect(DB_PATH)

# ── Sabhi 4H tables dhundo ────────────────────────────────────────────────────
all_tables = [
    row[0] for row in
    con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'candles_%'")
    .fetchall()
]

# Base 4H tables only (not the 1h_ or 2h_ prefixed ones)
base_tables = [t for t in all_tables if not t.startswith("candles_1h_") and not t.startswith("candles_2h_")]

if len(base_tables) < N:
    print(f"Only {len(base_tables)} stocks in DB, deleting all of them")
    N = len(base_tables)

chosen = random.sample(base_tables, N)

print(f"\n{'='*60}")
print(f"  {N} stocks chosen at random — deleting their last row")
print(f"{'='*60}\n")

deleted = []

for tbl in chosen:
    sid = tbl.replace("candles_", "")
    tbl_4h = f"candles_{sid}"
    tbl_2h = f"candles_2h_{sid}"
    tbl_1h = f"candles_1h_{sid}"

    # 4H: find the last trading DATE, then delete ALL rows for that date
    # (each trading day has 2 4H bars: 09:15 and 13:15)
    row_4h = con.execute(f"SELECT MAX(date) FROM {tbl_4h}").fetchone()
    if not row_4h or not row_4h[0]:
        print(f"  [{sid}] 4H table empty — skip")
        continue
    last_date_only = row_4h[0][:10]   # "YYYY-MM-DD"

    deleted_4h = con.execute(
        f"SELECT date FROM {tbl_4h} WHERE date LIKE ?", (f"{last_date_only}%",)
    ).fetchall()
    deleted_4h = [r[0] for r in deleted_4h]
    con.execute(f"DELETE FROM {tbl_4h} WHERE date LIKE ?", (f"{last_date_only}%",))

    # 2H: delete all rows for same date
    deleted_2h = []
    if tbl_2h in all_tables:
        rows = con.execute(
            f"SELECT date FROM {tbl_2h} WHERE date LIKE ?", (f"{last_date_only}%",)
        ).fetchall()
        deleted_2h = [r[0] for r in rows]
        if deleted_2h:
            con.execute(f"DELETE FROM {tbl_2h} WHERE date LIKE ?", (f"{last_date_only}%",))

    # 1H: delete all 1H candles for same date (7 candles: 09:15-15:15)
    deleted_1h = []
    if tbl_1h in all_tables:
        rows = con.execute(
            f"SELECT date FROM {tbl_1h} WHERE date LIKE ?", (f"{last_date_only}%",)
        ).fetchall()
        deleted_1h = [r[0] for r in rows]
        if deleted_1h:
            con.execute(f"DELETE FROM {tbl_1h} WHERE date LIKE ?", (f"{last_date_only}%",))

    # Verify: new MAX date should now be the PREVIOUS trading day
    new_max = con.execute(f"SELECT MAX(date) FROM {tbl_4h}").fetchone()[0]

    deleted.append({"sid": sid, "date_removed": last_date_only, "new_max": new_max})
    print(f"  [{sid}]")
    print(f"    Date deleted  : {last_date_only}  ({len(deleted_4h)} x 4H, {len(deleted_2h)} x 2H, {len(deleted_1h)} x 1H rows)")
    print(f"    New MAX date  : {new_max}  <- update_stock() will now detect this as stale")

con.commit()
con.close()

print(f"\n{'='*60}")
print(f"  Deleted {deleted[0]['date_removed']} from {N} stocks.")
print(f"  Now run main.py — update_all() should detect these stocks")
print(f"  and fetch the missing rows back.")
print(f"{'='*60}\n")
