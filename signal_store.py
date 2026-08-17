# ─────────────────────────────────────────────────────────────────────────────
#  signal_store.py  —  persists a session's LIVE signals to disk, per trade day.
#
#  WHY:
#    When the scanner is re-run POST-MARKET (or on a holiday/weekend), we want to
#    show the SAME signals that actually fired during that day's LIVE session —
#    NOT a freshly recomputed replay. Each trading day's signals live in their
#    own JSON file under signal_store/, so they persist until the next live
#    session overwrites that day's file (i.e. valid "until the next 09:15").
#
#  FILES:
#    signal_store/<YYYY-MM-DD>.json  →  list of signal entry dicts for that day
# ─────────────────────────────────────────────────────────────────────────────

import json
import os
from datetime import date

_HERE = os.path.dirname(os.path.abspath(__file__))
_DIR  = os.path.join(_HERE, "signal_store")


def _path(trade_day: date) -> str:
    return os.path.join(_DIR, f"{trade_day.isoformat()}.json")


def reset(trade_day: date) -> None:
    """
    Start a FRESH store for a live session (overwrites any prior file for the
    day). Call once at the start of the live scan loop so the store reflects
    only the current session's signals.
    """
    os.makedirs(_DIR, exist_ok=True)
    with open(_path(trade_day), "w", encoding="utf-8") as f:
        json.dump([], f)


def append(trade_day: date, entry: dict) -> None:
    """Append one signal entry to the day's store (creates file if missing)."""
    os.makedirs(_DIR, exist_ok=True)
    signals = load(trade_day)
    signals.append(entry)
    with open(_path(trade_day), "w", encoding="utf-8") as f:
        json.dump(signals, f, ensure_ascii=False, default=str)


def load(trade_day: date) -> list[dict]:
    """Return the day's stored signal entries ([] if file missing/empty/corrupt)."""
    try:
        with open(_path(trade_day), "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def exists(trade_day: date) -> bool:
    """True if a store file was written for this trade day (a live session ran)."""
    return os.path.exists(_path(trade_day))
