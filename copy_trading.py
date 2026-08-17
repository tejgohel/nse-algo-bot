# ─────────────────────────────────────────────────────────────────────────────
#  copy_trading.py — mirrors the master account's entries/exits onto follower
#  Dhan accounts (config.FOLLOWER_ACCOUNTS), each sized off its OWN capital.
#
#  FLOW:
#    1. main.py calls login_followers() once at startup (after master login).
#       Each enabled follower auto-logs-in via its own TOTP — failures are
#       skipped for the session, they don't block the master or other followers.
#    2. main.py calls replicate_entry(...) right after the master's entry
#       order is confirmed placed.
#    3. monitor.py calls replicate_exit(...) right after the master's exit
#       order is placed (SL / reverse exit / time-exit / manual-exit-on-master —
#       followers still get a real exit order regardless, since their own
#       position wasn't touched).
#
#  No-op everywhere if config.COPY_TRADING_ENABLED is False or no follower
#  logged in successfully — safe to leave wired in permanently.
# ─────────────────────────────────────────────────────────────────────────────

import threading
from dhanhq import dhanhq

import config
import auto_login

_followers: list[dict] = []   # logged-in followers this session
_open: dict = {}               # {security_id: [{...follower, "qty", "signal"}, ...]}
_lock = threading.Lock()


def login_followers():
    """Logs in every enabled follower account via its own TOTP and builds
    a dhanhq client for it. Call once at startup, after the master logs in."""
    global _followers
    _followers = []

    if not config.COPY_TRADING_ENABLED:
        return

    for acc in config.FOLLOWER_ACCOUNTS:
        if not acc.get("enabled", True):
            continue
        name = acc.get("name", acc["client_id"])
        token = auto_login.generate_token(acc["client_id"], acc["pin"], acc["totp_secret"])
        if not token:
            print(f"  ⚠️  [COPY] {name} login failed — skipping this account today.")
            continue
        _followers.append({
            "name"             : name,
            "client_id"        : acc["client_id"],
            "dhan"             : dhanhq(acc["client_id"], token),
            "deployed_capital" : acc["deployed_capital"],
            "leverage"         : acc.get("leverage", config.LEVERAGE),
        })
        print(f"  ✅ [COPY] {name} ready.")

    if _followers:
        print(f"  🔗 [COPY] {len(_followers)} follower account(s) active this session.\n")


def _calc_qty(follower: dict, ltp: float, leverage: float) -> int:
    buying_power = follower["deployed_capital"] * leverage
    return max(int(buying_power // ltp), 1)


def replicate_entry(security_id: str, signal: str, ltp: float, leverage: float):
    """Places the same-direction market order on every logged-in follower,
    each sized off its own capital × leverage."""
    if not _followers:
        return

    opened: list[dict] = []

    def _place_one(f):
        qty = _calc_qty(f, ltp, leverage)
        if config.PAPER_TRADING:
            print(f"  📋 [COPY-PAPER] {f['name']}: simulated {signal} qty {qty}")
            with _lock:
                opened.append({**f, "qty": qty, "signal": signal})
            return
        dhan = f["dhan"]
        txn_type = dhan.BUY if signal == "LONG" else dhan.SELL
        try:
            resp = dhan.place_order(
                security_id      = str(security_id),
                exchange_segment = dhan.NSE,
                transaction_type = txn_type,
                quantity         = qty,
                order_type       = dhan.MARKET,
                product_type     = dhan.INTRA,
                price            = 0,
            )
            if resp.get("status") == "success":
                order_id = resp["data"]["orderId"]
                print(f"  ✅ [COPY] {f['name']}: {signal} qty {qty} placed (OrderID {order_id})")
                with _lock:
                    opened.append({**f, "qty": qty, "signal": signal})
            else:
                print(f"  ❌ [COPY] {f['name']}: entry order failed — {resp}")
        except Exception as e:
            print(f"  ❌ [COPY] {f['name']}: entry order exception — {e}")

    threads = [threading.Thread(target=_place_one, args=(f,), daemon=True) for f in _followers]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    with _lock:
        _open[str(security_id)] = opened


def replicate_exit(security_id: str, reason: str):
    """Closes every follower's open position for this security_id."""
    with _lock:
        positions = _open.pop(str(security_id), [])
    if not positions:
        return

    def _close_one(p):
        signal = p["signal"]
        qty    = p["qty"]
        if config.PAPER_TRADING:
            print(f"  📋 [COPY-PAPER] {p['name']}: simulated exit [{reason}] qty {qty}")
            return
        dhan = p["dhan"]
        exit_txn = dhan.SELL if signal == "LONG" else dhan.BUY
        try:
            resp = dhan.place_order(
                security_id      = str(security_id),
                exchange_segment = dhan.NSE,
                transaction_type = exit_txn,
                quantity         = qty,
                order_type       = dhan.MARKET,
                product_type     = dhan.INTRA,
                price            = 0,
            )
            if resp.get("status") == "success":
                print(f"  ✅ [COPY] {p['name']}: exit [{reason}] qty {qty} placed")
            else:
                print(f"  ❌ [COPY] {p['name']}: exit order failed [{reason}] — {resp}")
        except Exception as e:
            print(f"  ❌ [COPY] {p['name']}: exit order exception [{reason}] — {e}")

    threads = [threading.Thread(target=_close_one, args=(p,), daemon=True) for p in positions]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
