# ─────────────────────────────────────────────────────────────────────────────
#  telegram_notify.py  —  Sends trade notifications to your Telegram
#
#  Setup (one time):
#    1. Message @BotFather on Telegram → /newbot → get BOT_TOKEN
#    2. Message your bot once
#    3. Open: https://api.telegram.org/bot<BOT_TOKEN>/getUpdates
#    4. Copy your chat_id from the response → paste below
# ─────────────────────────────────────────────────────────────────────────────

import os as _os
import requests

# ── Your Telegram credentials ─────────────────────────────────────────────────
BOT_TOKEN  = _os.getenv("TELEGRAM_BOT_TOKEN", "")   # ← set in .env (from @BotFather)
CHAT_ID    = _os.getenv("TELEGRAM_CHAT_ID", "")     # ← set in .env
CHANNEL_ID = _os.getenv("TELEGRAM_CHANNEL_ID", "")  # ← optional, set in .env

# ── All destinations that will receive every signal ───────────────────────────
def _destinations():
    ids = [CHAT_ID]
    if CHANNEL_ID:
        ids.append(CHANNEL_ID)
    return ids


def send(message: str):
    """Sends a message to all configured Telegram destinations. Silent fail if no internet."""
    if not BOT_TOKEN or not CHAT_ID:
        return   # not configured yet — skip silently

    for chat_id in _destinations():
        try:
            requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
                timeout=5
            )
        except Exception:
            pass   # never crash the algo because of telegram


# ── Pre-built message templates ───────────────────────────────────────────────

def notify_signal(symbol: str, direction: str, ltp: float, time_str: str = ""):
    """Scanner-mode alert — a signal was DETECTED (no trade placed)."""
    arrow = "📈 BUY" if direction == "BUY" else "📉 SELL"
    tstr  = f"  [{time_str}]" if time_str else ""
    send(
        f"<b>🔔 SIGNAL{tstr}</b>\n"
        f"Stock  : <b>{symbol}</b>\n"
        f"Signal : {arrow}\n"
        f"LTP    : ₹{ltp:.2f}"
    )


def notify_entry(symbol: str, signal: str, qty: int, sl: float):
    direction = "📈 LONG" if signal == "LONG" else "📉 SHORT"
    send(
        f"<b>🚀 ENTRY</b>\n"
        f"Stock   : <b>{symbol}</b>\n"
        f"Signal  : {direction}\n"
        f"Qty     : {qty}\n"
        f"SL      : ₹{sl}"
    )


def notify_exit(symbol: str, signal: str, entry_price: float,
                exit_price: float, qty: int, pnl: float, reason: str):
    emoji = "🎯" if "TARGET" in reason else "🛑" if "SL" in reason else "⏰"
    send(
        f"<b>{emoji} EXIT — {reason}</b>\n"
        f"Stock   : <b>{symbol}</b>\n"
        f"Entry   : ₹{entry_price}\n"
        f"Exit    : ₹{exit_price}\n"
        f"Qty     : {qty}\n"
        f"P&amp;L : <b>₹{pnl:+.2f}</b>"
    )


def notify_time_exit(symbol: str, signal: str, entry_price: float,
                     exit_price: float, qty: int, pnl: float,
                     entry_time: str, exit_time: str, reason: str):
    """Dedicated Telegram message for time-bound exits (15:15 / MARKET_EXIT_TIME)."""
    pnl_emoji   = "✅" if pnl >= 0 else "❌"
    move        = round(exit_price - entry_price, 2) if signal == "LONG" else round(entry_price - exit_price, 2)
    move_pct    = round(move / entry_price * 100, 2) if entry_price else 0
    direction   = "📈 LONG" if signal == "LONG" else "📉 SHORT"
    send(
        f"<b>⏰ TIME EXIT — {reason}</b>\n"
        f"Stock     : <b>{symbol}</b>\n"
        f"Direction : {direction}\n"
        f"Entry     : ₹{entry_price}  [{entry_time}]\n"
        f"Exit      : ₹{exit_price}   [{exit_time}]\n"
        f"Move      : ₹{move:+.2f}  ({move_pct:+.2f}%)\n"
        f"Qty       : {qty}\n"
        f"P&amp;L   : <b>{pnl_emoji} ₹{pnl:+.2f}</b>"
    )


def notify_manual_exit_detected(symbol: str, signal: str, est_pnl: float):
    """
    Urgent, distinct alert for when the manual-exit poller (broker position
    read showing qty=0) makes the bot drop a position from tracking. Unlike
    TP/SL/time exits this is an inference, not a certainty — send immediately
    so the user can verify the real broker position right away instead of
    discovering a possible unmanaged position later.
    """
    direction = "📈 LONG" if signal == "LONG" else "📉 SHORT"
    send(
        f"<b>⚠️ MANUAL EXIT DETECTED — VERIFY NOW</b>\n"
        f"Stock     : <b>{symbol}</b>\n"
        f"Direction : {direction}\n"
        f"Est. PnL  : ₹{est_pnl:+.2f}\n"
        f"Bot has stopped tracking this position based on a broker-side "
        f"qty=0 read. Please check Dhan app NOW to confirm it's actually "
        f"flat — if the entry order was just slow to fill, this could be "
        f"a live, unmanaged position."
    )


def notify_no_trade(reason: str):
    send(f"⏸️ <b>NO TRADE TODAY</b>\n{reason}")


def notify_daily_summary(trades: list):
    if not trades:
        send("📊 <b>Daily Summary</b>\nNo trades today.")
        return

    total_pnl = sum(t.get("gross_pnl", 0) for t in trades)
    wins  = sum(1 for t in trades if t.get("gross_pnl", 0) > 0)
    loses = len(trades) - wins
    emoji = "✅" if total_pnl > 0 else "❌"

    lines = [f"📊 <b>Daily Summary {emoji}</b>"]
    for t in trades:
        icon = "✅" if t.get("gross_pnl", 0) > 0 else "❌"
        lines.append(
            f"{icon} {t.get('symbol')} {t.get('signal')}  "
            f"₹{t.get('gross_pnl', 0):+.2f}  [{t.get('exit_reason','')}]"
        )
    lines.append(f"\nTotal P&amp;L : <b>₹{total_pnl:+.2f}</b>")
    lines.append(f"W/L           : {wins}/{loses}")

    send("\n".join(lines))