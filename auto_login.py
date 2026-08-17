# ─────────────────────────────────────────────────────────────────────────────
#  auto_login.py  —  Dhan access-token lifecycle
#
#  A Dhan access token lives about a day, and the broker kills the previous one
#  the moment a new one is issued. For a scanner that must stay connected from
#  09:11 to 15:30 that is a real failure mode, so the flow here is deliberate:
#
#    1. generate a FRESH token via TOTP, and cache it in access_token.txt
#    2. if that fails, fall back to a saved token — but ONLY if it both
#         (a) stays valid past 15:30 today, and
#         (b) is still ACCEPTED by Dhan (verified with a live API call)
#    3. if nothing is usable, return None so the caller aborts cleanly instead
#       of starting up and then losing the feed on every reconnect
#
#  ⚠️  NO CREDENTIALS ARE STORED IN THIS FILE — deliberately.
#
#      A previous version hardcoded the client ID, login PIN and TOTP secret
#      right here. They now come from config.py, which reads them from the
#      environment. Add YOUR OWN values in a local `.env` file:
#
#          DHAN_CLIENT_ID=...      ← your Dhan client ID
#          DHAN_PIN=...            ← your Dhan login PIN
#          DHAN_TOTP_SECRET=...    ← your base32 2FA seed
#
#      `.env` and access_token.txt are both git-ignored. Never put these
#      values in a tracked file.
# ─────────────────────────────────────────────────────────────────────────────

import base64
import json
import os
import time

import requests

import config

_HERE      = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.path.join(_HERE, "access_token.txt")   # git-ignored cache

AUTH_TIMEOUT  = 20    # auth.dhan.co is slow pre-open; 10s timed out too often
AUTH_ATTEMPTS = 3


# ── Token expiry helpers (decode the JWT `exp` claim) ────────────────────────

def _decode_token_exp(token: str) -> float:
    """Token expiry as a Unix timestamp, or 0.0 if it can't be parsed."""
    try:
        p = token.split(".")[1]
        p += "=" * (4 - len(p) % 4)
        return float(json.loads(base64.b64decode(p)).get("exp", 0))
    except Exception:
        return 0.0


def is_token_valid(token: str) -> bool:
    """True if the token has not expired yet."""
    return bool(token) and time.time() < _decode_token_exp(token)


def is_token_valid_until_market_close(token: str) -> bool:
    """True only if the token stays valid through 15:30 IST today."""
    from datetime import datetime
    exp = _decode_token_exp(token)
    if exp == 0:
        return False
    mc = datetime.now().replace(hour=15, minute=30, second=0, microsecond=0)
    return exp > mc.timestamp()


def verify_token(token: str) -> bool:
    """
    Ask Dhan whether the token is ACTUALLY accepted — the JWT `exp` claim is not
    enough. Dhan invalidates older tokens server-side as soon as a new one is
    issued (or the session is logged out elsewhere), so a token that still looks
    unexpired locally can come back as DH-906 "Invalid Token". Without this
    check the scanner starts happily and then the feed is dropped on every
    reconnect for the whole session.
    """
    try:
        r = requests.get(
            "https://api.dhan.co/v2/fundlimit",
            headers={"Accept": "application/json", "access-token": token,
                     "client-id": config.CLIENT_ID},
            timeout=15,
        )
    except Exception as e:
        # Network hiccup — don't condemn the token on this alone.
        print(f"  WARN could not verify token ({e}) — assuming OK.")
        return True
    if r.status_code == 200:
        return True
    body = r.text[:200]
    if "DH-906" in body or "Invalid Token" in body or r.status_code in (401, 403):
        print(f"  Token REJECTED by Dhan: HTTP {r.status_code} {body}")
        return False
    # Some other error (rate limit, maintenance) — the token itself looks fine.
    print(f"  WARN token check returned HTTP {r.status_code}: {body}")
    return True


def _save_token(token: str) -> None:
    """Cache the fresh token so the next run has a fallback. Never touches
    config.py — that file is tracked by git and must stay secret-free."""
    try:
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            f.write(token)
    except Exception as e:
        print(f"  WARN could not cache token: {e}")


def generate_token() -> "str | None":
    """
    Fresh token via TOTP. Retries on network/timeout errors — a new OTP is
    generated each attempt, so a retry is never rejected as a stale code.
    Returns None if TOTP credentials are not configured.
    """
    if not (config.CLIENT_ID and config.DHAN_PIN and config.DHAN_TOTP_SECRET):
        print("  TOTP credentials not configured (see .env) — skipping auto-login.")
        return None

    try:
        import pyotp
    except ImportError:
        print("  pyotp not installed — skipping auto-login.  pip install pyotp")
        return None

    for attempt in range(1, AUTH_ATTEMPTS + 1):
        try:
            response = requests.post(
                "https://auth.dhan.co/app/generateAccessToken",
                params={
                    "dhanClientId": config.CLIENT_ID,
                    "pin"         : config.DHAN_PIN,
                    "totp"        : pyotp.TOTP(config.DHAN_TOTP_SECRET).now(),
                },
                timeout=AUTH_TIMEOUT,
            )
            data = response.json()

            if response.status_code == 200 and "accessToken" in data:
                token = data["accessToken"]
                _save_token(token)
                print(f"  Token generated | Expires: {data.get('expiryTime', 'N/A')}")
                return token

            msg = str(data.get("remarks", data))
            print(f"  Token generation failed (attempt {attempt}/{AUTH_ATTEMPTS}): {msg}")

            # Dhan throttles token generation to once per 2 minutes. Retrying on
            # a 3s loop can never clear it — say so instead of silently falling
            # back to a token that may already be dead.
            if "2 minutes" in msg or "once every" in msg:
                print("        Dhan throttles token generation to 1 per 2 minutes.")
                print("        Wait ~2 min and re-run — do NOT hammer it.")
                return None

            # "Invalid TOTP" is usually transient: the 30s code rolled over
            # between generating it and Dhan validating it. Wait for a clean
            # window and retry with a fresh code rather than giving up.
            if "totp" in msg.lower() and attempt < AUTH_ATTEMPTS:
                pause = 31 - (int(time.time()) % 30)
                print(f"        TOTP window rolled — retrying in {pause}s "
                      f"with a fresh code...")
                time.sleep(pause)
                continue

            return None   # wrong PIN / blocked account — retrying won't help

        except Exception as e:
            print(f"  Token generation error (attempt {attempt}/{AUTH_ATTEMPTS}): {e}")
            if attempt < AUTH_ATTEMPTS:
                time.sleep(3)
    return None


def load_token() -> "str | None":
    """Read the cached token from access_token.txt, if present."""
    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            token = f.read().strip()
            return token or None
    except FileNotFoundError:
        return None


def resolve_token() -> "str | None":
    """
    Return a token that is safe to run a whole session on, or None.

    See the module docstring for the full policy. The key point: a token that
    merely looks unexpired is NOT good enough — it is verified against Dhan
    before being accepted as a fallback.
    """
    token = generate_token()
    if token:
        return token

    for label, fb in (("access_token.txt", load_token()),
                      ("DHAN_ACCESS_TOKEN", getattr(config, "ACCESS_TOKEN", ""))):
        fb = (fb or "").strip()
        if not fb:
            continue
        if not is_token_valid_until_market_close(fb):
            print(f"  Fallback ({label}) expires before 15:30 — skipping.")
            continue
        print(f"  Auto-login failed — checking fallback ({label}) against Dhan...")
        if verify_token(fb):
            print("  Fallback token accepted by Dhan — using it.")
            return fb
        print(f"  Fallback ({label}) is dead server-side — skipping.")

    print("\n  ABORT: no usable token. Auto-login failed and every saved token is")
    print("         expired or rejected by Dhan. Fix the login before running:")
    print("           1. Check the system clock (TOTP is time-based).")
    print("           2. Run  python auto_login.py  to see the raw auth error.")
    print("           3. If TOTP is genuinely rejected, re-pair 2FA in the Dhan")
    print("              portal and update DHAN_TOTP_SECRET in your .env.")
    return None


if __name__ == "__main__":
    tok = resolve_token()
    print("Token acquired." if tok else "No usable token.")
